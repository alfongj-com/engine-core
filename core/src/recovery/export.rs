//! Snapshot export with bounded row memory and atomic, no-overwrite publication.
use super::*;
use serde::ser::{Error as _, SerializeMap, SerializeSeq};
use std::io::{BufWriter, Write};

/// No output path is visible until every byte has been written and synced.
/// Hard-link publication is atomic and fails if the destination already exists;
/// both paths are in the same private directory on the supported local filesystem.
pub(super) fn publish_private(
    output: &Path,
    write: impl FnOnce(&mut File) -> Result<()>,
) -> Result<()> {
    prepare_directory(output, false)?;
    let parent = output
        .parent()
        .ok_or(RecoveryError::Invalid("export path needs a parent"))?;
    let temporary = parent.join(format!(
        ".engine-recovery-export-{}.tmp",
        uuid::Uuid::new_v4()
    ));
    struct PendingExport(PathBuf);
    impl Drop for PendingExport {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }
    let mut file = private_new_file(&temporary)?;
    let pending = PendingExport(temporary);
    write(&mut file)?;
    file.sync_all()?;
    fs::hard_link(&pending.0, output)?;
    fs::remove_file(&pending.0)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(super) fn write_snapshot(conn: &Connection, writer: impl Write) -> Result<()> {
    let mut writer = BufWriter::new(writer);
    serde_json::to_writer_pretty(&mut writer, &Snapshot(conn))
        .map_err(|_| RecoveryError::Storage)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

struct Snapshot<'a>(&'a Connection);
impl Serialize for Snapshot<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(7))?;
        map.serialize_entry("format", "engine-recovery-export-v1")?;
        map.serialize_entry("status", &status(self.0).map_err(S::Error::custom)?)?;
        map.serialize_entry(
            "admissions",
            &Rows {
                conn: self.0,
                kind: RowKind::Admissions,
            },
        )?;
        map.serialize_entry(
            "attempts",
            &Rows {
                conn: self.0,
                kind: RowKind::Attempts,
            },
        )?;
        map.serialize_entry(
            "terminal_evidence",
            &Rows {
                conn: self.0,
                kind: RowKind::Terminal,
            },
        )?;
        map.serialize_entry(
            "chain_checkpoints",
            &Rows {
                conn: self.0,
                kind: RowKind::Checkpoints,
            },
        )?;
        map.serialize_entry(
            "halted_chains",
            &Rows {
                conn: self.0,
                kind: RowKind::HaltedChains,
            },
        )?;
        map.end()
    }
}

#[derive(Clone, Copy)]
enum RowKind {
    Admissions,
    Attempts,
    Terminal,
    Checkpoints,
    HaltedChains,
}
struct Rows<'a> {
    conn: &'a Connection,
    kind: RowKind,
}
impl Serialize for Rows<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(None)?;
        let sql = match self.kind {
            RowKind::Admissions => {
                "SELECT id,kind,fingerprint,payload,state,replay_key FROM admissions ORDER BY id"
            }
            RowKind::Attempts => {
                "SELECT id,replay_key,digest,payload FROM attempts ORDER BY sequence"
            }
            RowKind::Terminal => "SELECT id,evidence FROM terminal_evidence ORDER BY sequence",
            RowKind::Checkpoints => {
                "SELECT chain_id,evidence FROM chain_checkpoints ORDER BY chain_id"
            }
            RowKind::HaltedChains => "SELECT chain_id FROM chain_halts ORDER BY chain_id",
        };
        let mut statement = self
            .conn
            .prepare(sql)
            .map_err(|_| S::Error::custom("export query failed"))?;
        let mut rows = statement
            .query([])
            .map_err(|_| S::Error::custom("export query failed"))?;
        while let Some(row) = rows
            .next()
            .map_err(|_| S::Error::custom("export row read failed"))?
        {
            // At most one record's JSON is materialized at a time. The snapshot
            // transaction belongs to the caller and spans status plus every row.
            let value = export_row(row, self.kind).map_err(S::Error::custom)?;
            sequence.serialize_element(&value)?;
        }
        sequence.end()
    }
}

fn export_row(row: &rusqlite::Row<'_>, kind: RowKind) -> Result<Value> {
    let json_field = |index| -> Result<Value> {
        let body: String = row.get(index)?;
        serde_json::from_str(&body).map_err(|_| RecoveryError::Storage)
    };
    Ok(match kind {
        RowKind::Admissions => {
            let record = AdmissionRecord {
                id: row.get(0)?,
                kind: row.get(1)?,
                fingerprint: row.get(2)?,
                payload: json_field(3)?,
                state: AdmissionState::parse(&row.get::<_, String>(4)?)?,
                replay_key: row.get(5)?,
            };
            serde_json::to_value(record).map_err(|_| RecoveryError::Storage)?
        }
        RowKind::Attempts => {
            serde_json::json!({"id":row.get::<_,String>(0)?,"replay_key":row.get::<_,String>(1)?,"digest":row.get::<_,String>(2)?,"attempt":json_field(3)?})
        }
        RowKind::Terminal => {
            serde_json::json!({"id":row.get::<_,String>(0)?,"evidence":json_field(1)?})
        }
        // Preserve the v1 export representation, including checkpoint JSON text.
        RowKind::Checkpoints => {
            serde_json::json!([row.get::<_, String>(0)?, row.get::<_, String>(1)?])
        }
        RowKind::HaltedChains => serde_json::json!(row.get::<_, String>(0)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery::tests::{Fixture, admit, request};
    use std::io;

    async fn large_fixture(journal: &RecoveryJournal, count: usize) {
        let old = journal.db(move |conn| {
            let tx = conn.transaction()?;
            for index in 0..count {
                let id = format!("export-{index:06}");
                let payload = serde_json::json!({"transactionId":id,"ordinal":index,"data":"x".repeat(1024)});
                let key = format!("evm:31337:0x1111111111111111111111111111111111111111:{index}");
                let attempt = serde_json::json!({"signedTransaction":format!("wire-{index}"),"note":"a".repeat(1024)});
                let terminal = serde_json::json!({"outcome":"success","ordinal":index});
                tx.execute("INSERT INTO admissions VALUES(?,?,?,?,?,?)", params![id,"eoa",admission_fingerprint("eoa",&payload)?,payload.to_string(),"terminal",key])?;
                tx.execute("INSERT INTO attempts(id,replay_key,digest,payload) VALUES(?,?,?,?)",params![id,key,index.to_string(),attempt.to_string()])?;
                tx.execute("INSERT INTO terminal_evidence(id,evidence) VALUES(?,?)",params![id,terminal.to_string()])?;
            }
            let old = advance(&tx)?;
            tx.commit()?;
            Ok(old)
        }).await.unwrap();
        journal.mirror(old).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn streaming_export_is_complete_private_and_snapshot_consistent() {
        let f = Fixture::fresh();
        f.initialize().await;
        let journal = f.open().await;
        const COUNT: usize = 2048;
        large_fixture(&journal, COUNT).await;
        let output = f.path.parent().unwrap().join("complete.json");
        let conn = connection(&f.path, true).unwrap();
        conn.execute_batch("BEGIN").unwrap();
        // Establish the exact read snapshot, then advance the live journal while
        // export serialization is in progress. No exported table may see it.
        assert_eq!(status(&conn).unwrap().admissions, COUNT as u64);
        let (start_tx, start_rx) = std::sync::mpsc::sync_channel(1);
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
        let live = journal.clone();
        let runtime = tokio::runtime::Handle::current();
        let writer_thread = std::thread::spawn(move || {
            start_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            runtime.block_on(admit(&live, "eoa", "later", request("later")));
            done_tx.send(()).unwrap();
        });
        struct ConcurrentWrite<'a> {
            file: &'a mut File,
            start: Option<std::sync::mpsc::SyncSender<()>>,
            done: std::sync::mpsc::Receiver<()>,
        }
        impl Write for ConcurrentWrite<'_> {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if let Some(start) = self.start.take() {
                    start.send(()).unwrap();
                    self.done.recv_timeout(Duration::from_secs(10)).unwrap();
                }
                self.file.write(bytes)
            }
            fn flush(&mut self) -> io::Result<()> {
                self.file.flush()
            }
        }
        publish_private(&output, |file| {
            write_snapshot(
                &conn,
                ConcurrentWrite {
                    file,
                    start: Some(start_tx),
                    done: done_rx,
                },
            )
        })
        .unwrap();
        writer_thread.join().unwrap();
        let exported: Value = serde_json::from_reader(File::open(&output).unwrap()).unwrap();
        assert_eq!(exported["format"], "engine-recovery-export-v1");
        for field in ["admissions", "attempts", "terminal_evidence"] {
            assert_eq!(exported[field].as_array().unwrap().len(), COUNT);
        }
        assert_eq!(exported["status"]["admissions"], COUNT);
        for index in 0..COUNT {
            assert_eq!(
                exported["admissions"][index]["id"],
                format!("export-{index:06}")
            );
            assert_eq!(exported["admissions"][index]["payload"]["ordinal"], index);
            assert_eq!(
                exported["admissions"][index]["payload"]["data"]
                    .as_str()
                    .unwrap()
                    .len(),
                1024
            );
            assert_eq!(
                exported["attempts"][index]["attempt"]["signedTransaction"],
                format!("wire-{index}")
            );
            assert_eq!(
                exported["terminal_evidence"][index]["evidence"]["ordinal"],
                index
            );
        }
        assert_eq!(
            RecoveryJournal::status(&f.path).unwrap().admissions,
            COUNT as u64 + 1
        );
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let original = fs::read(&output).unwrap();
        assert!(RecoveryJournal::export(&f.path, &output).is_err());
        assert_eq!(
            fs::read(&output).unwrap(),
            original,
            "no overwrite of a prior complete export"
        );
        drop(conn);
        drop(journal);
        f.cleanup(&[]).await;
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn failed_streaming_export_never_publishes_partial_destination() {
        let f = Fixture::fresh();
        f.initialize().await;
        let journal = f.open().await;
        large_fixture(&journal, 256).await;
        let output = f.path.parent().unwrap().join("must-not-exist.json");
        let conn = connection(&f.path, true).unwrap();
        conn.execute_batch("BEGIN").unwrap();
        struct FailAfter<'a> {
            file: &'a mut File,
            remaining: usize,
            written: usize,
        }
        impl Write for FailAfter<'_> {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.remaining == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::StorageFull,
                        "injected full device",
                    ));
                }
                let count = bytes.len().min(self.remaining);
                let written = self.file.write(&bytes[..count])?;
                self.remaining -= written;
                self.written += written;
                Ok(written)
            }
            fn flush(&mut self) -> io::Result<()> {
                self.file.flush()
            }
        }
        let mut accepted = 0;
        let error = publish_private(&output, |file| {
            let mut writer = FailAfter {
                file,
                remaining: 32 * 1024,
                written: 0,
            };
            let result = write_snapshot(&conn, &mut writer);
            accepted = writer.written;
            result
        });
        assert!(error.is_err());
        assert_eq!(accepted, 32 * 1024);
        assert!(!output.exists());
        assert!(
            !fs::read_dir(f.path.parent().unwrap())
                .unwrap()
                .any(|entry| entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".engine-recovery-export-"))
        );
        journal.ensure_healthy().await.unwrap();
        RecoveryJournal::export(&f.path, &output).unwrap();
        let value: Value = serde_json::from_reader(File::open(&output).unwrap()).unwrap();
        assert_eq!(value["admissions"].as_array().unwrap().len(), 256);
        drop(conn);
        drop(journal);
        f.cleanup(&[]).await;
    }
}
