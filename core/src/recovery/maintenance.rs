//! Offline physical compaction. Logical authority and idempotency never expire.
use super::*;
use rusqlite::types::ValueRef;

#[derive(Debug, Serialize)]
pub struct CompactionResult {
    pub database_before_bytes: u64,
    pub database_after_bytes: u64,
    pub wal_before_bytes: u64,
    pub wal_after_bytes: u64,
    pub backup_bytes: u64,
    pub retained_rows: u64,
    pub checkpoint_truncated: bool,
}

fn wal_bytes(path: &Path) -> Result<u64> {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    match fs::metadata(PathBuf::from(wal)) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

/// Hash every value, including recovery events and sqlite_sequence, in stable
/// table/column order. At most one SQLite row is materialized at a time. Length
/// and type prefixes distinguish NULL, empty strings/blobs and field boundaries.
/// Authority and ordering use explicit IDs/sequences, never implicit rowids,
/// which VACUUM may renumber. SQLite may spill this all-column sort to disk.
fn logical_digest(conn: &Connection) -> Result<([u8; 32], u64)> {
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")?
        .query_map([], |row| row.get(0))?
        .collect::<std::result::Result<_, _>>()?;
    let mut digest = Sha256::new();
    let mut count = 0u64;
    for table in tables {
        digest.update((table.len() as u64).to_be_bytes());
        digest.update(table.as_bytes());
        let quoted = format!("\"{}\"", table.replace('"', "\"\""));
        let columns = conn
            .prepare(&format!("SELECT * FROM {quoted} LIMIT 0"))?
            .column_count();
        let order = (1..=columns)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut statement = conn.prepare(&format!("SELECT * FROM {quoted} ORDER BY {order}"))?;
        let mut rows = statement.query([])?;
        digest.update((columns as u64).to_be_bytes());
        while let Some(row) = rows.next()? {
            digest.update([5]);
            count = count.checked_add(1).ok_or(RecoveryError::Storage)?;
            for column in 0..columns {
                match row.get_ref(column)? {
                    ValueRef::Null => digest.update([0]),
                    ValueRef::Integer(n) => {
                        digest.update([1]);
                        digest.update(n.to_be_bytes());
                    }
                    ValueRef::Real(n) => {
                        digest.update([2]);
                        digest.update(n.to_bits().to_be_bytes());
                    }
                    ValueRef::Text(bytes) | ValueRef::Blob(bytes) => {
                        digest.update([if matches!(row.get_ref(column)?, ValueRef::Text(_)) {
                            3
                        } else {
                            4
                        }]);
                        digest.update((bytes.len() as u64).to_be_bytes());
                        digest.update(bytes);
                    }
                }
            }
        }
        digest.update([6]);
    }
    Ok((digest.finalize().into(), count))
}

fn integrity(conn: &Connection) -> Result<()> {
    let check: String = conn.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    if check != "ok" {
        return Err(RecoveryError::Storage);
    }
    if conn
        .query_row("PRAGMA foreign_key_check", [], |_| Ok(()))
        .optional()?
        .is_some()
    {
        return Err(RecoveryError::Storage);
    }
    Ok(())
}

impl RecoveryJournal {
    /// Keep all logical records. Publish a verified, private SQLite snapshot
    /// before compacting the original; never replace the authority from a copy.
    /// No Redis access, history expiry, schema migration or halt reset occurs.
    pub fn compact(path: impl AsRef<Path>, backup: impl AsRef<Path>) -> Result<CompactionResult> {
        let path = path.as_ref();
        let backup = backup.as_ref();
        let _owner = owner_lock(path, false)?;
        let conn = connection(path, false)?;
        let current = control(&conn)?;
        if current.schema != SCHEMA
            || current.halted
            || projection::load(&conn, &current)?.is_some()
        {
            return Err(RecoveryError::RecoveryRequired(
                "compaction requires an unhalted current-schema journal with no pending checkpoint",
            ));
        }
        solana_projection::validate_schema(&conn)?;
        prepare_directory(backup, false)?;
        if backup.exists() {
            return Err(RecoveryError::Invalid("backup destination already exists"));
        }
        integrity(&conn)?;
        let pages: i64 = conn.pragma_query_value(None, "page_count", |row| row.get(0))?;
        let page_size: i64 = conn.pragma_query_value(None, "page_size", |row| row.get(0))?;
        let pages = u64::try_from(pages).map_err(|_| RecoveryError::Storage)?;
        let page_size = u64::try_from(page_size).map_err(|_| RecoveryError::Storage)?;
        let bytes = pages.checked_mul(page_size).ok_or(RecoveryError::Storage)?;
        let reserve = 64 * 1024 * 1024;
        let source_needed = bytes
            .checked_mul(3)
            .and_then(|n| n.checked_add(reserve))
            .ok_or(RecoveryError::Storage)?;
        if fs2::available_space(path.parent().ok_or(RecoveryError::Storage)?)? < source_needed
            || fs2::available_space(backup.parent().ok_or(RecoveryError::Storage)?)?
                < bytes.saturating_add(reserve)
        {
            return Err(RecoveryError::Invalid(
                "insufficient free space for backup and SQLite working files",
            ));
        }
        let before = logical_digest(&conn)?;
        let database_before_bytes = fs::metadata(path)?.len();
        let wal_before_bytes = wal_bytes(path)?;
        export::publish_private_path(backup, |temporary, _file| {
            let name = temporary
                .to_str()
                .ok_or(RecoveryError::Invalid("backup pathname must be UTF-8"))?;
            conn.execute("VACUUM INTO ?", [name])?;
            let snapshot = connection(temporary, true)?;
            integrity(&snapshot)?;
            if logical_digest(&snapshot)? != before {
                return Err(RecoveryError::Storage);
            }
            Ok(())
        })?;
        conn.execute_batch("VACUUM")?;
        let verified = (|| -> Result<bool> {
            integrity(&conn)?;
            Ok(control(&conn)? == current && logical_digest(&conn)? == before)
        })();
        if !matches!(verified, Ok(true)) {
            // An unexpected verification failure must not leave an apparently
            // healthy journal. There is no force-clear or automatic rollback.
            conn.execute("UPDATE control SET halted=1,reason=COALESCE(reason,'compaction verification failed') WHERE singleton=1",[])?;
            return Err(RecoveryError::RecoveryRequired(
                "compaction verification failed; preserve source and backup for reconciliation",
            ));
        }
        let (busy, _, _): (i64, i64, i64) =
            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        File::open(path)?.sync_all()?;
        File::open(path.parent().ok_or(RecoveryError::Storage)?)?.sync_all()?;
        Ok(CompactionResult {
            database_before_bytes,
            database_after_bytes: fs::metadata(path)?.len(),
            wal_before_bytes,
            wal_after_bytes: wal_bytes(path)?,
            backup_bytes: fs::metadata(backup)?.len(),
            retained_rows: before.1,
            checkpoint_truncated: busy == 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery::tests::{Fixture, admit, request};
    use serde_json::json;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    fn replay(n: u64) -> String {
        format!("evm:31337:0x1111111111111111111111111111111111111111:{n}")
    }
    fn snapshot(path: &Path) -> Vec<u8> {
        let output = path.with_file_name(format!("export-{}.json", uuid::Uuid::new_v4()));
        RecoveryJournal::export(path, &output).unwrap();
        fs::read(output).unwrap()
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn compact_preserves_every_logical_row_replay_fence_and_private_backup() {
        let fixture = Fixture::fresh();
        fixture.initialize().await;
        let journal = fixture.open().await;
        for (id, nonce) in [("pending", 0), ("done", 1)] {
            admit(&journal, "eoa", id, request(id)).await;
            journal.before_broadcast("eoa",id,&replay(nonce),json!({"transactionHash":alloy::primitives::B256::repeat_byte(7),"wire":"retained"})).await.unwrap();
        }
        journal.record_terminal("eoa","done",json!({"chainId":31337,"transactionHash":alloy::primitives::B256::repeat_byte(7),"outcome":"success"})).await.unwrap();
        drop(journal);
        {
            let _owner = owner_lock(&fixture.path, false).unwrap();
            let conn = connection(&fixture.path, false).unwrap();
            conn.execute_batch("CREATE TABLE freed(payload BLOB); INSERT INTO freed VALUES(zeroblob(4194304)); DROP TABLE freed; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        }
        let before = snapshot(&fixture.path);
        let backup = fixture.path.with_file_name("backup.sqlite");
        let digest = logical_digest(&connection(&fixture.path, true).unwrap()).unwrap();
        let report = RecoveryJournal::compact(&fixture.path, &backup).unwrap();
        assert!(report.database_after_bytes < report.database_before_bytes);
        assert!(report.checkpoint_truncated);
        assert_eq!(report.wal_after_bytes, 0);
        assert_eq!(snapshot(&fixture.path), before);
        assert_eq!(snapshot(&backup), before);
        assert_eq!(
            logical_digest(&connection(&fixture.path, true).unwrap()).unwrap(),
            digest
        );
        assert_eq!(
            logical_digest(&connection(&backup, true).unwrap()).unwrap(),
            digest
        );
        assert_eq!(
            fs::metadata(&backup).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(
            !lock_path(&backup).exists(),
            "a snapshot is not a new runnable deployment"
        );
        assert!(RecoveryJournal::compact(&fixture.path, &backup).is_err());
        assert_eq!(snapshot(&fixture.path), before);
        let journal = fixture.open().await;
        assert!(
            journal
                .reserve_admission(
                    "eoa",
                    "done",
                    &admission_fingerprint("eoa", &request("done")).unwrap(),
                    request("done")
                )
                .await
                .unwrap()
                .terminal
        );
        assert!(matches!(
            journal
                .validate_replay_binding("eoa", "pending", &replay(99))
                .await,
            Err(RecoveryError::Conflict)
        ));
        assert!(
            journal
                .latest_eoa_attempt("pending", &replay(0))
                .await
                .unwrap()
                .is_some()
        );
        drop(journal);
        fixture.cleanup(&[]).await;
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn compact_refuses_active_owner_halt_and_unacknowledged_checkpoint() {
        for mode in ["owner", "halt", "pending"] {
            let fixture = Fixture::fresh();
            fixture.initialize().await;
            let journal = fixture.open().await;
            let backup = fixture.path.with_file_name("backup.sqlite");
            if mode == "owner" {
                assert!(matches!(
                    RecoveryJournal::compact(&fixture.path, &backup),
                    Err(RecoveryError::Locked)
                ));
            } else {
                if mode == "pending" {
                    journal
                        .db(|conn| {
                            let tx = conn.transaction()?;
                            advance(&tx)?;
                            commit_transaction(tx)
                        })
                        .await
                        .unwrap();
                }
                drop(journal);
                if mode == "halt" {
                    RecoveryJournal::quarantine(&fixture.path).unwrap();
                }
                assert!(matches!(
                    RecoveryJournal::compact(&fixture.path, &backup),
                    Err(RecoveryError::RecoveryRequired(_))
                ));
                assert!(!backup.exists());
                fixture.cleanup(&[]).await;
                continue;
            }
            drop(journal);
            assert!(!backup.exists());
            fixture.cleanup(&[]).await;
        }
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn compact_never_publishes_to_a_public_directory_or_overwrites_a_file() {
        let fixture = Fixture::fresh();
        fixture.initialize().await;
        let public = fixture.path.with_file_name("public");
        fs::create_dir(&public).unwrap();
        fs::set_permissions(&public, fs::Permissions::from_mode(0o755)).unwrap();
        let backup = public.join("backup.sqlite");
        assert!(RecoveryJournal::compact(&fixture.path, &backup).is_err());
        assert!(!backup.exists());
        let backup = fixture.path.with_file_name("existing.sqlite");
        private_new_file(&backup)
            .unwrap()
            .write_all(b"keep")
            .unwrap();
        let before = snapshot(&fixture.path);
        assert!(RecoveryJournal::compact(&fixture.path, &backup).is_err());
        assert_eq!(fs::read(&backup).unwrap(), b"keep");
        assert_eq!(snapshot(&fixture.path), before);
        fixture.cleanup(&[]).await;
    }
}
