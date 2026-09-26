use super::*;
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use twmq::redis::AsyncCommands;

pub(super) struct Fixture {
    directory: PathBuf,
    pub(super) path: PathBuf,
    namespace: String,
    pub(super) url: String,
}
impl Fixture {
    pub(super) fn fresh() -> Self {
        let token = uuid::Uuid::new_v4().simple().to_string();
        let directory = std::env::temp_dir().join(format!("engine-recovery-{token}"));
        Self {
            path: directory.join("ledger.sqlite"),
            directory,
            namespace: format!("recovery_{token}"),
            url: std::env::var("TEST_REDIS_URL").expect("select a disposable local Redis"),
        }
    }
    pub(super) async fn initialize(&self) {
        RecoveryJournal::initialize(&self.path, &self.url, Some(self.namespace.clone()))
            .await
            .unwrap();
    }
    pub(super) async fn open(&self) -> Arc<RecoveryJournal> {
        RecoveryJournal::open(&self.path, &self.url, Some(self.namespace.clone()))
            .await
            .unwrap()
    }
    pub(super) async fn cleanup(&self, extra: &[&str]) {
        let mut conn = redis_connection(&self.url).await.unwrap();
        let mut keys = vec![marker_key(&Some(self.namespace.clone()))];
        keys.extend(extra.iter().map(|ns| marker_key(&Some((*ns).into()))));
        let _: () = conn.del(keys).await.unwrap();
        fs::remove_dir_all(&self.directory).unwrap();
    }
}
pub(super) fn request(id: &str) -> Value {
    json!({"transactionId":id,"chainId":31337,"from":"0x1111111111111111111111111111111111111111","value":"1","credential":"test-secret"})
}
pub(super) async fn admit(
    j: &RecoveryJournal,
    kind: &str,
    id: &str,
    payload: Value,
) -> AdmissionReservation {
    j.reserve_admission(
        kind,
        id,
        &admission_fingerprint(kind, &payload).unwrap(),
        payload,
    )
    .await
    .unwrap()
}
fn key(nonce: u64) -> String {
    format!("evm:31337:0x1111111111111111111111111111111111111111:{nonce}")
}
fn terminal(outcome: &str, checkpoint: u64) -> Value {
    json!({"chainId":31337,"transactionHash":alloy::primitives::B256::repeat_byte(1),"outcome":outcome,"finality":{"blockNumber":10,"blockHash":"0xdef","checkpointNumber":checkpoint,"checkpointHash":format!("hash-{checkpoint}"),"policy":{"mode":"finalized"}}})
}

#[test]
fn admission_fingerprint_excludes_only_proposed_random_replay_ids() {
    let a = json!({"b":2,"a":{"y":2,"x":1},"pregeneratedNonce":"1"});
    let b = json!({"a":{"x":1,"y":2},"b":2,"pregeneratedNonce":"2"});
    assert_eq!(
        admission_fingerprint("erc4337", &a).unwrap(),
        admission_fingerprint("erc4337", &b).unwrap()
    );
    assert_ne!(
        admission_fingerprint("eoa", &a).unwrap(),
        admission_fingerprint("eoa", &b).unwrap()
    );
    let mut changed = b;
    changed["a"]["x"] = json!(9);
    assert_ne!(
        admission_fingerprint("erc4337", &a).unwrap(),
        admission_fingerprint("erc4337", &changed).unwrap()
    );
    assert!(
        validate_replay_key(
            "eoa",
            "evm:031337:0x1111111111111111111111111111111111111111:0"
        )
        .is_err()
    );
    assert!(
        validate_replay_key(
            "eoa",
            "evm:31337:0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:0"
        )
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn initialization_is_explicit_private_exclusive_and_requires_empty_namespace() {
    let f = Fixture::fresh();
    assert!(matches!(
        RecoveryJournal::open(&f.path, &f.url, Some(f.namespace.clone())).await,
        Err(RecoveryError::Missing)
    ));
    assert!(!f.path.exists());
    let mut redis = redis_connection(&f.url).await.unwrap();
    let foreign = format!("{}:old-intent", f.namespace);
    let _: () = redis.set(&foreign, "existing").await.unwrap();
    assert!(
        RecoveryJournal::initialize(&f.path, &f.url, Some(f.namespace.clone()))
            .await
            .is_err()
    );
    assert!(!f.path.exists());
    let _: () = redis.del(foreign).await.unwrap();
    f.initialize().await;
    assert!(
        RecoveryJournal::initialize(&f.path, &f.url, Some(f.namespace.clone()))
            .await
            .is_err()
    );
    assert_eq!(
        fs::metadata(&f.directory).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(&f.path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let j = f.open().await;
    assert!(matches!(
        RecoveryJournal::open(&f.path, &f.url, Some(f.namespace.clone())).await,
        Err(RecoveryError::Locked)
    ));
    assert!(matches!(
        RecoveryJournal::quarantine(&f.path),
        Err(RecoveryError::Locked)
    ));
    admit(&j, "eoa", "a", request("a")).await;
    assert_eq!(RecoveryJournal::status(&f.path).unwrap().admissions, 1);
    let export = f.directory.join("export.json");
    RecoveryJournal::export(&f.path, &export).unwrap();
    assert_eq!(
        fs::metadata(&export).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(fs::read_to_string(export).unwrap().contains("test-secret"));
    assert!(
        !serde_json::to_string(&RecoveryJournal::status(&f.path).unwrap())
            .unwrap()
            .contains("test-secret")
    );
    assert!(!format!("{}", RecoveryError::Storage).contains("test-secret"));
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn immutable_admission_and_replay_ownership_survive_process_reopen() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    let original = json!({"transactionId":"aa","chainId":31337,"pregeneratedNonce":"100","secret":"credential"});
    admit(&j, "erc4337", "aa", original.clone()).await;
    let mut retry = original.clone();
    retry["pregeneratedNonce"] = json!("200");
    assert_eq!(
        admit(&j, "erc4337", "aa", retry.clone()).await.payload,
        original
    );
    assert!(j.validate_payload("erc4337", "aa", &retry).await.is_err());
    j.validate_payload("erc4337", "aa", &original)
        .await
        .unwrap();
    retry["secret"] = json!("changed");
    assert!(
        j.reserve_admission(
            "erc4337",
            "aa",
            &admission_fingerprint("erc4337", &retry).unwrap(),
            retry
        )
        .await
        .is_err()
    );
    assert!(
        j.before_broadcast("eoa", "untracked", &key(0), json!({"wire":"a"}))
            .await
            .is_err()
    );
    for id in ["a", "b"] {
        admit(&j, "eoa", id, request(id)).await;
    }
    j.before_broadcast("eoa", "a", &key(0), json!({"wire":"signed-original"}))
        .await
        .unwrap();
    j.before_broadcast(
        "eoa",
        "a",
        &key(0),
        json!({"wire":"same-nonce-fee-replacement"}),
    )
    .await
    .unwrap();
    assert!(
        j.before_broadcast("eoa", "a", &key(1), json!({"wire":"fresh-nonce"}))
            .await
            .is_err()
    );
    assert!(
        j.before_broadcast("eoa", "b", &key(0), json!({"wire":"conflicting-intent"}))
            .await
            .is_err()
    );
    admit(&j, "eoa_noop", "noop", request("noop")).await;
    assert!(
        j.before_broadcast(
            "eoa_noop",
            "noop",
            &key(0),
            json!({"wire":"nonce-stealing-noop"})
        )
        .await
        .is_err()
    );
    drop(j);
    let j = f.open().await;
    assert_eq!(
        j.admission("eoa", "a").await.unwrap().unwrap().replay_key,
        Some(key(0))
    );
    assert_eq!(RecoveryJournal::status(&f.path).unwrap().attempts, 2);
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn rollback_latches_halt_and_recovery_quarantines_ambiguous_attempts() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    let mut redis = redis_connection(&f.url).await.unwrap();
    let marker = marker_key(&Some(f.namespace.clone()));
    let stale: String = redis.get(&marker).await.unwrap();
    for id in ["unknown", "done", "unsent"] {
        admit(&j, "eoa", id, request(id)).await;
    }
    j.before_broadcast("eoa", "unknown", &key(0), json!({"wire":"unknown-wire"}))
        .await
        .unwrap();
    j.before_broadcast(
        "eoa",
        "done",
        &key(1),
        json!({"wire":"final-wire","transactionHash":alloy::primitives::B256::repeat_byte(1)}),
    )
    .await
    .unwrap();
    j.record_terminal("eoa", "done", terminal("success", 20))
        .await
        .unwrap();
    let current: String = redis.get(&marker).await.unwrap();
    let _: () = redis.set(&marker, stale).await.unwrap();
    assert!(j.ensure_healthy().await.is_err());
    let _: () = redis.set(&marker, current).await.unwrap();
    assert!(
        j.ensure_healthy().await.is_err(),
        "a repaired marker must not silently clear durable halt"
    );
    drop(j);
    assert!(
        RecoveryJournal::reattach(&f.path, &f.url, Some(f.namespace.clone()))
            .await
            .is_err()
    );
    let new = format!("{}_recovered", f.namespace);
    let s = RecoveryJournal::recover(&f.path, &f.url, Some(new.clone()))
        .await
        .unwrap();
    assert_eq!((s.quarantined, s.terminal, s.epoch), (1, 1, 2));
    assert!(
        RecoveryJournal::open(&f.path, &f.url, Some(f.namespace.clone()))
            .await
            .is_err()
    );
    let j = RecoveryJournal::open(&f.path, &f.url, Some(new.clone()))
        .await
        .unwrap();
    assert!(
        j.reserve_admission(
            "eoa",
            "unknown",
            &admission_fingerprint("eoa", &request("unknown")).unwrap(),
            request("unknown")
        )
        .await
        .is_err()
    );
    assert!(admit(&j, "eoa", "done", request("done")).await.terminal);
    assert!(!admit(&j, "eoa", "unsent", request("unsent")).await.terminal);
    admit(&j, "eoa", "new", request("new")).await;
    assert!(
        j.before_broadcast("eoa", "new", &key(0), json!({"wire":"forbidden-reuse"}))
            .await
            .is_err()
    );
    j.before_broadcast("eoa", "new", &key(2), json!({"wire":"new-unique-nonce"}))
        .await
        .unwrap();
    let keys: Vec<String> = redis.keys(format!("{new}:*")).await.unwrap();
    assert_eq!(
        keys,
        vec![marker_key(&Some(new.clone()))],
        "recovery never recreates executable jobs"
    );
    drop(j);
    f.cleanup(&[&new]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn lost_checkpoint_or_sql_commit_before_mirror_is_fail_closed() {
    for deletion in [true, false] {
        let f = Fixture::fresh();
        f.initialize().await;
        let j = f.open().await;
        admit(&j, "eoa", "a", request("a")).await;
        if deletion {
            let _: () = j
                .redis
                .clone()
                .del(marker_key(&Some(f.namespace.clone())))
                .await
                .unwrap();
        } else {
            // Crash cut immediately after a FULL SQL commit, before its Redis CAS.
            j.db(|conn| {
                let tx = conn.transaction()?;
                advance(&tx)?;
                tx.commit()?;
                Ok(())
            })
            .await
            .unwrap();
        }
        drop(j);
        assert!(
            RecoveryJournal::open(&f.path, &f.url, Some(f.namespace.clone()))
                .await
                .is_err()
        );
        assert!(RecoveryJournal::status(&f.path).unwrap().halted);
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn reattach_requires_exact_checkpoint_and_cannot_clear_other_halts() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    admit(&j, "eoa", "a", request("a")).await;
    // Stored prior process identity simulates intact AOF restored under a new run_id.
    j.db(|conn| {
        conn.execute("UPDATE control SET run_id='previous-process'", [])?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(j.ensure_healthy().await.is_err());
    drop(j);
    RecoveryJournal::reattach(&f.path, &f.url, Some(f.namespace.clone()))
        .await
        .unwrap();
    let j = f.open().await;
    j.ensure_healthy().await.unwrap();
    drop(j);
    RecoveryJournal::quarantine(&f.path).unwrap();
    assert!(
        RecoveryJournal::reattach(&f.path, &f.url, Some(f.namespace.clone()))
            .await
            .is_err()
    );
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn terminal_evidence_is_idempotent_and_conflicts_durably_stop_execution() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    admit(&j, "eoa", "a", request("a")).await;
    j.before_broadcast(
        "eoa",
        "a",
        &key(0),
        json!({"wire":"original","transactionHash":alloy::primitives::B256::repeat_byte(1)}),
    )
    .await
    .unwrap();
    j.record_terminal("eoa", "a", terminal("success", 20))
        .await
        .unwrap();
    let checkpoint = RecoveryJournal::status(&f.path).unwrap().checkpoint;
    j.record_terminal("eoa", "a", terminal("success", 30))
        .await
        .unwrap();
    assert_eq!(
        RecoveryJournal::status(&f.path).unwrap().checkpoint,
        checkpoint
    );
    assert!(
        j.record_terminal("eoa", "a", terminal("reverted", 30))
            .await
            .is_err()
    );
    assert!(j.ensure_healthy().await.is_err());
    drop(j);
    assert!(RecoveryJournal::status(&f.path).unwrap().halted);
    RecoveryJournal::quarantine(&f.path).unwrap();
    assert_eq!(
        RecoveryJournal::status(&f.path)
            .unwrap()
            .halt_reason
            .as_deref(),
        Some("terminal evidence conflict")
    );
    assert!(
        RecoveryJournal::recover(
            &f.path,
            &f.url,
            Some(format!("{}_cannot_clear", f.namespace))
        )
        .await
        .is_err(),
        "namespace recovery cannot override terminal contradictions"
    );
    assert!(
        RecoveryJournal::open(&f.path, &f.url, Some(f.namespace.clone()))
            .await
            .is_err()
    );
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn checkpoint_cas_prevents_stale_validation_and_conflicting_chain_progress() {
    use crate::finality::{FinalityEvidence, FinalityPolicy};
    use alloy::primitives::B256;
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    admit(&j, "eoa", "terminal-race", request("terminal-race")).await;
    j.before_broadcast(
        "eoa",
        "terminal-race",
        &key(3),
        json!({"wire":"recorded-before-chain-halt","transactionHash":alloy::primitives::B256::repeat_byte(1)}),
    )
    .await
    .unwrap();
    j.check_chain_healthy(31337).await.unwrap();
    let a = FinalityEvidence {
        block_number: 1,
        block_hash: B256::repeat_byte(1),
        checkpoint_number: 10,
        checkpoint_hash: B256::repeat_byte(10),
        policy: FinalityPolicy::Finalized,
    };
    assert!(j.commit_checkpoint(31337, None, a.clone()).await.unwrap());
    let mut b = a.clone();
    b.checkpoint_number = 11;
    b.checkpoint_hash = B256::repeat_byte(11);
    assert!(
        !j.commit_checkpoint(31337, None, b.clone()).await.unwrap(),
        "prior validation raced another commit"
    );
    assert!(
        j.commit_checkpoint(31337, Some(a), b.clone())
            .await
            .unwrap()
    );
    let mut conflict = b.clone();
    conflict.checkpoint_hash = B256::repeat_byte(99);
    assert!(j.commit_checkpoint(31337, Some(b), conflict).await.is_err());
    assert!(j.check_chain_healthy(31337).await.is_err());
    assert!(
        j.record_terminal("eoa", "terminal-race", terminal("success", 20))
            .await
            .is_err(),
        "chain can halt after caller's precheck; terminal transaction must recheck atomically"
    );
    assert_eq!(
        j.admission_state("eoa", "terminal-race").await.unwrap(),
        Some(AdmissionState::Admitted)
    );
    admit(&j, "eoa", "a", request("a")).await;
    assert!(
        j.before_broadcast("eoa", "a", &key(0), json!({"wire":"forbidden"}))
            .await
            .is_err()
    );
    drop(j);
    let j = f.open().await;
    assert!(j.check_chain_healthy(31337).await.is_err());
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn storage_write_failure_cannot_authorize_broadcast() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    admit(&j, "eoa", "a", request("a")).await;
    j.db(|c| {
        c.pragma_update(None, "query_only", "ON")?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(
        j.before_broadcast("eoa", "a", &key(0), json!({"wire":"never-sent"}))
            .await
            .is_err()
    );
    assert!(j.ensure_healthy().await.is_err());
    assert_eq!(RecoveryJournal::status(&f.path).unwrap().attempts, 0);
    drop(j);
    f.cleanup(&[]).await;
}

/// Separate executable process so SIGKILL really drops all volatile ownership.
#[tokio::test]
#[ignore = "child entry point; parent supplies private test environment"]
async fn subprocess_broadcast_cut() {
    use std::io::{Read, Write};
    let Ok(path) = std::env::var("ENGINE_RECOVERY_TEST_CHILD_PATH") else {
        return;
    };
    // Bound parent waiting even if a regression stops the child before its marker.
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(15));
        std::process::exit(99);
    });
    let url = std::env::var("TEST_REDIS_URL").unwrap();
    let ns = std::env::var("ENGINE_RECOVERY_TEST_CHILD_NS").unwrap();
    let journal = RecoveryJournal::open(path, &url, Some(ns)).await.unwrap();
    journal
        .validate_payload("eoa", "crash", &request("crash"))
        .await
        .unwrap();
    journal
        .before_broadcast("eoa", "crash", &key(7), json!({"wire":"exact-signed-wire"}))
        .await
        .unwrap();
    if let Ok(port) = std::env::var("ENGINE_RECOVERY_TEST_CHILD_PORT") {
        let port = port.parse::<u16>().unwrap();
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream.write_all(b"POST /send HTTP/1.1\r\nHost: localhost\r\nContent-Length: 17\r\nConnection: close\r\n\r\nexact-signed-wire").unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();
        assert!(
            response.is_empty(),
            "stub deliberately drops response after acceptance"
        );
    }
    println!("RECOVERY_TEST_READY_TO_KILL");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; subprocess SIGKILL and loopback HTTP"]
async fn sigkill_before_send_and_after_response_loss_preserves_exact_attempt_and_blocks_new_identity()
 {
    use std::io::{BufRead, BufReader, Read};
    use std::process::{Command, Stdio};
    for after_send in [false, true] {
        let f = Fixture::fresh();
        f.initialize().await;
        let journal = f.open().await;
        admit(&journal, "eoa", "crash", request("crash")).await;
        drop(journal);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "recovery::tests::subprocess_broadcast_cut",
                "--ignored",
                "--nocapture",
            ])
            .env("ENGINE_RECOVERY_TEST_CHILD_PATH", &f.path)
            .env("ENGINE_RECOVERY_TEST_CHILD_NS", &f.namespace)
            .env_remove("ENGINE_RECOVERY_TEST_CHILD_PORT")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let delivered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server = if after_send {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            command.env(
                "ENGINE_RECOVERY_TEST_CHILD_PORT",
                listener.local_addr().unwrap().port().to_string(),
            );
            let delivered = delivered.clone();
            Some(std::thread::spawn(move || {
                listener.set_nonblocking(true).unwrap();
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && std::time::Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(10))
                        }
                        other => panic!("stub did not receive expected request: {other:?}"),
                    }
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut request = Vec::new();
                let mut chunk = [0; 1024];
                while !request.ends_with(b"exact-signed-wire") {
                    let read = stream.read(&mut chunk).unwrap();
                    assert!(read > 0);
                    request.extend_from_slice(&chunk[..read]);
                }
                assert!(request.starts_with(b"POST /send HTTP/1.1\r\n"));
                delivered.fetch_add(1, Ordering::SeqCst);
                // Accepted request, deliberately no response; this is an unknown outcome.
            }))
        } else {
            None
        };
        let mut child = command.spawn().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert!(
                output.read_line(&mut line).unwrap() > 0,
                "child exited before crash cut"
            );
            if line.contains("RECOVERY_TEST_READY_TO_KILL") {
                break;
            }
        }
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
        if let Some(server) = server {
            server.join().unwrap();
        }
        assert_eq!(delivered.load(Ordering::SeqCst), usize::from(after_send));
        let export = f.directory.join("after-crash.json");
        RecoveryJournal::export(&f.path, &export).unwrap();
        let snapshot: Value = serde_json::from_str(&fs::read_to_string(export).unwrap()).unwrap();
        assert_eq!(
            snapshot["attempts"][0]["attempt"]["wire"],
            "exact-signed-wire"
        );
        let new = format!("{}_new", f.namespace);
        RecoveryJournal::recover(&f.path, &f.url, Some(new.clone()))
            .await
            .unwrap();
        let journal = RecoveryJournal::open(&f.path, &f.url, Some(new.clone()))
            .await
            .unwrap();
        assert_eq!(
            journal.admission_state("eoa", "crash").await.unwrap(),
            Some(AdmissionState::Quarantined)
        );
        assert!(
            journal
                .before_broadcast(
                    "eoa",
                    "crash",
                    &key(8),
                    json!({"wire":"forbidden-new-identity"})
                )
                .await
                .is_err()
        );
        assert_eq!(delivered.load(Ordering::SeqCst), usize::from(after_send));
        drop(journal);
        f.cleanup(&[&new]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn identical_attempt_replay_is_read_only_but_still_enforces_all_safety_fences() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    admit(&j, "eoa", "a", request("a")).await;
    let wire = json!({"wire":"exact-signed-request","fee":10});
    j.before_broadcast("eoa", "a", &key(0), wire.clone())
        .await
        .unwrap();
    let checkpoint = RecoveryJournal::status(&f.path).unwrap().checkpoint;
    j.db(|conn| {
        conn.pragma_update(None, "query_only", "ON")?;
        Ok(())
    })
    .await
    .unwrap();
    // Prior FULL-committed evidence is sufficient for an exact replay. A retry
    // must not write a duplicate row or advance the continuity checkpoint.
    j.before_broadcast(
        "eoa",
        "a",
        &key(0),
        json!({"fee":10,"wire":"exact-signed-request"}),
    )
    .await
    .unwrap();
    assert_eq!(
        RecoveryJournal::status(&f.path).unwrap().checkpoint,
        checkpoint
    );
    assert_eq!(RecoveryJournal::status(&f.path).unwrap().attempts, 1);
    assert!(matches!(
        j.before_broadcast("eoa", "a", &key(1), wire.clone()).await,
        Err(RecoveryError::Conflict)
    ));
    j.db(|conn| {
        conn.pragma_update(None, "query_only", "OFF")?;
        Ok(())
    })
    .await
    .unwrap();
    // A replacement at the same replay key is new evidence and must be durable.
    j.before_broadcast("eoa", "a", &key(0), json!({"wire":"replacement","fee":12}))
        .await
        .unwrap();
    assert_eq!(
        RecoveryJournal::status(&f.path).unwrap().checkpoint,
        checkpoint + 1
    );
    assert_eq!(RecoveryJournal::status(&f.path).unwrap().attempts, 2);
    drop(j);
    let j = f.open().await;
    j.before_broadcast("eoa", "a", &key(0), wire.clone())
        .await
        .unwrap();
    j.halt_chain(31337, "test conflict").await.unwrap();
    assert!(
        j.before_broadcast("eoa", "a", &key(0), wire).await.is_err(),
        "existing evidence cannot bypass a later chain halt"
    );
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn exact_replay_cannot_bypass_missing_redis_continuity_or_terminal_state() {
    for terminal_first in [false, true] {
        let f = Fixture::fresh();
        f.initialize().await;
        let j = f.open().await;
        admit(&j, "eoa", "a", request("a")).await;
        let wire = json!({"wire":"same-prior-attempt","transactionHash":alloy::primitives::B256::repeat_byte(1)});
        j.before_broadcast("eoa", "a", &key(0), wire.clone())
            .await
            .unwrap();
        if terminal_first {
            j.record_terminal("eoa", "a", terminal("success", 20))
                .await
                .unwrap();
        } else {
            let _: () = j
                .redis
                .clone()
                .del(marker_key(&Some(f.namespace.clone())))
                .await
                .unwrap();
        }
        assert!(j.before_broadcast("eoa", "a", &key(0), wire).await.is_err());
        assert_eq!(RecoveryJournal::status(&f.path).unwrap().attempts, 1);
        drop(j);
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn identical_checkpoint_is_read_only_without_bypassing_cas_or_chain_halt() {
    use crate::finality::{FinalityEvidence, FinalityPolicy};
    use alloy::primitives::B256;
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    let evidence = FinalityEvidence {
        block_number: 1,
        block_hash: B256::repeat_byte(1),
        checkpoint_number: 10,
        checkpoint_hash: B256::repeat_byte(10),
        policy: FinalityPolicy::Finalized,
    };
    assert!(
        j.commit_checkpoint(31337, None, evidence.clone())
            .await
            .unwrap()
    );
    let checkpoint = RecoveryJournal::status(&f.path).unwrap().checkpoint;
    j.db(|conn| {
        conn.pragma_update(None, "query_only", "ON")?;
        Ok(())
    })
    .await
    .unwrap();
    assert!(
        j.commit_checkpoint(31337, Some(evidence.clone()), evidence.clone())
            .await
            .unwrap()
    );
    assert_eq!(
        RecoveryJournal::status(&f.path).unwrap().checkpoint,
        checkpoint
    );
    let mut different_receipt = evidence.clone();
    different_receipt.block_number = 2;
    different_receipt.block_hash = B256::repeat_byte(2);
    assert!(
        j.commit_checkpoint(31337, Some(evidence.clone()), different_receipt)
            .await
            .unwrap()
    );
    assert_eq!(
        j.load_checkpoint(31337).await.unwrap(),
        Some(evidence.clone())
    );
    assert_eq!(
        RecoveryJournal::status(&f.path).unwrap().checkpoint,
        checkpoint
    );
    assert!(
        !j.commit_checkpoint(31337, None, evidence.clone())
            .await
            .unwrap(),
        "stale validation remains rejected even when candidate equals stored evidence"
    );
    j.db(|conn| {
        conn.pragma_update(None, "query_only", "OFF")?;
        Ok(())
    })
    .await
    .unwrap();
    j.halt_chain(31337, "test conflict").await.unwrap();
    assert!(
        j.commit_checkpoint(31337, Some(evidence.clone()), evidence)
            .await
            .is_err()
    );
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn admission_overload_is_bounded_retryable_and_does_not_halt_reconciliation() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    let guard = j.serial.lock().await;
    let mut requests = Vec::new();
    for i in 0..MAX_CONCURRENT_ADMISSIONS {
        let journal = j.clone();
        requests.push(tokio::spawn(async move {
            let id = format!("bounded-{i}");
            let payload = request(&id);
            journal
                .reserve_admission(
                    "eoa",
                    &id,
                    &admission_fingerprint("eoa", &payload).unwrap(),
                    payload,
                )
                .await
        }));
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while j.admission_slots.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let overflow = request("overflow");
    assert!(matches!(
        j.reserve_admission(
            "eoa",
            "overflow",
            &admission_fingerprint("eoa", &overflow).unwrap(),
            overflow
        )
        .await,
        Err(RecoveryError::Busy)
    ));
    assert_eq!(RecoveryJournal::status(&f.path).unwrap().admissions, 0);
    assert!(!RecoveryJournal::status(&f.path).unwrap().halted);
    drop(guard);
    for task in requests {
        assert!(!task.await.unwrap().unwrap().terminal);
    }
    j.ensure_healthy().await.unwrap();
    assert_eq!(
        RecoveryJournal::status(&f.path).unwrap().admissions,
        MAX_CONCURRENT_ADMISSIONS as u64
    );
    assert!(j.admission("eoa", "overflow").await.unwrap().is_none());
    // Capacity is returned even if an awaiting request is cancelled.
    let guard = j.serial.lock().await;
    let journal = j.clone();
    let pending =
        tokio::spawn(
            async move { admit(&journal, "eoa", "cancelled", request("cancelled")).await },
        );
    tokio::time::timeout(Duration::from_secs(2), async {
        while j.admission_slots.available_permits() == MAX_CONCURRENT_ADMISSIONS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    pending.abort();
    assert!(matches!(pending.await, Err(error) if error.is_cancelled()));
    drop(guard);
    assert_eq!(
        j.admission_slots.available_permits(),
        MAX_CONCURRENT_ADMISSIONS
    );
    admit(&j, "eoa", "overflow", request("overflow")).await;
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn terminal_identity_must_belong_to_same_admission_not_merely_same_chain() {
    use alloy::primitives::B256;
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    for (id, nonce, hash) in [
        ("a", 0, B256::repeat_byte(1)),
        ("b", 1, B256::repeat_byte(2)),
    ] {
        admit(&j, "eoa", id, request(id)).await;
        j.before_broadcast("eoa", id, &key(nonce), json!({"transactionHash":hash}))
            .await
            .unwrap();
    }
    for (field, value) in [
        ("transactionHash", json!(null)),
        ("transactionHash", json!("0x1234")),
        ("chainId", json!("31337")),
        ("chainId", json!(1)),
    ] {
        let mut malformed = terminal("success", 20);
        malformed[field] = value;
        assert!(
            j.validate_attempt_identity("eoa", "a", &malformed)
                .await
                .is_err()
        );
        assert!(j.record_terminal("eoa", "a", malformed).await.is_err());
    }
    let mut proof = terminal("success", 20);
    proof["transactionHash"] = json!(B256::repeat_byte(2));
    assert!(
        j.validate_attempt_identity("eoa", "a", &proof)
            .await
            .is_err()
    );
    assert!(
        j.record_terminal("eoa", "a", proof).await.is_err(),
        "another admitted intent's real receipt cannot settle this one"
    );
    assert_eq!(
        j.admission_state("eoa", "a").await.unwrap(),
        Some(AdmissionState::Admitted)
    );
    assert_eq!(RecoveryJournal::status(&f.path).unwrap().terminal, 0);
    // The first signed candidate may win after a fee replacement is recorded.
    j.before_broadcast(
        "eoa",
        "a",
        &key(0),
        json!({"transactionHash":B256::repeat_byte(3)}),
    )
    .await
    .unwrap();
    let proof = terminal("success", 20);
    j.validate_attempt_identity("eoa", "a", &proof)
        .await
        .unwrap();
    j.record_terminal("eoa", "a", proof).await.unwrap();
    assert_eq!(RecoveryJournal::status(&f.path).unwrap().terminal, 1);
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn solana_terminal_signature_and_chain_are_bound_to_recorded_attempt() {
    use solana_sdk::signature::Signature;
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    admit(&j, "solana", "s", request("s")).await;
    let signature = Signature::from([7u8; 64]).to_string();
    let replay = format!("solana:devnet:{signature}");
    j.before_broadcast("solana", "s", &replay, json!({"signature":signature}))
        .await
        .unwrap();
    let proof = json!({"chainId":"devnet","signature":signature,"slot":9,"commitment":"finalized","outcome":"success"});
    let mut wrong = proof.clone();
    wrong["signature"] = json!(Signature::from([8u8; 64]).to_string());
    assert!(j.record_terminal("solana", "s", wrong).await.is_err());
    let mut wrong = proof.clone();
    wrong["chainId"] = json!("mainnet");
    assert!(j.record_terminal("solana", "s", wrong).await.is_err());
    j.validate_attempt_identity("solana", "s", &proof)
        .await
        .unwrap();
    j.record_terminal("solana", "s", proof).await.unwrap();
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn userop_terminal_hash_is_derived_from_signed_attempt_and_custom_entrypoint() {
    use alloy::{
        primitives::{Address, B256, Bytes, U256},
        rpc::types::UserOperation,
    };
    use engine_aa_types::VersionedUserOp;
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    let sender = Address::repeat_byte(1);
    let entrypoint = Address::repeat_byte(2);
    let nonce = U256::from(7);
    let operation = VersionedUserOp::V0_6(UserOperation {
        sender,
        nonce,
        init_code: Bytes::new(),
        call_data: Bytes::from_static(&[1, 2, 3]),
        call_gas_limit: U256::from(100_000),
        verification_gas_limit: U256::from(100_000),
        pre_verification_gas: U256::from(21_000),
        max_fee_per_gas: U256::from(100),
        max_priority_fee_per_gas: U256::from(1),
        paymaster_and_data: Bytes::new(),
        signature: Bytes::from_static(&[1]),
    });
    let hash = operation
        .hash_with_custom_entrypoint(31337, entrypoint)
        .unwrap();
    admit(
        &j,
        "erc4337",
        "aa",
        json!({"transactionId":"aa","pregeneratedNonce":nonce}),
    )
    .await;
    let replay = format!("erc4337:31337:{entrypoint:#x}:{sender:#x}:{nonce}");
    j.before_broadcast("erc4337", "aa", &replay, json!({"chainId":31337,"entrypoint":entrypoint,"sender":sender,"nonce":nonce,"userOperation":operation})).await.unwrap();
    let mut proof = terminal("success", 20);
    proof["entrypoint"] = json!(entrypoint);
    proof["sender"] = json!(sender);
    proof["nonce"] = json!(nonce);
    proof["userOperationHash"] = json!(hash);
    for (field, value) in [
        ("userOperationHash", json!(B256::repeat_byte(99))),
        ("sender", json!(Address::repeat_byte(99))),
        ("entrypoint", json!(Address::repeat_byte(99))),
        ("nonce", json!(U256::from(8))),
        ("chainId", json!(1)),
    ] {
        let mut wrong = proof.clone();
        wrong[field] = value;
        assert!(
            j.validate_attempt_identity("erc4337", "aa", &wrong)
                .await
                .is_err(),
            "invalid {field} accepted"
        );
        assert!(j.record_terminal("erc4337", "aa", wrong).await.is_err());
    }
    j.validate_attempt_identity("erc4337", "aa", &proof)
        .await
        .unwrap();
    j.record_terminal("erc4337", "aa", proof).await.unwrap();
    // A bundler's opaque ID-to-transaction mapping alone cannot qualify 7702.
    admit(
        &j,
        "eip7702",
        "uid",
        json!({"transactionId":"uid","nonce":"7"}),
    )
    .await;
    j.before_broadcast(
        "eip7702",
        "uid",
        "eip7702:31337:owner:7",
        json!({"uid":"7"}),
    )
    .await
    .unwrap();
    assert!(
        j.record_terminal("eip7702", "uid", terminal("success", 20))
            .await
            .is_err()
    );
    assert_eq!(
        j.admission_state("eip7702", "uid").await.unwrap(),
        Some(AdmissionState::Admitted)
    );
    drop(j);
    f.cleanup(&[]).await;
}
