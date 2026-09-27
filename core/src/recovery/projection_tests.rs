//! Real process-kill cuts through the production checkpoint protocol.
use super::{tests::{Fixture, admit, request}, *};
use serde_json::json;
use std::process::{Command, Stdio};
use twmq::redis::AsyncCommands;

/// Compiled only into the unit-test executable. The parent kills this process
/// after the exact production phase; the release server has no fault switch.
pub(super) async fn crash_cut(point: &str) {
    if std::env::var("ENGINE_TEST_PROJECTION_CUT").ok().as_deref() != Some(point) {
        return;
    }
    let marker = std::env::var_os("ENGINE_TEST_PROJECTION_MARKER").unwrap();
    fs::write(&marker, point).unwrap();
    File::open(&marker).unwrap().sync_all().unwrap();
    std::future::pending::<()>().await;
}

fn replay() -> &'static str {
    "evm:31337:0x1111111111111111111111111111111111111111:0"
}
fn attempt() -> Value {
    json!({"wire":"original-exact-wire", "transactionHash":alloy::primitives::B256::repeat_byte(7)})
}

async fn reopen(f: &Fixture) -> Result<Arc<RecoveryJournal>> {
    RecoveryJournal::open(&f.path, &f.url, RecoveryJournal::status(&f.path)?.namespace).await
}

#[tokio::test]
#[ignore = "child process for projection crash cuts"]
async fn projection_crash_child() {
    let Some(path) = std::env::var_os("ENGINE_TEST_PROJECTION_PATH") else { return };
    let path = PathBuf::from(path);
    let journal = RecoveryJournal::open(&path, &std::env::var("TEST_REDIS_URL").unwrap(),
        RecoveryJournal::status(&path).unwrap().namespace).await.unwrap();
    journal.before_broadcast("eoa", "crash-attempt", replay(), attempt()).await.unwrap();
    // This file stands for permission returned to the actual caller. None of
    // the requested cuts may reach it before the parent kills the process.
    fs::write(path.with_file_name("caller-received-permission"), b"returned").unwrap();
}

async fn kill_at(f: &Fixture, point: &str) {
    let marker = f.path.with_file_name("phase-reached");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "recovery::projection_tests::projection_crash_child", "--ignored", "--nocapture"])
        .env("ENGINE_TEST_PROJECTION_PATH", &f.path)
        .env("ENGINE_TEST_PROJECTION_CUT", point)
        .env("ENGINE_TEST_PROJECTION_MARKER", &marker)
        .env("TEST_REDIS_URL", &f.url)
        .stdout(Stdio::null()).stderr(Stdio::inherit()).spawn().unwrap();
    let reached = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if marker.exists() { break; }
            assert!(child.try_wait().unwrap().is_none(), "child exited before crash cut {point}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await;
    // Always reap a child, including a missed phase, before reporting failure.
    let _ = child.kill();
    child.wait().unwrap();
    reached.expect("production checkpoint phase was not reached");
    assert!(!f.path.with_file_name("caller-received-permission").exists());
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; subprocess SIGKILL"]
async fn every_commit_apply_ack_crash_cut_reopens_without_new_identity() {
    for point in ["after_authority_commit", "after_redis_apply", "before_ack", "after_ack"] {
        let f = Fixture::fresh();
        f.initialize().await;
        let j = f.open().await;
        admit(&j, "eoa", "crash-attempt", request("crash-attempt")).await;
        let before = RecoveryJournal::status(&f.path).unwrap().checkpoint;
        drop(j);
        kill_at(&f, point).await;
        let conn = connection(&f.path, true).unwrap();
        let current = control(&conn).unwrap();
        assert_eq!(current.checkpoint, before + 1);
        assert_eq!(projection::load(&conn, &current).unwrap().is_some(), point != "after_ack");
        drop(conn);
        let j = reopen(&f).await.unwrap();
        j.ensure_healthy().await.unwrap();
        assert_eq!(j.latest_eoa_attempt("crash-attempt", replay()).await.unwrap(), Some(attempt()));
        j.before_broadcast("eoa", "crash-attempt", replay(), attempt()).await.unwrap();
        assert_eq!(RecoveryJournal::status(&f.path).unwrap().checkpoint, before + 1,
                   "exact wire retry is read-only after recovery");
        assert!(j.before_broadcast("eoa", "crash-attempt",
            "evm:31337:0x1111111111111111111111111111111111111111:1", attempt()).await.is_err());
        drop(j);
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn acknowledged_predecessor_rollback_is_never_repaired() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    let previous = j.db(|c| control(c)).await.unwrap();
    admit(&j, "eoa", "one", request("one")).await;
    let _: () = j.redis.clone().set(previous.key(), previous.token()).await.unwrap();
    assert!(j.ensure_healthy().await.is_err());
    assert_eq!(RecoveryJournal::status(&f.path).unwrap().halt_reason.as_deref(), Some("Redis checkpoint mismatch"));
    drop(j);
    assert!(reopen(&f).await.is_err());
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn canceled_blocking_mutation_keeps_exclusive_process_ownership() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    let worker = j.clone();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel();
    let task = tokio::spawn(async move {
        let _guard = worker.serial_guard("test_mutation").await;
        worker.db(move |conn| {
            let tx = conn.transaction()?;
            advance(&tx)?;
            let _ = entered.send(());
            held.recv_timeout(Duration::from_secs(10)).map_err(|_| RecoveryError::Storage)?;
            commit_transaction(tx)
        }).await
    });
    ready.await.unwrap();
    task.abort();
    let _ = task.await;
    drop(j);
    assert!(matches!(owner_lock(&f.path, false), Err(RecoveryError::Locked)),
            "blocking SQLite work must retain the owner after its async caller is gone");
    release.send(()).unwrap();
    let j = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match reopen(&f).await {
                Ok(j) => break j,
                Err(RecoveryError::Locked) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(error) => panic!("unexpected recovery failure: {error}"),
            }
        }
    }).await.unwrap();
    j.ensure_healthy().await.unwrap();
    assert_eq!(RecoveryJournal::status(&f.path).unwrap().checkpoint, 1);
    drop(j);
    f.cleanup(&[]).await;
}
