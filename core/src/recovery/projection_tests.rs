//! Real process-kill cuts through the production checkpoint protocol.
use super::{
    tests::{Fixture, admit, request},
    *,
};
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
    let Some(path) = std::env::var_os("ENGINE_TEST_PROJECTION_PATH") else {
        return;
    };
    let path = PathBuf::from(path);
    let journal = RecoveryJournal::open(
        &path,
        &std::env::var("TEST_REDIS_URL").unwrap(),
        RecoveryJournal::status(&path).unwrap().namespace,
    )
    .await
    .unwrap();
    journal
        .before_broadcast("eoa", "crash-attempt", replay(), attempt())
        .await
        .unwrap();
    // This file stands for permission returned to the actual caller. None of
    // the requested cuts may reach it before the parent kills the process.
    fs::write(
        path.with_file_name("caller-received-permission"),
        b"returned",
    )
    .unwrap();
}

async fn kill_at(f: &Fixture, point: &str) {
    let marker = f.path.with_file_name("phase-reached");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "recovery::projection_tests::projection_crash_child",
            "--ignored",
            "--nocapture",
        ])
        .env("ENGINE_TEST_PROJECTION_PATH", &f.path)
        .env("ENGINE_TEST_PROJECTION_CUT", point)
        .env("ENGINE_TEST_PROJECTION_MARKER", &marker)
        .env("TEST_REDIS_URL", &f.url)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let reached = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if marker.exists() {
                break;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before crash cut {point}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    // Always reap a child, including a missed phase, before reporting failure.
    let _ = child.kill();
    child.wait().unwrap();
    reached.expect("production checkpoint phase was not reached");
    assert!(!f.path.with_file_name("caller-received-permission").exists());
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; subprocess SIGKILL"]
async fn every_commit_apply_ack_crash_cut_reopens_without_new_identity() {
    for point in [
        "after_authority_commit",
        "after_redis_apply",
        "before_ack",
        "after_ack",
    ] {
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
        assert_eq!(
            projection::load(&conn, &current).unwrap().is_some(),
            point != "after_ack"
        );
        drop(conn);
        let j = reopen(&f).await.unwrap();
        j.ensure_healthy().await.unwrap();
        assert_eq!(
            j.latest_eoa_attempt("crash-attempt", replay())
                .await
                .unwrap(),
            Some(attempt())
        );
        j.before_broadcast("eoa", "crash-attempt", replay(), attempt())
            .await
            .unwrap();
        assert_eq!(
            RecoveryJournal::status(&f.path).unwrap().checkpoint,
            before + 1,
            "exact wire retry is read-only after recovery"
        );
        assert!(
            j.before_broadcast(
                "eoa",
                "crash-attempt",
                "evm:31337:0x1111111111111111111111111111111111111111:1",
                attempt()
            )
            .await
            .is_err()
        );
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
    let _: () = j
        .redis
        .clone()
        .set(previous.key(), previous.token())
        .await
        .unwrap();
    assert!(j.ensure_healthy().await.is_err());
    assert_eq!(
        RecoveryJournal::status(&f.path)
            .unwrap()
            .halt_reason
            .as_deref(),
        Some("Redis checkpoint mismatch")
    );
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
        worker
            .db(move |conn| {
                let tx = conn.transaction()?;
                advance(&tx)?;
                let _ = entered.send(());
                held.recv_timeout(Duration::from_secs(10))
                    .map_err(|_| RecoveryError::Storage)?;
                commit_transaction(tx)
            })
            .await
    });
    ready.await.unwrap();
    task.abort();
    let _ = task.await;
    drop(j);
    assert!(
        matches!(owner_lock(&f.path, false), Err(RecoveryError::Locked)),
        "blocking SQLite work must retain the owner after its async caller is gone"
    );
    release.send(()).unwrap();
    let j = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match reopen(&f).await {
                Ok(j) => break j,
                Err(RecoveryError::Locked) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(error) => panic!("unexpected recovery failure: {error}"),
            }
        }
    })
    .await
    .unwrap();
    j.ensure_healthy().await.unwrap();
    assert_eq!(RecoveryJournal::status(&f.path).unwrap().checkpoint, 1);
    drop(j);
    f.cleanup(&[]).await;
}

// The fixture has the exact schema1 layout used by the previous binary. Only
// the schema2 table/token are removed; authority, bindings and history remain.
async fn make_legacy(f: &Fixture) {
    let conn = connection(&f.path, false).unwrap();
    conn.execute_batch("BEGIN IMMEDIATE; DROP TABLE projection_pending; UPDATE control SET schema_version=1; COMMIT;").unwrap();
    let current = control(&conn).unwrap();
    let mut redis = redis_connection(&f.url).await.unwrap();
    let _: () = redis.set(current.key(), current.token()).await.unwrap();
}

async fn stage_pending(j: &Arc<RecoveryJournal>) -> Control {
    j.db(|conn| {
        let tx = conn.transaction()?;
        let old = advance(&tx)?;
        commit_transaction(tx)?;
        Ok(old)
    })
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; subprocess SIGKILL"]
async fn legacy_migration_crash_cuts_preserve_history_and_complete_exact_transition() {
    for point in [
        "after_migration_commit",
        "after_redis_apply",
        "before_ack",
        "after_ack",
    ] {
        let f = Fixture::fresh();
        f.initialize().await;
        let j = f.open().await;
        admit(&j, "eoa", "crash-attempt", request("crash-attempt")).await;
        j.before_broadcast("eoa", "crash-attempt", replay(), attempt())
            .await
            .unwrap();
        let before = RecoveryJournal::status(&f.path).unwrap();
        drop(j);
        make_legacy(&f).await;
        assert_eq!(RecoveryJournal::status(&f.path).unwrap().schema, 1);
        let export = f.path.with_file_name("legacy-export.json");
        RecoveryJournal::export(&f.path, &export).unwrap();
        let legacy: Value = serde_json::from_slice(&fs::read(export).unwrap()).unwrap();
        assert_eq!(legacy["status"]["schema"], 1);
        assert_eq!(legacy["projection_pending"], Value::Null);
        kill_at(&f, point).await;
        let after = RecoveryJournal::status(&f.path).unwrap();
        assert_eq!(after.schema, 2);
        assert_eq!(after.checkpoint, before.checkpoint);
        assert_eq!(after.projection_pending, point != "after_ack");
        let j = reopen(&f).await.unwrap();
        j.ensure_healthy().await.unwrap();
        assert!(!RecoveryJournal::status(&f.path).unwrap().projection_pending);
        assert_eq!(
            j.latest_eoa_attempt("crash-attempt", replay())
                .await
                .unwrap(),
            Some(attempt())
        );
        j.before_broadcast("eoa", "crash-attempt", replay(), attempt())
            .await
            .unwrap();
        assert_eq!(
            RecoveryJournal::status(&f.path).unwrap().checkpoint,
            before.checkpoint
        );
        drop(j);
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn unhealthy_legacy_migration_does_not_create_pending_or_advance_schema() {
    for fault in ["missing", "old", "halted", "run"] {
        let f = Fixture::fresh();
        f.initialize().await;
        let j = f.open().await;
        admit(&j, "eoa", "a", request("a")).await;
        drop(j);
        make_legacy(&f).await;
        let conn = connection(&f.path, false).unwrap();
        let mut current = control(&conn).unwrap();
        let mut redis = redis_connection(&f.url).await.unwrap();
        match fault {
            "missing" => {
                let _: () = redis.del(current.key()).await.unwrap();
            }
            "old" => {
                current.checkpoint -= 1;
                let _: () = redis.set(current.key(), current.token()).await.unwrap();
            }
            "halted" => {
                conn.execute(
                    "UPDATE control SET halted=1,reason='operator quarantine'",
                    [],
                )
                .unwrap();
            }
            "run" => {
                conn.execute("UPDATE control SET run_id='different-process'", [])
                    .unwrap();
            }
            _ => unreachable!(),
        }
        drop(conn);
        assert!(reopen(&f).await.is_err(), "{fault}");
        let state = RecoveryJournal::status(&f.path).unwrap();
        assert_eq!(state.schema, 1);
        assert!(!state.projection_pending);
        assert!(state.halted);
        let conn = connection(&f.path, true).unwrap();
        let tables: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name='projection_pending'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(tables, 0);
        drop(conn);
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn pending_marker_contradictions_and_run_change_stay_fenced() {
    for fault in ["missing", "foreign", "earlier", "run"] {
        let f = Fixture::fresh();
        f.initialize().await;
        let j = f.open().await;
        admit(&j, "eoa", "a", request("a")).await;
        let old = stage_pending(&j).await;
        match fault {
            "missing" => {
                let _: () = j.redis.clone().del(old.key()).await.unwrap();
            }
            "foreign" => {
                let _: () = j
                    .redis
                    .clone()
                    .set(old.key(), "v2:foreign:1:2")
                    .await
                    .unwrap();
            }
            "earlier" => {
                let mut older = old;
                older.checkpoint -= 1;
                let _: () = j
                    .redis
                    .clone()
                    .set(older.key(), older.token())
                    .await
                    .unwrap();
            }
            "run" => {
                j.db(|conn| { conn.execute_batch("BEGIN; UPDATE control SET run_id='different-process'; UPDATE projection_pending SET run_id='different-process'; COMMIT")?; Ok(()) }).await.unwrap();
            }
            _ => unreachable!(),
        }
        assert!(j.ensure_healthy().await.is_err(), "{fault}");
        let state = RecoveryJournal::status(&f.path).unwrap();
        assert!(state.halted && state.projection_pending);
        drop(j);
        assert!(reopen(&f).await.is_err());
        assert!(
            RecoveryJournal::reattach(&f.path, &f.url, state.namespace)
                .await
                .is_err()
        );
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn malformed_pending_and_ack_storage_failure_never_grant_permission() {
    for fault in ["previous", "target", "key", "run", "ack"] {
        let f = Fixture::fresh();
        f.initialize().await;
        let j = f.open().await;
        stage_pending(&j).await;
        j.db(move |conn| {
            let sql = match fault {
                "previous" => "UPDATE projection_pending SET previous_token='invalid'",
                "target" => "UPDATE projection_pending SET target_token='invalid'",
                "key" => "UPDATE projection_pending SET redis_key='foreign:key'",
                "run" => "UPDATE projection_pending SET run_id='different-process'",
                "ack" => "CREATE TRIGGER reject_ack BEFORE DELETE ON projection_pending BEGIN SELECT RAISE(ABORT,'test ack failure'); END",
                _ => unreachable!(),
            };
            conn.execute_batch(sql)?;
            Ok(())
        }).await.unwrap();
        assert!(j.ensure_healthy().await.is_err(), "{fault}");
        let conn = connection(&f.path, true).unwrap();
        assert!(control(&conn).unwrap().halted);
        let retained: i64 = conn
            .query_row("SELECT count(*) FROM projection_pending", [], |r| r.get(0))
            .unwrap();
        assert_eq!(retained, 1);
        drop(conn);
        drop(j);
        assert!(reopen(&f).await.is_err());
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn pending_reattach_is_refused_and_explicit_recovery_quarantines_attempts() {
    for applied in [false, true] {
        let f = Fixture::fresh();
        f.initialize().await;
        let j = f.open().await;
        admit(&j, "eoa", "a", request("a")).await;
        j.before_broadcast("eoa", "a", replay(), attempt())
            .await
            .unwrap();
        let old = stage_pending(&j).await;
        let mut target = old.clone();
        target.checkpoint += 1;
        if applied {
            let _: () = j
                .redis
                .clone()
                .set(target.key(), target.token())
                .await
                .unwrap();
        }
        drop(j);
        assert!(
            RecoveryJournal::reattach(&f.path, &f.url, old.namespace.clone())
                .await
                .is_err()
        );
        let export = f.path.with_file_name("pending-export.json");
        RecoveryJournal::export(&f.path, &export).unwrap();
        let pending: Value = serde_json::from_slice(&fs::read(export).unwrap()).unwrap();
        assert_eq!(pending["projection_pending"]["previous_token"], old.token());
        assert_eq!(
            pending["projection_pending"]["target_token"],
            target.token()
        );
        let namespace = format!("recovered_{}", uuid::Uuid::new_v4().simple());
        let recovered = RecoveryJournal::recover(&f.path, &f.url, Some(namespace.clone()))
            .await
            .unwrap();
        assert_eq!(recovered.schema, 2);
        assert_eq!(recovered.epoch, old.epoch + 1);
        assert!(!recovered.projection_pending);
        assert_eq!(recovered.quarantined, 1);
        let j = reopen(&f).await.unwrap();
        assert!(
            j.before_broadcast("eoa", "a", replay(), attempt())
                .await
                .is_err()
        );
        drop(j);
        f.cleanup(&[&namespace]).await;
    }
}

#[path = "projection_transport_tests.rs"]
mod transport;

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancelled_health_acknowledgement_is_owned_and_drained() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    stage_pending(&j).await;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resuming) = tokio::sync::oneshot::channel();
    *j.before_ack.lock().unwrap() = Some((entered, resuming));
    let health_journal = j.clone();
    let health = tokio::spawn(async move { health_journal.ensure_healthy().await });
    tokio::time::timeout(Duration::from_secs(5), entering)
        .await
        .unwrap()
        .unwrap();
    health.abort();
    assert!(health.await.unwrap_err().is_cancelled());
    assert!(j.serial.try_lock().is_err());
    assert!(RecoveryJournal::status(&f.path).unwrap().projection_pending);
    let mut drained = Box::pin(j.drain_mutations());
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(drained.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), drained)
        .await
        .unwrap();
    assert!(!RecoveryJournal::status(&f.path).unwrap().projection_pending);
    j.ensure_healthy().await.unwrap();
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn legacy_inventory_is_read_only_and_incomplete_or_unknown_schemas_are_rejected() {
    let f = Fixture::fresh();
    f.initialize().await;
    make_legacy(&f).await;
    let conn = connection(&f.path, true).unwrap();
    let before = control(&conn).unwrap();
    let revision: i64 = conn
        .pragma_query_value(None, "data_version", |row| row.get(0))
        .unwrap();
    let status = RecoveryJournal::status(&f.path).unwrap();
    assert_eq!(status.schema, 1);
    let export = f.path.with_file_name("inventory.json");
    RecoveryJournal::export(&f.path, &export).unwrap();
    assert_eq!(
        conn.pragma_query_value::<i64, _>(None, "data_version", |row| row.get(0))
            .unwrap(),
        revision,
        "inventory commands must not migrate or commit authority"
    );
    assert_eq!(control(&conn).unwrap().schema, 1);
    assert_eq!(
        conn.query_row::<i64, _, _>(
            "SELECT count(*) FROM sqlite_schema WHERE name='projection_pending'",
            [],
            |row| row.get(0)
        )
        .unwrap(),
        0
    );
    let mut redis = redis_connection(&f.url).await.unwrap();
    assert_eq!(
        redis.get::<_, String>(before.key()).await.unwrap(),
        before.token()
    );
    drop(conn);
    f.cleanup(&[]).await;

    for fault in ["unknown", "legacy_missing_table", "current_missing_pending"] {
        let f = Fixture::fresh();
        f.initialize().await;
        if fault == "legacy_missing_table" {
            make_legacy(&f).await;
        }
        let conn = connection(&f.path, false).unwrap();
        let namespace = control(&conn).unwrap().namespace;
        conn.execute_batch(match fault {
            "unknown" => "UPDATE control SET schema_version=99",
            "legacy_missing_table" => "DROP TABLE attempts",
            "current_missing_pending" => "DROP TABLE projection_pending",
            _ => unreachable!(),
        })
        .unwrap();
        drop(conn);
        assert!(RecoveryJournal::status(&f.path).is_err(), "{fault}");
        let export = f.path.with_file_name("invalid-export.json");
        assert!(RecoveryJournal::export(&f.path, &export).is_err());
        assert!(!export.exists());
        assert!(
            RecoveryJournal::open(&f.path, &f.url, namespace)
                .await
                .is_err(),
            "{fault}"
        );
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancelled_health_callers_cannot_create_unbounded_owned_tasks() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    let held = j.serial.lock().await;
    // Poll and drop callers before their spawned operations can run. Detached
    // work must retain each slot, rather than recycling it on caller cancellation.
    for _ in 0..MAX_CONCURRENT_HEALTH_CHECKS {
        let mut caller = Box::pin(j.ensure_healthy());
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(caller.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(caller);
    }
    assert_eq!(j.health_slots.available_permits(), 0);
    let retained = Arc::strong_count(&j);
    // These additional callers wait outside owned registration/spawn. Dropping
    // their futures must release their semaphore waiters and retain no journal.
    for _ in 0..(MAX_CONCURRENT_HEALTH_CHECKS * 4) {
        let mut waiting = Box::pin(j.ensure_healthy());
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(waiting);
    }
    assert_eq!(Arc::strong_count(&j), retained);
    assert_eq!(j.health_slots.available_permits(), 0);
    assert!(j.owned_operations.try_write().is_err());
    drop(held);
    tokio::time::timeout(Duration::from_secs(10), j.drain_mutations())
        .await
        .unwrap();
    assert_eq!(
        j.health_slots.available_permits(),
        MAX_CONCURRENT_HEALTH_CHECKS
    );
    j.ensure_healthy().await.unwrap();
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn rollback_between_observation_and_acknowledgement_does_not_return_send_permission() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    admit(&j, "eoa", "a", request("a")).await;
    let previous = j.db(|conn| control(conn)).await.unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resuming) = tokio::sync::oneshot::channel();
    *j.before_ack.lock().unwrap() = Some((entered, resuming));
    let broadcaster = j.clone();
    let result = tokio::spawn(async move {
        broadcaster
            .before_broadcast("eoa", "a", replay(), attempt())
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entering)
        .await
        .unwrap()
        .unwrap();
    let _: () = j
        .redis
        .clone()
        .set(previous.key(), previous.token())
        .await
        .unwrap();
    resume.send(()).unwrap();
    assert!(
        result.await.unwrap().is_err(),
        "post-ack fence must deny send permission"
    );
    let state = RecoveryJournal::status(&f.path).unwrap();
    assert!(
        !state.projection_pending,
        "acknowledgement committed before final observation"
    );
    assert!(state.halted);
    assert_eq!(
        state.halt_reason.as_deref(),
        Some("Redis checkpoint mismatch")
    );
    assert_eq!(state.attempts, 1);
    drop(j);
    assert!(reopen(&f).await.is_err());
    f.cleanup(&[]).await;
}
