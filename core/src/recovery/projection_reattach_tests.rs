use super::*;

/// Retain authority/attempts, but make the recorded Redis process the predecessor
/// of the actual test server. The process harness separately exercises a real restart.
async fn pending_on_previous_process(f: &Fixture) -> (Control, Control) {
    f.initialize().await;
    let j = f.open().await;
    admit(&j, "eoa", "a", request("a")).await;
    j.before_broadcast("eoa", "a", replay(), attempt())
        .await
        .unwrap();
    let old = stage_pending(&j).await;
    j.db(|conn| {
        conn.execute_batch("BEGIN; UPDATE control SET run_id='previous-test-process'; UPDATE projection_pending SET run_id='previous-test-process'; COMMIT")?;
        Ok(())
    }).await.unwrap();
    let target = j.db(|conn| control(conn)).await.unwrap();
    let _: () = j
        .redis
        .clone()
        .set(target.key(), target.token())
        .await
        .unwrap();
    drop(j);
    (old, target)
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn exact_target_reattach_preserves_identity_and_acknowledges_pending_atomically() {
    for reason in [
        None,
        Some("Redis process changed"),
        Some("Redis continuity unavailable"),
        Some("Redis primary role changed"),
    ] {
        let f = Fixture::fresh();
        let (_, target) = pending_on_previous_process(&f).await;
        let conn = connection(&f.path, false).unwrap();
        if let Some(reason) = reason {
            conn.execute("UPDATE control SET halted=1,reason=?", [reason])
                .unwrap();
        }
        let before = RecoveryJournal::status(&f.path).unwrap();
        drop(conn);
        let after = RecoveryJournal::reattach(&f.path, &f.url, target.namespace.clone())
            .await
            .unwrap();
        assert!(!after.projection_pending && !after.halted);
        assert_eq!(
            (
                after.epoch,
                after.checkpoint,
                after.attempts,
                after.quarantined
            ),
            (
                before.epoch,
                before.checkpoint,
                before.attempts,
                before.quarantined
            )
        );
        // A lost CLI response may be retried without checkpoint/identity change.
        let again = RecoveryJournal::reattach(&f.path, &f.url, target.namespace.clone())
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(again).unwrap(),
            serde_json::to_value(after).unwrap()
        );
        let j = f.open().await;
        assert_eq!(
            j.latest_eoa_attempt("a", replay()).await.unwrap(),
            Some(attempt())
        );
        assert_eq!(
            j.admission("eoa", "a")
                .await
                .unwrap()
                .unwrap()
                .replay_key
                .as_deref(),
            Some(replay())
        );
        let changed = replay().replace(":0", ":1");
        assert!(
            j.before_broadcast("eoa", "a", &changed, attempt())
                .await
                .is_err()
        );
        drop(j);
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn pending_reattach_rejects_predecessor_missing_foreign_and_forbidden_halts_without_ack() {
    for fault in [
        "predecessor",
        "missing",
        "foreign",
        "namespace",
        "operator quarantine",
        "ledger unavailable",
        "terminal evidence conflict",
        "Redis checkpoint mismatch",
    ] {
        let f = Fixture::fresh();
        let (previous, target) = pending_on_previous_process(&f).await;
        let mut redis = redis_connection(&f.url).await.unwrap();
        let conn = connection(&f.path, false).unwrap();
        let mut namespace = target.namespace.clone();
        match fault {
            "predecessor" => {
                let _: () = redis.set(target.key(), previous.token()).await.unwrap();
            }
            "missing" => {
                let _: () = redis.del(target.key()).await.unwrap();
            }
            "foreign" => {
                let _: () = redis.set(target.key(), "foreign-checkpoint").await.unwrap();
            }
            "namespace" => {
                namespace = Some("another_namespace".into());
            }
            reason => {
                conn.execute("UPDATE control SET halted=1,reason=?", [reason])
                    .unwrap();
            }
        }
        let before = control(&conn).unwrap();
        let pending = projection::load(&conn, &before).unwrap();
        drop(conn);
        assert!(
            RecoveryJournal::reattach(&f.path, &f.url, namespace)
                .await
                .is_err(),
            "{fault}"
        );
        let conn = connection(&f.path, true).unwrap();
        let after = control(&conn).unwrap();
        assert!(after == before, "{fault}");
        assert_eq!(projection::load(&conn, &after).unwrap(), pending, "{fault}");
        drop(conn);
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn failed_run_adoption_rolls_back_pending_acknowledgement() {
    let f = Fixture::fresh();
    let (_, target) = pending_on_previous_process(&f).await;
    let conn = connection(&f.path, false).unwrap();
    conn.execute_batch("CREATE TRIGGER reject_reattach BEFORE UPDATE OF run_id ON control BEGIN SELECT RAISE(ABORT,'test refusal'); END").unwrap();
    drop(conn);
    assert!(
        RecoveryJournal::reattach(&f.path, &f.url, target.namespace.clone())
            .await
            .is_err()
    );
    let conn = connection(&f.path, false).unwrap();
    assert!(control(&conn).unwrap() == target);
    assert!(projection::load(&conn, &target).unwrap().is_some());
    conn.execute_batch("DROP TRIGGER reject_reattach").unwrap();
    drop(conn);
    RecoveryJournal::reattach(&f.path, &f.url, target.namespace)
        .await
        .unwrap();
    assert!(!RecoveryJournal::status(&f.path).unwrap().projection_pending);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; subprocess SIGKILL"]
async fn reattach_process_cuts_keep_ack_and_run_adoption_together_and_retry_without_new_identity() {
    for cut in ["before_reattach_commit", "after_reattach_commit"] {
        let f = Fixture::fresh();
        let (_, target) = pending_on_previous_process(&f).await;
        kill_action_at(&f, cut, "reattach").await;
        let conn = connection(&f.path, true).unwrap();
        let current = control(&conn).unwrap();
        let pending = projection::load(&conn, &current).unwrap();
        assert_eq!(pending.is_some(), cut == "before_reattach_commit");
        assert_eq!(
            current.run_id == target.run_id,
            cut == "before_reattach_commit"
        );
        assert_eq!(
            (current.epoch, current.checkpoint),
            (target.epoch, target.checkpoint)
        );
        drop(conn);
        let after = RecoveryJournal::reattach(&f.path, &f.url, target.namespace)
            .await
            .unwrap();
        assert_eq!(after.attempts, 1);
        assert_eq!(after.quarantined, 0);
        assert!(!after.projection_pending);
        let j = f.open().await;
        assert_eq!(
            j.latest_eoa_attempt("a", replay()).await.unwrap(),
            Some(attempt())
        );
        drop(j);
        f.cleanup(&[]).await;
    }
}

async fn schema_two(f: &Fixture, pending_kind: Option<&str>, applied: bool) {
    let mut conn = connection(&f.path, false).unwrap();
    let tx = conn.transaction().unwrap();
    tx.execute_batch("DROP TABLE solana_recovery; UPDATE control SET schema_version=2")
        .unwrap();
    let previous = match pending_kind {
        Some("write") => advance(&tx).unwrap(),
        Some("migration") => {
            let target = control(&tx).unwrap();
            let mut old = target.clone();
            old.schema = 1;
            projection::record(&tx, &old, &target).unwrap();
            old
        }
        None => control(&tx).unwrap(),
        _ => unreachable!(),
    };
    let target = control(&tx).unwrap();
    commit_transaction(tx).unwrap();
    let mut redis = redis_connection(&f.url).await.unwrap();
    let _: () = redis
        .set(
            target.key(),
            if applied {
                target.token()
            } else {
                previous.token()
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn schema_two_pending_is_finished_before_upgrade_and_inventory_is_read_only() {
    for kind in [None, Some("write"), Some("migration")] {
        for applied in [false, true] {
            let f = Fixture::fresh();
            f.initialize().await;
            let j = f.open().await;
            admit(&j, "eoa", "a", request("a")).await;
            drop(j);
            schema_two(&f, kind, applied).await;
            let before = RecoveryJournal::status(&f.path).unwrap();
            assert_eq!(before.schema, 2);
            let export = f.path.with_file_name("schema2-export.json");
            RecoveryJournal::export(&f.path, export).unwrap();
            assert_eq!(RecoveryJournal::status(&f.path).unwrap().schema, 2);
            let j = f.open().await;
            let after = RecoveryJournal::status(&f.path).unwrap();
            assert_eq!(after.schema, SCHEMA);
            assert_eq!(after.checkpoint, before.checkpoint);
            assert!(!after.projection_pending);
            assert_eq!(after.solana_recovery_broadcasts, 0);
            assert_eq!(
                j.admission("eoa", "a").await.unwrap().unwrap().payload,
                request("a")
            );
            drop(j);
            f.cleanup(&[]).await;
        }
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn offline_reattach_accepts_schema_one_and_two_without_migrating_them() {
    for schema in [1, 2] {
        let f = Fixture::fresh();
        f.initialize().await;
        if schema == 1 {
            make_legacy(&f).await;
        } else {
            schema_two(&f, Some("write"), true).await;
        }
        let before = RecoveryJournal::status(&f.path).unwrap();
        let after = RecoveryJournal::reattach(&f.path, &f.url, before.namespace)
            .await
            .unwrap();
        assert_eq!(after.schema, schema);
        assert!(!after.projection_pending);
        let conn = connection(&f.path, true).unwrap();
        assert_eq!(
            conn.query_row::<i64, _, _>(
                "SELECT count(*) FROM sqlite_schema WHERE name='solana_recovery'",
                [],
                |r| r.get(0)
            )
            .unwrap(),
            0
        );
        drop(conn);
        f.cleanup(&[]).await;
    }
}

#[path = "../../../test-support/redis_reply_loss.rs"]
mod redis_reply_loss;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn observation_reply_loss_before_and_after_reattach_commit_never_reports_success() {
    for ordinal in [1, 2] {
        let f = Fixture::fresh();
        let (_, target) = pending_on_previous_process(&f).await;
        let proxy = redis_reply_loss::ReplyLossProxy::observation_loss(&f.url, ordinal).await;
        proxy.armed.store(true, Ordering::SeqCst);
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            RecoveryJournal::reattach(&f.path, &proxy.url, target.namespace.clone()),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        assert!(
            proxy.dropped.load(Ordering::SeqCst),
            "must drop an actual Redis observation"
        );
        let state = RecoveryJournal::status(&f.path).unwrap();
        let conn = connection(&f.path, true).unwrap();
        let current = control(&conn).unwrap();
        if ordinal == 1 {
            assert!(current == target);
            assert!(state.projection_pending);
        } else {
            assert!(!state.projection_pending);
            assert_ne!(current.run_id, target.run_id);
            assert!(state.halted);
            assert_eq!(
                current.reason.as_deref(),
                Some("Redis continuity unavailable")
            );
            assert!(reopen(&f).await.is_err());
        }
        drop(conn);
        drop(proxy);
        let repaired = RecoveryJournal::reattach(&f.path, &f.url, target.namespace)
            .await
            .unwrap();
        assert!(!repaired.halted && !repaired.projection_pending);
        assert_eq!(
            (repaired.checkpoint, repaired.attempts),
            (target.checkpoint, 1)
        );
        let j = f.open().await;
        assert_eq!(
            j.latest_eoa_attempt("a", replay()).await.unwrap(),
            Some(attempt())
        );
        drop(j);
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn schema_three_missing_budget_table_is_rejected_without_recreating_empty_budget() {
    let f = Fixture::fresh();
    f.initialize().await;
    let conn = connection(&f.path, false).unwrap();
    conn.execute_batch("DROP TABLE solana_recovery").unwrap();
    drop(conn);
    assert!(reopen(&f).await.is_err());
    assert!(RecoveryJournal::status(&f.path).is_err());
    let conn = connection(&f.path, true).unwrap();
    assert_eq!(
        conn.query_row::<i64, _, _>(
            "SELECT count(*) FROM sqlite_schema WHERE name='solana_recovery'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    drop(conn);
    f.cleanup(&[]).await;
}
