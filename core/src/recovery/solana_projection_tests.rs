use super::*;
use crate::recovery::tests::{Fixture, admit};
use serde_json::json;
#[path = "../../../test-support/redis_reply_loss.rs"]
mod redis_reply_loss;

fn payload(id: &str) -> Value {
    json!({"transactionId":id,"intent":"original"})
}
fn attempt() -> Value {
    let signature = solana_sdk::signature::Signature::from([7u8; 64]).to_string();
    json!({"chainId":"devnet","signature":signature,"attempt":{
        "signature":signature,"signed_transaction":"retained-wire",
        "broadcast_attempts":0,"last_broadcast_at":0,"reconciliation_checks":0,
    }})
}
fn replay() -> String {
    format!("solana:devnet:{}", attempt()["signature"].as_str().unwrap())
}
async fn bound(j: &Arc<RecoveryJournal>, id: &str) {
    admit(j, "solana", id, payload(id)).await;
    j.before_broadcast("solana", id, &replay(), attempt())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn restoration_preserves_exact_wire_and_durable_budget_and_time_across_reopen() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    bound(&j, "s").await;
    let first = j
        .prepare_solana_restore("s", &payload("s"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.payload, attempt());
    assert_eq!(first.replay_key, replay());
    let prepared = RecoveryJournal::status(&f.path).unwrap();
    assert_eq!(prepared.solana_recovery_intents, 1);
    assert_eq!(first.recovery_broadcasts, 0);
    j.prepare_solana_restore("s", &payload("s")).await.unwrap();
    assert_eq!(
        RecoveryJournal::status(&f.path).unwrap().checkpoint,
        prepared.checkpoint
    );
    assert!(matches!(
        j.before_broadcast("solana", "s", &replay(), attempt())
            .await,
        Err(RecoveryError::RecoveryRequired(_))
    ));
    let SolanaRecoveryBroadcast::Granted { broadcasts, at_ms } = j
        .before_solana_recovery_broadcast("s", &payload("s"), &attempt())
        .await
        .unwrap()
    else {
        panic!("first durable recovery claim must grant")
    };
    assert_eq!(broadcasts, 1);
    let after_claim = RecoveryJournal::status(&f.path).unwrap();
    assert_eq!(after_claim.solana_recovery_broadcasts, 1);
    // Deterministically exercise same-time and backward-clock claims through the
    // exact production SQL claim function, without sleeping in the test suite.
    j.db(move |conn| {
        let stored = retained(conn, "s", &payload("s"))?.unwrap();
        for now in [at_ms, at_ms.saturating_sub(10_000), at_ms + 1_999] {
            assert!(matches!(
                claim(conn, "s", &stored.public, now)?,
                SolanaRecoveryBroadcast::Wait { .. }
            ));
        }
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        RecoveryJournal::status(&f.path).unwrap().checkpoint,
        after_claim.checkpoint
    );
    drop(j);
    let j = f.open().await;
    let restored = j
        .prepare_solana_restore("s", &payload("s"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            restored.recovery_broadcasts,
            restored.last_recovery_broadcast_at
        ),
        (1, at_ms)
    );
    assert_eq!(restored.payload, attempt());
    let mut changed = attempt();
    changed["attempt"]["signed_transaction"] = json!("changed-wire");
    assert!(matches!(
        j.before_solana_recovery_broadcast("s", &payload("s"), &changed)
            .await,
        Err(RecoveryError::Conflict)
    ));
    assert_eq!(
        RecoveryJournal::status(&f.path)
            .unwrap()
            .solana_recovery_broadcasts,
        1
    );
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn twenty_claims_exhaust_cumulative_budget_without_blocking_terminal_reconciliation() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    bound(&j, "s").await;
    j.prepare_solana_restore("s", &payload("s")).await.unwrap();
    // The transaction helper's clock is supplied directly to cover all twenty
    // persisted transitions without thirty-eight seconds of timer sleeps.
    let old = j
        .db(|conn| {
            let tx = conn.transaction()?;
            for index in 1..=MAX_SOLANA_RECOVERY_BROADCASTS {
                let stored = retained(&tx, "s", &payload("s"))?.unwrap();
                assert_eq!(
                    claim(&tx, "s", &stored.public, 10_000 + u64::from(index) * 2_000)?,
                    SolanaRecoveryBroadcast::Granted {
                        broadcasts: index,
                        at_ms: 10_000 + u64::from(index) * 2_000
                    }
                );
            }
            let stored = retained(&tx, "s", &payload("s"))?.unwrap();
            assert_eq!(
                claim(&tx, "s", &stored.public, 1_000_000)?,
                SolanaRecoveryBroadcast::Exhausted
            );
            let old = advance(&tx)?;
            commit_transaction(tx)?;
            Ok(old)
        })
        .await
        .unwrap();
    j.mirror(old).await.unwrap();
    let before = RecoveryJournal::status(&f.path).unwrap();
    for _ in 0..3 {
        let restored = j
            .prepare_solana_restore("s", &payload("s"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.recovery_broadcasts, MAX_SOLANA_RECOVERY_BROADCASTS);
        assert_eq!(
            j.before_solana_recovery_broadcast("s", &payload("s"), &attempt())
                .await
                .unwrap(),
            SolanaRecoveryBroadcast::Exhausted
        );
    }
    assert_eq!(
        RecoveryJournal::status(&f.path).unwrap().checkpoint,
        before.checkpoint
    );
    j.record_terminal("solana", "s", json!({"chainId":"devnet","signature":attempt()["signature"],"slot":9,"commitment":"finalized","outcome":"success"})).await.unwrap();
    let terminal = j
        .prepare_solana_restore("s", &payload("s"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.state, AdmissionState::Terminal);
    assert_eq!(terminal.recovery_broadcasts, MAX_SOLANA_RECOVERY_BROADCASTS);
    assert!(
        j.before_solana_recovery_broadcast("s", &payload("s"), &attempt())
            .await
            .is_err()
    );
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn restoration_rejects_changed_intent_ambiguous_history_digest_or_binding_corruption() {
    for fault in [
        "intent",
        "ambiguous",
        "digest",
        "binding",
        "missing",
        "quarantined",
        "missing_wire",
    ] {
        let f = Fixture::fresh();
        f.initialize().await;
        let j = f.open().await;
        bound(&j, "s").await;
        let fault_owned = fault.to_owned();
        j.db(move |conn| {
            match fault_owned.as_str() {
                "ambiguous" => { conn.execute("INSERT INTO attempts(id,replay_key,digest,payload) SELECT id,replay_key,'another-digest',payload FROM attempts", [])?; }
                "digest" => { conn.execute("UPDATE attempts SET digest='wrong'", [])?; }
                "binding" => { conn.execute("UPDATE attempts SET replay_key='solana:devnet:wrong'", [])?; }
                "missing" => { conn.execute("DELETE FROM attempts", [])?; }
                "quarantined" => { conn.execute("UPDATE admissions SET state='quarantined'", [])?; }
                "missing_wire" => {
                    let mut record = attempt(); record["attempt"]["signed_transaction"] = Value::Null;
                    let encoded = canonical(&record)?; let digest = format!("{:x}", Sha256::digest(encoded.as_bytes()));
                    conn.execute("UPDATE attempts SET digest=?,payload=?", params![digest, encoded])?;
                }
                _ => {}
            }
            Ok(())
        }).await.unwrap();
        let mut requested = payload("s");
        if fault == "intent" {
            requested["intent"] = json!("changed");
        }
        assert!(
            j.prepare_solana_restore("s", &requested).await.is_err(),
            "{fault}"
        );
        assert_eq!(
            RecoveryJournal::status(&f.path)
                .unwrap()
                .solana_recovery_intents,
            0
        );
        drop(j);
        f.cleanup(&[]).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn namespace_recovery_retains_budget_and_mode_while_quarantining_unknown_effects() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    bound(&j, "s").await;
    j.prepare_solana_restore("s", &payload("s")).await.unwrap();
    let SolanaRecoveryBroadcast::Granted { at_ms, .. } = j
        .before_solana_recovery_broadcast("s", &payload("s"), &attempt())
        .await
        .unwrap()
    else {
        panic!("claim required")
    };
    drop(j);
    let namespace = format!("solana_recovered_{}", uuid::Uuid::new_v4().simple());
    let status = RecoveryJournal::recover(&f.path, &f.url, Some(namespace.clone()))
        .await
        .unwrap();
    assert_eq!(
        (
            status.quarantined,
            status.solana_recovery_intents,
            status.solana_recovery_broadcasts
        ),
        (1, 1, 1)
    );
    let export = f.path.with_file_name("retained-budget.json");
    RecoveryJournal::export(&f.path, &export).unwrap();
    let exported: Value = serde_json::from_slice(&fs::read(export).unwrap()).unwrap();
    assert_eq!(exported["solana_recovery"][0]["last_broadcast_at"], at_ms);
    let j = RecoveryJournal::open(&f.path, &f.url, Some(namespace.clone()))
        .await
        .unwrap();
    assert!(j.prepare_solana_restore("s", &payload("s")).await.is_err());
    assert!(
        j.before_solana_recovery_broadcast("s", &payload("s"), &attempt())
            .await
            .is_err()
    );
    drop(j);
    f.cleanup(&[&namespace]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn lost_recovery_claim_publication_reply_burns_credit_and_preserves_timestamp() {
    let f = Fixture::fresh();
    f.initialize().await;
    let proxy = redis_reply_loss::ReplyLossProxy::start(&f.url).await;
    let namespace = RecoveryJournal::status(&f.path).unwrap().namespace;
    let j = RecoveryJournal::open(&f.path, &proxy.url, namespace)
        .await
        .unwrap();
    bound(&j, "s").await;
    j.prepare_solana_restore("s", &payload("s")).await.unwrap();
    proxy.armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        j.before_solana_recovery_broadcast("s", &payload("s"), &attempt())
            .await,
        Err(RecoveryError::PublicationPending)
    ));
    assert!(proxy.dropped.load(Ordering::SeqCst));
    let status = RecoveryJournal::status(&f.path).unwrap();
    assert!(status.projection_pending);
    assert_eq!(status.solana_recovery_broadcasts, 1);
    let conn = connection(&f.path, true).unwrap();
    let recorded = retained(&conn, "s", &payload("s")).unwrap().unwrap().public;
    assert!(recorded.last_recovery_broadcast_at > 0);
    drop(conn);
    drop(j);
    drop(proxy);
    let j = f.open().await;
    let restored = j
        .prepare_solana_restore("s", &payload("s"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            restored.recovery_broadcasts,
            restored.last_recovery_broadcast_at
        ),
        (1, recorded.last_recovery_broadcast_at)
    );
    assert_eq!(restored.payload, attempt());
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn canceled_recovery_claim_keeps_ownership_through_ack_and_shutdown() {
    let f = Fixture::fresh();
    f.initialize().await;
    let j = f.open().await;
    bound(&j, "s").await;
    j.prepare_solana_restore("s", &payload("s")).await.unwrap();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (resume, resuming) = tokio::sync::oneshot::channel();
    *j.before_ack.lock().unwrap() = Some((entered, resuming));
    let caller_j = j.clone();
    let caller = tokio::spawn(async move {
        caller_j
            .before_solana_recovery_broadcast("s", &payload("s"), &attempt())
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entering)
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let mut closing = Box::pin(j.close_and_drain());
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(closing.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), closing)
        .await
        .unwrap();
    let status = RecoveryJournal::status(&f.path).unwrap();
    assert!(!status.projection_pending);
    assert_eq!(status.solana_recovery_broadcasts, 1);
    drop(j);
    let j = f.open().await;
    assert_eq!(
        j.prepare_solana_restore("s", &payload("s"))
            .await
            .unwrap()
            .unwrap()
            .recovery_broadcasts,
        1
    );
    drop(j);
    f.cleanup(&[]).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; subprocess SIGKILL"]
async fn recovery_claim_process_crashes_burn_one_credit_without_recreating_permission() {
    for cut in [
        "after_authority_commit",
        "after_redis_apply",
        "before_ack",
        "after_ack",
    ] {
        let f = Fixture::fresh();
        f.initialize().await;
        let j = f.open().await;
        bound(&j, "solana-recovery-cut").await;
        j.prepare_solana_restore("solana-recovery-cut", &payload("solana-recovery-cut"))
            .await
            .unwrap();
        fs::write(
            f.path.with_file_name("solana-job.json"),
            serde_json::to_vec(&payload("solana-recovery-cut")).unwrap(),
        )
        .unwrap();
        fs::write(
            f.path.with_file_name("solana-attempt.json"),
            serde_json::to_vec(&attempt()).unwrap(),
        )
        .unwrap();
        drop(j);
        crate::recovery::projection_tests::kill_action_at(&f, cut, "solana_recovery").await;
        assert_eq!(
            RecoveryJournal::status(&f.path)
                .unwrap()
                .solana_recovery_broadcasts,
            1
        );
        let j = f.open().await;
        let restored = j
            .prepare_solana_restore("solana-recovery-cut", &payload("solana-recovery-cut"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.recovery_broadcasts, 1);
        assert!(restored.last_recovery_broadcast_at > 0);
        assert_eq!(restored.payload, attempt());
        drop(j);
        f.cleanup(&[]).await;
    }
}
