//! A refused per-intent failure must not starve independent work on the signer.
use super::*;
use crate::eoa::store::{EoaHealth, PendingFailureReport};
use crate::webhook::WebhookJobPayload;
use engine_core::recovery::AdmissionState;

fn failure() -> EoaExecutorWorkerError {
    EoaExecutorWorkerError::TransactionBuildFailed {
        message: "permanent preparation error".into(),
    }
}

async fn failure_notifications(fixture: &Fixture) -> Vec<String> {
    let jobs: Vec<String> = fixture
        .worker
        .store
        .redis
        .clone()
        .hvals(fixture.worker.webhook_queue.job_data_hash_name())
        .await
        .unwrap();
    jobs.into_iter()
        .filter_map(|job| {
            let job: WebhookJobPayload = serde_json::from_str(&job).unwrap();
            let envelope: Value = serde_json::from_str(&job.body).unwrap();
            (envelope["eventType"] == "FAILURE")
                .then(|| envelope["transactionId"].as_str().unwrap().to_owned())
        })
        .collect()
}

async fn assert_unfailed(fixture: &Fixture, id: &str) {
    let mut conn = fixture.worker.store.redis.clone();
    let key = fixture.worker.store.transaction_data_key_name(id);
    assert!(
        conn.hget::<_, _, Option<String>>(&key, "failure_reason")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(conn.ttl::<_, i64>(&key).await.unwrap(), -1);
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; isolated journal/signing subprocess"]
async fn mixed_refusals_publish_only_eligible_failures_and_keep_global_halts_fatal() {
    if !child(
        "failure_progress_tests::mixed_refusals_publish_only_eligible_failures_and_keep_global_halts_fatal",
    ) {
        return;
    }
    let fixture = Fixture::new(3, 1).await;
    let pending = fixture
        .worker
        .store
        .peek_pending_transactions(3)
        .await
        .unwrap();
    let attempted = pending
        .iter()
        .find(|p| p.transaction_id == "intent-00")
        .unwrap();
    let mut changed = pending
        .iter()
        .find(|p| p.transaction_id == "intent-01")
        .cloned()
        .unwrap();
    changed.user_request.value = U256::from(999);
    let eligible = pending
        .iter()
        .find(|p| p.transaction_id == "intent-02")
        .unwrap();
    crate::recovery::before_eoa(
        &fixture.prepared[0].user_request,
        &fixture.prepared[0].signed_transaction,
    )
    .await
    .unwrap();
    let mut unknown = (*attempted).clone();
    unknown.transaction_id = "unknown".into();
    unknown.user_request.transaction_id = unknown.transaction_id.clone();
    fixture
        .worker
        .store
        .add_transaction(unknown.user_request.clone())
        .await
        .unwrap();
    let mut cancelled = unknown.clone();
    cancelled.transaction_id = "cancelled-foreign".into();
    cancelled.user_request.transaction_id = cancelled.transaction_id.clone();
    let payload = serde_json::to_value(&cancelled.user_request).unwrap();
    fixture
        .journal
        .reserve_admission(
            "erc4337",
            &cancelled.transaction_id,
            &recovery::admission_fingerprint("erc4337", &payload).unwrap(),
            payload,
        )
        .await
        .unwrap();
    fixture
        .journal
        .cancel_unbroadcast(&cancelled.transaction_id)
        .await
        .unwrap();
    // Foreign-kind work in an EOA projection needs operator reconciliation;
    // it cannot become an EOA failure or prevent the genuine unsigned cleanup.
    fixture
        .worker
        .store
        .add_transaction(cancelled.user_request.clone())
        .await
        .unwrap();
    let report = fixture
        .worker
        .store
        .fail_pending_transactions_batch(
            vec![
                (&unknown, failure()),
                (&cancelled, failure()),
                (&changed, failure()),
                (attempted, failure()),
                (eligible, failure()),
            ],
            fixture.worker.webhook_queue.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        report,
        PendingFailureReport {
            published: 1,
            terminal_cleanup: 0,
            retained: 4
        }
    );
    assert_eq!(
        fixture
            .worker
            .store
            .get_pending_transactions_count()
            .await
            .unwrap(),
        4
    );
    for id in [
        &unknown.transaction_id,
        &cancelled.transaction_id,
        &changed.transaction_id,
        &attempted.transaction_id,
    ] {
        assert_unfailed(&fixture, id).await;
    }
    assert_eq!(
        failure_notifications(&fixture).await,
        [eligible.transaction_id.clone()]
    );
    assert!(
        fixture
            .journal
            .admission("eoa", &unknown.transaction_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture
            .journal
            .admission_state("eoa", &changed.transaction_id)
            .await
            .unwrap(),
        Some(AdmissionState::Admitted)
    );
    assert_eq!(
        fixture
            .journal
            .admission_state("erc4337", &cancelled.transaction_id)
            .await
            .unwrap(),
        Some(AdmissionState::Quarantined)
    );
    assert_eq!(
        fixture
            .journal
            .admission_state("eoa", &eligible.transaction_id)
            .await
            .unwrap(),
        Some(AdmissionState::Terminal)
    );
    fixture.journal.ensure_healthy().await.unwrap();
    fixture
        .journal
        .halt("global authority failure fixture")
        .await;
    assert!(
        fixture
            .worker
            .store
            .fail_pending_transactions_batch(
                vec![(&unknown, failure())],
                fixture.worker.webhook_queue.clone()
            )
            .await
            .is_err(),
        "an all-refused batch cannot hide a deployment halt"
    );
    assert!(
        fixture
            .worker
            .clean_prepration_results(vec![(&unknown, Err(failure()))], true)
            .await
            .is_err(),
        "preparation cleanup must propagate global authority errors"
    );
    assert_eq!(
        failure_notifications(&fixture).await,
        [eligible.transaction_id.clone()]
    );
    fixture.cleanup().await;
}

fn receipt(hash: &str) -> Value {
    json!({"transactionHash":hash,"transactionIndex":"0x0","blockNumber":"0xa",
        "blockHash":alloy::primitives::B256::repeat_byte(9),"from":Address::ZERO,"to":Address::ZERO,
        "cumulativeGasUsed":"0x5208","gasUsed":"0x5208","effectiveGasPrice":"0x1",
        "contractAddress":null,"logs":[],"logsBloom":format!("0x{}","00".repeat(256)),"status":"0x1","type":"0x0"})
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; isolated journal/signing subprocess"]
async fn stale_refused_attempt_does_not_starve_unsigned_cull_rebroadcast_or_confirmation() {
    if !child(
        "failure_progress_tests::stale_refused_attempt_does_not_starve_unsigned_cull_rebroadcast_or_confirmation",
    ) {
        return;
    }
    tokio::time::timeout(LIMIT, async {
        let mut fixture = Fixture::new(3, 1).await;
        fixture.worker.max_inflight = 1;
        let mut conn = fixture.worker.store.redis.clone();
        // Nonce zero is already submitted; nonce one was authorized but its
        // response was lost. A stale projection also lists nonce one's ID as
        // pending, ahead of an actually unsigned stale request.
        fixture
            .worker
            .store
            .atomic_move_pending_to_borrowed_with_incremented_nonces(&[
                fixture.prepared[0].data.clone(),
                fixture.prepared[1].data.clone(),
            ])
            .await
            .unwrap();
        for row in &fixture.prepared[..2] {
            crate::recovery::before_eoa(&row.user_request, &row.signed_transaction)
                .await
                .unwrap();
        }
        fixture
            .worker
            .store
            .process_borrowed_transactions(
                vec![SubmissionResult {
                    transaction: fixture.prepared[0].clone().into(),
                    result: SubmissionResultType::Reconcile,
                }],
                fixture.worker.webhook_queue.clone(),
            )
            .await
            .unwrap();
        let _: () = conn
            .zadd(
                fixture.worker.store.pending_transactions_zset_name(),
                "intent-01",
                1,
            )
            .await
            .unwrap();
        let _: () = conn
            .zadd(
                fixture.worker.store.pending_transactions_zset_name(),
                "intent-02",
                1,
            )
            .await
            .unwrap();
        let now = EoaExecutorStore::now();
        fixture
            .worker
            .store
            .update_health_data(&EoaHealth {
                balance: U256::from(1_000_000),
                balance_threshold: U256::ZERO,
                balance_fetched_at: now,
                last_confirmation_at: now,
                last_nonce_movement_at: now,
                nonce_resets: vec![],
                last_finality_poll_at: 0,
                finality_scan_offset: 0,
            })
            .await
            .unwrap();
        {
            let mut state = fixture.state.lock().unwrap();
            state.latest_nonce = 1;
            state.receipts.insert(
                fixture.prepared[0].data.hash.clone(),
                receipt(&fixture.prepared[0].data.hash),
            );
        }
        fixture.release_all();
        let result = fixture.worker.execute_main_workflow().await.unwrap();
        assert_eq!(
            result.recovered_transactions, 1,
            "retained nonce-one wire must be rebroadcast"
        );
        assert_eq!(
            result.confirmed_transactions, 1,
            "independent submitted nonce zero must finalize"
        );
        assert_eq!(
            result.failed_transactions, 0,
            "no receipt-based failure was observed"
        );
        assert_eq!(
            result.pending_transactions, 1,
            "the refused pending projection stays available for reconciliation"
        );
        assert_eq!(result.submitted_transactions, 1);
        assert_eq!(result.borrowed_transactions, 0);
        let seen = fixture.state.lock().unwrap();
        assert_eq!(
            seen.wires,
            [(
                1,
                format!(
                    "0x{}",
                    hex::encode(
                        TxEnvelope::from(fixture.prepared[1].signed_transaction.clone())
                            .encoded_2718()
                    )
                )
            )]
        );
        drop(seen);
        assert_unfailed(&fixture, "intent-01").await;
        assert_eq!(failure_notifications(&fixture).await, ["intent-02"]);
        let status: String = conn
            .hget(
                fixture.worker.store.transaction_data_key_name("intent-02"),
                "status",
            )
            .await
            .unwrap();
        assert_eq!(status, "failed");
        assert!(
            conn.ttl::<_, i64>(fixture.worker.store.transaction_data_key_name("intent-02"))
                .await
                .unwrap()
                > 0
        );
        assert_eq!(
            fixture
                .journal
                .admission_state("eoa", "intent-00")
                .await
                .unwrap(),
            Some(AdmissionState::Terminal)
        );
        assert_eq!(
            fixture
                .journal
                .admission_state("eoa", "intent-01")
                .await
                .unwrap(),
            Some(AdmissionState::Admitted)
        );
        assert_eq!(
            fixture
                .journal
                .admission_state("eoa", "intent-02")
                .await
                .unwrap(),
            Some(AdmissionState::Terminal)
        );
        assert_eq!(
            RecoveryJournal::status(fixture.directory.join("journal.sqlite"))
                .unwrap()
                .attempts,
            2
        );
        fixture.cleanup().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; isolated journal/signing subprocess"]
async fn exact_terminal_pending_cleanup_preserves_outcome_and_allows_next_send() {
    if !child(
        "failure_progress_tests::exact_terminal_pending_cleanup_preserves_outcome_and_allows_next_send",
    ) {
        return;
    }
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(2, 1).await;
        let row = &fixture.prepared[0];
        crate::recovery::before_eoa(&row.user_request, &row.signed_transaction).await.unwrap();
        // Trusted terminal fixture: nonce zero is settled, but a selective
        // scheduler rollback kept its pending member. Keep its business outcome.
        fixture.journal.record_terminal("eoa", "intent-00", json!({
            "chainId":31337,"transactionHash":row.data.hash,"outcome":"success",
            "finality":{"blockNumber":10,"blockHash":alloy::primitives::B256::repeat_byte(9),"checkpointNumber":10,"checkpointHash":alloy::primitives::B256::repeat_byte(9),"policy":{"mode":"finalized"}}
        })).await.unwrap();
        let mut conn = fixture.worker.store.redis.clone();
        let key = fixture.worker.store.transaction_data_key_name("intent-00");
        let _: () = conn.hset(&key, "status", "confirmed").await.unwrap();
        let _: () = conn.hset(&key, "receipt", receipt(&row.data.hash).to_string()).await.unwrap();
        let _: () = conn.expire(&key, 600).await.unwrap();
        let before: std::collections::HashMap<String,String> = conn.hgetall(&key).await.unwrap();
        let before_ttl: i64 = conn.ttl(&key).await.unwrap();
        // A fresh stale-projection entry has not reached the cull age. Normal
        // preparation must still recognize terminal authority and unblock it.
        let _: () = conn.zadd(fixture.worker.store.pending_transactions_zset_name(), "intent-00", EoaExecutorStore::now() - 1).await.unwrap();
        let _: () = conn.set(fixture.worker.store.optimistic_transaction_count_key_name(), 1).await.unwrap();
        let _: () = conn.set(fixture.worker.store.last_transaction_count_key_name(), 1).await.unwrap();
        let now = EoaExecutorStore::now();
        fixture.worker.store.update_health_data(&EoaHealth {
            balance:U256::from(1_000_000), balance_threshold:U256::ZERO,
            balance_fetched_at:now, last_confirmation_at:now, last_nonce_movement_at:now,
            nonce_resets:vec![], last_finality_poll_at:0, finality_scan_offset:0,
        }).await.unwrap();
        fixture.state.lock().unwrap().latest_nonce = 1;
        fixture.release_all();
        let result = fixture.worker.execute_main_workflow().await.unwrap();
        assert_eq!(result.sent_transactions, 1);
        assert_eq!(result.pending_transactions, 0);
        assert_eq!(result.submitted_transactions, 1);
        assert_eq!(result.failed_transactions, 0);
        assert_eq!(fixture.state.lock().unwrap().wires.len(), 1);
        assert_eq!(fixture.state.lock().unwrap().wires[0].0, 1);
        let after: std::collections::HashMap<String,String> = conn.hgetall(&key).await.unwrap();
        assert_eq!(after, before, "terminal scheduling cleanup cannot overwrite settled outcome data");
        let after_ttl: i64 = conn.ttl(&key).await.unwrap();
        assert!(after_ttl > 0 && after_ttl <= before_ttl, "preserve existing retention rather than starting failure TTL");
        assert!(failure_notifications(&fixture).await.is_empty());
        assert_eq!(fixture.journal.admission_state("eoa", "intent-00").await.unwrap(), Some(AdmissionState::Terminal));
        assert_eq!(RecoveryJournal::status(fixture.directory.join("journal.sqlite")).unwrap().attempts, 2);
        fixture.cleanup().await;
    }).await.unwrap();
}
