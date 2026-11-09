//! Exercise the real permanent-failure store and worker cleanup against Redis.
use super::*;
use engine_core::error::{EngineError, SerialisableAwsSdkError, SerialisableAwsSignerError};
use engine_core::recovery::AdmissionState;

fn failure(message: &str) -> EoaExecutorWorkerError {
    EoaExecutorWorkerError::TransactionBuildFailed {
        message: message.into(),
    }
}

async fn webhook_count(fixture: &Fixture) -> usize {
    fixture
        .worker
        .store
        .redis
        .clone()
        .llen(fixture.worker.webhook_queue.pending_list_name())
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; isolated journal/signing subprocess"]
async fn durable_failure_resumes_pending_projection_without_signing_or_revival() {
    if !child(
        "preparation_failure_tests::durable_failure_resumes_pending_projection_without_signing_or_revival",
    ) {
        return;
    }
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(2, 1).await;
        let pending = fixture
            .worker
            .store
            .peek_pending_transactions(2)
            .await
            .unwrap();
        assert_eq!(pending.len(), 2);
        let original = failure("permanent preflight rejection");
        fixture
            .worker
            .store
            .fail_pending_transactions_batch(
                vec![(&pending[0], original.clone())],
                fixture.worker.webhook_queue.clone(),
            )
            .await
            .unwrap();
        // Stop after the second FULL journal commit, before publishing its
        // failed Redis status/webhook or removing its still-pending queue entry.
        let payload = serde_json::to_value(&pending[1].user_request).unwrap();
        fixture
            .journal
            .record_eoa_preparation_failure(
                &pending[1].transaction_id,
                &payload,
                serde_json::to_value(&original).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fixture
                .worker
                .store
                .get_pending_transactions_count()
                .await
                .unwrap(),
            1
        );
        assert_eq!(webhook_count(&fixture).await, 1);
        assert_eq!(fixture.worker.process_new_transactions(1).await.unwrap(), 0);
        assert_eq!(
            fixture
                .worker
                .store
                .get_pending_transactions_count()
                .await
                .unwrap(),
            0
        );
        assert_eq!(webhook_count(&fixture).await, 2);
        assert_eq!(fixture.worker.process_new_transactions(1).await.unwrap(), 0);
        assert_eq!(
            webhook_count(&fixture).await,
            2,
            "cleanup retry cannot duplicate failure notification"
        );
        let mut conn = fixture.worker.store.redis.clone();
        for row in &pending {
            let key = fixture
                .worker
                .store
                .transaction_data_key_name(&row.transaction_id);
            let status: String = conn.hget(&key, "status").await.unwrap();
            let reason: String = conn.hget(&key, "failure_reason").await.unwrap();
            assert_eq!(status, "failed");
            assert_eq!(
                reason,
                original.to_string(),
                "publish the original error, not the cleanup sentinel"
            );
            let payload = serde_json::to_value(&row.user_request).unwrap();
            let reservation = fixture
                .journal
                .reserve_admission(
                    "eoa",
                    &row.transaction_id,
                    &recovery::admission_fingerprint("eoa", &payload).unwrap(),
                    payload,
                )
                .await
                .unwrap();
            assert!(reservation.terminal);
            // Expired/lost business status cannot revive either admission.
            let _: () = conn.del(key).await.unwrap();
        }
        assert!(fixture.journal.rebuild_page(None).await.unwrap().is_empty());
        assert_eq!(
            RecoveryJournal::status(fixture.directory.join("journal.sqlite"))
                .unwrap()
                .attempts,
            0
        );
        assert!(
            fixture
                .worker
                .store
                .peek_borrowed_transactions()
                .await
                .unwrap()
                .is_empty()
        );
        let next: u64 = conn
            .get(fixture.worker.store.optimistic_transaction_count_key_name())
            .await
            .unwrap();
        assert_eq!(next, 0);
        let observed = fixture.state.lock().unwrap();
        assert!(observed.wires.is_empty());
        assert!(
            observed.rpc_methods.is_empty(),
            "terminal cleanup cannot contact the RPC"
        );
        drop(observed);
        fixture.cleanup().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; isolated journal/signing subprocess"]
async fn authorized_attempt_prevents_false_failure_status_webhook_and_expiry() {
    if !child(
        "preparation_failure_tests::authorized_attempt_prevents_false_failure_status_webhook_and_expiry",
    ) {
        return;
    }
    let fixture = Fixture::new(1, 1).await;
    let pending = fixture
        .worker
        .store
        .peek_pending_transactions(1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let row = &fixture.prepared[0];
    crate::recovery::before_eoa(&row.user_request, &row.signed_transaction)
        .await
        .unwrap();
    assert!(
        fixture
            .worker
            .store
            .fail_pending_transactions_batch(
                vec![(&pending, failure("stale preparation error"))],
                fixture.worker.webhook_queue.clone()
            )
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .worker
            .store
            .get_pending_transactions_count()
            .await
            .unwrap(),
        1
    );
    assert_eq!(webhook_count(&fixture).await, 0);
    let record = fixture
        .journal
        .admission("eoa", &pending.transaction_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, AdmissionState::Admitted);
    assert!(record.replay_key.is_some());
    assert_eq!(
        RecoveryJournal::status(fixture.directory.join("journal.sqlite"))
            .unwrap()
            .attempts,
        1
    );
    let mut conn = fixture.worker.store.redis.clone();
    let key = fixture
        .worker
        .store
        .transaction_data_key_name(&pending.transaction_id);
    let reason: Option<String> = conn.hget(&key, "failure_reason").await.unwrap();
    let ttl: i64 = conn.ttl(&key).await.unwrap();
    assert!(reason.is_none());
    assert_eq!(ttl, -1, "uncertain attempt data must remain unexpired");
    assert!(
        fixture
            .journal
            .eoa_preparation_failure(
                &pending.transaction_id,
                &serde_json::to_value(&pending.user_request).unwrap()
            )
            .await
            .unwrap()
            .is_none()
    );
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; isolated journal/signing subprocess"]
async fn stale_eoa_owner_cannot_commit_a_new_durable_preparation_failure() {
    if !child(
        "preparation_failure_tests::stale_eoa_owner_cannot_commit_a_new_durable_preparation_failure",
    ) {
        return;
    }
    let fixture = Fixture::new(1, 1).await;
    let pending = fixture
        .worker
        .store
        .peek_pending_transactions(1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut conn = fixture.worker.store.redis.clone();
    let _: () = conn
        .set(fixture.worker.store.eoa_lock_key_name(), "new-owner")
        .await
        .unwrap();
    assert!(
        fixture
            .worker
            .store
            .fail_pending_transactions_batch(
                vec![(&pending, failure("stale preparation error"))],
                fixture.worker.webhook_queue.clone()
            )
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .journal
            .admission_state("eoa", &pending.transaction_id)
            .await
            .unwrap(),
        Some(AdmissionState::Admitted)
    );
    assert!(
        fixture
            .journal
            .terminal_evidence("eoa", &pending.transaction_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture
            .worker
            .store
            .get_pending_transactions_count()
            .await
            .unwrap(),
        1
    );
    assert_eq!(webhook_count(&fixture).await, 0);
    fixture.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; isolated journal/signing subprocess"]
async fn uncertain_kms_failure_keeps_unsigned_work_for_successful_retry() {
    if !child(
        "preparation_failure_tests::uncertain_kms_failure_keeps_unsigned_work_for_successful_retry",
    ) {
        return;
    }
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(1, 1).await;
        let pending = fixture
            .worker
            .store
            .peek_pending_transactions(1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let error = EoaExecutorWorkerError::SigningError {
            message: "KMS signing response lost".into(),
            inner_error: EngineError::AwsKmsSignerError {
                error: SerialisableAwsSignerError::Sign {
                    aws_sdk_error: SerialisableAwsSdkError::Other {
                        message: "SDK erased the transient cause".into(),
                    },
                },
            },
        };
        assert!(
            fixture
                .worker
                .clean_prepration_results(vec![(&pending, Err(error.clone()))], true)
                .await
                .unwrap()
                .is_empty()
        );
        // A caller bypassing worker classification still cannot make this
        // ambiguous infrastructure failure an irreversible failure decision.
        assert!(
            fixture
                .worker
                .store
                .fail_pending_transactions_batch(
                    vec![(&pending, error)],
                    fixture.worker.webhook_queue.clone()
                )
                .await
                .is_err()
        );
        assert_eq!(
            fixture
                .worker
                .store
                .get_pending_transactions_count()
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            fixture
                .journal
                .admission_state("eoa", &pending.transaction_id)
                .await
                .unwrap(),
            Some(AdmissionState::Admitted)
        );
        assert!(
            fixture
                .journal
                .terminal_evidence("eoa", &pending.transaction_id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(webhook_count(&fixture).await, 0);
        assert_eq!(
            RecoveryJournal::status(fixture.directory.join("journal.sqlite"))
                .unwrap()
                .attempts,
            0
        );
        let mut conn = fixture.worker.store.redis.clone();
        let key = fixture
            .worker
            .store
            .transaction_data_key_name(&pending.transaction_id);
        let reason: Option<String> = conn.hget(&key, "failure_reason").await.unwrap();
        let ttl: i64 = conn.ttl(&key).await.unwrap();
        assert!(reason.is_none());
        assert_eq!(ttl, -1);
        // Exercise the ordinary worker again with a healthy signer: the same
        // original request can now reserve nonce zero and dispatch exactly once.
        fixture.release_all();
        assert_eq!(fixture.worker.process_new_transactions(1).await.unwrap(), 1);
        assert_eq!(
            fixture
                .worker
                .store
                .get_submitted_transactions_count()
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            RecoveryJournal::status(fixture.directory.join("journal.sqlite"))
                .unwrap()
                .attempts,
            1
        );
        assert_eq!(fixture.state.lock().unwrap().wires.len(), 1);
        assert_eq!(fixture.state.lock().unwrap().wires[0].0, 0);
        fixture.cleanup().await;
    })
    .await
    .unwrap();
}
