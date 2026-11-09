//! KMS rejection policy through durable failure, crash recovery and later work.
//! The actual AWS SDK metadata conversion is covered by aws_kms_protocol.rs.
use super::*;
use engine_core::{
    error::{EngineError, SerialisableAwsSdkError, SerialisableAwsSignerError},
    recovery::AdmissionState,
};

fn rejection(public_key: bool) -> EoaExecutorWorkerError {
    let aws_sdk_error = SerialisableAwsSdkError::ServiceError {
        message: "Fixture service error".into(),
        code: Some(if public_key {
            "NotFoundException".into()
        } else {
            "AccessDeniedException".into()
        }),
        http_status: Some(if public_key { 400 } else { 403 }),
    };
    EoaExecutorWorkerError::SigningError {
        message: if public_key {
            "KMS key does not exist".into()
        } else {
            "KMS signing not authorized".into()
        },
        inner_error: EngineError::AwsKmsSignerError {
            error: if public_key {
                SerialisableAwsSignerError::GetPublicKey { aws_sdk_error }
            } else {
                SerialisableAwsSignerError::Sign { aws_sdk_error }
            },
        },
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; isolated journal/signing subprocess"]
async fn rejected_kms_heads_publish_original_errors_and_allow_later_valid_work() {
    if !child(
        "kms_service_tests::rejected_kms_heads_publish_original_errors_and_allow_later_valid_work",
    ) {
        return;
    }
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(3, 1).await;
        let mut conn = fixture.worker.store.redis.clone();
        // Queue the two bad requests before intent-00. The healthy request
        // must still use nonce zero, which the independent RPC peer checks.
        for (offset, id) in ["intent-02", "intent-01", "intent-00"].iter().enumerate() {
            let _: () = conn
                .zadd(
                    fixture.worker.store.pending_transactions_zset_name(),
                    id,
                    EoaExecutorStore::now() + offset as u64,
                )
                .await
                .unwrap();
        }
        let pending = fixture
            .worker
            .store
            .peek_pending_transactions(3)
            .await
            .unwrap();
        assert_eq!(
            pending
                .iter()
                .map(|row| row.transaction_id.as_str())
                .collect::<Vec<_>>(),
            ["intent-02", "intent-01", "intent-00"]
        );
        let denied = rejection(false);
        assert!(
            fixture
                .worker
                .clean_prepration_results(vec![(&pending[0], Err(denied.clone()))], true)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            fixture
                .worker
                .store
                .get_pending_transactions_count()
                .await
                .unwrap(),
            2
        );
        let count: usize = conn
            .llen(fixture.worker.webhook_queue.pending_list_name())
            .await
            .unwrap();
        assert_eq!(count, 1, "the live rejection must publish once");

        // Cut the second rejection after its FULL journal commit and before
        // Redis status/removal/webhook publication, then resume the real worker.
        let missing = rejection(true);
        let retained = serde_json::to_value(&missing).unwrap();
        assert_eq!(retained["inner_error"]["type"], "AWS_KMS_SIGNER_ERROR");
        assert_eq!(retained["inner_error"]["error"]["type"], "GET_PUBLIC_KEY");
        assert_eq!(
            fixture
                .journal
                .record_eoa_preparation_failure(
                    &pending[1].transaction_id,
                    &serde_json::to_value(&pending[1].user_request).unwrap(),
                    retained.clone(),
                )
                .await
                .unwrap(),
            Some(retained)
        );
        assert_eq!(
            fixture
                .worker
                .store
                .get_pending_transactions_count()
                .await
                .unwrap(),
            2
        );
        fixture.release_all();
        assert_eq!(fixture.worker.process_new_transactions(2).await.unwrap(), 1);
        assert_eq!(
            fixture
                .worker
                .store
                .get_pending_transactions_count()
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            fixture
                .worker
                .store
                .get_submitted_transactions_count()
                .await
                .unwrap(),
            1
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

        for (row, original) in [(&pending[0], denied), (&pending[1], missing)] {
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
                "recovery must retain the original rejection"
            );
            let admission = fixture
                .journal
                .admission("eoa", &row.transaction_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(admission.state, AdmissionState::Terminal);
            assert!(
                admission.replay_key.is_none(),
                "rejected work cannot consume a nonce"
            );
            let retained = fixture
                .journal
                .eoa_preparation_failure(
                    &row.transaction_id,
                    &serde_json::to_value(&row.user_request).unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
            let restored: EoaExecutorWorkerError = serde_json::from_value(retained).unwrap();
            assert_eq!(restored.to_string(), original.to_string());
        }
        assert_eq!(
            RecoveryJournal::status(fixture.directory.join("journal.sqlite"))
                .unwrap()
                .attempts,
            1
        );
        let next: u64 = conn
            .get(fixture.worker.store.optimistic_transaction_count_key_name())
            .await
            .unwrap();
        assert_eq!(next, 1);
        assert_eq!(fixture.state.lock().unwrap().wires.len(), 1);
        assert_eq!(fixture.state.lock().unwrap().wires[0].0, 0);
        let count: usize = conn
            .llen(fixture.worker.webhook_queue.pending_list_name())
            .await
            .unwrap();
        assert_eq!(count, 3, "two original failures and one successful send");
        assert_eq!(fixture.worker.process_new_transactions(2).await.unwrap(), 0);
        let count: usize = conn
            .llen(fixture.worker.webhook_queue.pending_list_name())
            .await
            .unwrap();
        assert_eq!(count, 3, "retry must not duplicate failure notifications");
        fixture.cleanup().await;
    })
    .await
    .unwrap();
}
