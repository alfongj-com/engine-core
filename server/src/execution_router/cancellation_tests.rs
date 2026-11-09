use super::*;
use engine_core::recovery::{RecoveryJournal, admission_fingerprint};
use engine_executors::external_bundler::send::ExternalBundlerSendError;
use serde_json::json;
use twmq::{
    DurableExecution,
    job::{BorrowedJob, JobResult, JobStatus},
    redis::AsyncCommands,
};

struct NoDispatch;
impl DurableExecution for NoDispatch {
    type Output = ();
    type ErrorData = ExternalBundlerSendError;
    type JobData = ExternalBundlerSendJobData;
    async fn process(&self, _: &BorrowedJob<Self::JobData>) -> JobResult<(), Self::ErrorData> {
        panic!("the enqueue regression must not dispatch work")
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancellation_during_intake_cleans_only_revoked_work_before_acceptance() {
    let url = std::env::var("TEST_REDIS_URL").expect("select disposable Redis");
    for cut in [
        "before-push",
        "after-push",
        "absent-job",
        "healthy",
        "continuity-loss",
    ] {
        let namespace = format!("cancel_intake_{}", rand::random::<u64>());
        let directory = std::env::temp_dir().join(&namespace);
        let path = directory.join("journal.sqlite");
        let client = twmq::redis::Client::open(url.clone()).unwrap();
        let mut redis = client.get_connection_manager().await.unwrap();
        RecoveryJournal::initialize(&path, &url, Some(namespace.clone()))
            .await
            .unwrap();
        let journal = RecoveryJournal::open(&path, &url, Some(namespace.clone()))
            .await
            .unwrap();
        let registry = TransactionRegistry::new(redis.clone(), Some(namespace.clone()));
        let queue = Queue::builder()
            .name(format!("{namespace}_send"))
            .redis_client(client)
            .handler(NoDispatch)
            .build()
            .await
            .unwrap()
            .arc();
        let data = ExternalBundlerSendJobData {
            transaction_id: "order".into(),
            chain_id: 31337,
            transactions: vec![],
            execution_options: serde_json::from_value(
                json!({"signerAddress":"0x1111111111111111111111111111111111111111"}),
            )
            .unwrap(),
            signing_credential: SigningCredential::Environment {
                address: alloy::primitives::Address::repeat_byte(1),
            },
            rpc_credentials: RpcCredentials::Configured,
            webhook_options: vec![],
            pregenerated_nonce: Some(U256::from(1)),
        };
        let payload = serde_json::to_value(&data).unwrap();
        journal
            .reserve_admission(
                "erc4337",
                "order",
                &admission_fingerprint("erc4337", &payload).unwrap(),
                payload,
            )
            .await
            .unwrap();
        registry
            .set_transaction_queue("order", "external_bundler_send")
            .await
            .unwrap();
        if matches!(cut, "before-push" | "absent-job" | "continuity-loss") {
            journal.cancel_unbroadcast("order").await.unwrap();
        }
        if cut != "absent-job" {
            queue
                .clone()
                .job(data.clone())
                .with_id("order")
                .push()
                .await
                .unwrap();
        }
        if cut == "after-push" {
            journal.cancel_unbroadcast("order").await.unwrap();
        }
        if cut == "continuity-loss" {
            let keys: Vec<String> = redis.keys(format!("{namespace}:*")).await.unwrap();
            for key in keys {
                if key != registry.registry_key() {
                    let _: () = redis.del(key).await.unwrap();
                }
            }
        }
        let result = reconcile_external_bundler_enqueue(&queue, &registry, &journal, &data).await;
        if matches!(cut, "before-push" | "after-push" | "absent-job") {
            assert!(
                matches!(result, Err(EngineError::ValidationError { .. })),
                "{cut}"
            );
            assert!(
                registry
                    .get_transaction_queue("order")
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(queue.count(JobStatus::Pending).await.unwrap(), 0);
            assert_eq!(
                queue.count(JobStatus::Failed).await.unwrap(),
                usize::from(cut != "absent-job")
            );
            assert!(
                journal
                    .admission("erc4337", "order")
                    .await
                    .unwrap()
                    .unwrap()
                    .replay_key
                    .is_none()
            );
            assert_eq!(RecoveryJournal::status(&path).unwrap().attempts, 0);
        } else {
            assert_eq!(result.is_ok(), cut == "healthy", "{cut}");
            assert!(
                registry
                    .get_transaction_queue("order")
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(queue.count(JobStatus::Pending).await.unwrap(), 1);
            assert_eq!(queue.count(JobStatus::Failed).await.unwrap(), 0);
        }
        for pattern in [format!("{namespace}:*"), format!("twmq:{namespace}_*")] {
            let keys: Vec<String> = redis.keys(pattern).await.unwrap();
            if !keys.is_empty() {
                let _: () = redis.del(keys).await.unwrap();
            }
        }
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
