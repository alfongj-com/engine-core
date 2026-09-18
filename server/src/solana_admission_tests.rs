use super::*;
use engine_core::{
    credentials::SigningCredential, execution_options::solana::SolanaTransactionOptions,
};
use engine_executors::solana_executor::worker::SolanaExecutorError;
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use twmq::{
    FailHookData, SuccessHookData,
    hooks::TransactionContext,
    job::{BorrowedJob, JobResult, JobStatus},
    queue::QueueOptions,
    redis::AsyncCommands,
};

struct EffectHandler {
    effects: Arc<AtomicUsize>,
    storage: Arc<SolanaTransactionStorage>,
    registry: Arc<TransactionRegistry>,
}
impl DurableExecution for EffectHandler {
    type Output = ();
    type ErrorData = SolanaExecutorError;
    type JobData = SolanaExecutorJobData;
    async fn process(&self, job: &BorrowedJob<Self::JobData>) -> JobResult<(), Self::ErrorData> {
        if job.id() == "definitive-failure" {
            return Err(twmq::job::JobError::Fail(
                SolanaExecutorError::TransactionFailed {
                    reason: "test definitive failure".into(),
                },
            ));
        }
        self.effects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _: SuccessHookData<'_, ()>,
        tx: &mut TransactionContext<'_>,
    ) {
        self.storage.add_terminal_admission_command(
            tx.pipeline(),
            job.id(),
            &solana_admission_fingerprint(job.data()).unwrap(),
            "completed",
            false,
        );
        self.registry.add_remove_command(tx.pipeline(), job.id());
    }
    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _: FailHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        self.registry.add_remove_command(tx.pipeline(), job.id());
    }
}

struct Fixture {
    client: twmq::redis::Client,
    redis: ConnectionManager,
    queue: Arc<Queue<EffectHandler>>,
    registry: Arc<TransactionRegistry>,
    storage: Arc<SolanaTransactionStorage>,
    namespace: String,
    data: SolanaExecutorJobData,
    effects: Arc<AtomicUsize>,
}
impl Fixture {
    async fn new() -> Self {
        let client = twmq::redis::Client::open(
            std::env::var("TEST_REDIS_URL").expect("select disposable Redis"),
        )
        .unwrap();
        let redis = client.get_connection_manager().await.unwrap();
        let namespace = format!("solana-admission-{}", rand::random::<u64>());
        let storage = Arc::new(
            SolanaTransactionStorage::new(redis.clone(), Some(namespace.clone()))
                .with_completed_transaction_ttl_seconds(60),
        );
        let registry = Arc::new(TransactionRegistry::new(
            redis.clone(),
            Some(namespace.clone()),
        ));
        let effects = Arc::new(AtomicUsize::new(0));
        let queue = Queue::builder()
            .name(format!("{namespace}_queue"))
            .redis_client(client.clone())
            .options(QueueOptions {
                max_success: 1,
                max_failed: 1,
                local_concurrency: 2,
                polling_interval: std::time::Duration::from_millis(5),
                ..Default::default()
            })
            .handler(EffectHandler {
                effects: effects.clone(),
                storage: storage.clone(),
                registry: registry.clone(),
            })
            .build()
            .await
            .unwrap()
            .arc();
        let payer = Pubkey::new_unique();
        let transaction: SolanaTransactionOptions = serde_json::from_value(json!({"instructions":[],"executionOptions":{"signerAddress":payer.to_string(),"chainId":"solana:local"}})).unwrap();
        let data = SolanaExecutorJobData {
            transaction_id: "first".into(),
            transaction,
            signing_credential: SigningCredential::SolanaEnvironment { public_key: payer },
            webhook_options: vec![],
        };
        Self {
            client,
            redis,
            queue,
            registry,
            storage,
            namespace,
            data,
            effects,
        }
    }
    async fn admit(&self, data: &SolanaExecutorJobData) -> Result<(), EngineError> {
        admit(
            &self.redis,
            &self.queue,
            &self.registry,
            &self.storage,
            data,
        )
        .await
    }
    async fn cleanup(&self) {
        for pattern in [
            format!("{}:*", self.namespace),
            format!("twmq:{}*", self.namespace),
        ] {
            let keys: Vec<String> = self.redis.clone().keys(pattern).await.unwrap();
            if !keys.is_empty() {
                let _: () = self.redis.clone().del(keys).await.unwrap();
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn concurrent_admission_and_actual_queue_pruning_preserve_completed_identity() {
    let f = Fixture::new().await;
    let admitted = futures::future::join_all((0..32).map(|_| f.admit(&f.data))).await;
    assert!(admitted.into_iter().all(|result| result.is_ok()));
    assert_eq!(f.queue.count(JobStatus::Pending).await.unwrap(), 1);
    assert_eq!(
        f.redis
            .clone()
            .ttl::<_, i64>(f.storage.admission_key("first"))
            .await
            .unwrap(),
        -1
    );
    let mut changed = f.data.clone();
    changed.transaction.execution_options.compute_unit_limit = Some(42);
    assert!(f.admit(&changed).await.is_err());
    let worker = f.queue.work();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if f.redis
                .clone()
                .hget::<_, _, Option<String>>(f.storage.admission_key("first"), "state")
                .await
                .unwrap()
                .as_deref()
                == Some("completed")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let mut second = f.data.clone();
    second.transaction_id = "second".into();
    f.admit(&second).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while f.queue.get_job("first").await.unwrap().is_some() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !f.redis
            .clone()
            .sismember::<_, _, bool>(f.queue.dedupe_set_name(), "first")
            .await
            .unwrap(),
        "real TWMQ pruning removed ordinary queue dedupe"
    );
    f.admit(&f.data).await.unwrap();
    assert!(f.admit(&changed).await.is_err());
    assert_eq!(f.queue.count(JobStatus::Pending).await.unwrap(), 0);
    assert_eq!(
        f.effects.load(Ordering::SeqCst),
        2,
        "replay produced a second business effect"
    );
    let ttl: i64 = f
        .redis
        .clone()
        .ttl(f.storage.admission_key("first"))
        .await
        .unwrap();
    assert!((1..=60).contains(&ttl));
    assert!(
        f.registry
            .get_transaction_queue("first")
            .await
            .unwrap()
            .is_none()
    );
    worker.shutdown().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancelled_pruned_and_orphaned_attempts_require_reconciliation() {
    let f = Fixture::new().await;
    f.admit(&f.data).await.unwrap();
    let _: () = f
        .redis
        .clone()
        .set(f.storage.attempt_key("first"), "retained signed evidence")
        .await
        .unwrap();
    f.queue.cancel_job("first").await.unwrap();
    let mut second = f.data.clone();
    second.transaction_id = "definitive-failure".into();
    f.admit(&second).await.unwrap();
    let worker = f.queue.work();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while f.queue.get_job("first").await.unwrap().is_some() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual failure completion must prune older cancellation history");
    worker.shutdown().await.unwrap();
    assert!(f.admit(&f.data).await.is_err());
    let mut changed = f.data.clone();
    changed.transaction.execution_options.compute_unit_limit = Some(1);
    assert!(f.admit(&changed).await.is_err());
    assert_eq!(
        f.redis
            .clone()
            .ttl::<_, i64>(f.storage.admission_key("first"))
            .await
            .unwrap(),
        -1
    );
    assert!(f.storage.has_attempt("first").await.unwrap());
    let _: () = f
        .redis
        .clone()
        .del(f.storage.admission_key("first"))
        .await
        .unwrap();
    assert!(
        f.admit(&f.data).await.is_err(),
        "legacy attempt cannot attach to a new request"
    );
    assert_eq!(f.queue.count(JobStatus::Pending).await.unwrap(), 0);
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn migration_and_key_type_failures_leave_no_partial_admission() {
    let f = Fixture::new().await;
    let _: () = f
        .redis
        .clone()
        .rpush(f.queue.pending_list_name(), "orphan")
        .await
        .unwrap();
    assert!(f.admit(&f.data).await.is_err());
    let schema = format!("twmq:{}:solana_admission_schema", f.queue.name());
    assert!(!f.redis.clone().exists::<_, bool>(&schema).await.unwrap());
    assert!(
        !f.redis
            .clone()
            .exists::<_, bool>(f.storage.admission_key("first"))
            .await
            .unwrap()
    );
    let _: () = f
        .redis
        .clone()
        .del(f.queue.pending_list_name())
        .await
        .unwrap();
    let _: () = f
        .redis
        .clone()
        .set(f.queue.dedupe_set_name(), "wrong type")
        .await
        .unwrap();
    assert!(f.admit(&f.data).await.is_err());
    assert!(
        f.registry
            .get_transaction_queue("first")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !f.redis
            .clone()
            .exists::<_, bool>(f.queue.job_data_hash_name())
            .await
            .unwrap()
    );
    let _: () = f
        .redis
        .clone()
        .del(f.queue.dedupe_set_name())
        .await
        .unwrap();
    let _: () = f
        .redis
        .clone()
        .set(&schema, "future-version")
        .await
        .unwrap();
    assert!(f.admit(&f.data).await.is_err());
    let _: () = f.redis.clone().del(&schema).await.unwrap();
    f.admit(&f.data).await.unwrap();
    assert_eq!(
        f.redis.clone().get::<_, String>(&schema).await.unwrap(),
        "1"
    );
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn aborted_terminal_commit_wrong_identity_and_existing_attempt_cannot_expire_protection() {
    let f = Fixture::new().await;
    f.admit(&f.data).await.unwrap();
    let admission = f.storage.admission_key("first");
    let fingerprint = solana_admission_fingerprint(&f.data).unwrap();
    let _: () = f
        .redis
        .clone()
        .set(f.storage.attempt_key("first"), "evidence")
        .await
        .unwrap();
    let mut connection = f.client.get_multiplexed_async_connection().await.unwrap();
    let _: () = twmq::redis::cmd("WATCH")
        .arg(&admission)
        .query_async(&mut connection)
        .await
        .unwrap();
    let _: () = f
        .redis
        .clone()
        .hset(&admission, "owner_changed", "yes")
        .await
        .unwrap();
    let mut tx = twmq::redis::pipe();
    tx.atomic();
    f.storage
        .add_terminal_admission_command(&mut tx, "first", &fingerprint, "completed", false);
    f.storage.add_delete_attempt_command(&mut tx, "first");
    let committed: Option<Vec<twmq::redis::Value>> = tx.query_async(&mut connection).await.unwrap();
    assert!(committed.is_none());
    assert!(f.storage.has_attempt("first").await.unwrap());
    assert_eq!(f.redis.clone().ttl::<_, i64>(&admission).await.unwrap(), -1);
    for (identity, require_no_attempt) in
        [("wrong-fingerprint", false), (fingerprint.as_str(), true)]
    {
        let mut tx = twmq::redis::pipe();
        tx.atomic();
        f.storage.add_terminal_admission_command(
            &mut tx,
            "first",
            identity,
            "failed",
            require_no_attempt,
        );
        let _: () = tx.query_async(&mut f.redis.clone()).await.unwrap();
        assert_eq!(
            f.redis
                .clone()
                .hget::<_, _, String>(&admission, "state")
                .await
                .unwrap(),
            "active"
        );
        assert_eq!(f.redis.clone().ttl::<_, i64>(&admission).await.unwrap(), -1);
    }
    let _: () = f
        .redis
        .clone()
        .del(f.storage.attempt_key("first"))
        .await
        .unwrap();
    let mut tx = twmq::redis::pipe();
    tx.atomic();
    f.storage
        .add_terminal_admission_command(&mut tx, "first", &fingerprint, "failed", true);
    let _: () = tx.query_async(&mut f.redis.clone()).await.unwrap();
    assert_eq!(
        f.redis
            .clone()
            .hget::<_, _, String>(&admission, "state")
            .await
            .unwrap(),
        "failed"
    );
    assert!((1..=60).contains(&f.redis.clone().ttl::<_, i64>(&admission).await.unwrap()));
    f.cleanup().await;
}
