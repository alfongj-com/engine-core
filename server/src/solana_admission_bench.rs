//! Local admission regression screen, not transaction execution throughput.
//! Requires an explicitly selected disposable loopback Redis. No worker is started.
use super::admit;
use engine_core::{
    credentials::SigningCredential,
    execution_options::solana::{
        CommitmentLevel, SolanaChainId, SolanaExecutionOptions, SolanaTransactionOptions,
    },
};
use engine_executors::{
    solana_executor::{
        storage::{SolanaTransactionStorage, solana_admission_fingerprint},
        worker::{SolanaExecutorError, SolanaExecutorJobData},
    },
    transaction_registry::TransactionRegistry,
};
use engine_solana_core::transaction::{
    InstructionDataEncoding, SolanaAccountMeta, SolanaInstructionData, SolanaTransactionInput,
};
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashSet,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use twmq::{
    DurableExecution, Queue,
    job::{BorrowedJob, JobResult},
    redis::{self, AsyncCommands, aio::ConnectionManager},
};

struct NoWorker;
impl DurableExecution for NoWorker {
    type JobData = SolanaExecutorJobData;
    type Output = ();
    type ErrorData = SolanaExecutorError;
    async fn process(&self, _: &BorrowedJob<Self::JobData>) -> JobResult<(), Self::ErrorData> {
        panic!("Admission benchmark must never execute a transaction")
    }
}
struct Fixture {
    redis: ConnectionManager,
    queue: Queue<NoWorker>,
    registry: TransactionRegistry,
    storage: SolanaTransactionStorage,
    namespace: String,
    template: SolanaExecutorJobData,
}
impl Fixture {
    async fn new(case: usize) -> Self {
        let url = std::env::var("TEST_REDIS_URL").expect("select disposable loopback Redis");
        let parsed = reqwest::Url::parse(&url).unwrap();
        assert!(
            matches!(parsed.host_str(), Some("127.0.0.1" | "[::1]" | "::1")),
            "benchmark requires literal loopback Redis"
        );
        let client = redis::Client::open(url).unwrap();
        let redis = client.get_connection_manager().await.unwrap();
        let namespace = format!(
            "solana-admission-bench:{}:{}:{case}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let queue = Queue::builder()
            .redis_connection_manager(redis.clone(), client)
            .name(format!("{namespace}:solana_executor"))
            .handler(NoWorker)
            .build()
            .await
            .unwrap();
        let signer = Pubkey::new_from_array([1; 32]);
        let template = SolanaExecutorJobData {
            transaction_id: String::new(),
            transaction: SolanaTransactionOptions {
                input: SolanaTransactionInput::new_with_instructions(vec![SolanaInstructionData {
                    program_id: Pubkey::default(),
                    accounts: vec![
                        SolanaAccountMeta {
                            pubkey: signer,
                            is_signer: true,
                            is_writable: true,
                        },
                        SolanaAccountMeta {
                            pubkey: Pubkey::new_from_array([2; 32]),
                            is_signer: false,
                            is_writable: true,
                        },
                    ],
                    // System transfer discriminator (2) and one lamport, little endian.
                    data: "020000000100000000000000".into(),
                    encoding: InstructionDataEncoding::Hex,
                }]),
                execution_options: SolanaExecutionOptions {
                    signer_address: signer,
                    chain_id: SolanaChainId::SolanaLocal,
                    max_blockhash_retries: 0,
                    commitment: CommitmentLevel::Finalized,
                    priority_fee: None,
                    compute_unit_limit: None,
                },
            },
            signing_credential: SigningCredential::SolanaEnvironment { public_key: signer },
            webhook_options: vec![],
        };
        let fixture = Self {
            registry: TransactionRegistry::new(redis.clone(), Some(namespace.clone())),
            storage: SolanaTransactionStorage::new(redis.clone(), Some(namespace.clone())),
            redis,
            queue,
            namespace,
            template,
        };
        // Exercise initialization once, then remove the warmup job while keeping
        // the schema marker. Every measured case starts after healthy initialization.
        let warm = fixture.data("warmup".into());
        fixture.add(&warm).await;
        redis::pipe()
            .atomic()
            .lrem(fixture.queue.pending_list_name(), 0, "warmup")
            .ignore()
            .hdel(fixture.queue.job_data_hash_name(), "warmup")
            .ignore()
            .del(fixture.queue.job_meta_hash_name("warmup"))
            .ignore()
            .srem(fixture.queue.dedupe_set_name(), "warmup")
            .ignore()
            .hdel(fixture.registry.registry_key(), "warmup")
            .ignore()
            .del(fixture.storage.admission_key("warmup"))
            .ignore()
            .query_async::<()>(&mut fixture.redis.clone())
            .await
            .unwrap();
        fixture
    }
    fn data(&self, id: String) -> SolanaExecutorJobData {
        let mut data = self.template.clone();
        data.transaction_id = id;
        data
    }
    async fn add(&self, data: &SolanaExecutorJobData) {
        admit(
            &self.redis,
            &self.queue,
            &self.registry,
            &self.storage,
            data,
        )
        .await
        .unwrap();
    }
    async fn seed_backlog(&self, count: usize) {
        for start in (0..count).step_by(500) {
            let mut pipe = redis::pipe();
            for i in start..(start + 500).min(count) {
                let data = self.data(format!("seed-{i:08}"));
                let id = &data.transaction_id;
                pipe.hset(
                    self.queue.job_data_hash_name(),
                    id,
                    serde_json::to_string(&data).unwrap(),
                )
                .ignore()
                .hset_multiple(
                    self.queue.job_meta_hash_name(id),
                    &[("created_at", 1_u64), ("attempts", 0)],
                )
                .ignore()
                .sadd(self.queue.dedupe_set_name(), id)
                .ignore()
                .rpush(self.queue.pending_list_name(), id)
                .ignore()
                .hset(self.registry.registry_key(), id, "solana_executor")
                .ignore()
                .hset_multiple(
                    self.storage.admission_key(id),
                    &[
                        ("fingerprint", solana_admission_fingerprint(&data).unwrap()),
                        ("state", "active".into()),
                    ],
                )
                .ignore();
            }
            pipe.query_async::<()>(&mut self.redis.clone())
                .await
                .unwrap();
        }
    }
    async fn cleanup(&self) {
        let mut cursor = 0_u64;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("*{}*", self.namespace))
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut self.redis.clone())
                .await
                .unwrap();
            if !keys.is_empty() {
                redis::cmd("UNLINK")
                    .arg(keys)
                    .query_async::<u64>(&mut self.redis.clone())
                    .await
                    .unwrap();
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
    }
}
fn quantile(samples: &[f64], percentile: f64) -> f64 {
    samples[((samples.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(samples.len() - 1)]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "finite performance screen; requires disposable TEST_REDIS_URL and ADMISSION_BENCH_OUTPUT"]
async fn initialized_admission_backlog_regression() {
    let output = std::env::var("ADMISSION_BENCH_OUTPUT").expect("select report filename");
    // Reserve the artifact before doing expensive setup; never replace evidence.
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)
        .unwrap();
    let mut cases = Vec::<Value>::new();
    for (case, (initial_backlog, count, concurrency)) in [
        (0_usize, 1000_usize, 1_usize),
        (100_000, 1000, 1),
        (0, 10_000, 32),
        (100_000, 10_000, 32),
    ]
    .into_iter()
    .enumerate()
    {
        let f = Fixture::new(case).await;
        f.seed_backlog(initial_backlog).await;
        let initial: usize = f
            .redis
            .clone()
            .llen(f.queue.pending_list_name())
            .await
            .unwrap();
        assert_eq!(initial, initial_backlog);
        let payload_bytes = serde_json::to_vec(&f.data("measure-00000000".into()))
            .unwrap()
            .len();
        let start = Instant::now();
        let mut latency_ms: Vec<f64> = stream::iter(0..count)
            .map(|i| {
                let fixture = &f;
                async move {
                    let data = fixture.data(format!("measure-{i:08}"));
                    let start = Instant::now();
                    fixture.add(&data).await;
                    start.elapsed().as_secs_f64() * 1000.0
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;
        let elapsed_seconds = start.elapsed().as_secs_f64();
        latency_ms.sort_by(f64::total_cmp);
        // Verify actual unique durable jobs and all new admission identities,
        // after timing. Duplicate retries must leave pending length unchanged.
        let pending: Vec<String> = f
            .redis
            .clone()
            .lrange(f.queue.pending_list_name(), 0, -1)
            .await
            .unwrap();
        let unique: HashSet<&String> = pending.iter().collect();
        assert_eq!(pending.len(), initial_backlog + count);
        assert_eq!(unique.len(), pending.len());
        for start in (0..count).step_by(500) {
            let mut pipe = redis::pipe();
            for i in start..(start + 500).min(count) {
                let data = f.data(format!("measure-{i:08}"));
                pipe.hget(f.storage.admission_key(&data.transaction_id), "fingerprint");
            }
            let fingerprints: Vec<String> = pipe.query_async(&mut f.redis.clone()).await.unwrap();
            for (offset, fingerprint) in fingerprints.into_iter().enumerate() {
                assert_eq!(
                    fingerprint,
                    solana_admission_fingerprint(&f.data(format!("measure-{:08}", start + offset)))
                        .unwrap()
                );
            }
        }
        for i in 0..100 {
            f.add(&f.data(format!("measure-{i:08}"))).await;
        }
        let after_duplicate: usize = f
            .redis
            .clone()
            .llen(f.queue.pending_list_name())
            .await
            .unwrap();
        assert_eq!(after_duplicate, pending.len());
        let record = json!({"initialPending":initial_backlog,"newAdmissions":count,"concurrency":concurrency,"payloadBytes":payload_bytes,"elapsedSeconds":elapsed_seconds,"admissionsPerSecond":count as f64/elapsed_seconds,"latencyMs":{"p50":quantile(&latency_ms,0.5),"p95":quantile(&latency_ms,0.95),"p99":quantile(&latency_ms,0.99),"max":latency_ms.last()},"durablePending":pending.len(),"uniquePending":unique.len(),"matchingAdmissionFingerprints":count,"duplicateRetries":100,"pendingAfterDuplicateRetries":after_duplicate});
        println!("ADMISSION_BENCH {record}");
        cases.push(record);
        f.cleanup().await;
    }
    let report = json!({"version":1,"scope":"Actual initialized Solana admit() path plus canonical fingerprint/serialization and one Redis roundtrip; no worker, signing, RPC or transaction execution.","profile":if cfg!(debug_assertions){"debug"}else{"release"},"setup":"Fresh isolated namespaces; valid pending jobs seeded in batches after healthy schema initialization. Setup, assertions, duplicate retries and cleanup excluded from timings.","limitations":["Single run per case; timings are an O(backlog) regression screen, not a sustainable capacity guarantee.","Synthetic one-lamport transfer intent, no webhooks.","Loopback Redis; persistence settings and machine load must be recorded by the operator."],"cases":cases});
    use std::io::Write;
    file.write_all(serde_json::to_string_pretty(&report).unwrap().as_bytes())
        .unwrap();
    file.write_all(b"\n").unwrap();
    file.sync_all().unwrap();
}
