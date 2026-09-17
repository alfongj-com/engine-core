//! Fault tests use real Redis and a loopback HTTP JSON-RPC boundary. They never
//! contact a chain, spend fees, or read the operator's signing key.
use super::*;
use crate::webhook::{WebhookDestinationPolicy, WebhookRetryConfig};
use engine_core::execution_options::solana::{
    CommitmentLevel as ExecutionCommitment, SolanaChainId, SolanaExecutionOptions,
};
use serde_json::Value;
use solana_sdk::{
    hash::Hash,
    message::{VersionedMessage, v0},
    signature::{Keypair, Signer},
    transaction::VersionedTransaction,
};
use std::{collections::VecDeque, sync::Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use twmq::{
    job::Job,
    redis::{self, AsyncCommands},
};

struct Fixture {
    handler: SolanaExecutorJobHandler,
    redis: redis::aio::ConnectionManager,
    namespace: String,
    data: SolanaExecutorJobData,
    attempt: SolanaTransactionAttempt,
}

impl Fixture {
    async fn new() -> Self {
        let client =
            redis::Client::open(std::env::var("TEST_REDIS_URL").expect("select disposable Redis"))
                .unwrap();
        let redis = client.get_connection_manager().await.unwrap();
        let namespace = format!("solana-recovery:{}", uuid::Uuid::new_v4());
        let key = Keypair::new();
        let hash = Hash::new_unique();
        let message =
            VersionedMessage::V0(v0::Message::try_compile(&key.pubkey(), &[], &[], hash).unwrap());
        let signed = VersionedTransaction::try_new(message, &[&key]).unwrap();
        let wire = base64::engine::general_purpose::STANDARD
            .encode(encode_transaction_wire(&signed).unwrap());
        let attempt = SolanaTransactionAttempt::new(signed.signatures[0], hash, Some(100), 1, wire);
        let webhooks = Arc::new(
            Queue::builder()
                .redis_connection_manager(redis.clone(), client)
                .name(format!("{namespace}:webhooks"))
                .handler(
                    WebhookJobHandler::new(
                        WebhookDestinationPolicy::default(),
                        Arc::new(WebhookRetryConfig::default()),
                    )
                    .unwrap(),
                )
                .build()
                .await
                .unwrap(),
        );
        let handler = SolanaExecutorJobHandler {
            solana_signer: Arc::new(SolanaSigner::new(
                thirdweb_core::iaw::IAWClient::new("http://127.0.0.1:1").unwrap(),
            )),
            rpc_cache: Arc::new(SolanaRpcCache::new(
                crate::solana_executor::rpc_cache::SolanaRpcUrls {
                    devnet: "http://127.0.0.1:1".into(),
                    mainnet: "http://127.0.0.1:1".into(),
                    local: "http://127.0.0.1:1".into(),
                },
            )),
            storage: Arc::new(SolanaTransactionStorage::new(
                redis.clone(),
                Some(namespace.clone()),
            )),
            webhook_queue: webhooks,
            transaction_registry: Arc::new(TransactionRegistry::new(
                redis.clone(),
                Some(namespace.clone()),
            )),
        };
        let data = SolanaExecutorJobData {
            transaction_id: "intent".into(),
            transaction: SolanaTransactionOptions {
                input: SolanaTransactionInput::new_with_instructions(vec![]),
                execution_options: SolanaExecutionOptions {
                    signer_address: key.pubkey(),
                    chain_id: SolanaChainId::SolanaLocal,
                    max_blockhash_retries: 0,
                    commitment: ExecutionCommitment::Finalized,
                    priority_fee: None,
                    compute_unit_limit: None,
                },
            },
            // Deliberately unusable for Solana. Recovery must never invoke signing.
            signing_credential: SigningCredential::Environment {
                address: alloy::primitives::Address::ZERO,
            },
            webhook_options: vec![],
        };
        Self {
            handler,
            redis,
            namespace,
            data,
            attempt,
        }
    }
    async fn seed(&self) -> TransactionLock {
        let lock = self
            .handler
            .storage
            .try_acquire_lock(&self.data.transaction_id)
            .await
            .unwrap();
        assert!(
            self.handler
                .storage
                .store_attempt_if_not_exists(&self.data.transaction_id, &self.attempt, &lock)
                .await
                .unwrap()
        );
        lock
    }
    async fn stored(&self, lock: &TransactionLock) -> SolanaTransactionAttempt {
        self.handler
            .storage
            .get_attempt(&self.data.transaction_id, lock)
            .await
            .unwrap()
            .unwrap()
    }
    fn job(&self) -> BorrowedJob<SolanaExecutorJobData> {
        BorrowedJob::new(
            Job {
                id: self.data.transaction_id.clone(),
                data: self.data.clone(),
                attempts: 1,
                created_at: 0,
                processed_at: None,
                finished_at: None,
            },
            "lease".into(),
        )
    }
    async fn cleanup(&self) {
        let keys: Vec<String> = self
            .redis
            .clone()
            .keys(format!("{}:*", self.namespace))
            .await
            .unwrap();
        if !keys.is_empty() {
            let _: () = self.redis.clone().del(keys).await.unwrap();
        }
    }
}

/// None closes the socket after reading the request: acceptance without a response.
struct Reply {
    method: &'static str,
    body: Option<Value>,
}
fn reply(method: &'static str, result: Value) -> Reply {
    Reply {
        method,
        body: Some(json!({"result": result})),
    }
}
fn status(value: Value) -> Reply {
    reply(
        "getSignatureStatuses",
        json!({"context":{"slot":150}, "value":[value]}),
    )
}
fn finalized() -> Value {
    json!({"slot":90,"confirmations":null,"err":null,"status":{"Ok":null},"confirmationStatus":"finalized"})
}
fn details(attempt: &SolanaTransactionAttempt) -> Reply {
    reply(
        "getTransaction",
        json!({"slot":90,"blockTime":123,
        "transaction":{"signatures":[attempt.signature.to_string()],"message":{
            "accountKeys":[],"header":{"numRequiredSignatures":1,"numReadonlySignedAccounts":0,"numReadonlyUnsignedAccounts":0},
            "recentBlockhash":attempt.blockhash.to_string(),"instructions":[]}},
        "meta":{"err":null,"status":{"Ok":null},"fee":5000,"preBalances":[],"postBalances":[]},"version":0}),
    )
}

async fn rpc(
    replies: Vec<Reply>,
) -> (
    RpcClient,
    Arc<Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    rpc_with_timeout(replies, Duration::from_secs(2)).await
}

async fn rpc_with_timeout(
    replies: Vec<Reply>,
    timeout: Duration,
) -> (
    RpcClient,
    Arc<Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/secret-path?apiKey=RPC_SECRET_SENTINEL",
        listener.local_addr().unwrap()
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let requests = seen.clone();
    let server = tokio::spawn(async move {
        let mut replies = VecDeque::from(replies);
        while let Some(reply) = replies.pop_front() {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            let (start, len) = loop {
                let mut buffer = [0; 4096];
                let n = stream.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let len = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end + 4, len);
                }
            };
            while bytes.len() < start + len {
                let mut buffer = [0; 4096];
                let n = stream.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
            }
            let request: Value = serde_json::from_slice(&bytes[start..start + len]).unwrap();
            assert_eq!(request["method"], reply.method);
            if reply.method == "getSignatureStatuses" {
                assert_eq!(request["params"][1]["searchTransactionHistory"], true);
            }
            requests.lock().unwrap().push(request.clone());
            if let Some(mut body) = reply.body {
                if body.get("__stall").is_some() {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                body["id"] = request["id"].clone();
                body["jsonrpc"] = json!("2.0");
                let body = body.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        }
    });
    (
        RpcClient::new_sender(
            super::super::rpc_cache::BoundedRpcSender::new_with_timeout(url, timeout),
            solana_rpc_client::rpc_client::RpcClientConfig::default(),
        ),
        seen,
        server,
    )
}
async fn finish(server: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}
fn assert_nack(
    result: JobResult<SolanaExecutorResult, SolanaExecutorError>,
) -> SolanaExecutorError {
    match result {
        Err(JobError::Nack { error, .. }) => error,
        Err(JobError::Fail(e)) => panic!("unexpected permanent failure: {e}"),
        Ok(_) => panic!("unexpected success"),
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn redis_lock_command_failure_requeues_without_rpc_or_losing_attempt() {
    let f = Fixture::new().await;
    let lock = f.seed().await;
    lock.release().await.unwrap();
    let attempt_key = format!("{}:solana_tx_attempt:intent", f.namespace);
    let before: String = f.redis.clone().get(&attempt_key).await.unwrap();
    // SET ... GET rejects a non-string lock key. Inject a real Redis command
    // error without disturbing other workers or simulating a chain failure.
    let _: () = f
        .redis
        .clone()
        .rpush(
            format!("{}:solana_tx_lock:intent", f.namespace),
            "corrupt lock",
        )
        .await
        .unwrap();
    let result = f.handler.process(&f.job()).await;
    match result {
        Err(JobError::Nack {
            error: SolanaExecutorError::InternalError { .. },
            delay,
            ..
        }) => {
            assert_eq!(delay, Some(NETWORK_ERROR_RETRY_DELAY));
        }
        _ => panic!("Redis command failure must requeue"),
    }
    assert_eq!(f.handler.rpc_cache.len(), 0);
    let after: String = f.redis.clone().get(&attempt_key).await.unwrap();
    assert_eq!(after, before);
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn persisted_before_send_crash_recovers_identical_bytes_without_signing() {
    let f = Fixture::new().await;
    let old_lock = f.seed().await;
    let key = format!("{}:solana_tx_attempt:intent", f.namespace);
    let ttl: i64 = f.redis.clone().ttl(&key).await.unwrap();
    assert_eq!(ttl, -1);
    old_lock.release().await.unwrap(); // equivalent durable state to death before first send
    let lock = f.handler.storage.try_acquire_lock("intent").await.unwrap();
    let (rpc, seen, server) = rpc(vec![
        status(Value::Null),
        reply("getBlockHeight", json!(100)),
        reply("sendTransaction", json!(f.attempt.signature.to_string())),
    ])
    .await;
    assert!(matches!(
        assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, 1).await),
        SolanaExecutorError::TransactionSent { .. }
    ));
    finish(server).await;
    let seen = seen.lock().unwrap();
    assert_eq!(
        seen[2]["params"][0],
        f.attempt.signed_transaction.as_ref().unwrap().as_str()
    );
    assert_eq!(seen[2]["params"][1]["maxRetries"], 0);
    drop(seen);
    let stored = f.stored(&lock).await;
    assert_eq!(stored.signature, f.attempt.signature);
    assert_eq!(stored.broadcast_attempts, 1);
    assert_eq!(stored.signed_transaction, f.attempt.signed_transaction);
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn lost_response_and_already_processed_reconcile_one_signature() {
    let f = Fixture::new().await;
    let lock = f.seed().await;
    let (rpc, seen, server) = rpc(vec![
        Reply {method:"sendTransaction",body:None},
        status(Value::Null), reply("getBlockHeight",json!(90)),
        Reply {method:"sendTransaction",body:Some(json!({"error":{"code":-32002,"message":"AlreadyProcessed RPC_SECRET_SENTINEL","data":null}}))},
        status(finalized()), details(&f.attempt),
    ]).await;
    let error = assert_nack(
        f.handler
            .broadcast_attempt(
                &rpc,
                &f.data,
                f.attempt.clone(),
                &lock,
                CommitmentLevel::Finalized,
            )
            .await,
    );
    assert!(matches!(error, SolanaExecutorError::SendFailed { .. }));
    assert!(
        !serde_json::to_string(&error)
            .unwrap()
            .contains("RPC_SECRET_SENTINEL")
    );
    assert!(!format!("{error:?}").contains("RPC_SECRET_SENTINEL"));
    let mut stored = f.stored(&lock).await;
    stored.last_broadcast_at = 0;
    f.handler
        .storage
        .update_attempt("intent", &stored, &lock)
        .await
        .unwrap();
    let error = assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, 2).await);
    assert!(matches!(error, SolanaExecutorError::SendFailed { .. }));
    assert!(
        !serde_json::to_string(&error)
            .unwrap()
            .contains("RPC_SECRET_SENTINEL")
    );
    let result = f.handler.execute_transaction(&rpc, &f.data, &lock, 3).await;
    assert!(result.is_ok());
    assert_eq!(
        result.ok().unwrap().signature,
        f.attempt.signature.to_string()
    );
    finish(server).await;
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0]["params"][0], seen[3]["params"][0]);
    assert_eq!(
        seen.iter()
            .filter(|r| r["method"] == "sendTransaction")
            .count(),
        2
    );
    drop(seen);
    assert_eq!(f.stored(&lock).await.broadcast_attempts, 2);
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn landing_between_first_status_and_finalized_expiry_never_rebuilds() {
    let f = Fixture::new().await;
    let lock = f.seed().await;
    let (rpc, seen, server) = rpc(vec![
        status(Value::Null),
        reply("getBlockHeight", json!(101)),
        status(finalized()),
        details(&f.attempt),
    ])
    .await;
    assert!(
        f.handler
            .execute_transaction(&rpc, &f.data, &lock, 2)
            .await
            .is_ok()
    );
    finish(server).await;
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .all(|r| r["method"] != "sendTransaction" && r["method"] != "getLatestBlockhash")
    );
    assert_eq!(f.stored(&lock).await.signature, f.attempt.signature);
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn expiry_boundary_rebroadcasts_then_parks_without_re_signing() {
    let mut f = Fixture::new().await;
    f.attempt.broadcast_attempts = MAX_BROADCASTS_PER_ATTEMPT;
    let lock = f.seed().await;
    let (rpc, seen, server) = rpc(vec![
        status(Value::Null),
        reply("getBlockHeight", json!(100)),
        status(Value::Null),
        reply("getBlockHeight", json!(101)),
        status(Value::Null),
    ])
    .await;
    assert!(matches!(
        assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, 1).await),
        SolanaExecutorError::NotYetConfirmed { .. }
    ));
    assert!(matches!(
        f.handler.execute_transaction(&rpc, &f.data, &lock, 2).await,
        Err(JobError::Nack {
            error: SolanaExecutorError::RecoveryRequired { .. },
            ..
        })
    ));
    finish(server).await;
    assert_eq!(
        seen.lock().unwrap()[1]["params"][0]["commitment"],
        "finalized"
    );
    assert!(
        f.handler.storage.has_attempt("intent").await.unwrap(),
        "processing cannot delete recovery evidence before terminal commit"
    );
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn visible_unfinalized_status_and_history_errors_cannot_trigger_replacement() {
    let mut f = Fixture::new().await;
    f.data.transaction.execution_options.max_blockhash_retries = 10;
    let lock = f.seed().await;
    let mut pending = finalized();
    pending["confirmations"] = json!(0);
    pending["confirmationStatus"] = json!("processed");
    let (rpc, seen, server) = rpc(vec![
        status(pending),
        status(Value::Null),
        reply("getBlockHeight", json!(101)),
        Reply {
            method: "getSignatureStatuses",
            body: Some(
                json!({"error":{"code":-32000,"message":"history temporarily unavailable"}}),
            ),
        },
    ])
    .await;
    assert!(matches!(
        assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, 1).await),
        SolanaExecutorError::NotYetConfirmed { .. }
    ));
    assert!(matches!(
        assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, 2).await),
        SolanaExecutorError::RpcError { .. }
    ));
    finish(server).await;
    assert_eq!(seen.lock().unwrap().len(), 4);
    assert_eq!(f.stored(&lock).await.signature, f.attempt.signature);
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn serialized_attempt_checks_its_own_hash_and_parks_unknown_expiry() {
    let mut f = Fixture::new().await;
    f.data.transaction.input =
        SolanaTransactionInput::new_with_serialized(f.attempt.signed_transaction.clone().unwrap());
    f.attempt.blockhash_last_valid_height = None;
    let lock = f.seed().await;
    let (rpc, seen, server) = rpc(vec![
        status(Value::Null),
        reply(
            "isBlockhashValid",
            json!({"context":{"slot":150},"value":false}),
        ),
        status(Value::Null),
    ])
    .await;
    assert!(matches!(
        assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, 1).await),
        SolanaExecutorError::RecoveryRequired { .. }
    ));
    finish(server).await;
    assert_eq!(
        seen.lock().unwrap()[1]["params"][0],
        f.attempt.blockhash.to_string()
    );
    assert_eq!(f.stored(&lock).await.signature, f.attempt.signature);
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn reconciliation_budget_parks_without_rpc_and_explicit_resume_preserves_identity() {
    let mut f = Fixture::new().await;
    f.attempt.reconciliation_checks = MAX_RECONCILIATION_CHECKS;
    f.attempt.broadcast_attempts = MAX_BROADCASTS_PER_ATTEMPT;
    let lock = f.seed().await;
    let (rpc, seen, server) = rpc(vec![status(finalized()), details(&f.attempt)]).await;
    assert!(matches!(
        assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, 1).await),
        SolanaExecutorError::RecoveryRequired { .. }
    ));
    assert!(seen.lock().unwrap().is_empty());
    assert!(
        f.handler
            .storage
            .resume_reconciliation("intent", &lock)
            .await
            .unwrap()
    );
    let stored = f.stored(&lock).await;
    assert_eq!(stored.reconciliation_checks, 0);
    assert_eq!(stored.broadcast_attempts, MAX_BROADCASTS_PER_ATTEMPT);
    assert_eq!(stored.signed_transaction, f.attempt.signed_transaction);
    assert!(
        f.handler
            .execute_transaction(&rpc, &f.data, &lock, 2)
            .await
            .is_ok()
    );
    finish(server).await;
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn legacy_attempt_is_made_persistent_and_never_rebuilt() {
    let mut f = Fixture::new().await;
    f.attempt.signed_transaction = None;
    let lock = f.seed().await;
    let key = format!("{}:solana_tx_attempt:intent", f.namespace);
    let _: () = f.redis.clone().expire(&key, 600).await.unwrap();
    let (rpc, _, server) = rpc(vec![status(Value::Null)]).await;
    assert!(matches!(
        assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, 1).await),
        SolanaExecutorError::RecoveryRequired { .. }
    ));
    finish(server).await;
    let ttl: i64 = f.redis.clone().ttl(&key).await.unwrap();
    assert_eq!(ttl, -1);
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn terminal_cleanup_waits_for_commit_and_cancellation_retains_unknown_attempt() {
    let f = Fixture::new().await;
    let lock = f.seed().await;
    let admission = f.handler.storage.admission_key("intent");
    let fingerprint =
        crate::solana_executor::storage::solana_admission_fingerprint(&f.data).unwrap();
    let _: () = f
        .redis
        .clone()
        .hset_multiple(
            &admission,
            &[("fingerprint", fingerprint.as_str()), ("state", "active")],
        )
        .await
        .unwrap();
    let failure = SolanaExecutorError::TransactionFailed {
        reason: "finalized instruction error".into(),
    };
    let mut aborted = redis::pipe();
    aborted.atomic();
    f.handler
        .on_fail(
            &f.job(),
            FailHookData { error: &failure },
            &mut TransactionContext::new(&mut aborted, "test".into()),
        )
        .await;
    assert!(f.handler.storage.has_attempt("intent").await.unwrap());
    // Redis WATCH abort: terminal cleanup must not escape the aborted commit.
    let client = redis::Client::open(std::env::var("TEST_REDIS_URL").unwrap()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let guard = format!("{}:completion-guard", f.namespace);
    let _: () = redis::cmd("WATCH")
        .arg(&guard)
        .query_async(&mut connection)
        .await
        .unwrap();
    let _: () = f.redis.clone().set(&guard, "new-owner").await.unwrap();
    let committed: Option<Vec<redis::Value>> = aborted.query_async(&mut connection).await.unwrap();
    assert!(committed.is_none());
    assert!(f.handler.storage.has_attempt("intent").await.unwrap());
    assert_eq!(
        f.redis
            .clone()
            .hget::<_, _, String>(&admission, "state")
            .await
            .unwrap(),
        "active"
    );
    assert_eq!(f.redis.clone().ttl::<_, i64>(&admission).await.unwrap(), -1);
    let mut cancelled = redis::pipe();
    cancelled.atomic();
    f.handler
        .on_fail(
            &f.job(),
            FailHookData {
                error: &SolanaExecutorError::UserCancelled,
            },
            &mut TransactionContext::new(&mut cancelled, "test".into()),
        )
        .await;
    let _: () = cancelled.query_async(&mut f.redis.clone()).await.unwrap();
    assert!(f.handler.storage.has_attempt("intent").await.unwrap());
    assert_eq!(
        f.redis
            .clone()
            .hget::<_, _, String>(&admission, "state")
            .await
            .unwrap(),
        "active"
    );
    assert_eq!(f.redis.clone().ttl::<_, i64>(&admission).await.unwrap(), -1);
    let mut terminal = redis::pipe();
    terminal.atomic();
    f.handler
        .on_fail(
            &f.job(),
            FailHookData { error: &failure },
            &mut TransactionContext::new(&mut terminal, "test".into()),
        )
        .await;
    let _: () = terminal.query_async(&mut f.redis.clone()).await.unwrap();
    assert!(!f.handler.storage.has_attempt("intent").await.unwrap());
    assert_eq!(
        f.redis
            .clone()
            .hget::<_, _, String>(&admission, "state")
            .await
            .unwrap(),
        "failed"
    );
    assert!((1..=86_400).contains(&f.redis.clone().ttl::<_, i64>(&admission).await.unwrap()));
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn lost_storage_lock_cannot_replace_or_resume_attempt() {
    let f = Fixture::new().await;
    let old = f.seed().await;
    let lock_key = format!("{}:solana_tx_lock:intent", f.namespace);
    let _: () = f.redis.clone().del(&lock_key).await.unwrap();
    let new = f.handler.storage.try_acquire_lock("intent").await.unwrap();
    let mut changed = f.attempt.clone();
    changed.signed_transaction = Some("corrupt".into());
    assert!(
        f.handler
            .storage
            .update_attempt("intent", &changed, &old)
            .await
            .is_err()
    );
    assert!(
        f.handler
            .storage
            .resume_reconciliation("intent", &old)
            .await
            .is_err()
    );
    old.release().await.unwrap();
    assert!(new.still_held().await.unwrap());
    assert_eq!(
        f.stored(&new).await.signed_transaction,
        f.attempt.signed_transaction
    );
    new.release().await.unwrap();
    f.cleanup().await;
}

#[test]
fn cache_key_debug_redacts_path_query_and_userinfo() {
    let key = crate::solana_executor::rpc_cache::RpcCacheKey {
        chain_id: SolanaChainId::SolanaDevnet,
        rpc_url: "https://user:SECRET@example.invalid/SECRET?apiKey=SECRET".into(),
    };
    assert!(!format!("{key:?}").contains("SECRET"));
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn response_timeout_retains_the_exact_attempt_for_recovery() {
    let f = Fixture::new().await;
    let lock = f.seed().await;
    let (client, seen, server) = rpc_with_timeout(
        vec![Reply {
            method: "sendTransaction",
            body: Some(json!({"__stall":true})),
        }],
        Duration::from_millis(50),
    )
    .await;
    let error = assert_nack(
        f.handler
            .broadcast_attempt(
                &client,
                &f.data,
                f.attempt.clone(),
                &lock,
                CommitmentLevel::Finalized,
            )
            .await,
    );
    assert!(matches!(error, SolanaExecutorError::SendFailed { .. }));
    assert!(
        !serde_json::to_string(&error)
            .unwrap()
            .contains("RPC_SECRET_SENTINEL")
    );
    let stored = f.stored(&lock).await;
    assert_eq!(stored.signature, f.attempt.signature);
    assert_eq!(stored.signed_transaction, f.attempt.signed_transaction);
    assert_eq!(stored.broadcast_attempts, 1);
    finish(server).await;
    assert_eq!(seen.lock().unwrap().len(), 1);
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn a_receipt_for_another_signature_is_not_success() {
    let f = Fixture::new().await;
    let lock = f.seed().await;
    let mut wrong = details(&f.attempt);
    wrong.body.as_mut().unwrap()["result"]["transaction"]["signatures"][0] =
        json!(solana_sdk::signature::Signature::default().to_string());
    let (rpc, _, server) = rpc(vec![status(finalized()), wrong]).await;
    assert!(matches!(
        assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, 1).await),
        SolanaExecutorError::InternalError { .. }
    ));
    finish(server).await;
    assert!(f.handler.storage.has_attempt("intent").await.unwrap());
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn previously_visible_then_stale_absence_never_re_signs_or_erases_evidence() {
    let mut f = Fixture::new().await;
    f.data.transaction.execution_options.max_blockhash_retries = 10; // legacy queued policy
    let lock = f.seed().await;
    let mut pending = finalized();
    pending["confirmations"] = json!(0);
    pending["confirmationStatus"] = json!("processed");
    let mut stale = status(Value::Null);
    stale.body.as_mut().unwrap()["result"]["context"]["slot"] = json!(1);
    let (rpc, seen, server) = rpc(vec![
        status(pending),
        status(Value::Null),
        reply("getBlockHeight", json!(101)),
        stale,
    ])
    .await;
    assert!(matches!(
        assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, 1).await),
        SolanaExecutorError::NotYetConfirmed { .. }
    ));
    assert!(matches!(
        assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, 2).await),
        SolanaExecutorError::RecoveryRequired { .. }
    ));
    finish(server).await;
    assert_eq!(seen.lock().unwrap().len(), 4);
    assert_eq!(
        f.stored(&lock).await.signed_transaction,
        f.attempt.signed_transaction
    );
    lock.release().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn malformed_status_count_and_disagreeing_error_fields_do_not_succeed() {
    let f = Fixture::new().await;
    let lock = f.seed().await;
    let extra = reply(
        "getSignatureStatuses",
        json!({"context":{"slot":150},"value":[finalized(),null]}),
    );
    let mut inconsistent = finalized();
    inconsistent["err"] = json!("AccountNotFound");
    let (rpc, _, server) = rpc(vec![extra, status(inconsistent)]).await;
    for n in 1..=2 {
        assert!(matches!(
            assert_nack(f.handler.execute_transaction(&rpc, &f.data, &lock, n).await),
            SolanaExecutorError::InternalError { .. }
        ));
    }
    finish(server).await;
    assert!(f.handler.storage.has_attempt("intent").await.unwrap());
    lock.release().await.unwrap();
    f.cleanup().await;
}
