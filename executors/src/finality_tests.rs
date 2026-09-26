//! Local JSON-RPC + Redis tests exercise the actual executor completion boundary.
use super::*;
use crate::{
    eip7702_executor::{
        confirm::{Eip7702ConfirmationHandler, Eip7702ConfirmationJobData},
        send::Eip7702Sender,
    },
    eoa::{
        EoaExecutorStore,
        store::{EoaTransactionRequest, SubmittedTransactionDehydrated},
        worker::EoaExecutorWorker,
    },
    external_bundler::{
        confirm::{UserOpConfirmationError, UserOpConfirmationHandler, UserOpConfirmationJobData},
        deployment::RedisDeploymentLock,
    },
    metrics::EoaMetrics,
    transaction_registry::TransactionRegistry,
    webhook::{WebhookDestinationPolicy, WebhookJobHandler, WebhookRetryConfig},
};
use alloy::{
    primitives::{Address, Bytes, U256, keccak256},
    providers::RootProvider,
    rpc::{client::RpcClient, types::Block},
};
use engine_core::{
    chain::{ChainService, RpcCredentials},
    credentials::SigningCredential,
    rpc_clients::{BundlerClient, PaymasterClient},
    signer::EoaSigner,
};
use reqwest::{Url, header::HeaderMap};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use twmq::{
    DurableExecution, Queue,
    job::{BorrowedJob, Job, JobError},
    redis::{self, AsyncCommands},
};

#[derive(Clone)]
struct TestChain {
    provider: RootProvider,
    bundler: BundlerClient,
    paymaster: PaymasterClient,
    policy: engine_core::finality::FinalityPolicy,
}
impl Chain for TestChain {
    fn finality_policy(&self) -> engine_core::finality::FinalityPolicy {
        self.policy
    }
    fn chain_id(&self) -> u64 {
        31337
    }
    fn rpc_url(&self) -> Url {
        Url::parse("http://127.0.0.1").unwrap()
    }
    fn bundler_url(&self) -> Url {
        self.rpc_url()
    }
    fn paymaster_url(&self) -> Url {
        self.rpc_url()
    }
    fn provider(&self) -> &RootProvider {
        &self.provider
    }
    fn bundler_client(&self) -> &BundlerClient {
        &self.bundler
    }
    fn paymaster_client(&self) -> &PaymasterClient {
        &self.paymaster
    }
    fn bundler_client_with_headers(&self, _: HeaderMap) -> BundlerClient {
        self.bundler.clone()
    }
    fn paymaster_client_with_headers(&self, _: HeaderMap) -> PaymasterClient {
        self.paymaster.clone()
    }
    fn with_new_default_headers(&self, _: HeaderMap) -> Self {
        self.clone()
    }
}
#[derive(Clone)]
struct Service(TestChain);
impl ChainService for Service {
    fn get_chain(&self, _: u64) -> Result<impl Chain + Clone, EngineError> {
        Ok(self.0.clone())
    }
}
struct State {
    receipt: Option<Value>,
    userop: Option<Value>,
    head: u64,
    block_hash: B256,
    block_overrides: std::collections::BTreeMap<u64, B256>,
    unsupported: bool,
    calls: Vec<String>,
}
struct Fixture {
    state: Arc<Mutex<State>>,
    chain: TestChain,
    task: tokio::task::JoinHandle<()>,
    client: redis::Client,
    redis: redis::aio::ConnectionManager,
    namespace: String,
    webhooks: Arc<Queue<WebhookJobHandler>>,
    registry: Arc<TransactionRegistry>,
}
impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url: Url = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let client = RpcClient::new_http(url);
        let provider = RootProvider::new(client.clone());
        let chain = TestChain {
            bundler: BundlerClient {
                inner: client.clone(),
            },
            paymaster: PaymasterClient {
                inner: client.clone(),
            },
            provider,
            policy: engine_core::finality::FinalityPolicy::Finalized,
        };
        let state = Arc::new(Mutex::new(State {
            receipt: Some(receipt(true)),
            userop: None,
            head: 9,
            block_hash: B256::repeat_byte(10),
            block_overrides: Default::default(),
            unsupported: false,
            calls: vec![],
        }));
        let remote = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (start, len) = loop {
                    let mut buffer = [0u8; 4096];
                    let n = stream.read(&mut buffer).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buffer[..n]);
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let len = String::from_utf8_lossy(&bytes[..end])
                            .lines()
                            .find_map(|line| {
                                let (k, v) = line.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (end + 4, len);
                    }
                };
                while bytes.len() < start + len {
                    let mut buffer = [0u8; 4096];
                    let n = stream.read(&mut buffer).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buffer[..n]);
                }
                let request: Value = serde_json::from_slice(&bytes[start..start + len]).unwrap();
                let mut response = {
                    let mut state = remote.lock().unwrap();
                    let method = request["method"].as_str().unwrap();
                    state.calls.push(method.to_owned());
                    match method {
                        "eth_getTransactionReceipt" => json!({"result":state.receipt}),
                        "eth_getUserOperationReceipt" => json!({"result":state.userop}),
                        "tw_getTransactionHash" => {
                            json!({"result":{"status":"success","transactionHash":B256::repeat_byte(1)}})
                        }
                        "eth_getTransactionCount" => json!({"result":"0x1"}),
                        "eth_getBalance" => json!({"result":"0xde0b6b3a7640000"}),
                        "eth_getBlockByNumber" => {
                            let tag = request["params"][0].as_str().unwrap();
                            if tag == "finalized" && state.unsupported {
                                json!({"error":{"code":-32602,"message":"finalized unsupported"}})
                            } else {
                                let n = if tag == "finalized" || tag == "latest" {
                                    state.head
                                } else {
                                    u64::from_str_radix(tag.trim_start_matches("0x"), 16).unwrap()
                                };
                                let mut block: Block = Block::default();
                                block.header.number = n;
                                block.header.hash =
                                    state.block_overrides.get(&n).copied().unwrap_or_else(|| {
                                        if n == 10 {
                                            state.block_hash
                                        } else {
                                            B256::repeat_byte(n as u8)
                                        }
                                    });
                                json!({"result":block})
                            }
                        }
                        _ => panic!("Unexpected RPC method {method}"),
                    }
                };
                response["id"] = request["id"].clone();
                response["jsonrpc"] = json!("2.0");
                let body = response.to_string();
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
            }
        });
        let client =
            redis::Client::open(std::env::var("TEST_REDIS_URL").expect("disposable Redis"))
                .unwrap();
        let redis = client.get_connection_manager().await.unwrap();
        let namespace = format!("finality-executors:{}", uuid::Uuid::new_v4());
        let webhooks = Arc::new(
            Queue::builder()
                .redis_connection_manager(redis.clone(), client.clone())
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
        let registry = Arc::new(TransactionRegistry::new(
            redis.clone(),
            Some(namespace.clone()),
        ));
        Self {
            state,
            chain,
            task,
            client,
            redis,
            namespace,
            webhooks,
            registry,
        }
    }
    async fn cleanup(self) {
        self.task.abort();
        let mut keys: Vec<String> = self
            .redis
            .clone()
            .keys(format!("{}:*", self.namespace))
            .await
            .unwrap();
        let queue: Vec<String> = self
            .redis
            .clone()
            .keys(format!("twmq:{}:*", self.namespace))
            .await
            .unwrap();
        keys.extend(queue);
        if !keys.is_empty() {
            let _: () = self.redis.clone().del(keys).await.unwrap();
        }
    }
}
fn receipt(success: bool) -> Value {
    json!({
    "transactionHash":B256::repeat_byte(1),"transactionIndex":"0x0","blockNumber":"0xa","blockHash":B256::repeat_byte(10),
    "from":Address::ZERO,"to":Address::ZERO,"cumulativeGasUsed":"0x5208","gasUsed":"0x5208","effectiveGasPrice":"0x1",
    "contractAddress":null,"logs":[],"logsBloom":format!("0x{}","00".repeat(256)),"status":if success{"0x1"}else{"0x0"},"type":"0x2"})
}
fn job<T: Clone>(data: T) -> BorrowedJob<T> {
    BorrowedJob::new(
        Job {
            id: "intent".into(),
            data,
            attempts: 1000,
            created_at: 0,
            processed_at: None,
            finished_at: None,
        },
        "lease".into(),
    )
}
fn assert_nack<T, E>(result: Result<T, JobError<E>>) {
    assert!(
        matches!(result, Err(JobError::Nack { .. })),
        "expected unresolved retry"
    );
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn eoa_provisional_revert_or_orphan_retains_nonce_and_sending_capacity_until_finality() {
    let f = Fixture::new().await;
    let store = EoaExecutorStore::new(
        f.redis.clone(),
        Some(f.namespace.clone()),
        Address::ZERO,
        31337,
        3600,
    )
    .acquire_eoa_lock_aggressively("owner", EoaMetrics::new(10, 60, 60), &f.client)
    .await
    .unwrap();
    let worker = EoaExecutorWorker {
        store,
        chain: f.chain.clone(),
        eoa: Address::ZERO,
        chain_id: 31337,
        noop_signing_credential: SigningCredential::Environment {
            address: Address::ZERO,
        },
        max_inflight: 50,
        max_recycled_nonces: 50,
        webhook_queue: f.webhooks.clone(),
        signer: Arc::new(EoaSigner::new(
            thirdweb_core::iaw::IAWClient::new("http://127.0.0.1:1").unwrap(),
        )),
        kms_client_cache: moka::future::Cache::new(1),
    };
    let request = EoaTransactionRequest {
        transaction_id: "intent".into(),
        chain_id: 31337,
        from: Address::ZERO,
        to: Some(Address::ZERO),
        value: U256::from(1),
        data: Bytes::new(),
        gas_limit: Some(21000),
        webhook_options: vec![],
        signing_credential: SigningCredential::Environment {
            address: Address::ZERO,
        },
        rpc_credentials: RpcCredentials::Configured,
        transaction_type_data: None,
    };
    let data_key = worker.store.transaction_data_key_name("intent");
    let attempt_key = worker.store.transaction_attempts_list_name("intent");
    let tx = SubmittedTransactionDehydrated {
        nonce: 0,
        transaction_hash: B256::repeat_byte(1).to_string(),
        transaction_id: "intent".into(),
        submitted_at: 1,
        queued_at: 1,
    };
    let (member, nonce) = tx.to_redis_string_with_nonce();
    let _: () = redis::pipe()
        .hset(
            &data_key,
            "user_request",
            serde_json::to_string(&request).unwrap(),
        )
        .hset(&data_key, "status", "submitted")
        .rpush(&attempt_key, "exact-signed-attempt")
        .zadd(
            worker.store.submitted_transactions_zset_name(),
            member,
            nonce,
        )
        .set(worker.store.optimistic_transaction_count_key_name(), 1)
        .set(worker.store.last_transaction_count_key_name(), 0)
        .query_async(&mut f.redis.clone())
        .await
        .unwrap();
    f.state.lock().unwrap().receipt = Some(receipt(false));
    for phase in 0..4 {
        if phase == 1 {
            f.state.lock().unwrap().block_hash = B256::repeat_byte(99);
        }
        if phase == 2 {
            let mut s = f.state.lock().unwrap();
            s.block_hash = B256::repeat_byte(10);
            s.head = 20;
            s.unsupported = true;
        }
        if phase == 3 {
            f.state.lock().unwrap().receipt = None;
        }
        if let Some(mut health) = worker.store.get_eoa_health().await.unwrap() {
            health.last_finality_poll_at = 0;
            worker.store.update_health_data(&health).await.unwrap();
        }
        let report = worker.confirm_flow().await.unwrap();
        assert_eq!(
            (
                report.moved_to_success,
                report.moved_to_failed,
                report.moved_to_pending
            ),
            (0, 0, 0)
        );
        assert_eq!(
            worker.store.get_inflight_budget(50).await.unwrap(),
            50,
            "inclusion frees send capacity without deleting recovery state"
        );
        assert_eq!(
            worker
                .store
                .get_submitted_transactions_count()
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            worker
                .store
                .get_optimistic_transaction_count()
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            f.redis.clone().ttl::<_, i64>(&attempt_key).await.unwrap(),
            -1
        );
    }
    {
        let mut s = f.state.lock().unwrap();
        s.receipt = Some(receipt(true));
        s.unsupported = false;
        s.head = 20;
    }
    let mut health = worker.store.get_eoa_health().await.unwrap().unwrap();
    health.last_finality_poll_at = 0;
    worker.store.update_health_data(&health).await.unwrap();
    let report = worker.confirm_flow().await.unwrap();
    assert_eq!(report.moved_to_success, 1);
    assert_eq!(
        worker
            .store
            .get_submitted_transactions_count()
            .await
            .unwrap(),
        0
    );
    assert!(f.redis.clone().ttl::<_, i64>(&attempt_key).await.unwrap() > 0);
    assert_eq!(
        f.redis
            .clone()
            .hget::<_, _, String>(&data_key, "status")
            .await
            .unwrap(),
        "confirmed"
    );
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn eip7702_unqualified_jobs_park_without_rpc_or_registry_cleanup() {
    let f = Fixture::new().await;
    let handler = Eip7702ConfirmationHandler {
        chain_service: Arc::new(Service(f.chain.clone())),
        webhook_queue: f.webhooks.clone(),
        transaction_registry: f.registry.clone(),
    };
    let queued = job(Eip7702ConfirmationJobData {
        transaction_id: "intent".into(),
        chain_id: 31337,
        bundler_transaction_id: "bundler-id".into(),
        sender_details: Eip7702Sender::Owner {
            eoa_address: Address::ZERO,
        },
        rpc_credentials: RpcCredentials::Configured,
        webhook_options: vec![],
        original_queued_timestamp: None,
    });
    f.registry
        .set_transaction_queue("intent", "eip7702_confirm")
        .await
        .unwrap();
    f.state.lock().unwrap().receipt = Some(receipt(false));
    assert_nack(handler.process(&queued).await);
    {
        let mut s = f.state.lock().unwrap();
        s.head = 20;
        s.block_hash = B256::repeat_byte(99);
    }
    assert_nack(handler.process(&queued).await);
    {
        let mut s = f.state.lock().unwrap();
        s.block_hash = B256::repeat_byte(10);
        s.unsupported = true;
    }
    assert_nack(handler.process(&queued).await);
    f.state.lock().unwrap().unsupported = false;
    assert_nack(handler.process(&queued).await);
    assert!(
        f.state.lock().unwrap().calls.is_empty(),
        "unqualified execution must not query or broadcast"
    );
    assert_eq!(
        f.registry
            .get_transaction_queue("intent")
            .await
            .unwrap()
            .as_deref(),
        Some("eip7702_confirm"),
        "process does not clean before durable queue commit"
    );
    let confirm_queue = Arc::new(
        Queue::builder()
            .redis_connection_manager(f.redis.clone(), f.client.clone())
            .name(format!("{}:disabled-eip-confirm", f.namespace))
            .handler(handler)
            .build()
            .await
            .unwrap(),
    );
    let sender = crate::eip7702_executor::send::Eip7702SendHandler {
        chain_service: Arc::new(Service(f.chain.clone())),
        eoa_signer: Arc::new(EoaSigner::new(
            thirdweb_core::iaw::IAWClient::new("http://127.0.0.1:1").unwrap(),
        )),
        webhook_queue: f.webhooks.clone(),
        confirm_queue,
        transaction_registry: f.registry.clone(),
        delegation_contract_cache:
            crate::eip7702_executor::delegation_cache::DelegationContractCache::new(
                moka::future::Cache::new(1),
            ),
    };
    for nonce in [None, Some(U256::from(123))] {
        let send_job = job(crate::eip7702_executor::send::Eip7702SendJobData {
            transaction_id: "intent".into(),
            chain_id: 31337,
            transactions: vec![],
            execution_options: serde_json::from_value(json!({"from":Address::ZERO})).unwrap(),
            signing_credential: SigningCredential::Environment {
                address: Address::ZERO,
            },
            webhook_options: vec![],
            rpc_credentials: RpcCredentials::Configured,
            nonce,
        });
        assert_nack(sender.process(&send_job).await);
    }
    assert!(
        f.state.lock().unwrap().calls.is_empty(),
        "legacy send must park without signing or RPC"
    );
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn erc4337_requires_identity_canonical_event_and_finality_for_both_outcomes() {
    let f = Fixture::new().await;
    let handler = UserOpConfirmationHandler::new(
        Arc::new(Service(f.chain.clone())),
        RedisDeploymentLock::new(f.client.clone())
            .await
            .unwrap()
            .with_namespace(Some(f.namespace.clone())),
        f.webhooks.clone(),
        f.registry.clone(),
    );
    let entrypoint = Address::repeat_byte(4);
    let sender = Address::repeat_byte(5);
    let ophash = B256::repeat_byte(6);
    let queued = job(UserOpConfirmationJobData {
        transaction_id: "intent".into(),
        chain_id: 31337,
        account_address: sender,
        user_op_hash: Bytes::copy_from_slice(ophash.as_slice()),
        nonce: U256::from(7),
        entrypoint_address: Some(entrypoint),
        deployment_lock_acquired: false,
        deployment_lock_id: None,
        webhook_options: vec![],
        rpc_credentials: RpcCredentials::Configured,
        original_queued_timestamp: None,
    });
    // Old/over-budget and missing receipt remains unresolved, with replay identity intact.
    assert_nack(handler.process(&queued).await);
    let mut sender_topic = [0u8; 32];
    sender_topic[12..].copy_from_slice(sender.as_slice());
    let mut event_data = Vec::new();
    for word in [U256::from(7), U256::ZERO, U256::from(100), U256::from(10)] {
        event_data.extend_from_slice(&word.to_be_bytes::<32>());
    }
    let log = json!({"address":entrypoint,"topics":[keccak256("UserOperationEvent(bytes32,address,address,uint256,bool,uint256,uint256)"),ophash,B256::from(sender_topic),B256::ZERO],
        "data":format!("0x{}",hex::encode(event_data)),"blockHash":B256::repeat_byte(10),"blockNumber":"0xa","transactionHash":B256::repeat_byte(1),"transactionIndex":"0x0","logIndex":"0x0","removed":false});
    let mut outer = receipt(true);
    outer["logs"] = json!([log]);
    let mut bundled_outer = outer.clone();
    bundled_outer.as_object_mut().unwrap().remove("type");
    let operation = json!({"userOpHash":ophash,"entryPoint":entrypoint,"sender":sender,"nonce":"0x7","paymaster":Address::ZERO,
        "actualGasCost":"0x64","actualGasUsed":"0xa","success":false,"logs":[],"receipt":bundled_outer});
    {
        let mut s = f.state.lock().unwrap();
        s.receipt = Some(outer.clone());
        s.userop = Some(operation.clone());
    }
    assert_nack(handler.process(&queued).await); // canonical included revert, not final
    {
        let mut s = f.state.lock().unwrap();
        s.head = 20;
        s.userop.as_mut().unwrap()["success"] = json!(true);
    }
    assert_nack(handler.process(&queued).await); // contradicts canonical EntryPoint event
    {
        let mut s = f.state.lock().unwrap();
        s.userop = Some(operation);
        s.receipt.as_mut().unwrap()["logs"] = json!([]);
    }
    assert_nack(handler.process(&queued).await); // missing event cannot claim even final inclusion
    f.state.lock().unwrap().receipt = Some(outer);
    assert!(matches!(
        handler.process(&queued).await,
        Err(JobError::Fail(
            UserOpConfirmationError::TransactionFailed { .. }
        ))
    ));
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn terminal_journal_restores_winning_eoa_hash_only_for_original_intent_and_nonce() {
    use engine_core::recovery::{self, RecoveryJournal};
    let f = Fixture::new().await;
    let directory =
        std::env::temp_dir().join(format!("eoa-finality-journal-{}", uuid::Uuid::new_v4()));
    let path = directory.join("ledger.sqlite");
    let journal_namespace = format!("{}_journal", f.namespace.replace(':', "_"));
    let namespace = Some(journal_namespace.clone());
    let url = std::env::var("TEST_REDIS_URL").unwrap();
    RecoveryJournal::initialize(&path, &url, namespace.clone())
        .await
        .unwrap();
    let journal = RecoveryJournal::open(&path, &url, namespace).await.unwrap();
    let request = EoaTransactionRequest {
        transaction_id: "intent".into(),
        chain_id: 31337,
        from: Address::ZERO,
        to: Some(Address::ZERO),
        value: U256::from(1),
        data: Bytes::new(),
        gas_limit: Some(21000),
        webhook_options: vec![],
        signing_credential: SigningCredential::Environment {
            address: Address::ZERO,
        },
        rpc_credentials: RpcCredentials::Configured,
        transaction_type_data: None,
    };
    let payload = serde_json::to_value(&request).unwrap();
    let fingerprint = recovery::admission_fingerprint("eoa", &payload).unwrap();
    journal
        .reserve_admission("eoa", "intent", &fingerprint, payload)
        .await
        .unwrap();
    let key = format!("evm:31337:{:#x}:7", Address::ZERO);
    journal
        .before_broadcast(
            "eoa",
            "intent",
            &key,
            json!({"transactionHash":B256::repeat_byte(1)}),
        )
        .await
        .unwrap();
    let proof = json!({"chainId":31337,"transactionHash":B256::repeat_byte(1),"outcome":"success","finality":{
        "blockNumber":10,"blockHash":B256::repeat_byte(10),"checkpointNumber":12,"checkpointHash":B256::repeat_byte(12),"policy":{"mode":"finalized"}}});
    journal
        .record_terminal("eoa", "intent", proof)
        .await
        .unwrap();
    assert_eq!(
        terminal_eoa_projection(&journal, &request, 7)
            .await
            .unwrap(),
        Some(B256::repeat_byte(1))
    );
    assert!(
        terminal_eoa_projection(&journal, &request, 8)
            .await
            .is_err()
    );
    assert!(
        validate_eoa_confirmation_with_journal(
            &journal,
            "eoa",
            "intent",
            31337,
            Address::ZERO,
            7,
            B256::repeat_byte(1)
        )
        .await
        .is_ok()
    );
    for (chain, sender, nonce, hash) in [
        (31337, Address::ZERO, 8, B256::repeat_byte(1)),
        (1, Address::ZERO, 7, B256::repeat_byte(1)),
        (31337, Address::repeat_byte(9), 7, B256::repeat_byte(1)),
        (31337, Address::ZERO, 7, B256::repeat_byte(99)),
    ] {
        assert!(
            validate_eoa_confirmation_with_journal(
                &journal, "eoa", "intent", chain, sender, nonce, hash
            )
            .await
            .is_err(),
            "known hash cannot authorize cleanup with a changed nonce/chain/sender, nor can an unrelated hash settle this intent"
        );
    }
    let mut changed = request;
    changed.value = U256::from(2);
    assert!(
        terminal_eoa_projection(&journal, &changed, 7)
            .await
            .is_err()
    );
    assert!(
        f.state.lock().unwrap().calls.is_empty(),
        "projection recovery never broadcasts or invents chain proof"
    );
    drop(journal);
    let _: () = f
        .redis
        .clone()
        .del(format!("{journal_namespace}:recovery:checkpoint"))
        .await
        .unwrap();
    f.cleanup().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn active_poll_detects_checkpoint_rollback_without_new_receipt() {
    const CHILD: &str = "ENGINE_TEST_CONTINUITY_CHILD";
    let Ok(scenario) = std::env::var(CHILD) else {
        // Global journal installation is intentionally process-wide; exercise each
        // real worker and direct pending-assessment path in an isolated process.
        for scenario in ["eoa", "erc4337", "pending", "policy"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "finality::tests::active_poll_detects_checkpoint_rollback_without_new_receipt",
                    "--ignored",
                    "--nocapture",
                ])
                .env(CHILD, scenario)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{scenario}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    };
    use engine_core::recovery::RecoveryJournal;
    let mut f = Fixture::new().await;
    let directory =
        std::env::temp_dir().join(format!("active-finality-journal-{}", uuid::Uuid::new_v4()));
    let path = directory.join("ledger.sqlite");
    let journal_namespace = format!("{}_journal", f.namespace.replace(':', "_"));
    let namespace = Some(journal_namespace.clone());
    let url = std::env::var("TEST_REDIS_URL").unwrap();
    RecoveryJournal::initialize(&path, &url, namespace.clone())
        .await
        .unwrap();
    let journal = RecoveryJournal::open(&path, &url, namespace).await.unwrap();
    recovery::install(journal.clone()).unwrap();
    f.state.lock().unwrap().head = 10;
    let receipt: TransactionReceipt = serde_json::from_value(receipt(true)).unwrap();
    assert!(matches!(
        assess(&f.chain, B256::repeat_byte(1), &receipt)
            .await
            .unwrap(),
        FinalityAssessment::Finalized(_)
    ));
    assert_eq!(
        journal
            .load_checkpoint(31337)
            .await
            .unwrap()
            .unwrap()
            .checkpoint_number,
        10
    );
    {
        let mut state = f.state.lock().unwrap();
        state.receipt = None;
        state.userop = None;
        state.head = 9; // no new candidate can meet finality
        state.calls.clear();
        if scenario != "policy" {
            state.block_hash = B256::repeat_byte(99);
        }
    }
    match scenario.as_str() {
        "eoa" => {
            let store = EoaExecutorStore::new(
                f.redis.clone(),
                Some(f.namespace.clone()),
                Address::ZERO,
                31337,
                3600,
            )
            .acquire_eoa_lock_aggressively("owner", EoaMetrics::new(10, 60, 60), &f.client)
            .await
            .unwrap();
            let worker = EoaExecutorWorker {
                store,
                chain: f.chain.clone(),
                eoa: Address::ZERO,
                chain_id: 31337,
                noop_signing_credential: SigningCredential::Environment {
                    address: Address::ZERO,
                },
                max_inflight: 50,
                max_recycled_nonces: 50,
                webhook_queue: f.webhooks.clone(),
                signer: Arc::new(EoaSigner::new(
                    thirdweb_core::iaw::IAWClient::new("http://127.0.0.1:1").unwrap(),
                )),
                kms_client_cache: moka::future::Cache::new(1),
            };
            assert_eq!(
                worker
                    .store
                    .get_submitted_transactions_count()
                    .await
                    .unwrap(),
                0
            );
            assert!(
                worker.confirm_flow().await.is_err(),
                "empty candidate set must still detect rollback"
            );
        }
        "erc4337" => {
            let handler = UserOpConfirmationHandler::new(
                Arc::new(Service(f.chain.clone())),
                RedisDeploymentLock::new(f.client.clone())
                    .await
                    .unwrap()
                    .with_namespace(Some(f.namespace.clone())),
                f.webhooks.clone(),
                f.registry.clone(),
            );
            let mut queued = job(UserOpConfirmationJobData {
                transaction_id: "intent".into(),
                chain_id: 31337,
                account_address: Address::ZERO,
                user_op_hash: Bytes::copy_from_slice(B256::repeat_byte(6).as_slice()),
                nonce: U256::ZERO,
                entrypoint_address: Some(Address::repeat_byte(4)),
                deployment_lock_acquired: false,
                deployment_lock_id: None,
                webhook_options: vec![],
                rpc_credentials: RpcCredentials::Configured,
                original_queued_timestamp: None,
            });
            seed_userop_journal(&journal, &mut queued.job.data).await;
            assert_nack(handler.process(&queued).await);
        }
        "eip7702" => {
            let handler = Eip7702ConfirmationHandler {
                chain_service: Arc::new(Service(f.chain.clone())),
                webhook_queue: f.webhooks.clone(),
                transaction_registry: f.registry.clone(),
            };
            let queued = job(Eip7702ConfirmationJobData {
                transaction_id: "intent".into(),
                chain_id: 31337,
                bundler_transaction_id: "bundler-id".into(),
                sender_details: Eip7702Sender::Owner {
                    eoa_address: Address::ZERO,
                },
                rpc_credentials: RpcCredentials::Configured,
                webhook_options: vec![],
                original_queued_timestamp: None,
            });
            assert_nack(handler.process(&queued).await);
        }
        "pending" => {
            assert!(
                assess(&f.chain, B256::repeat_byte(1), &receipt)
                    .await
                    .is_err()
            );
        }
        "policy" => {
            f.chain.policy = engine_core::finality::FinalityPolicy::Depth { confirmations: 1 };
            assert!(check_continuity(&f.chain).await.is_err());
            assert!(
                f.state.lock().unwrap().calls.is_empty(),
                "policy conflict needs no provider call"
            );
        }
        _ => unreachable!(),
    }
    assert!(
        journal.check_chain_healthy(31337).await.is_err(),
        "positive contradiction must persist a chain halt"
    );
    assert!(
        f.state.lock().unwrap().calls.iter().all(|method| matches!(
            method.as_str(),
            "eth_getBlockByNumber" | "eth_getTransactionCount" | "eth_getBalance"
        )),
        "continuity must run before missing receipt/bundler lookup, without broadcasting"
    );
    let _: () = f
        .redis
        .clone()
        .del(format!("{journal_namespace}:recovery:checkpoint"))
        .await
        .unwrap();
    f.cleanup().await;
    std::fs::remove_dir_all(directory).unwrap();
}

async fn seed_userop_journal(
    journal: &recovery::RecoveryJournal,
    confirmation: &mut UserOpConfirmationJobData,
) -> crate::external_bundler::send::ExternalBundlerSendJobData {
    use crate::external_bundler::send::ExternalBundlerSendJobData;
    use alloy::rpc::types::UserOperation;
    let entrypoint = confirmation.entrypoint_address.unwrap();
    let userop = engine_aa_types::VersionedUserOp::V0_6(UserOperation {
        sender: confirmation.account_address,
        nonce: confirmation.nonce,
        init_code: Bytes::new(),
        call_data: Bytes::new(),
        call_gas_limit: U256::from(100_000),
        verification_gas_limit: U256::from(200_000),
        pre_verification_gas: U256::from(30_000),
        max_fee_per_gas: U256::from(10),
        max_priority_fee_per_gas: U256::from(1),
        paymaster_and_data: Bytes::new(),
        signature: Bytes::from(vec![1; 65]),
    });
    confirmation.user_op_hash = Bytes::copy_from_slice(
        userop
            .hash_with_custom_entrypoint(confirmation.chain_id, entrypoint)
            .unwrap()
            .as_slice(),
    );
    let data=ExternalBundlerSendJobData {
        transaction_id:confirmation.transaction_id.clone(), chain_id:confirmation.chain_id,
        transactions:vec![], execution_options:serde_json::from_value(json!({"signerAddress":Address::ZERO,
            "entrypointAddress":entrypoint,"entrypointVersion":"0.6","smartAccountAddress":confirmation.account_address})).unwrap(),
        signing_credential:SigningCredential::Environment{address:Address::ZERO}, webhook_options:vec![],
        rpc_credentials:RpcCredentials::Configured,pregenerated_nonce:Some(confirmation.nonce),
    };
    let payload = serde_json::to_value(&data).unwrap();
    journal
        .reserve_admission(
            "erc4337",
            &data.transaction_id,
            &recovery::admission_fingerprint("erc4337", &payload).unwrap(),
            payload,
        )
        .await
        .unwrap();
    journal.before_broadcast("erc4337",&data.transaction_id,&format!("erc4337:{}:{entrypoint:#x}:{:#x}:{}",data.chain_id,confirmation.account_address,confirmation.nonce),
        json!({"chainId":data.chain_id,"sender":confirmation.account_address,"entrypoint":entrypoint,"nonce":confirmation.nonce,"userOperation":userop})).await.unwrap();
    data
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn userop_confirmation_rejects_projection_substitution_before_rpc() {
    const CHILD: &str = "ENGINE_TEST_USEROP_IDENTITY_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "finality::tests::userop_confirmation_rejects_projection_substitution_before_rpc",
                "--ignored",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let f = Fixture::new().await;
    let directory = std::env::temp_dir().join(format!("userop-identity-{}", uuid::Uuid::new_v4()));
    let path = directory.join("ledger.sqlite");
    let journal_namespace = format!("{}_journal", f.namespace.replace(':', "_"));
    let url = std::env::var("TEST_REDIS_URL").unwrap();
    recovery::RecoveryJournal::initialize(&path, &url, Some(journal_namespace.clone()))
        .await
        .unwrap();
    let journal = recovery::RecoveryJournal::open(&path, &url, Some(journal_namespace.clone()))
        .await
        .unwrap();
    recovery::install(journal.clone()).unwrap();
    let mut queued = job(UserOpConfirmationJobData {
        transaction_id: "intent".into(),
        chain_id: 31337,
        account_address: Address::repeat_byte(5),
        user_op_hash: Bytes::new(),
        nonce: U256::from(7),
        entrypoint_address: Some(Address::repeat_byte(4)),
        deployment_lock_acquired: false,
        deployment_lock_id: None,
        webhook_options: vec![],
        rpc_credentials: RpcCredentials::Configured,
        original_queued_timestamp: None,
    });
    let admitted = seed_userop_journal(&journal, &mut queued.job.data).await;
    let handler = UserOpConfirmationHandler::new(
        Arc::new(Service(f.chain.clone())),
        RedisDeploymentLock::new(f.client.clone())
            .await
            .unwrap()
            .with_namespace(Some(f.namespace.clone())),
        f.webhooks.clone(),
        f.registry.clone(),
    );
    let mut variants = vec![];
    let mut changed = queued.clone();
    changed.job.data.user_op_hash = Bytes::from(vec![99; 32]);
    variants.push(changed);
    let mut changed = queued.clone();
    changed.job.data.account_address = Address::repeat_byte(99);
    variants.push(changed);
    let mut changed = queued.clone();
    changed.job.data.nonce += U256::from(1);
    variants.push(changed);
    let mut changed = queued.clone();
    changed.job.data.entrypoint_address = Some(Address::repeat_byte(99));
    variants.push(changed);
    let mut changed = queued.clone();
    changed.job.data.chain_id = 1;
    variants.push(changed);
    let mut changed = queued.clone();
    changed.job.id = "different-id".into();
    variants.push(changed);
    for changed in variants {
        assert_nack(handler.process(&changed).await);
    }
    assert!(
        f.state.lock().unwrap().calls.is_empty(),
        "corrupted confirmation must not even query an unrelated operation"
    );
    assert_nack(handler.process(&queued).await);
    assert_eq!(
        f.state.lock().unwrap().calls,
        vec!["eth_getUserOperationReceipt"],
        "unchanged identity reaches normal reconciliation"
    );

    // Configuration can disappear after an earlier accepted/unknown send.
    // It must not convert that already reserved operation into queue failure.
    #[derive(Clone)]
    struct OfflineService(TestChain);
    impl ChainService for OfflineService {
        fn get_chain(&self, _: u64) -> Result<impl Chain + Clone, EngineError> {
            let _ = &self.0;
            Err::<TestChain, _>(EngineError::ValidationError {
                message: "chain configuration unavailable".into(),
            })
        }
    }
    use crate::external_bundler::{
        deployment::RedisDeploymentCache, send::ExternalBundlerSendHandler,
    };
    let service = Arc::new(OfflineService(f.chain.clone()));
    let confirm = Arc::new(
        Queue::builder()
            .redis_connection_manager(f.redis.clone(), f.client.clone())
            .name(format!("{}:confirm", f.namespace))
            .handler(UserOpConfirmationHandler::new(
                service.clone(),
                RedisDeploymentLock::new(f.client.clone()).await.unwrap(),
                f.webhooks.clone(),
                f.registry.clone(),
            ))
            .build()
            .await
            .unwrap(),
    );
    let sender = ExternalBundlerSendHandler {
        chain_service: service,
        userop_signer: Arc::new(engine_core::userop::UserOpSigner {
            iaw_client: thirdweb_core::iaw::IAWClient::new("http://127.0.0.1:1").unwrap(),
        }),
        deployment_cache: RedisDeploymentCache::new(f.client.clone()).await.unwrap(),
        deployment_lock: RedisDeploymentLock::new(f.client.clone()).await.unwrap(),
        webhook_queue: f.webhooks.clone(),
        confirm_queue: confirm,
        transaction_registry: f.registry.clone(),
    };
    let mut send_job = job(admitted);
    assert_nack(sender.process(&send_job).await); // old age also parks
    send_job.job.created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert_nack(sender.process(&send_job).await); // later deterministic setup error preserves unknown outcome
    assert_eq!(
        journal
            .admission("erc4337", "intent")
            .await
            .unwrap()
            .unwrap()
            .state,
        recovery::AdmissionState::Admitted
    );
    let _: () = f
        .redis
        .clone()
        .del(format!("{journal_namespace}:recovery:checkpoint"))
        .await
        .unwrap();
    f.cleanup().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn depth_continuity_anchors_qualified_history_and_finalized_tip_conflicts_still_halt() {
    const CHILD: &str = "ENGINE_TEST_DEPTH_BOUNDARY_CHILD";
    let Ok(scenario) = std::env::var(CHILD) else {
        for scenario in ["depth-success", "depth-revert", "finalized", "legacy-depth"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "finality::tests::depth_continuity_anchors_qualified_history_and_finalized_tip_conflicts_still_halt", "--ignored", "--nocapture"])
                .env(CHILD, scenario)
                .output().unwrap();
            assert!(
                output.status.success(),
                "{scenario}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    };
    use engine_core::{finality::FinalityPolicy, recovery::RecoveryJournal};
    let mut f = Fixture::new().await;
    let directory =
        std::env::temp_dir().join(format!("depth-boundary-journal-{}", uuid::Uuid::new_v4()));
    let path = directory.join("ledger.sqlite");
    let journal_namespace = format!("{}_journal", f.namespace.replace(':', "_"));
    let namespace = Some(journal_namespace.clone());
    let url = std::env::var("TEST_REDIS_URL").unwrap();
    RecoveryJournal::initialize(&path, &url, namespace.clone())
        .await
        .unwrap();
    let journal = RecoveryJournal::open(&path, &url, namespace).await.unwrap();
    recovery::install(journal.clone()).unwrap();
    let depth = scenario != "finalized";
    f.chain.policy = if depth {
        FinalityPolicy::Depth { confirmations: 2 }
    } else {
        FinalityPolicy::Finalized
    };
    f.state.lock().unwrap().head = 12;
    let candidate: TransactionReceipt =
        serde_json::from_value(receipt(scenario != "depth-revert")).unwrap();
    if scenario == "legacy-depth" {
        // Existing deployment records used the unqualified latest tip. They
        // cannot silently be reinterpreted/downgraded to the new boundary.
        let old = FinalityEvidence {
            block_number: 10,
            block_hash: B256::repeat_byte(10),
            checkpoint_number: 12,
            checkpoint_hash: B256::repeat_byte(12),
            policy: f.chain.policy,
        };
        assert!(
            journal
                .commit_checkpoint(31337, None, old.clone())
                .await
                .unwrap()
        );
        assert!(matches!(
            assess(&f.chain, B256::repeat_byte(1), &candidate)
                .await
                .unwrap(),
            FinalityAssessment::Pending { canonical: true }
        ));
        assert_eq!(journal.load_checkpoint(31337).await.unwrap(), Some(old));
        f.state
            .lock()
            .unwrap()
            .block_overrides
            .insert(12, B256::repeat_byte(99));
        assert!(
            check_continuity(&f.chain).await.is_err(),
            "legacy tip contradiction must not be silently excused"
        );
    } else {
        let assessment = assess(&f.chain, B256::repeat_byte(1), &candidate)
            .await
            .unwrap();
        let FinalityAssessment::Finalized(proof) = assessment else {
            panic!("qualified receipt stayed pending");
        };
        assert_eq!(proof.checkpoint_number, if depth { 10 } else { 12 });
        assert_eq!(
            journal.load_checkpoint(31337).await.unwrap().unwrap(),
            proof
        );
        {
            let mut state = f.state.lock().unwrap();
            state.block_overrides.insert(12, B256::repeat_byte(99));
            state.receipt = None;
        }
        if depth {
            // A two-block tip replacement leaves the qualified block10 intact.
            assert!(check_continuity(&f.chain).await.is_ok());
            journal.check_chain_healthy(31337).await.unwrap();
            assert!(matches!(
                assess(&f.chain, B256::repeat_byte(1), &candidate)
                    .await
                    .unwrap(),
                FinalityAssessment::Finalized(_)
            ));
            f.state.lock().unwrap().head = 13;
            let FinalityAssessment::Finalized(advanced) =
                assess(&f.chain, B256::repeat_byte(1), &candidate)
                    .await
                    .unwrap()
            else {
                panic!("qualified boundary did not advance");
            };
            assert_eq!(advanced.checkpoint_number, 11);
            assert_eq!(
                journal
                    .load_checkpoint(31337)
                    .await
                    .unwrap()
                    .unwrap()
                    .checkpoint_number,
                11
            );
            f.state
                .lock()
                .unwrap()
                .block_overrides
                .insert(11, B256::repeat_byte(77));
            assert!(
                check_continuity(&f.chain).await.is_err(),
                "changed qualified history must durably halt"
            );
        } else {
            assert!(
                check_continuity(&f.chain).await.is_err(),
                "changed finalized-tag checkpoint must still halt"
            );
        }
    }
    assert!(journal.check_chain_healthy(31337).await.is_err());
    let _: () = f
        .redis
        .clone()
        .del(format!("{journal_namespace}:recovery:checkpoint"))
        .await
        .unwrap();
    f.cleanup().await;
    std::fs::remove_dir_all(directory).unwrap();
}
