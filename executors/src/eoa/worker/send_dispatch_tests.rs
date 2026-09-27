//! Deterministic overlap/failure cuts backed by a real FULL journal, Redis, and
//! a loopback HTTP receiver. The gates hold actual I/O; no timing speed claim.
use super::*;
use crate::{
    eoa::{
        EoaTransactionRequest,
        store::{BorrowedTransactionData, SubmissionResultType},
    },
    metrics::EoaMetrics,
    webhook::{WebhookDestinationPolicy, WebhookJobHandler, WebhookRetryConfig},
};
use alloy::{
    consensus::{SignableTransaction, Transaction, TxEnvelope, TxLegacy, TypedTransaction},
    eips::eip2718::Decodable2718,
    primitives::{Address, Bytes, TxKind, U256},
    providers::ProviderBuilder,
    signers::{SignerSync, local::PrivateKeySigner},
};
use engine_core::{
    chain::{RpcCredentials, ThirdwebChain, ThirdwebChainConfig},
    credentials::SigningCredential,
    execution_options::WebhookOptions,
    recovery::{self, RecoveryJournal},
    signer::EoaSigner,
    transaction::{TransactionLegacyData, TransactionTypeData},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Semaphore, mpsc},
};
use twmq::redis::AsyncCommands;

const KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const LIMIT: Duration = Duration::from_secs(15);

fn child(test: &str) -> bool {
    const FLAG: &str = "ENGINE_EOA_DISPATCH_TEST_CHILD";
    if std::env::var(FLAG).as_deref() == Ok(test) {
        return true;
    }
    let exact = format!("eoa::worker::send::dispatch_tests::{test}");
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &exact, "--ignored", "--nocapture"])
        .env(FLAG, test)
        .env("ENGINE_PRIVATE_KEY", KEY)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    false
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> Value {
    let mut bytes = Vec::new();
    let (start, length) = loop {
        let mut part = [0; 4096];
        let n = stream.read(&mut part).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&part[..n]);
        if let Some(end) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
            let length = String::from_utf8_lossy(&bytes[..end])
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            break (end + 4, length);
        }
    };
    while bytes.len() < start + length {
        let mut part = [0; 4096];
        let n = stream.read(&mut part).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&part[..n]);
    }
    serde_json::from_slice(&bytes[start..start + length]).unwrap()
}

#[derive(Default)]
struct Observed {
    wires: Vec<(usize, String)>,
    response_order: Vec<usize>,
    active: usize,
    peak: usize,
    reject: BTreeSet<usize>,
}

struct Fixture {
    worker: EoaExecutorWorker<ThirdwebChain>,
    journal: Arc<RecoveryJournal>,
    directory: PathBuf,
    namespace: String,
    prepared: Vec<BorrowedTransaction>,
    replies: Vec<Arc<Semaphore>>,
    accepted: Option<mpsc::UnboundedReceiver<usize>>,
    response_done: Vec<Arc<Semaphore>>,
    state: Arc<Mutex<Observed>>,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(count: usize, concurrency: usize) -> Self {
        let url =
            std::env::var("TEST_REDIS_URL").expect("set TEST_REDIS_URL to a disposable Redis");
        let client = twmq::redis::Client::open(url.clone()).unwrap();
        let redis = client.get_connection_manager().await.unwrap();
        let namespace = format!("dispatch_{}", uuid::Uuid::new_v4().simple());
        let directory = std::env::temp_dir().join(&namespace);
        let ledger = directory.join("journal.sqlite");
        RecoveryJournal::initialize(&ledger, &url, Some(namespace.clone()))
            .await
            .unwrap();
        let journal = RecoveryJournal::open(&ledger, &url, Some(namespace.clone()))
            .await
            .unwrap();
        recovery::install(journal.clone()).unwrap();
        let signer: PrivateKeySigner = KEY.parse().unwrap();
        let sender = signer.address();
        let store =
            EoaExecutorStore::new(redis.clone(), Some(namespace.clone()), sender, 31337, 3600)
                .acquire_eoa_lock_aggressively("owner", EoaMetrics::new(10, 60, 60), &client)
                .await
                .unwrap();
        let webhooks = Arc::new(
            twmq::Queue::builder()
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(
                format!("http://{}", listener.local_addr().unwrap())
                    .parse()
                    .unwrap(),
            );
        let mut chain = ThirdwebChainConfig {
            secret_key: "unused",
            client_id: "unused",
            chain_id: 31337,
            rpc_base_url: "invalid",
            bundler_base_url: "invalid",
            paymaster_base_url: "invalid",
        }
        .to_chain()
        .unwrap();
        chain.provider = provider;
        let worker = EoaExecutorWorker {
            store,
            chain,
            eoa: sender,
            chain_id: 31337,
            noop_signing_credential: SigningCredential::Environment { address: sender },
            max_inflight: count as u64,
            broadcast_concurrency: concurrency,
            max_recycled_nonces: count as u64,
            webhook_queue: webhooks,
            signer: Arc::new(EoaSigner::new(
                thirdweb_core::iaw::IAWClient::new("http://127.0.0.1:1").unwrap(),
            )),
            kms_client_cache: moka::future::Cache::new(1),
        };
        let mut conn = redis.clone();
        let _: () = conn
            .set(worker.store.optimistic_transaction_count_key_name(), 0)
            .await
            .unwrap();
        let _: () = conn
            .set(worker.store.last_transaction_count_key_name(), 0)
            .await
            .unwrap();
        let mut prepared = Vec::new();
        for index in 0..count {
            let request = EoaTransactionRequest {
                transaction_id: format!("intent-{index:02}"),
                chain_id: 31337,
                from: sender,
                to: Some(Address::repeat_byte(7)),
                value: U256::from(index + 1),
                data: Bytes::new(),
                gas_limit: Some(21000),
                webhook_options: vec![WebhookOptions {
                    url: "https://example.com/dispatch".into(),
                    secret: None,
                    user_metadata: None,
                }],
                signing_credential: SigningCredential::Environment { address: sender },
                rpc_credentials: RpcCredentials::Configured,
                transaction_type_data: Some(TransactionTypeData::Legacy(TransactionLegacyData {
                    gas_price: Some(1),
                })),
            };
            let payload = serde_json::to_value(&request).unwrap();
            journal
                .reserve_admission(
                    "eoa",
                    &request.transaction_id,
                    &recovery::admission_fingerprint("eoa", &payload).unwrap(),
                    payload,
                )
                .await
                .unwrap();
            worker.store.add_transaction(request.clone()).await.unwrap();
            let transaction: TypedTransaction = TxLegacy {
                chain_id: Some(31337),
                nonce: index as u64,
                gas_price: 1,
                gas_limit: 21000,
                to: TxKind::Call(Address::repeat_byte(7)),
                value: request.value,
                input: Bytes::new(),
            }
            .into();
            let signature = signer
                .sign_hash_sync(&transaction.signature_hash())
                .unwrap();
            let signed_transaction = transaction.into_signed(signature);
            prepared.push(BorrowedTransaction {
                data: BorrowedTransactionData {
                    transaction_id: request.transaction_id.clone(),
                    hash: signed_transaction.hash().to_string(),
                    signed_transaction,
                    queued_at: EoaExecutorStore::now(),
                    borrowed_at: EoaExecutorStore::now(),
                },
                user_request: request,
            });
        }
        let state = Arc::new(Mutex::new(Observed::default()));
        let replies: Vec<_> = (0..count).map(|_| Arc::new(Semaphore::new(0))).collect();
        let (accepted_tx, accepted) = mpsc::unbounded_channel();
        let response_done: Vec<_> = (0..count).map(|_| Arc::new(Semaphore::new(0))).collect();
        let server_done = response_done.clone();
        let server_state = state.clone();
        let server_replies = replies.clone();
        let server_journal = journal.clone();
        let borrowed_key = worker.store.borrowed_transactions_hashmap_name();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let state = server_state.clone();
                let replies = server_replies.clone();
                let done = server_done.clone();
                let journal = server_journal.clone();
                let mut redis = redis.clone();
                let borrowed_key = borrowed_key.clone();
                let accepted = accepted_tx.clone();
                tokio::spawn(async move {
                    let request = read_request(&mut socket).await;
                    let mut response = json!({"jsonrpc":"2.0", "id": request["id"]});
                    let mut sent_nonce = None;
                    match request["method"].as_str().unwrap() {
                        "eth_sendRawTransaction" => {
                            let wire = request["params"][0].as_str().unwrap();
                            let bytes = hex::decode(wire.strip_prefix("0x").unwrap()).unwrap();
                            let envelope = TxEnvelope::decode_2718(&mut bytes.as_slice()).unwrap();
                            let nonce = envelope.nonce() as usize;
                            let id = format!("intent-{nonce:02}");
                            let replay = format!("evm:31337:{sender:#x}:{nonce}");
                            // Independent receiver checks actual authority/projection before
                            // acknowledging a real wire. Missing/mismatched evidence fails.
                            let attempt = journal
                                .latest_eoa_attempt(&id, &replay)
                                .await
                                .unwrap()
                                .unwrap();
                            assert_eq!(attempt["signedTransaction"].as_str(), Some(wire));
                            assert_eq!(
                                attempt["transactionHash"],
                                json!(alloy::primitives::keccak256(&bytes))
                            );
                            let borrowed: String = redis.hget(&borrowed_key, &id).await.unwrap();
                            let borrowed: BorrowedTransactionData =
                                serde_json::from_str(&borrowed).unwrap();
                            assert_eq!(
                                *borrowed.signed_transaction.hash(),
                                alloy::primitives::keccak256(&bytes)
                            );
                            {
                                let mut seen = state.lock().unwrap();
                                seen.wires.push((nonce, wire.to_owned()));
                                seen.active += 1;
                                seen.peak = seen.peak.max(seen.active);
                            }
                            accepted.send(nonce).unwrap();
                            replies[nonce].acquire().await.unwrap().forget();
                            let reject = state.lock().unwrap().reject.contains(&nonce);
                            if reject {
                                response["error"] =
                                    json!({"code": -32000, "message": "fixture outcome uncertain"});
                            } else {
                                response["result"] = json!(alloy::primitives::keccak256(&bytes));
                            }
                            sent_nonce = Some(nonce);
                        }
                        "eth_getTransactionReceipt" => response["result"] = Value::Null,
                        "eth_estimateGas" => response["result"] = json!("0x5208"),
                        "eth_gasPrice" => response["result"] = json!("0x1"),
                        "eth_feeHistory" => {
                            response["error"] =
                                json!({"code": -32601, "message": "unsupported fixture method"})
                        }
                        method => panic!("unexpected RPC method {method}"),
                    }
                    let body = response.to_string();
                    let bytes = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    // A caller-cancelled response can fail to write; the accepted
                    // wire remains recorded and must be reconciled byte-for-byte.
                    let _ = socket.write_all(bytes.as_bytes()).await;
                    if let Some(nonce) = sent_nonce {
                        let mut seen = state.lock().unwrap();
                        seen.active -= 1;
                        seen.response_order.push(nonce);
                        done[nonce].add_permits(1);
                    }
                });
            }
        });
        Self {
            worker,
            journal,
            directory,
            namespace,
            prepared,
            replies,
            accepted: Some(accepted),
            response_done,
            state,
            server,
        }
    }

    async fn reserve_all(&self) {
        self.worker
            .store
            .atomic_move_pending_to_borrowed_with_incremented_nonces(
                &self
                    .prepared
                    .iter()
                    .map(|item| item.data.clone())
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
    }

    async fn retained(&self, attempts: u64) {
        let mut borrowed = self
            .worker
            .store
            .peek_borrowed_transactions()
            .await
            .unwrap();
        borrowed.sort_by_key(|row| row.transaction_id.clone());
        assert_eq!(
            serde_json::to_value(&borrowed).unwrap(),
            serde_json::to_value(
                self.prepared
                    .iter()
                    .map(|item| &item.data)
                    .collect::<Vec<_>>()
            )
            .unwrap()
        );
        assert_eq!(
            RecoveryJournal::status(&self.directory.join("journal.sqlite"))
                .unwrap()
                .attempts,
            attempts
        );
        assert_eq!(
            self.worker
                .store
                .get_submitted_transactions_count()
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            self.worker
                .store
                .get_pending_transactions_count()
                .await
                .unwrap(),
            0
        );
        assert!(
            self.worker
                .store
                .clean_and_get_recycled_nonces()
                .await
                .unwrap()
                .is_empty()
        );
        let mut conn = self.worker.store.redis.clone();
        let next: u64 = conn
            .get(self.worker.store.optimistic_transaction_count_key_name())
            .await
            .unwrap();
        assert_eq!(next, self.prepared.len() as u64);
        let count: usize = conn
            .llen(self.worker.webhook_queue.pending_list_name())
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "authorization failure cannot announce success or failure"
        );
    }

    fn release_all(&self) {
        for reply in &self.replies {
            reply.add_permits(16);
        }
    }

    async fn cleanup(self) {
        self.server.abort();
        let mut conn = self.worker.store.redis.clone();
        let mut keys: Vec<String> = conn.keys(format!("{}:*", self.namespace)).await.unwrap();
        keys.extend(
            conn.keys::<_, Vec<String>>(format!("twmq:{}:*", self.namespace))
                .await
                .unwrap(),
        );
        if !keys.is_empty() {
            let _: () = conn.del(keys).await.unwrap();
        }
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

async fn actual_send(fixture: &Fixture, borrowed: &BorrowedTransaction) -> SubmissionResult {
    let sent = fixture
        .worker
        .chain
        .provider()
        .send_tx_envelope(borrowed.signed_transaction.clone().into())
        .await;
    SubmissionResult::from_send_result(
        borrowed,
        sent,
        SendContext::InitialBroadcast,
        &fixture.worker.chain,
    )
}

#[tokio::test]
#[ignore = "requires disposable TEST_REDIS_URL; real journal and loopback HTTP"]
async fn authorization_overlaps_http_and_preserves_order_and_uncertain_results() {
    if !child("authorization_overlaps_http_and_preserves_order_and_uncertain_results") {
        return;
    }
    tokio::time::timeout(LIMIT, async {
        let mut fixture = Fixture::new(3, 2).await;
        let mut accepted = fixture.accepted.take().unwrap();
        fixture.reserve_all().await;
        fixture.state.lock().unwrap().reject.insert(1);
        let gate = Semaphore::new(0);
        let calls = Mutex::new(Vec::new());
        let mut run = Box::pin(authorize_then_dispatch(3, 2,
            |index| { let fixture = &fixture; let gate = &gate; let calls = &calls; async move {
                calls.lock().unwrap().push(index);
                if index == 1 { gate.acquire().await.unwrap().forget(); }
                let row = &fixture.prepared[index];
                fixture.worker.authorize_while_owned(crate::recovery::before_eoa(&row.user_request, &row.signed_transaction)).await
            }},
            |index| actual_send(&fixture, &fixture.prepared[index])));
        // The first real HTTP request must begin while authorization two is
        // blocked. The old whole-batch authorization barrier times out here.
        let first = tokio::select! { value = accepted.recv() => value.unwrap(), _ = &mut run => panic!("dispatch completed while authorization was held") };
        assert_eq!(first, 0);
        assert_eq!(RecoveryJournal::status(&fixture.directory.join("journal.sqlite")).unwrap().attempts, 1);
        gate.add_permits(1);
        let second = tokio::select! { value = accepted.recv() => value.unwrap(), _ = &mut run => panic!("responses are held") };
        assert_eq!(second, 1);
        // Return nonce one first, including an ambiguous RPC error; nonce zero
        // still blocks ordered delivery. This must not stop authorization three.
        fixture.replies[1].add_permits(1);
        tokio::select! {
            permit = fixture.response_done[1].acquire() => permit.unwrap().forget(),
            _ = &mut run => panic!("nonce zero response is still held"),
        }
        assert_eq!(fixture.state.lock().unwrap().response_order, vec![1]);
        fixture.replies[0].add_permits(1);
        let third = tokio::select! { value = accepted.recv() => value.unwrap(), _ = &mut run => panic!("third response is held") };
        assert_eq!(third, 2);
        fixture.replies[2].add_permits(1);
        let results = run.await.unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![0, 1, 2]);
        assert_eq!(results.iter().map(|r| r.transaction.nonce).collect::<Vec<_>>(), vec![0, 1, 2]);
        assert!(matches!(results[0].result, SubmissionResultType::Success));
        assert!(matches!(results[1].result, SubmissionResultType::Uncertain));
        assert!(matches!(results[2].result, SubmissionResultType::Success));
        assert_eq!(fixture.state.lock().unwrap().peak, 2);
        fixture.worker.store.process_borrowed_transactions(results, fixture.worker.webhook_queue.clone()).await.unwrap();
        let retained = fixture.worker.store.peek_borrowed_transactions().await.unwrap();
        assert_eq!(retained.len(), 1); assert_eq!(retained[0].transaction_id, "intent-01");
        assert_eq!(fixture.worker.store.get_submitted_transactions_count().await.unwrap(), 2);
        fixture.cleanup().await;
    }).await.expect("controlled dispatch gates must finish");
}

#[tokio::test]
#[ignore = "requires disposable TEST_REDIS_URL; real journal and loopback HTTP"]
async fn failed_authorization_drains_prefix_and_retains_whole_batch_for_exact_recovery() {
    if !child("failed_authorization_drains_prefix_and_retains_whole_batch_for_exact_recovery") {
        return;
    }
    tokio::time::timeout(LIMIT, async {
        let mut fixture = Fixture::new(3, 2).await;
        let mut accepted = fixture.accepted.take().unwrap();
        fixture.reserve_all().await;
        let mut tampered = fixture.prepared.clone(); tampered[1].user_request.value += U256::from(1);
        let gate = Semaphore::new(0); let failed = Semaphore::new(0); let calls = Mutex::new(Vec::new());
        let mut run = Box::pin(authorize_then_dispatch(3, 2,
            |index| { let fixture = &fixture; let row = &tampered[index]; let gate = &gate; let failed = &failed; let calls = &calls; async move {
                calls.lock().unwrap().push(index);
                if index == 1 { gate.acquire().await.unwrap().forget(); }
                let result = fixture.worker.authorize_while_owned(crate::recovery::before_eoa(&row.user_request, &row.signed_transaction)).await;
                if result.is_err() { failed.add_permits(1); }
                result
            }}, |index| actual_send(&fixture, &tampered[index])));
        let first = tokio::select! { value = accepted.recv() => value.unwrap(), _ = &mut run => panic!("prefix must reach HTTP before failed suffix authorization") };
        assert_eq!(first, 0); gate.add_permits(1);
        tokio::select! { permit = failed.acquire() => permit.unwrap().forget(), _ = &mut run => panic!("accepted prefix response is still held") }
        assert_eq!(*calls.lock().unwrap(), vec![0, 1]);
        assert_eq!(fixture.state.lock().unwrap().wires.len(), 1);
        // Returning the auth error before this response would cancel, not drain,
        // the already-created send. Retention remains conservative either way.
        fixture.replies[0].add_permits(1);
        assert!(run.await.is_err());
        assert_eq!(*calls.lock().unwrap(), vec![0, 1], "the suffix must never be authorized, including while prefix responses drain");
        fixture.retained(1).await;
        fixture.release_all();
        assert_eq!(fixture.worker.recover_borrowed_state().await.unwrap(), 3);
        assert_eq!(RecoveryJournal::status(&fixture.directory.join("journal.sqlite")).unwrap().attempts, 3);
        let seen = fixture.state.lock().unwrap();
        assert_eq!(seen.wires.len(), 4);
        let prefix: Vec<_> = seen.wires.iter().filter(|(nonce, _)| *nonce == 0).map(|(_, wire)| wire).collect();
        assert_eq!(prefix.len(), 2); assert_eq!(prefix[0], prefix[1]);
        assert_eq!(seen.wires.iter().map(|(nonce, _)| *nonce).collect::<BTreeSet<_>>(), [0, 1, 2].into_iter().collect());
        drop(seen);
        assert_eq!(fixture.worker.store.get_submitted_transactions_count().await.unwrap(), 3);
        assert!(fixture.worker.store.peek_borrowed_transactions().await.unwrap().is_empty());
        fixture.cleanup().await;
    }).await.expect("controlled prefix failure and recovery must finish");
}

#[tokio::test]
#[ignore = "requires disposable TEST_REDIS_URL; real journal and Redis owner fence"]
async fn owner_changes_before_or_during_authorization_stop_dispatch_without_recycling() {
    if !child("owner_changes_before_or_during_authorization_stop_dispatch_without_recycling") {
        return;
    }
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(2, 2).await;
        fixture.reserve_all().await;
        let mut conn = fixture.worker.store.redis.clone();
        let _: () = conn
            .set(fixture.worker.store.eoa_lock_key_name(), "other")
            .await
            .unwrap();
        assert!(
            fixture
                .worker
                .dispatch_reserved_transactions(&fixture.prepared)
                .await
                .is_err()
        );
        let _: () = conn
            .set(fixture.worker.store.eoa_lock_key_name(), "owner")
            .await
            .unwrap();
        fixture.retained(0).await;
        let calls = Mutex::new(Vec::new());
        let result = authorize_then_dispatch(
            2,
            2,
            |index| {
                let fixture = &fixture;
                let calls = &calls;
                async move {
                    calls.lock().unwrap().push(index);
                    fixture
                        .worker
                        .authorize_while_owned(async {
                            let row = &fixture.prepared[index];
                            crate::recovery::before_eoa(&row.user_request, &row.signed_transaction)
                                .await?;
                            // Takeover after an actual durable commit but before this
                            // authorization returns exercises the second ownership fence.
                            let _: () = fixture
                                .worker
                                .store
                                .redis
                                .clone()
                                .set(fixture.worker.store.eoa_lock_key_name(), "other")
                                .await
                                .unwrap();
                            Ok(())
                        })
                        .await
                }
            },
            |index| actual_send(&fixture, &fixture.prepared[index]),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(*calls.lock().unwrap(), vec![0]);
        assert!(fixture.state.lock().unwrap().wires.is_empty());
        // restore only the fixture owner so read/cleanup assertions can run;
        // production does not reacquire or repair ownership inside this helper.
        let _: () = conn
            .set(fixture.worker.store.eoa_lock_key_name(), "owner")
            .await
            .unwrap();
        fixture.retained(1).await;
        assert!(
            fixture
                .journal
                .admission("eoa", "intent-01")
                .await
                .unwrap()
                .unwrap()
                .replay_key
                .is_none()
        );
        fixture.cleanup().await;
    })
    .await
    .expect("owner fences must finish");
}

#[tokio::test]
#[ignore = "requires disposable TEST_REDIS_URL; real new/recycled send paths and loopback HTTP"]
async fn new_and_recycled_paths_preserve_actual_wire_identity_with_bounded_dispatch() {
    if !child("new_and_recycled_paths_preserve_actual_wire_identity_with_bounded_dispatch") {
        return;
    }
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(5, 2).await;
        fixture.release_all();
        assert_eq!(fixture.worker.process_new_transactions(2).await.unwrap(), 2);
        let mut conn = fixture.worker.store.redis.clone();
        let _: () = conn
            .set(
                fixture.worker.store.optimistic_transaction_count_key_name(),
                4,
            )
            .await
            .unwrap();
        // Model two earlier preparation gaps below a real higher reservation.
        // With only submitted nonces 0/1, cleanup correctly discards 2/3 as
        // future fresh nonces; those are not a recycled-nonce fixture.
        let higher = fixture.prepared[4].clone();
        assert_eq!(
            fixture
                .worker
                .store
                .atomic_move_pending_to_borrowed_with_incremented_nonces(&[higher.data.clone()])
                .await
                .unwrap(),
            1
        );
        let higher_results = fixture
            .worker
            .dispatch_reserved_transactions(&[higher])
            .await
            .unwrap();
        let higher_report = fixture
            .worker
            .store
            .process_borrowed_transactions(higher_results, fixture.worker.webhook_queue.clone())
            .await
            .unwrap();
        assert_eq!(higher_report.moved_to_submitted, 1);
        assert_eq!(
            RecoveryJournal::status(&fixture.directory.join("journal.sqlite"))
                .unwrap()
                .attempts,
            3,
            "the higher nonce must have a real durable wire, not a fabricated Redis score"
        );
        for nonce in [2u64, 3] {
            let _: () = conn
                .zadd(
                    fixture.worker.store.recycled_nonces_zset_name(),
                    nonce,
                    nonce,
                )
                .await
                .unwrap();
        }
        assert_eq!(
            fixture
                .worker
                .store
                .clean_and_get_recycled_nonces()
                .await
                .unwrap(),
            vec![2, 3],
            "both holes must survive cleanup below the submitted nonce 4"
        );
        assert_eq!(fixture.worker.process_recycled_nonces().await.unwrap(), 2);
        assert_eq!(
            RecoveryJournal::status(&fixture.directory.join("journal.sqlite"))
                .unwrap()
                .attempts,
            5
        );
        assert_eq!(
            fixture
                .worker
                .store
                .get_submitted_transactions_count()
                .await
                .unwrap(),
            5
        );
        assert_eq!(
            fixture
                .worker
                .store
                .get_pending_transactions_count()
                .await
                .unwrap(),
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
        let seen = fixture.state.lock().unwrap();
        assert_eq!(seen.wires.len(), 5);
        assert!(seen.peak <= 2);
        assert_eq!(
            seen.wires
                .iter()
                .map(|(nonce, _)| *nonce)
                .collect::<BTreeSet<_>>(),
            [0, 1, 2, 3, 4].into_iter().collect()
        );
        drop(seen);
        fixture.cleanup().await;
    })
    .await
    .expect("both actual send paths must finish");
}

#[tokio::test]
#[ignore = "requires disposable TEST_REDIS_URL; accepted-response cancellation and exact recovery"]
async fn cancelled_dispatch_keeps_accepted_wire_and_reserved_suffix_for_recovery() {
    if !child("cancelled_dispatch_keeps_accepted_wire_and_reserved_suffix_for_recovery") {
        return;
    }
    tokio::time::timeout(LIMIT, async {
        let mut fixture = Fixture::new(2, 2).await;
        let mut accepted = fixture.accepted.take().unwrap();
        fixture.reserve_all().await;
        let hold_second = Semaphore::new(0);
        let mut run = Box::pin(authorize_then_dispatch(
            2,
            2,
            |index| {
                let fixture = &fixture;
                let hold = &hold_second;
                async move {
                    if index == 1 {
                        hold.acquire().await.unwrap().forget();
                    }
                    let row = &fixture.prepared[index];
                    fixture
                        .worker
                        .authorize_while_owned(crate::recovery::before_eoa(
                            &row.user_request,
                            &row.signed_transaction,
                        ))
                        .await
                }
            },
            |index| actual_send(&fixture, &fixture.prepared[index]),
        ));
        let first = tokio::select! {
            value = accepted.recv() => value.unwrap(),
            _ = &mut run => panic!("accepted response is deliberately held"),
        };
        assert_eq!(first, 0);
        // Simulate cancellation after the receiver accepted the first wire. No
        // response or unsent suffix can be classified as a definitive failure.
        drop(run);
        fixture.retained(1).await;
        fixture.release_all();
        assert_eq!(fixture.worker.recover_borrowed_state().await.unwrap(), 2);
        assert_eq!(
            RecoveryJournal::status(&fixture.directory.join("journal.sqlite"))
                .unwrap()
                .attempts,
            2
        );
        let seen = fixture.state.lock().unwrap();
        assert_eq!(seen.wires.len(), 3);
        let prefix: Vec<_> = seen
            .wires
            .iter()
            .filter(|(nonce, _)| *nonce == 0)
            .map(|(_, wire)| wire)
            .collect();
        assert_eq!(prefix.len(), 2);
        assert_eq!(prefix[0], prefix[1]);
        drop(seen);
        assert!(
            fixture
                .worker
                .store
                .peek_borrowed_transactions()
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            fixture
                .worker
                .store
                .get_submitted_transactions_count()
                .await
                .unwrap(),
            2
        );
        fixture.cleanup().await;
    })
    .await
    .expect("accepted cancellation recovery must finish");
}

#[tokio::test]
#[ignore = "requires disposable TEST_REDIS_URL; immediate-ready authorization cut with real journal/HTTP"]
async fn first_auth_error_suppresses_ready_prefix_before_network_start() {
    if !child("first_auth_error_suppresses_ready_prefix_before_network_start") {
        return;
    }
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(3, 2).await;
        fixture.reserve_all().await;
        fixture.release_all();
        let first = &fixture.prepared[0];
        fixture
            .worker
            .authorize_while_owned(crate::recovery::before_eoa(
                &first.user_request,
                &first.signed_transaction,
            ))
            .await
            .unwrap();
        let mut tampered = fixture.prepared[1].clone();
        tampered.user_request.value += U256::from(1);
        let failure = fixture
            .worker
            .authorize_while_owned(crate::recovery::before_eoa(
                &tampered.user_request,
                &tampered.signed_transaction,
            ))
            .await
            .unwrap_err();
        // These are completed REAL journal/owner outcomes, returned immediately
        // to force Buffered's refill-before-first-poll scheduling cut. No fake
        // successful authorization or synthetic missing durable attempt is used.
        let outcomes = Mutex::new(std::collections::VecDeque::from([Ok(()), Err(failure)]));
        let authorized = Mutex::new(Vec::new());
        let dispatch_started = Mutex::new(Vec::new());
        let result = authorize_then_dispatch(
            3,
            2,
            |index| {
                authorized.lock().unwrap().push(index);
                futures::future::ready(
                    outcomes
                        .lock()
                        .unwrap()
                        .pop_front()
                        .expect("no suffix authorization"),
                )
            },
            |index| {
                dispatch_started.lock().unwrap().push(index);
                actual_send(&fixture, &fixture.prepared[index])
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(*authorized.lock().unwrap(), vec![0, 1]);
        assert!(
            dispatch_started.lock().unwrap().is_empty(),
            "known authorization failure must prevent even constructing network work"
        );
        assert!(fixture.state.lock().unwrap().wires.is_empty());
        fixture.retained(1).await;
        assert!(
            fixture
                .journal
                .admission("eoa", "intent-02")
                .await
                .unwrap()
                .unwrap()
                .replay_key
                .is_none()
        );
        fixture.cleanup().await;
    })
    .await
    .expect("immediately-ready failure cut must finish");
}
