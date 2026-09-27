use super::*;
use crate::eoa::{
    EoaExecutorStore, store::SubmittedTransactionDehydrated, worker::EoaExecutorWorker,
};
use crate::metrics::current_timestamp_ms;
use crate::{
    eoa::{EoaTransactionRequest, store::EoaHealth},
    metrics::EoaMetrics,
    webhook::{WebhookDestinationPolicy, WebhookJobHandler, WebhookRetryConfig},
};
use alloy::{
    consensus::Transaction,
    primitives::{Address, Bytes, U256},
    providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
};
use engine_core::{
    chain::{RpcCredentials, ThirdwebChainConfig},
    credentials::SigningCredential,
    execution_options::WebhookOptions,
    recovery::{self, RecoveryJournal},
    signer::EoaSigner,
    transaction::{TransactionLegacyData, TransactionTypeData},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use twmq::redis::AsyncCommands;

#[derive(Default)]
struct RpcState {
    wires: Vec<String>,
    included_hash: Option<String>,
    acknowledge: bool,
    steal_owner: bool,
}

async fn request(stream: &mut tokio::net::TcpStream) -> Value {
    let mut data = Vec::new();
    let (start, len) = loop {
        let mut buf = [0; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0);
        data.extend_from_slice(&buf[..n]);
        if let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&data[..end]);
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
    while data.len() < start + len {
        let mut buf = [0; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0);
        data.extend_from_slice(&buf[..n]);
    }
    serde_json::from_slice(&data[start..start + len]).unwrap()
}

fn receipt(hash: &str) -> Value {
    json!({"transactionHash":hash,"transactionIndex":"0x0","blockNumber":"0xa",
        "blockHash":alloy::primitives::B256::repeat_byte(9),"from":Address::ZERO,"to":Address::ZERO,
        "cumulativeGasUsed":"0x5208","gasUsed":"0x5208","effectiveGasPrice":"0x1",
        "contractAddress":null,"logs":[],"logsBloom":format!("0x{}","00".repeat(256)),"status":"0x1","type":"0x0"})
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; isolated journal and test signer subprocess"]
async fn gap_replay_retains_exact_wires_and_bounded_recovery_across_progress() {
    const CHILD: &str = "ENGINE_TEST_EOA_GAP_CHILD";
    const KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    let Ok(scenario) = std::env::var(CHILD) else {
        for scenario in [
            "bounded",
            "confirm-rollback",
            "stall",
            "normal",
            "missing",
            "unknown",
            "wrong-id",
            "terminal",
            "halted",
            "stale-owner",
            "lose-owner-mid-round",
        ] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "eoa::worker::confirm::gap_replay::tests::gap_replay_retains_exact_wires_and_bounded_recovery_across_progress", "--ignored", "--nocapture"])
            .env(CHILD,scenario).env("ENGINE_PRIVATE_KEY",KEY).output().unwrap();
            assert!(
                output.status.success(),
                "{} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    };
    let url = std::env::var("TEST_REDIS_URL").unwrap();
    let client = twmq::redis::Client::open(url.clone()).unwrap();
    let redis = client.get_connection_manager().await.unwrap();
    let namespace = format!("gap_eoa_{}", uuid::Uuid::new_v4().simple());
    let dir = std::env::temp_dir().join(&namespace);
    let ledger = dir.join("ledger.sqlite");
    RecoveryJournal::initialize(&ledger, &url, Some(namespace.clone()))
        .await
        .unwrap();
    let journal = RecoveryJournal::open(&ledger, &url, Some(namespace.clone()))
        .await
        .unwrap();
    recovery::install(journal.clone()).unwrap();
    let eoa = KEY.parse::<PrivateKeySigner>().unwrap().address();
    let store = EoaExecutorStore::new(redis.clone(), Some(namespace.clone()), eoa, 31337, 3600)
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
    let state = Arc::new(Mutex::new(RpcState {
        acknowledge: true,
        ..Default::default()
    }));
    let observed = state.clone();
    let rpc_redis = redis.clone();
    let rpc_lock_key = store.eoa_lock_key_name();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let observed = observed.clone();
            let mut rpc_redis = rpc_redis.clone();
            let rpc_lock_key = rpc_lock_key.clone();
            tokio::spawn(async move {
                let req = request(&mut socket).await;
                let response = {
                    let mut state = observed.lock().unwrap();
                    let mut response = json!({"jsonrpc":"2.0","id":req["id"]});
                    match req["method"].as_str().unwrap() {
                        "eth_sendRawTransaction" => {
                            let wire = req["params"][0].as_str().unwrap().to_owned();
                            let hash = alloy::primitives::keccak256(
                                hex::decode(wire.strip_prefix("0x").unwrap()).unwrap(),
                            );
                            state.wires.push(wire);
                            if state.acknowledge {
                                response["result"] = json!(hash);
                            } else {
                                // The fixture has recorded the accepted wire before returning
                                // each apparently deterministic error; client text cannot undo it.
                                let errors = [
                                    "nonce too high",
                                    "insufficient funds",
                                    "invalid signature",
                                    "intrinsic gas too low",
                                    "malformed transaction",
                                    "oversized",
                                    "already known",
                                    "nonce too low",
                                ];
                                response["error"] = json!({"code":-32000,"message":errors[(state.wires.len()-1)%errors.len()]});
                            }
                        }
                        "eth_getTransactionCount" => {
                            response["result"] = json!("0x0");
                        }
                        "eth_estimateGas" => {
                            response["result"] = json!("0x5208");
                        }
                        "eth_feeHistory" => {
                            response["error"] = json!({"code":-32601,"message":"method not found"});
                        }
                        "eth_gasPrice" => {
                            response["result"] = json!("0x1");
                        }
                        "eth_getTransactionReceipt" => {
                            let hash = req["params"][0].as_str().unwrap();
                            response["result"] = if state.included_hash.as_deref() == Some(hash) {
                                receipt(hash)
                            } else {
                                Value::Null
                            };
                        }
                        method => panic!("unexpected RPC method {method}"),
                    }
                    response.to_string()
                };
                if observed.lock().unwrap().steal_owner {
                    let _: () = rpc_redis.set(&rpc_lock_key, "new-owner").await.unwrap();
                }
                let bytes = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                    response.len()
                );
                socket.write_all(bytes.as_bytes()).await.unwrap();
            });
        }
    });
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
        eoa,
        chain_id: 31337,
        noop_signing_credential: SigningCredential::Environment { address: eoa },
        max_inflight: 50,
        broadcast_concurrency: 32,
        max_recycled_nonces: 50,
        webhook_queue: webhooks.clone(),
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
    worker
        .store
        .update_health_data(&EoaHealth {
            balance: U256::from(1000000000),
            balance_threshold: U256::ZERO,
            balance_fetched_at: current_timestamp_ms(),
            last_confirmation_at: current_timestamp_ms(),
            last_nonce_movement_at: current_timestamp_ms(),
            nonce_resets: vec![],
            last_finality_poll_at: 0,
            finality_scan_offset: 0,
        })
        .await
        .unwrap();
    for nonce in 0..40 {
        let id = format!("intent-{nonce}");
        let tx = EoaTransactionRequest {
            transaction_id: id.clone(),
            chain_id: 31337,
            from: eoa,
            to: Some(Address::repeat_byte(7)),
            value: U256::from(1),
            data: Bytes::new(),
            gas_limit: Some(21000),
            webhook_options: vec![WebhookOptions {
                url: "https://example.com/test".into(),
                secret: None,
                user_metadata: None,
            }],
            signing_credential: SigningCredential::Environment { address: eoa },
            rpc_credentials: RpcCredentials::Configured,
            transaction_type_data: Some(TransactionTypeData::Legacy(TransactionLegacyData {
                gas_price: Some(1),
            })),
        };
        let payload = serde_json::to_value(&tx).unwrap();
        journal
            .reserve_admission(
                "eoa",
                &id,
                &recovery::admission_fingerprint("eoa", &payload).unwrap(),
                payload,
            )
            .await
            .unwrap();
        worker.store.add_transaction(tx).await.unwrap();
    }
    assert_eq!(worker.send_flow().await.unwrap(), 40);
    // Initial sends are concurrent. Derive the oracle from signed nonce, never
    // HTTP arrival order; actual replay observations remain unsorted.
    let arrivals = state.lock().unwrap().wires.clone();
    assert_eq!(arrivals.len(), 40);
    let mut by_nonce = std::collections::BTreeMap::new();
    for wire in arrivals {
        let bytes = hex::decode(wire.trim_start_matches("0x")).unwrap();
        let decoded = TxEnvelope::decode_2718(&mut bytes.as_slice()).unwrap();
        assert!(by_nonce.insert(decoded.nonce(), wire).is_none());
    }
    assert_eq!(
        by_nonce.keys().copied().collect::<Vec<_>>(),
        (0..40).collect::<Vec<_>>()
    );
    let originals: Vec<_> = by_nonce.into_values().collect();
    let first_id = worker
        .store
        .get_submitted_transactions_for_nonce(0)
        .await
        .unwrap()[0]
        .transaction_id
        .clone();
    let second_id = worker
        .store
        .get_submitted_transactions_for_nonce(1)
        .await
        .unwrap()[0]
        .transaction_id
        .clone();
    assert_eq!(
        worker
            .store
            .peek_borrowed_transactions()
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        worker
            .store
            .get_submitted_transactions_count()
            .await
            .unwrap(),
        40
    );
    assert_eq!(RecoveryJournal::status(&ledger).unwrap().attempts, 40);
    // Acknowledged initial attempts have no Redis wire list: recovery must use SQL.
    assert_eq!(
        worker
            .store
            .get_transaction_attempts_count(&first_id)
            .await
            .unwrap(),
        0
    );
    state.lock().unwrap().wires.clear();
    let optimistic = worker
        .store
        .get_optimistic_transaction_count()
        .await
        .unwrap();
    let health_before = worker
        .get_eoa_health()
        .await
        .unwrap()
        .last_nonce_movement_at;
    let gap_key = worker.gap_replay_key();
    let mut connection = redis.clone();
    match scenario.as_str() {
        "normal" => {
            worker.replay_submitted_gap(0, 0).await.unwrap();
            assert!(state.lock().unwrap().wires.is_empty());
            assert!(!connection.exists::<_, bool>(&gap_key).await.unwrap());
        }
        "confirm-rollback" => {
            let _: () = connection
                .set(worker.store.last_transaction_count_key_name(), 40)
                .await
                .unwrap();
            let mut health = worker.get_eoa_health().await.unwrap();
            health.last_finality_poll_at = current_timestamp_ms();
            worker.store.update_health_data(&health).await.unwrap();
            worker.confirm_flow().await.unwrap();
            assert_eq!(
                worker.store.get_cached_transaction_count().await.unwrap(),
                40,
                "rollback must not reduce the consumed high-water allocator floor"
            );
            assert_eq!(state.lock().unwrap().wires, originals[..32]);
        }
        "stall" => {
            let mut health = worker.get_eoa_health().await.unwrap();
            health.last_nonce_movement_at = 1;
            worker.store.update_health_data(&health).await.unwrap();
            worker.replay_submitted_gap(0, 0).await.unwrap();
            assert_eq!(state.lock().unwrap().wires, originals[..32]);
        }
        "bounded" => {
            worker.replay_submitted_gap(0, 40).await.unwrap();
            assert_eq!(state.lock().unwrap().wires, originals[..32]);
            worker.replay_submitted_gap(0, 40).await.unwrap();
            assert_eq!(
                state.lock().unwrap().wires.len(),
                32,
                "cooldown must survive repeated worker cycles"
            );
            // Consumed nonce progress must not lose the original recovery suffix.
            worker.replay_submitted_gap(20, 40).await.unwrap();
            let mut gap: GapReplayState =
                serde_json::from_str(&connection.get::<_, String>(&gap_key).await.unwrap())
                    .unwrap();
            assert_eq!(gap.through_nonce, 40);
            assert_eq!(gap.latest_observed, 20);
            gap.next_at = 0;
            let _: () = connection
                .set(&gap_key, serde_json::to_string(&gap).unwrap())
                .await
                .unwrap();
            worker.replay_submitted_gap(32, 40).await.unwrap();
            assert_eq!(state.lock().unwrap().wires, originals);
            worker.replay_submitted_gap(40, 40).await.unwrap();
            assert!(!connection.exists::<_, bool>(&gap_key).await.unwrap());
        }
        "missing" => {
            let _: usize = connection
                .zrembyscore(worker.store.submitted_transactions_zset_name(), 0, 0)
                .await
                .unwrap();
            worker.replay_submitted_gap(0, 40).await.unwrap();
            assert!(
                state.lock().unwrap().wires.is_empty(),
                "never skip the missing lowest nonce"
            );
        }
        "unknown" => {
            state.lock().unwrap().acknowledge = false;
            worker.replay_submitted_gap(0, 40).await.unwrap();
            assert_eq!(state.lock().unwrap().wires, originals[..1]);
            assert_eq!(
                worker
                    .get_eoa_health()
                    .await
                    .unwrap()
                    .last_nonce_movement_at,
                health_before
            );
            worker.replay_submitted_gap(0, 40).await.unwrap();
            assert_eq!(state.lock().unwrap().wires.len(), 1);
            let mut gap: GapReplayState =
                serde_json::from_str(&connection.get::<_, String>(&gap_key).await.unwrap())
                    .unwrap();
            gap.next_at = 0;
            let _: () = connection
                .set(&gap_key, serde_json::to_string(&gap).unwrap())
                .await
                .unwrap();
            state.lock().unwrap().acknowledge = true;
            worker.replay_submitted_gap(0, 40).await.unwrap();
            let expected: Vec<_> = originals[..1]
                .iter()
                .chain(originals[..32].iter())
                .cloned()
                .collect();
            assert_eq!(state.lock().unwrap().wires, expected);
        }
        "wrong-id" => {
            let _: usize = connection
                .zrembyscore(worker.store.submitted_transactions_zset_name(), 0, 0)
                .await
                .unwrap();
            let wrong = SubmittedTransactionDehydrated {
                nonce: 0,
                transaction_hash: "0x00".into(),
                transaction_id: second_id.clone(),
                submitted_at: 1,
                queued_at: 1,
            };
            let (member, nonce) = wrong.to_redis_string_with_nonce();
            let _: () = connection
                .zadd(
                    worker.store.submitted_transactions_zset_name(),
                    member,
                    nonce,
                )
                .await
                .unwrap();
            assert!(worker.replay_submitted_gap(0, 40).await.is_err());
            assert!(state.lock().unwrap().wires.is_empty());
        }
        "terminal" => {
            let hash = alloy::primitives::keccak256(
                hex::decode(originals[0].trim_start_matches("0x")).unwrap(),
            );
            journal
                .record_terminal(
                    "eoa",
                    &first_id,
                    json!({"chainId":31337,"transactionHash":hash,"outcome":"success"}),
                )
                .await
                .unwrap();
            assert!(worker.replay_submitted_gap(0, 40).await.is_err());
            assert!(state.lock().unwrap().wires.is_empty());
        }
        "halted" => {
            journal
                .halt_chain(31337, "test contradiction")
                .await
                .unwrap();
            assert!(worker.replay_submitted_gap(0, 40).await.is_err());
            assert!(state.lock().unwrap().wires.is_empty());
        }
        "stale-owner" => {
            let _: () = connection
                .set(worker.store.eoa_lock_key_name(), "new-owner")
                .await
                .unwrap();
            assert!(worker.replay_submitted_gap(0, 40).await.is_err());
            assert!(state.lock().unwrap().wires.is_empty());
        }
        "lose-owner-mid-round" => {
            state.lock().unwrap().steal_owner = true;
            assert!(worker.replay_submitted_gap(0, 40).await.is_err());
            assert_eq!(
                state.lock().unwrap().wires,
                originals[..1],
                "lease loss must stop the rest of a claimed round"
            );
        }
        _ => unreachable!(),
    }
    assert_eq!(
        worker
            .store
            .get_optimistic_transaction_count()
            .await
            .unwrap(),
        optimistic
    );
    assert_eq!(worker.store.get_recycled_nonces_count().await.unwrap(), 0);
    assert_eq!(
        RecoveryJournal::status(&ledger).unwrap().attempts,
        40,
        "exact replay must not append a new signed identity"
    );
    server.abort();
    let keys: Vec<String> = connection.keys(format!("{namespace}:*")).await.unwrap();
    if !keys.is_empty() {
        let _: () = connection.del(keys).await.unwrap();
    }
    let keys: Vec<String> = connection
        .keys(format!("twmq:{namespace}:*"))
        .await
        .unwrap();
    if !keys.is_empty() {
        let _: () = connection.del(keys).await.unwrap();
    }
    drop(worker);
    drop(journal);
    std::fs::remove_dir_all(dir).unwrap();
}
