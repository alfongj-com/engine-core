//! Real Redis + HTTP regressions across preparation, reservation and recovery.
use super::*;
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
async fn rpc_rejection_keeps_original_nonce_wire_and_unknown_webhook_state() {
    const CHILD: &str = "ENGINE_TEST_EOA_UNCERTAIN_CHILD";
    const KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "eoa::worker::send::tests::rpc_rejection_keeps_original_nonce_wire_and_unknown_webhook_state", "--ignored", "--nocapture"])
            .env(CHILD,"1").env("ENGINE_PRIVATE_KEY",KEY).output().unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let url = std::env::var("TEST_REDIS_URL").unwrap();
    let client = twmq::redis::Client::open(url.clone()).unwrap();
    let redis = client.get_connection_manager().await.unwrap();
    let namespace = format!("uncertain_eoa_{}", uuid::Uuid::new_v4().simple());
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
    let state = Arc::new(Mutex::new(RpcState::default()));
    let observed = state.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let observed = observed.clone();
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
        max_inflight: 2,
        max_recycled_nonces: 2,
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
    for id in ["first", "second"] {
        let tx = EoaTransactionRequest {
            transaction_id: id.into(),
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
                id,
                &recovery::admission_fingerprint("eoa", &payload).unwrap(),
                payload,
            )
            .await
            .unwrap();
        worker.store.add_transaction(tx).await.unwrap();
        // Use the actual preparation/reservation/dispatch pipeline. Each uncertain
        // reservation debits its budget, so the next intent must get a new nonce.
        assert_eq!(worker.process_new_transactions(1).await.unwrap(), 0);
    }
    let mut before = worker.store.peek_borrowed_transactions().await.unwrap();
    before.sort_by_key(|tx| tx.transaction_id.clone());
    assert_eq!(before.len(), 2);
    assert_eq!(
        before
            .iter()
            .map(|tx| tx.signed_transaction.nonce())
            .collect::<std::collections::BTreeSet<_>>(),
        [0, 1].into_iter().collect()
    );
    assert!(
        worker
            .store
            .clean_and_get_recycled_nonces()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        worker.store.get_pending_transactions_count().await.unwrap(),
        0
    );
    assert_eq!(
        worker
            .store
            .get_submitted_transactions_count()
            .await
            .unwrap(),
        0
    );
    // Repeated provider rejections all retain the same bytes and durable identity.
    for _ in 0..3 {
        assert_eq!(worker.recover_borrowed_state().await.unwrap(), 0);
    }
    assert_eq!(RecoveryJournal::status(&ledger).unwrap().attempts, 2);
    let mut after = worker.store.peek_borrowed_transactions().await.unwrap();
    after.sort_by_key(|tx| tx.transaction_id.clone());
    assert_eq!(
        serde_json::to_value(&before).unwrap(),
        serde_json::to_value(&after).unwrap()
    );
    assert_eq!(
        conn.llen::<_, usize>(webhooks.pending_list_name())
            .await
            .unwrap(),
        0
    );
    for id in ["first", "second"] {
        let binding = journal
            .admission("eoa", id)
            .await
            .unwrap()
            .unwrap()
            .replay_key
            .unwrap();
        assert!(binding.ends_with(if id == "first" { ":0" } else { ":1" }));
        let status: String = conn
            .hget(worker.store.transaction_data_key_name(id), "status")
            .await
            .unwrap();
        assert_eq!(
            status, "pending",
            "unknown dispatch must not announce submission or failure"
        );
    }
    let first = before
        .iter()
        .find(|tx| tx.transaction_id == "first")
        .unwrap();
    {
        let mut state = state.lock().unwrap();
        state.included_hash = Some(first.signed_transaction.hash().to_string());
        state.acknowledge = true;
    }
    // Already included: reconcile without resending/webhook. Still unknown:
    // rebroadcast byte-for-byte, then acknowledge one send webhook.
    assert_eq!(worker.recover_borrowed_state().await.unwrap(), 2);
    assert!(
        worker
            .store
            .peek_borrowed_transactions()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        worker
            .store
            .get_submitted_transactions_count()
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        conn.llen::<_, usize>(webhooks.pending_list_name())
            .await
            .unwrap(),
        1
    );
    let wires = state.lock().unwrap().wires.clone();
    assert_eq!(wires.len(), 9);
    assert_eq!(
        wires
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2
    );
    for wire in &wires {
        assert!(wire == &wires[0] || wire == &wires[1]);
    }
    assert!(
        worker
            .store
            .clean_and_get_recycled_nonces()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(RecoveryJournal::status(&ledger).unwrap().attempts, 2);
    // A rejected NOOP cannot leave its durably bound nonce available to a user
    // intent. It is retained for receipt reconciliation, with no success webhook.
    state.lock().unwrap().acknowledge = false;
    let _: () = conn
        .zadd(worker.store.recycled_nonces_zset_name(), 2, 2)
        .await
        .unwrap();
    assert!(worker.send_noop_transaction(2).await.is_err());
    assert!(
        worker
            .store
            .clean_and_get_recycled_nonces()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        worker
            .store
            .get_submitted_transactions_count()
            .await
            .unwrap(),
        3
    );
    assert_eq!(RecoveryJournal::status(&ledger).unwrap().attempts, 3);
    assert_eq!(
        conn.llen::<_, usize>(webhooks.pending_list_name())
            .await
            .unwrap(),
        1
    );
    server.abort();
    let mut keys: Vec<String> = conn.keys(format!("{namespace}:*")).await.unwrap();
    keys.extend(
        conn.keys::<_, Vec<String>>(format!("twmq:{namespace}:*"))
            .await
            .unwrap(),
    );
    if !keys.is_empty() {
        let _: () = conn.del(keys).await.unwrap();
    }
    std::fs::remove_dir_all(dir).unwrap();
}
