use super::*;
use crate::{
    TransactionCounts,
    eoa::EoaTransactionRequest,
    metrics::EoaMetrics,
    webhook::{WebhookJobHandler, WebhookRetryConfig},
};
use alloy::{
    primitives::{Address, Bytes, U256},
    providers::ProviderBuilder,
};
use engine_core::{chain::RpcCredentials, credentials::SigningCredential};
use serde_json::{Value, json};
use std::sync::Arc;
use thirdweb_core::auth::ThirdwebAuth;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use twmq::redis::AsyncCommands;

async fn read_rpc_request(stream: &mut tokio::net::TcpStream) -> Value {
    let mut bytes = vec![];
    let (body_start, body_length) = loop {
        let mut buffer = [0u8; 4096];
        let read = stream.read(&mut buffer).await.unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]);
            let length = headers
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            break (end + 4, length);
        }
    };
    while bytes.len() < body_start + body_length {
        let mut buffer = [0u8; 4096];
        let read = stream.read(&mut buffer).await.unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&buffer[..read]);
    }
    let request: Value =
        serde_json::from_slice(&bytes[body_start..body_start + body_length]).unwrap();
    request
}

/// A real HTTP JSON-RPC boundary, with no chain process or remote account.
async fn rpc_fixture(reply: Value) -> (RootProvider, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_rpc_request(&mut stream).await;
        assert_eq!(request["method"], "eth_getTransactionReceipt");
        let mut response = reply;
        response["jsonrpc"] = json!("2.0");
        response["id"] = request["id"].clone();
        let body = response.to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });
    (
        ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(url.parse().unwrap()),
        server,
    )
}

fn submitted(id: &str, hash_byte: u8, nonce: u64) -> SubmittedTransactionDehydrated {
    SubmittedTransactionDehydrated {
        transaction_id: id.into(),
        transaction_hash: B256::repeat_byte(hash_byte).to_string(),
        nonce,
        submitted_at: 1,
        queued_at: 1,
    }
}

fn receipt(hash: &str) -> Value {
    json!({
        "transactionHash": hash, "transactionIndex": "0x0", "blockNumber": "0xa",
        "blockHash": B256::repeat_byte(9).to_string(), "from": Address::ZERO,
        "to": Address::ZERO, "cumulativeGasUsed": "0x5208", "gasUsed": "0x5208",
        "effectiveGasPrice": "0x1", "contractAddress": null, "logs": [],
        "logsBloom": format!("0x{}", "00".repeat(256)), "status": "0x1", "type": "0x2"
    })
}

#[tokio::test]
async fn receipt_rpc_returns_only_matching_known_receipts() {
    let tx = submitted("intent", 1, 7);
    for reply in [
        json!({"result": null}),
        json!({"error": {"code": -32000, "message": "receipt storage temporarily unavailable"}}),
        json!({"result": receipt(&B256::repeat_byte(2).to_string())}),
    ] {
        let (provider, server) = rpc_fixture(reply).await;
        let confirmed = fetch_confirmed_transaction_receipts(&provider, vec![tx.clone()]).await;
        assert!(confirmed.is_empty());
        server.await.unwrap();
    }
    let (provider, server) = rpc_fixture(json!({"result": receipt(&tx.transaction_hash)})).await;
    let confirmed = fetch_confirmed_transaction_receipts(&provider, vec![tx]).await;
    assert_eq!(confirmed.len(), 1);
    assert_eq!(confirmed[0].nonce, 7);
    server.await.unwrap();
}

async fn seed(store: &EoaExecutorStore, tx: &SubmittedTransactionDehydrated) {
    let request = EoaTransactionRequest {
        transaction_id: tx.transaction_id.clone(),
        chain_id: 31337,
        from: Address::ZERO,
        to: Some(Address::ZERO),
        value: U256::from(7),
        data: Bytes::new(),
        gas_limit: Some(21000),
        webhook_options: vec![],
        transaction_type_data: None,
        signing_credential: SigningCredential::Iaw {
            auth_token: "unused".into(),
            thirdweb_auth: ThirdwebAuth::SecretKey("unused".into()),
        },
        rpc_credentials: RpcCredentials::Thirdweb(ThirdwebAuth::SecretKey("unused".into())),
    };
    let mut conn = store.redis.clone();
    let _: () = conn
        .hset_multiple(
            store.transaction_data_key_name(&tx.transaction_id),
            &[
                ("user_request", serde_json::to_string(&request).unwrap()),
                ("status", "submitted".into()),
            ],
        )
        .await
        .unwrap();
    let (member, nonce) = tx.to_redis_string_with_nonce();
    let _: () = conn
        .zadd(store.submitted_transactions_zset_name(), member, nonce)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn receipt_outage_cannot_requeue_an_already_broadcast_intent() {
    let client = twmq::redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("select disposable Redis"),
    )
    .unwrap();
    let redis = client.get_connection_manager().await.unwrap();
    let namespace = format!("receipt-proof:{}", uuid::Uuid::new_v4());
    let store = EoaExecutorStore::new(
        redis.clone(),
        Some(namespace.clone()),
        Address::ZERO,
        31337,
        3600,
    );
    let owner = store
        .acquire_eoa_lock_aggressively("receipt-owner", EoaMetrics::new(10, 60, 60), &client)
        .await
        .unwrap();
    let store = &*owner;
    let webhooks = Arc::new(
        twmq::Queue::builder()
            .redis_connection_manager(redis.clone(), client.clone())
            .name(format!("{namespace}:webhooks"))
            .handler(
                WebhookJobHandler::new(
                    crate::webhook::WebhookDestinationPolicy::default(),
                    Arc::new(WebhookRetryConfig::default()),
                )
                .unwrap(),
            )
            .build()
            .await
            .unwrap(),
    );
    let tx = submitted("uncertain", 1, 7);
    seed(&store, &tx).await;
    for reply in [
        json!({"result": null}),
        json!({"error": {"code": -32000, "message": "receipt outage"}}),
    ] {
        let (provider, rpc_server) = rpc_fixture(reply).await;
        let receipts = fetch_confirmed_transaction_receipts(&provider, vec![tx.clone()]).await;
        assert!(receipts.is_empty());
        rpc_server.await.unwrap();
        let report = owner
            .clean_submitted_transactions(
                &[],
                TransactionCounts {
                    latest: 9,
                    preconfirmed: 9,
                },
                webhooks.clone(),
            )
            .await
            .unwrap();
        assert_eq!(
            report.moved_to_pending, 0,
            "missing receipts are not replacement evidence"
        );
        assert_eq!(store.get_submitted_transactions_count().await.unwrap(), 1);
        assert_eq!(store.get_pending_transactions_count().await.unwrap(), 0);
    }
    // A known different intent mined at the SAME nonce is affirmative proof.
    let winner = submitted("known-winner", 2, 7);
    seed(&store, &winner).await;
    // Evidence at nonce 7 must not affect an uncertain transaction at nonce 8.
    seed(&store, &submitted("other-nonce", 3, 8)).await;
    let winner_receipt = receipt(&winner.transaction_hash);
    let confirmed = ConfirmedTransaction {
        nonce: 7,
        transaction_hash: winner.transaction_hash.clone(),
        transaction_id: winner.transaction_id,
        receipt: serde_json::from_value(winner_receipt.clone()).unwrap(),
        receipt_serialized: winner_receipt.to_string(),
        finality: fixture_finality(),
    };
    let report = owner
        .clean_submitted_transactions(
            &[confirmed],
            TransactionCounts {
                latest: 9,
                preconfirmed: 9,
            },
            webhooks.clone(),
        )
        .await
        .unwrap();
    assert_eq!(report.moved_to_success, 1);
    assert_eq!(report.moved_to_pending, 1);
    assert_eq!(store.get_submitted_transactions_count().await.unwrap(), 1);
    assert_eq!(store.get_pending_transactions_count().await.unwrap(), 1);
    let remaining = store
        .get_submitted_transactions_below_chain_transaction_count(10)
        .await
        .unwrap();
    assert_eq!(remaining[0].transaction_id, "other-nonce");
    // A preconfirmed nonce-zero receipt is not proof of canonical replacement.
    let uncertain_zero = submitted("uncertain-zero", 4, 0);
    let preconfirmed_zero = submitted("preconfirmed-zero", 5, 0);
    seed(store, &uncertain_zero).await;
    seed(store, &preconfirmed_zero).await;
    let zero_receipt = receipt(&preconfirmed_zero.transaction_hash);
    let report = owner
        .clean_submitted_transactions(
            &[ConfirmedTransaction {
                nonce: 0,
                transaction_hash: preconfirmed_zero.transaction_hash.clone(),
                transaction_id: preconfirmed_zero.transaction_id,
                receipt: serde_json::from_value(zero_receipt.clone()).unwrap(),
                receipt_serialized: zero_receipt.to_string(),
                finality: fixture_finality(),
            }],
            TransactionCounts {
                latest: 0,
                preconfirmed: 1,
            },
            webhooks,
        )
        .await
        .unwrap();
    assert_eq!(report.moved_to_success, 1);
    assert_eq!(
        report.moved_to_pending, 0,
        "nonce zero is not canonical when latest count is zero"
    );
    assert_eq!(store.get_submitted_transactions_count().await.unwrap(), 2);
    assert_eq!(store.get_pending_transactions_count().await.unwrap(), 1);
    let mut conn = redis;
    let keys: Vec<String> = conn.keys(format!("{namespace}:*")).await.unwrap();
    if !keys.is_empty() {
        let _: () = conn.del(keys).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL; no RPC calls or live keys"]
async fn legacy_fee_request_survives_storage_and_signed_wire_encoding() {
    use alloy::{
        consensus::{Transaction, TxEnvelope},
        eips::eip2718::{Decodable2718, Encodable2718},
        signers::local::PrivateKeySigner,
    };
    use engine_core::{chain::ThirdwebChainConfig, signer::EoaSigner};
    let client = twmq::redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("select disposable Redis"),
    )
    .unwrap();
    let redis = client.get_connection_manager().await.unwrap();
    let namespace = format!("legacy-wire:{}", uuid::Uuid::new_v4());
    let key = PrivateKeySigner::random();
    let eoa = key.address();
    let store = EoaExecutorStore::new(redis.clone(), Some(namespace.clone()), eoa, 31337, 3600)
        .acquire_eoa_lock_aggressively("legacy-owner", EoaMetrics::new(10, 60, 60), &client)
        .await
        .unwrap();
    let webhooks = Arc::new(
        twmq::Queue::builder()
            .redis_connection_manager(redis.clone(), client.clone())
            .name(format!("{namespace}:webhooks"))
            .handler(
                WebhookJobHandler::new(
                    crate::webhook::WebhookDestinationPolicy::default(),
                    Arc::new(WebhookRetryConfig::default()),
                )
                .unwrap(),
            )
            .build()
            .await
            .unwrap(),
    );
    let worker = EoaExecutorWorker {
        store,
        chain: ThirdwebChainConfig {
            secret_key: "unused",
            client_id: "unused",
            chain_id: 31337,
            rpc_base_url: "invalid",
            bundler_base_url: "invalid",
            paymaster_base_url: "invalid",
        }
        .to_chain()
        .unwrap(),
        eoa,
        chain_id: 31337,
        noop_signing_credential: SigningCredential::PrivateKey(key.clone()),
        max_inflight: 1,
        max_recycled_nonces: 1,
        webhook_queue: webhooks,
        signer: Arc::new(EoaSigner::new(
            thirdweb_core::iaw::IAWClient::new("http://127.0.0.1:1").unwrap(),
        )),
        kms_client_cache: moka::future::Cache::new(1),
    };
    let gas_price = 1_000_000_007u128;
    let request_json = json!({
        "transactionId":"legacy-intent", "chainId":31337, "from":eoa, "to":Address::repeat_byte(7),
        "value":"0x7", "data":"0x", "gasLimit":21000, "gasPrice":gas_price,
        "signingCredential": SigningCredential::Iaw { auth_token:"unused".into(), thirdweb_auth:ThirdwebAuth::SecretKey("unused".into()) },
        "rpcCredentials":RpcCredentials::Thirdweb(ThirdwebAuth::SecretKey("unused".into()))
    });
    let request: EoaTransactionRequest = serde_json::from_value(request_json.clone()).unwrap();
    // This is the same serialization boundary used for queued request payloads.
    let mut stored: EoaTransactionRequest =
        serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
    stored.signing_credential = SigningCredential::PrivateKey(key);
    let typed = worker.build_typed_transaction(&stored, 5).await.unwrap();
    let signed = worker
        .sign_transaction(typed, &stored.signing_credential)
        .await
        .unwrap();
    let envelope: TxEnvelope = signed.into();
    let bytes = envelope.encoded_2718();
    let decoded = TxEnvelope::decode_2718(&mut bytes.as_slice()).unwrap();
    assert!(
        matches!(&decoded, TxEnvelope::Legacy(_)),
        "legacy gasPrice must produce a legacy envelope"
    );
    assert_eq!(decoded.gas_price(), Some(gas_price));
    assert_eq!(decoded.nonce(), 5);
    assert_eq!(decoded.chain_id(), Some(31337));
    assert_eq!(decoded.value(), U256::from(7));
    let mut conflicting = request_json;
    conflicting["maxFeePerGas"] = json!(10);
    assert!(
        serde_json::from_value::<EoaTransactionRequest>(conflicting).is_err(),
        "stored request flatten must not swallow conflicting fee fields"
    );
    let mut conn = redis;
    let keys: Vec<String> = conn.keys(format!("{namespace}:*")).await.unwrap();
    if !keys.is_empty() {
        let _: () = conn.del(keys).await.unwrap();
    }
}

fn fixture_finality() -> engine_core::finality::FinalityEvidence {
    engine_core::finality::FinalityEvidence {
        block_number: 10,
        block_hash: alloy::primitives::B256::repeat_byte(9),
        checkpoint_number: 12,
        checkpoint_hash: alloy::primitives::B256::repeat_byte(12),
        policy: engine_core::finality::FinalityPolicy::Finalized,
    }
}

#[tokio::test]
async fn receipt_reads_bound_concurrency_and_drain_the_entire_page() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Semaphore, mpsc};
    let count = 2 * RECEIPT_RPC_CONCURRENCY + 3;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(
            format!("http://{}", listener.local_addr().unwrap())
                .parse()
                .unwrap(),
        );
    let gate = Arc::new(Semaphore::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (accepted, mut observations) = mpsc::unbounded_channel();
    let server_gate = gate.clone();
    let server_peak = peak.clone();
    let server = tokio::spawn(async move {
        let mut tasks = Vec::new();
        for _ in 0..count {
            let (mut stream, _) = listener.accept().await.unwrap();
            let gate = server_gate.clone();
            let active = active.clone();
            let peak = server_peak.clone();
            let accepted = accepted.clone();
            tasks.push(tokio::spawn(async move {
                let request = read_rpc_request(&mut stream).await;
                assert_eq!(request["method"], "eth_getTransactionReceipt");
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                accepted.send(()).unwrap();
                let _ = gate.acquire().await;
                active.fetch_sub(1, Ordering::SeqCst);
                let body = json!({"jsonrpc":"2.0", "id":request["id"], "result":null}).to_string();
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
                stream.write_all(response.as_bytes()).await.unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    });
    let work = tokio::spawn(async move {
        let txs = (0..count)
            .map(|i| submitted(&format!("intent-{i}"), i as u8, i as u64))
            .collect();
        fetch_confirmed_transaction_receipts(&provider, txs).await
    });
    for _ in 0..RECEIPT_RPC_CONCURRENCY {
        tokio::time::timeout(Duration::from_secs(5), observations.recv())
            .await
            .unwrap()
            .unwrap();
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), observations.recv())
            .await
            .is_err(),
        "no further RPC may start while every allowed slot is blocked"
    );
    gate.close();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), work)
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );
    server.await.unwrap();
    let mut remaining = 0;
    while observations.recv().await.is_some() {
        remaining += 1;
    }
    assert_eq!(remaining + RECEIPT_RPC_CONCURRENCY, count);
    assert_eq!(peak.load(Ordering::SeqCst), RECEIPT_RPC_CONCURRENCY);
}

async fn policy_fixture(
    policy: FinalityPolicy,
    replies: Vec<(&'static str, Value, Value)>,
) -> (
    engine_core::chain::ThirdwebChain,
    tokio::task::JoinHandle<()>,
) {
    use engine_core::chain::{RpcEndpointConfig, ThirdwebChainConfig};
    use std::time::Duration;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = RpcEndpointConfig {
        url: format!("http://{}", listener.local_addr().unwrap()),
        finality: policy,
        ..Default::default()
    };
    let chain = ThirdwebChainConfig {
        chain_id: 31337,
        secret_key: "unused",
        client_id: "unused",
        rpc_base_url: "invalid",
        bundler_base_url: "invalid",
        paymaster_base_url: "invalid",
    }
    .to_chain_with_rpc(
        Some(&endpoint),
        Duration::from_secs(2),
        Duration::from_secs(1),
    )
    .unwrap();
    let server = tokio::spawn(async move {
        for (method, params, mut response) in replies {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_rpc_request(&mut stream).await;
            assert_eq!(request["method"], method);
            assert_eq!(request["params"], params);
            response["id"] = request["id"].clone();
            response["jsonrpc"] = json!("2.0");
            let body = response.to_string();
            let wire = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(wire.as_bytes()).await.unwrap();
        }
    });
    (chain, server)
}

#[tokio::test]
async fn candidate_cutoff_uses_policy_head_and_never_falls_back_on_error() {
    let (chain, server) = policy_fixture(
        FinalityPolicy::Finalized,
        vec![(
            "eth_getTransactionCount",
            json!([Address::ZERO, "finalized"]),
            json!({"result":"0x7"}),
        )],
    )
    .await;
    assert_eq!(
        receipt_candidate_count(&chain, Address::ZERO, 100)
            .await
            .unwrap(),
        7
    );
    server.await.unwrap();

    let (chain, server) = policy_fixture(
        FinalityPolicy::Depth { confirmations: 3 },
        vec![
            ("eth_blockNumber", json!(null), json!({"result":"0xa"})),
            (
                "eth_getTransactionCount",
                json!([Address::ZERO, "0x7"]),
                json!({"result":"0x5"}),
            ),
        ],
    )
    .await;
    assert_eq!(
        receipt_candidate_count(&chain, Address::ZERO, 100)
            .await
            .unwrap(),
        5
    );
    server.await.unwrap();

    let (chain, server) = policy_fixture(FinalityPolicy::Depth { confirmations: 0 }, vec![]).await;
    assert_eq!(
        receipt_candidate_count(&chain, Address::ZERO, 100)
            .await
            .unwrap(),
        100
    );
    server.await.unwrap();
    let (chain, server) = policy_fixture(
        FinalityPolicy::Depth { confirmations: 11 },
        vec![("eth_blockNumber", json!(null), json!({"result":"0xa"}))],
    )
    .await;
    assert_eq!(
        receipt_candidate_count(&chain, Address::ZERO, 100)
            .await
            .unwrap(),
        0
    );
    server.await.unwrap();
    for (policy, method, params) in [
        (
            FinalityPolicy::Finalized,
            "eth_getTransactionCount",
            json!([Address::ZERO, "finalized"]),
        ),
        (
            FinalityPolicy::Depth { confirmations: 3 },
            "eth_blockNumber",
            json!(null),
        ),
    ] {
        let (chain, server) = policy_fixture(
            policy,
            vec![(
                method,
                params,
                json!({"error":{"code":-32000,"message":"unsupported or unavailable"}}),
            )],
        )
        .await;
        assert!(
            receipt_candidate_count(&chain, Address::ZERO, 100)
                .await
                .is_err()
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn falsely_high_candidate_nonce_cannot_turn_provisional_receipt_into_finality() {
    let tx = submitted("intent", 1, 7);
    let mut canonical: alloy::rpc::types::Block = Default::default();
    canonical.header.number = 10;
    canonical.header.hash = B256::repeat_byte(9);
    let mut head: alloy::rpc::types::Block = Default::default();
    head.header.number = 9;
    head.header.hash = B256::repeat_byte(8);
    let (chain, server) = policy_fixture(
        FinalityPolicy::Finalized,
        vec![
            (
                "eth_getTransactionCount",
                json!([Address::ZERO, "finalized"]),
                json!({"result":"0x64"}),
            ),
            (
                "eth_getTransactionReceipt",
                json!([tx.transaction_hash]),
                json!({"result":receipt(&tx.transaction_hash)}),
            ),
            (
                "eth_getBlockByNumber",
                json!(["0xa", false]),
                json!({"result":canonical}),
            ),
            (
                "eth_getBlockByNumber",
                json!(["finalized", false]),
                json!({"result":head}),
            ),
        ],
    )
    .await;
    let hint = receipt_candidate_count(&chain, Address::ZERO, 100)
        .await
        .unwrap();
    assert!(
        tx.nonce < hint,
        "corrupt high hint deliberately selects this receipt"
    );
    let receipts = fetch_confirmed_transaction_receipts(chain.provider(), vec![tx]).await;
    assert_eq!(receipts.len(), 1);
    assert!(
        matches!(
            assess_receipt_finality(
                &chain,
                receipts[0].receipt.transaction_hash,
                &receipts[0].receipt
            )
            .await
            .unwrap(),
            FinalityAssessment::Pending { canonical: true }
        ),
        "the independent gate must reject the optimistic scheduling hint"
    );
    server.await.unwrap();
}
