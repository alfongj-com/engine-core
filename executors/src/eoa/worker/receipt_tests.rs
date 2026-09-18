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

/// A real HTTP JSON-RPC boundary, with no chain process or remote account.
async fn rpc_fixture(reply: Value) -> (RootProvider, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
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
        transaction_hash: winner.transaction_hash.clone(),
        transaction_id: winner.transaction_id,
        receipt: serde_json::from_value(winner_receipt.clone()).unwrap(),
        receipt_serialized: winner_receipt.to_string(),
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
                transaction_hash: preconfirmed_zero.transaction_hash.clone(),
                transaction_id: preconfirmed_zero.transaction_id,
                receipt: serde_json::from_value(zero_receipt.clone()).unwrap(),
                receipt_serialized: zero_receipt.to_string(),
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
