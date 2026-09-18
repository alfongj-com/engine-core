use super::*;
use alloy::{
    consensus::{TxEip1559, TxEip7702, TxEnvelope, TxLegacy},
    eips::eip2718::{Decodable2718, Encodable2718},
    signers::{SignerSync, local::PrivateKeySigner},
};
use engine_core::transaction::{Transaction1559Data, Transaction7702Data, TransactionLegacyData};

fn dynamic(fee: u128, priority: u128) -> TypedTransaction {
    TypedTransaction::Eip1559(TxEip1559 {
        chain_id: 31337,
        nonce: 7,
        gas_limit: 21000,
        max_fee_per_gas: fee,
        max_priority_fee_per_gas: priority,
        to: Address::repeat_byte(7).into(),
        value: U256::from(1),
        ..Default::default()
    })
}

fn signed_wire(tx: TypedTransaction) -> TxEnvelope {
    let key = PrivateKeySigner::random();
    let signature = key.sign_hash_sync(&tx.signature_hash()).unwrap();
    let envelope: TxEnvelope = tx.into_signed(signature).into();
    let bytes = envelope.encoded_2718();
    let decoded = TxEnvelope::decode_2718(&mut bytes.as_slice()).unwrap();
    assert_eq!(decoded.nonce(), 7);
    assert_eq!(decoded.chain_id(), Some(31337));
    assert_eq!(decoded.value(), U256::from(1));
    decoded
}

#[test]
fn explicit_fee_ceilings_skip_unchanged_replacements() {
    let legacy = TypedTransaction::Legacy(TxLegacy {
        gas_price: 100,
        ..Default::default()
    });
    assert!(
        bump_transaction_fees(
            legacy,
            120,
            Some(&TransactionTypeData::Legacy(TransactionLegacyData {
                gas_price: Some(100)
            }))
        )
        .is_none()
    );
    assert!(
        bump_transaction_fees(
            dynamic(100, 4),
            120,
            Some(&TransactionTypeData::Eip1559(Transaction1559Data {
                max_fee_per_gas: Some(100),
                max_priority_fee_per_gas: Some(4)
            }))
        )
        .is_none()
    );
    let delegated = TypedTransaction::Eip7702(TxEip7702 {
        max_fee_per_gas: 100,
        max_priority_fee_per_gas: 4,
        ..Default::default()
    });
    assert!(
        bump_transaction_fees(
            delegated,
            120,
            Some(&TransactionTypeData::Eip7702(Transaction7702Data {
                authorization_list: None,
                max_fee_per_gas: Some(100),
                max_priority_fee_per_gas: Some(4)
            }))
        )
        .is_none()
    );
}

#[test]
fn signed_replacement_obeys_partial_fee_caps_and_preserves_transaction() {
    let max_fee_only = TransactionTypeData::Eip1559(Transaction1559Data {
        max_fee_per_gas: Some(100),
        max_priority_fee_per_gas: None,
    });
    let tx = bump_transaction_fees(dynamic(100, 90), 120, Some(&max_fee_only)).unwrap();
    let decoded = signed_wire(tx);
    assert_eq!(decoded.max_fee_per_gas(), 100);
    assert_eq!(decoded.max_priority_fee_per_gas(), Some(100));
    assert_eq!(decoded.gas_limit(), 21000);
    assert_eq!(decoded.to(), Some(Address::repeat_byte(7)));
    assert!(decoded.input().is_empty());

    let priority_only = TransactionTypeData::Eip1559(Transaction1559Data {
        max_fee_per_gas: None,
        max_priority_fee_per_gas: Some(4),
    });
    let decoded =
        signed_wire(bump_transaction_fees(dynamic(100, 4), 120, Some(&priority_only)).unwrap());
    assert_eq!(decoded.max_fee_per_gas(), 120);
    assert_eq!(decoded.max_priority_fee_per_gas(), Some(4));

    let legacy = TypedTransaction::Legacy(TxLegacy {
        chain_id: Some(31337),
        nonce: 7,
        gas_limit: 21000,
        gas_price: 90,
        to: Address::repeat_byte(7).into(),
        value: U256::from(1),
        ..Default::default()
    });
    let legacy_cap = TransactionTypeData::Legacy(TransactionLegacyData {
        gas_price: Some(100),
    });
    let decoded = signed_wire(bump_transaction_fees(legacy, 120, Some(&legacy_cap)).unwrap());
    assert_eq!(decoded.gas_price(), Some(100));
}

#[test]
fn estimated_fee_bumps_saturate_without_wrapping_or_panicking() {
    let decoded = signed_wire(bump_transaction_fees(dynamic(101, 4), 120, None).unwrap());
    assert_eq!(decoded.max_fee_per_gas(), 121);
    let tx = bump_transaction_fees(dynamic(u128::MAX - 1, 4), u32::MAX, None).unwrap();
    assert_eq!(tx.max_fee_per_gas(), u128::MAX);
    assert_eq!(tx.max_priority_fee_per_gas(), Some(171_798_691));
    assert!(bump_transaction_fees(dynamic(100, 4), 100, None).is_none());
}

/// Exercise the real confirmation path against a disposable Redis and HTTP RPC
/// boundary. No live credentials, external nodes, or chain broadcasts are used.
#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn stalled_nonce_preserves_capped_unbuildable_and_missing_intents() {
    use crate::{
        eoa::{
            EoaExecutorStore,
            store::{EoaHealth, SubmittedTransactionDehydrated},
        },
        metrics::EoaMetrics,
        webhook::{WebhookJobHandler, WebhookRetryConfig},
    };
    use alloy::providers::ProviderBuilder;
    use engine_core::{
        chain::{RpcCredentials, ThirdwebChainConfig},
        signer::EoaSigner,
    };
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};
    use thirdweb_core::auth::ThirdwebAuth;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use twmq::redis::AsyncCommands;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let methods = Arc::new(Mutex::new(Vec::new()));
    let seen = methods.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (start, length) = loop {
                let mut buffer = [0u8; 4096];
                let read = stream.read(&mut buffer).await.unwrap();
                assert!(read > 0);
                bytes.extend_from_slice(&buffer[..read]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let length = String::from_utf8_lossy(&bytes[..end])
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
            while bytes.len() < start + length {
                let mut buffer = [0u8; 4096];
                let read = stream.read(&mut buffer).await.unwrap();
                assert!(read > 0);
                bytes.extend_from_slice(&buffer[..read]);
            }
            let request: Value = serde_json::from_slice(&bytes[start..start + length]).unwrap();
            let method = request["method"].as_str().unwrap();
            seen.lock().unwrap().push(method.to_owned());
            let mut reply = if method == "eth_getTransactionCount" {
                json!({"result":"0x0"})
            } else if method == "eth_feeHistory" || method == "eth_getBlockByNumber" {
                json!({"error":{"code":-32601,"message":"method not found"}})
            } else {
                json!({"error":{"code":-32000,"message":"temporary estimate unavailable"}})
            };
            reply["jsonrpc"] = json!("2.0");
            reply["id"] = request["id"].clone();
            let body = reply.to_string();
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
        }
    });
    let client = twmq::redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("select disposable Redis"),
    )
    .unwrap();
    let redis = client.get_connection_manager().await.unwrap();
    for scenario in ["capped", "unbuildable", "missing", "unsupported-partial"] {
        methods.lock().unwrap().clear();
        let namespace = format!("fee-ceiling:{}", uuid::Uuid::new_v4());
        let key = PrivateKeySigner::random();
        let eoa = key.address();
        let owner = EoaExecutorStore::new(redis.clone(), Some(namespace.clone()), eoa, 31337, 3600)
            .acquire_eoa_lock_aggressively("fee-owner", EoaMetrics::new(10, 60, 60), &client)
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
        chain.provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(url.parse().unwrap());
        let worker = EoaExecutorWorker {
            store: owner,
            chain,
            eoa,
            chain_id: 31337,
            noop_signing_credential: SigningCredential::PrivateKey(key),
            max_inflight: 1,
            max_recycled_nonces: 1,
            webhook_queue: webhooks,
            signer: Arc::new(EoaSigner::new(
                thirdweb_core::iaw::IAWClient::new("http://127.0.0.1:1").unwrap(),
            )),
            kms_client_cache: moka::future::Cache::new(1),
        };
        worker
            .store
            .update_cached_transaction_count(0)
            .await
            .unwrap();
        worker
            .store
            .update_health_data(&EoaHealth {
                balance: U256::from(10u64.pow(18)),
                balance_threshold: U256::ZERO,
                balance_fetched_at: 1,
                last_confirmation_at: 1,
                last_nonce_movement_at: 1,
                nonce_resets: vec![],
            })
            .await
            .unwrap();
        let tx = SubmittedTransactionDehydrated {
            transaction_id: scenario.into(),
            transaction_hash: alloy::primitives::B256::repeat_byte(7).to_string(),
            nonce: 0,
            submitted_at: 1,
            queued_at: 1,
        };
        let mut conn = redis.clone();
        let (member, nonce) = tx.to_redis_string_with_nonce();
        let _: () = conn
            .zadd(
                worker.store.submitted_transactions_zset_name(),
                &member,
                nonce,
            )
            .await
            .unwrap();
        if scenario != "missing" {
            let request = EoaTransactionRequest {
                transaction_id: scenario.into(),
                chain_id: 31337,
                from: eoa,
                to: Some(Address::repeat_byte(7)),
                value: U256::from(1),
                data: Bytes::new(),
                gas_limit: (scenario != "unbuildable").then_some(21000),
                webhook_options: vec![],
                transaction_type_data: Some(TransactionTypeData::Eip1559(Transaction1559Data {
                    max_fee_per_gas: Some(100),
                    max_priority_fee_per_gas: (scenario != "unsupported-partial").then_some(4),
                })),
                signing_credential: SigningCredential::Iaw {
                    auth_token: "must-not-sign".into(),
                    thirdweb_auth: ThirdwebAuth::SecretKey("unused".into()),
                },
                rpc_credentials: RpcCredentials::Configured,
            };
            if scenario == "unsupported-partial" {
                let error = worker
                    .build_typed_transaction(&request, 0)
                    .await
                    .unwrap_err();
                assert!(
                    matches!(error, EoaExecutorWorkerError::TransactionBuildFailed { message } if message.contains("refusing legacy fallback"))
                );
            }
            let _: () = conn
                .hset_multiple(
                    worker.store.transaction_data_key_name(scenario),
                    &[
                        ("user_request", serde_json::to_string(&request).unwrap()),
                        ("created_at", "1".into()),
                        ("status", "submitted".into()),
                    ],
                )
                .await
                .unwrap();
        }
        let report = worker.confirm_flow().await.unwrap();
        assert_eq!(report.moved_to_success, 0);
        assert_eq!(report.moved_to_pending, 0);
        assert_eq!(
            worker
                .store
                .get_submitted_transactions_count()
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            worker.store.get_pending_transactions_count().await.unwrap(),
            0
        );
        assert!(!worker.store.is_manual_reset_scheduled().await.unwrap());
        let remaining: Vec<String> = conn
            .zrange(worker.store.submitted_transactions_zset_name(), 0, -1)
            .await
            .unwrap();
        assert_eq!(remaining, vec![member]);
        let attempts: u64 = conn
            .llen(worker.store.transaction_attempts_list_name(scenario))
            .await
            .unwrap();
        assert_eq!(attempts, 0, "no replacement signature should be persisted");
        let seen = methods.lock().unwrap().clone();
        assert!(
            !seen.iter().any(|m| m == "eth_sendRawTransaction"),
            "{scenario}: {seen:?}"
        );
        assert_eq!(
            seen.iter().filter(|m| *m == "eth_estimateGas").count(),
            usize::from(scenario == "unbuildable"),
            "a failed bump must not estimate an unrelated fallback self-transfer: {seen:?}"
        );
        assert!(
            seen.iter().all(|m| matches!(
                m.as_str(),
                "eth_getTransactionCount"
                    | "eth_estimateGas"
                    | "eth_feeHistory"
                    | "eth_getBlockByNumber"
            )),
            "{seen:?}"
        );
        if scenario == "unsupported-partial" {
            assert!(seen.iter().any(|m| m == "eth_feeHistory"));
            assert!(!seen.iter().any(|m| m == "eth_gasPrice"));
        }
        drop(worker);
        let keys: Vec<String> = conn.keys(format!("*{namespace}*")).await.unwrap();
        if !keys.is_empty() {
            let _: () = conn.del(keys).await.unwrap();
        }
    }
    server.abort();
}
