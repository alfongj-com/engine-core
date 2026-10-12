use super::*;
use alloy::{
    consensus::{SignableTransaction, TxEip1559, TxEip7702},
    eips::eip7702::Authorization,
    primitives::{Address, Bytes, TxKind, U256},
    signers::{SignerSync, local::PrivateKeySigner},
};
use engine_core::{
    chain::RpcCredentials,
    credentials::SigningCredential,
    transaction::{Transaction7702Data, TransactionTypeData},
};

fn signer() -> PrivateKeySigner {
    "1111111111111111111111111111111111111111111111111111111111111111"
        .parse()
        .unwrap()
}
fn signed(tx: TypedTransaction, signer: &PrivateKeySigner) -> Signed<TypedTransaction> {
    let signature = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    tx.into_signed(signature)
}
fn plain(sender: Address) -> TxEip1559 {
    TxEip1559 {
        chain_id: 31337,
        nonce: 4,
        gas_limit: 21000,
        max_fee_per_gas: 10,
        max_priority_fee_per_gas: 1,
        to: TxKind::Call(sender),
        value: U256::ZERO,
        input: Bytes::new(),
        access_list: Default::default(),
    }
}
fn request(sender: Address) -> EoaTransactionRequest {
    EoaTransactionRequest {
        transaction_id: "intent".into(),
        chain_id: 31337,
        from: sender,
        to: Some(sender),
        value: U256::ZERO,
        data: Bytes::new(),
        gas_limit: None,
        webhook_options: vec![],
        signing_credential: SigningCredential::Environment { address: sender },
        rpc_credentials: RpcCredentials::Configured,
        transaction_type_data: None,
    }
}

#[test]
fn signed_noop_cannot_hide_chain_signer_recipient_value_calldata_or_authorization_changes() {
    let signer = signer();
    let sender = signer.address();
    let base = plain(sender);
    assert!(validate_noop_wire(31337, sender, &signed(base.clone().into(), &signer)).is_ok());
    let mut candidates = vec![];
    let mut changed = base.clone();
    changed.chain_id = 1;
    candidates.push(changed);
    let mut changed = base.clone();
    changed.to = TxKind::Call(Address::repeat_byte(2));
    candidates.push(changed);
    let mut changed = base.clone();
    changed.value = U256::from(1);
    candidates.push(changed);
    let mut changed = base.clone();
    changed.input = Bytes::from_static(&[1, 2, 3, 4]);
    candidates.push(changed);
    for candidate in candidates {
        assert!(validate_noop_wire(31337, sender, &signed(candidate.into(), &signer)).is_err());
    }
    let other = PrivateKeySigner::random();
    assert!(validate_noop_wire(31337, sender, &signed(base.into(), &other)).is_err());
    let authorization = Authorization {
        chain_id: U256::from(31337),
        address: Address::repeat_byte(9),
        nonce: 0,
    };
    let auth_signature = signer
        .sign_hash_sync(&authorization.signature_hash())
        .unwrap();
    let delegated = TxEip7702 {
        chain_id: 31337,
        nonce: 4,
        gas_limit: 100000,
        max_fee_per_gas: 10,
        max_priority_fee_per_gas: 1,
        to: sender,
        value: U256::ZERO,
        input: Bytes::new(),
        access_list: Default::default(),
        authorization_list: vec![authorization.into_signed(auth_signature)],
    };
    assert!(validate_noop_wire(31337, sender, &signed(delegated.into(), &signer)).is_err());
}

#[test]
fn eoa_wire_requires_exact_authorization_intent_and_valid_signer() {
    let signer = signer();
    let sender = signer.address();
    let mut request = request(sender);
    let auth = Authorization {
        chain_id: U256::from(31337),
        address: Address::repeat_byte(9),
        nonce: 0,
    };
    let signature = signer.sign_hash_sync(&auth.signature_hash()).unwrap();
    let auth = auth.into_signed(signature);
    let tx = TxEip7702 {
        chain_id: 31337,
        nonce: 4,
        gas_limit: 100000,
        max_fee_per_gas: 10,
        max_priority_fee_per_gas: 1,
        to: sender,
        value: U256::ZERO,
        input: Bytes::new(),
        access_list: Default::default(),
        authorization_list: vec![auth.clone()],
    };
    let signed_tx = signed(tx.into(), &signer);
    assert!(
        validate_eoa_wire(&request, &signed_tx).is_err(),
        "an unrequested delegation changes authority"
    );
    request.transaction_type_data = Some(TransactionTypeData::Eip7702(Transaction7702Data {
        authorization_list: Some(vec![auth]),
        max_fee_per_gas: None,
        max_priority_fee_per_gas: None,
    }));
    assert!(validate_eoa_wire(&request, &signed_tx).is_ok());
    assert!(
        validate_eoa_wire(&request, &signed(plain(sender).into(), &signer)).is_err(),
        "requested delegation cannot disappear"
    );
    request.value = U256::from(1);
    assert!(validate_eoa_wire(&request, &signed_tx).is_err());
}

/// Recipient delegation is not permission to remove signed authorizations for
/// that recipient or for other authorities in the same admitted transaction.
#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn builder_preserves_authorizations_even_when_recipient_is_already_delegated() {
    use crate::{
        eoa::{EoaExecutorStore, worker::EoaExecutorWorker},
        metrics::EoaMetrics,
        webhook::{WebhookJobHandler, WebhookRetryConfig},
    };
    use alloy::providers::ProviderBuilder;
    use engine_core::{chain::ThirdwebChainConfig, signer::EoaSigner};
    use serde_json::{Value, json};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use twmq::redis::AsyncCommands;

    let target = Address::repeat_byte(9);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
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
            observed.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request["method"], "eth_getCode");
            let reply = json!({"jsonrpc":"2.0", "id":request["id"],
                "result":format!("0xef0100{}", hex::encode(target))})
            .to_string();
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",reply.len(),reply).as_bytes()).await.unwrap();
        }
    });
    let client = twmq::redis::Client::open(
        std::env::var("TEST_REDIS_URL").expect("select disposable Redis"),
    )
    .unwrap();
    let redis = client.get_connection_manager().await.unwrap();
    let namespace = format!("auth-intent:{}", uuid::Uuid::new_v4());
    let key = signer();
    let sender = key.address();
    let recipient = PrivateKeySigner::random();
    let other_authority = PrivateKeySigner::random();
    let store = EoaExecutorStore::new(redis.clone(), Some(namespace.clone()), sender, 31337, 3600)
        .acquire_eoa_lock_aggressively("auth-owner", EoaMetrics::new(10, 60, 60), &client)
        .await
        .unwrap();
    let webhook_name = format!("{namespace}:webhooks");
    let webhooks = Arc::new(
        twmq::Queue::builder()
            .redis_connection_manager(redis.clone(), client.clone())
            .name(webhook_name.clone())
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
        store,
        chain,
        eoa: sender,
        chain_id: 31337,
        noop_signing_credential: SigningCredential::PrivateKey(key.clone()),
        max_inflight: 1,
        broadcast_concurrency: 32,
        max_recycled_nonces: 1,
        webhook_queue: webhooks,
        signer: Arc::new(EoaSigner::new(
            thirdweb_core::iaw::IAWClient::new("http://127.0.0.1:1").unwrap(),
        )),
        kms_client_cache: moka::future::Cache::new(1),
    };
    let mut request = request(sender);
    request.to = Some(recipient.address());
    request.gas_limit = Some(100_000);
    request.signing_credential = SigningCredential::PrivateKey(key);
    let authorizations: Vec<_> = [&recipient, &other_authority]
        .into_iter()
        .map(|authority| {
            let auth = Authorization {
                chain_id: U256::from(31337),
                address: target,
                nonce: 2,
            };
            let signature = authority.sign_hash_sync(&auth.signature_hash()).unwrap();
            auth.into_signed(signature)
        })
        .collect();
    request.transaction_type_data = Some(TransactionTypeData::Eip7702(Transaction7702Data {
        authorization_list: Some(authorizations.clone()),
        max_fee_per_gas: Some(10),
        max_priority_fee_per_gas: Some(1),
    }));
    let typed = worker.build_typed_transaction(&request, 4).await.unwrap();
    assert_eq!(typed.authorization_list(), Some(authorizations.as_slice()));
    let signed = worker
        .sign_transaction(typed, &request.signing_credential)
        .await
        .unwrap();
    validate_eoa_wire(&request, &signed).unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "building immutable authorizations must not depend on recipient code"
    );
    server.abort();
    worker.store.release_eoa_lock().await.unwrap();
    let mut conn = redis;
    for pattern in [format!("{namespace}:*"), format!("twmq:{webhook_name}:*")] {
        let keys: Vec<String> = conn.keys(pattern).await.unwrap();
        if !keys.is_empty() {
            let _: () = conn.del(keys).await.unwrap();
        }
    }
}

#[test]
fn deserialized_cached_hash_cannot_redefine_eoa_or_noop_identity() {
    let signer = signer();
    let valid = signed(plain(signer.address()).into(), &signer);
    let mut encoded = serde_json::to_value(&valid).unwrap();
    encoded["hash"] = serde_json::json!(alloy::primitives::B256::repeat_byte(99));
    let tampered: Signed<TypedTransaction> = serde_json::from_value(encoded).unwrap();
    assert_ne!(
        tampered.hash(),
        valid.hash(),
        "fixture must exercise unchecked cached-hash deserialization"
    );
    assert!(validate_eoa_wire(&request(signer.address()), &tampered).is_err());
    assert!(validate_noop_wire(31337, signer.address(), &tampered).is_err());
    assert!(validate_eoa_wire(&request(signer.address()), &valid).is_ok());
}
