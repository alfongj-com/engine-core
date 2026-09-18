#![recursion_limit = "256"]

//! Actual HTTP routes with a disposable Redis process and no blockchain workers.
//! Run: REDIS_SERVER_BIN=/path/to/redis-server cargo test -p thirdweb-engine --test api_safety -- --ignored

use std::{
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use engine_core::{
    chain::RpcCredentials,
    credentials::{KmsClientCache, SigningCredential},
    signer::{EoaSigner, SolanaSigner},
    userop::UserOpSigner,
};
use engine_executors::{
    eoa::authorization_cache::EoaAuthorizationCache,
    external_bundler::send::ExternalBundlerSendJobData,
    solana_executor::rpc_cache::{SolanaRpcCache, SolanaRpcUrls},
};
use serde_json::json;
use thirdweb_core::{abi::ThirdwebAbiServiceBuilder, auth::ThirdwebAuth, iaw::IAWClient};
use thirdweb_engine::{
    EngineServer, EngineServerState, ExecutionRouter, MonitoringConfig, QueueConfig, QueueManager,
    SolanRpcConfigData, SolanaConfig, ThirdwebChainService,
};
use twmq::{
    DurableExecution,
    job::{BorrowedJob, JobError, JobStatus},
    redis::AsyncCommands,
};

struct DisposableRedis(Child);
impl Drop for DisposableRedis {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start_redis() -> (DisposableRedis, twmq::redis::Client) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let child =
        Command::new(std::env::var("REDIS_SERVER_BIN").unwrap_or_else(|_| "redis-server".into()))
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect(
                "Install redis-server or set REDIS_SERVER_BIN to run the Redis integration tests",
            );
    let redis = DisposableRedis(child);
    let client = twmq::redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();
    for _ in 0..100 {
        if client.get_multiplexed_async_connection().await.is_ok() {
            return (redis, client);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("Disposable Redis did not become ready");
}

async fn state(client: twmq::redis::Client) -> EngineServerState {
    state_with_rpc(client, thirdweb_engine::EvmRpcConfig::default()).await
}

async fn state_with_rpc(
    client: twmq::redis::Client,
    evm_rpc: thirdweb_engine::EvmRpcConfig,
) -> EngineServerState {
    let config = QueueConfig {
        webhook_workers: 1,
        external_bundler_send_workers: 1,
        userop_confirm_workers: 1,
        eoa_executor_workers: 1,
        solana_executor_workers: 1,
        execution_namespace: Some("api_safety".into()),
        local_concurrency: 1,
        polling_interval_ms: 10,
        lease_duration_seconds: 30,
        monitoring: MonitoringConfig::default(),
        completed_transaction_ttl_seconds: 3600,
    };
    // Port 1 has no test RPC service. An accidental network dependency must fail.
    let rpc_url = "http://127.0.0.1:1";
    let chains = Arc::new(
        ThirdwebChainService::new(
            &thirdweb_engine::ThirdwebConfig {
                client_id: "test".into(),
                secret: "test".into(),
                urls: thirdweb_engine::ThirdwebUrls {
                    rpc: "invalid".into(),
                    bundler: "invalid".into(),
                    paymaster: "invalid".into(),
                    abi_service: rpc_url.into(),
                    iaw_service: rpc_url.into(),
                },
            },
            &evm_rpc,
        )
        .unwrap(),
    );
    let iaw = IAWClient::new(rpc_url).unwrap();
    let userop_signer = Arc::new(UserOpSigner {
        iaw_client: iaw.clone(),
    });
    let eoa_signer = Arc::new(EoaSigner::new(iaw.clone()));
    let solana_signer = Arc::new(SolanaSigner::new(iaw));
    let kms_client_cache: KmsClientCache = moka::future::Cache::new(10);
    let authorization_cache = EoaAuthorizationCache::new(moka::future::Cache::new(10));
    let solana_rpc = SolanRpcConfigData {
        http_url: rpc_url.into(),
        ws_url: "ws://127.0.0.1:1".into(),
    };
    let solana_config = SolanaConfig {
        local: solana_rpc.clone(),
        devnet: solana_rpc.clone(),
        mainnet: solana_rpc,
    };
    let queue_manager = Arc::new(
        QueueManager::new(
            client.clone(),
            &config,
            &solana_config,
            chains.clone(),
            userop_signer.clone(),
            eoa_signer.clone(),
            authorization_cache.clone(),
            kms_client_cache.clone(),
        )
        .await
        .unwrap(),
    );
    let execution_router = Arc::new(ExecutionRouter {
        redis: client.get_connection_manager().await.unwrap(),
        namespace: config.execution_namespace,
        chains: chains.clone(),
        authorization_cache,
        webhook_queue: queue_manager.webhook_queue.clone(),
        external_bundler_send_queue: queue_manager.external_bundler_send_queue.clone(),
        userop_confirm_queue: queue_manager.userop_confirm_queue.clone(),
        eoa_executor_queue: queue_manager.eoa_executor_queue.clone(),
        eip7702_send_queue: queue_manager.eip7702_send_queue.clone(),
        eip7702_confirm_queue: queue_manager.eip7702_confirm_queue.clone(),
        solana_executor_queue: queue_manager.solana_executor_queue.clone(),
        transaction_registry: queue_manager.transaction_registry.clone(),
    });
    EngineServerState {
        chains,
        userop_signer,
        eoa_signer,
        solana_signer,
        queue_manager,
        execution_router,
        abi_service: Arc::new(
            ThirdwebAbiServiceBuilder::new(rpc_url, ThirdwebAuth::SecretKey("test".into()))
                .unwrap()
                .build()
                .unwrap(),
        ),
        solana_rpc_cache: Arc::new(SolanaRpcCache::new(SolanaRpcUrls {
            devnet: rpc_url.into(),
            mainnet: rpc_url.into(),
            local: rpc_url.into(),
        })),
        diagnostic_access_password: Some("test-admin-password".into()),
        metrics_registry: Arc::new(prometheus::Registry::new()),
        kms_client_cache,
    }
}

#[tokio::test]
#[ignore = "requires redis-server; starts its own disposable loopback process"]
async fn public_routes_reject_unauthorized_mutations_and_invalid_execution() {
    let (_redis, redis_client) = start_redis().await;
    let state = state(redis_client.clone()).await;
    let queue = state.queue_manager.external_bundler_send_queue.clone();
    let registry = state.queue_manager.transaction_registry.clone();
    let creds = ThirdwebAuth::SecretKey("test-only".into());
    queue
        .clone()
        .job(ExternalBundlerSendJobData {
            transaction_id: "protected-job".into(),
            chain_id: 31337,
            transactions: vec![],
            execution_options: serde_json::from_value(
                json!({"signerAddress": "0x1111111111111111111111111111111111111111"}),
            )
            .unwrap(),
            signing_credential: SigningCredential::Iaw {
                auth_token: "test-only".into(),
                thirdweb_auth: creds.clone(),
            },
            rpc_credentials: RpcCredentials::Thirdweb(creds),
            webhook_options: vec![],
            pregenerated_nonce: None,
        })
        .with_id("protected-job")
        .push()
        .await
        .unwrap();
    registry
        .set_transaction_queue("protected-job", "external_bundler_send")
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let mut server = EngineServer::new(state.clone()).await;
    server.start(listener).unwrap();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let dedupe = queue.dedupe_set_name();
    let legacy = BorrowedJob::new(
        queue.get_job("protected-job").await.unwrap().unwrap(),
        "test-lease".into(),
    );
    assert!(
        matches!(queue.handler.process(&legacy).await, Err(JobError::Fail(_))),
        "legacy jobs without replay identity must stop before reaching the unavailable RPC"
    );
    for path in [
        "/v1/admin/queue/external_bundler_send/empty-idempotency-set",
        "/v1/transactions/protected-job/cancel",
    ] {
        for password in [None, Some("wrong")] {
            let mut request = http.post(format!("{base_url}{path}"));
            if let Some(password) = password {
                request = request.header("x-diagnostic-access-password", password);
            }
            assert!(request.send().await.unwrap().status().is_client_error());
            assert!(
                conn.sismember::<_, _, bool>(&dedupe, "protected-job")
                    .await
                    .unwrap()
            );
            assert_eq!(queue.count(JobStatus::Pending).await.unwrap(), 1);
            assert_eq!(
                registry
                    .get_transaction_queue("protected-job")
                    .await
                    .unwrap()
                    .as_deref(),
                Some("external_bundler_send")
            );
        }
    }
    // These valid HTTP/credential envelopes must fail validation before RPC/Redis effects.
    for body in [
        json!({"executionOptions": {"type":"EOA", "chainId":31337, "from":"0x1111111111111111111111111111111111111111", "idempotencyKey":"invalid-job"}, "params": []}),
        json!({"executionOptions": {"chainId":31337, "from":"0x1111111111111111111111111111111111111111", "idempotencyKey":"invalid-job"}, "params": [{"to":"0x1111111111111111111111111111111111111111"}]}),
    ] {
        let response = http
            .post(format!("{base_url}/v1/write/transaction"))
            .header("x-thirdweb-client-id", "test")
            .header("x-thirdweb-service-key", "test")
            .header("x-wallet-access-token", "test")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        assert!(response.json::<serde_json::Value>().await.is_ok());
        assert!(
            registry
                .get_transaction_queue("invalid-job")
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(
        http.post(format!("{base_url}/v1/transactions/protected-job/cancel"))
            .header("x-diagnostic-access-password", "test-admin-password")
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    assert_eq!(queue.count(JobStatus::Pending).await.unwrap(), 0);
    assert!(
        registry
            .get_transaction_queue("protected-job")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        http.post(format!(
            "{base_url}/v1/admin/queue/external_bundler_send/empty-idempotency-set"
        ))
        .header("x-diagnostic-access-password", "test-admin-password")
        .send()
        .await
        .unwrap()
        .status()
        .is_success()
    );
    assert!(
        !conn
            .sismember::<_, _, bool>(&dedupe, "protected-job")
            .await
            .unwrap()
    );
    // Admission persists replay identity; duplicate HTTP retries reuse the
    // queued nonce even though a fresh nonce was proposed by the second request.
    let body = json!({
        "executionOptions": {"type":"ERC4337", "chainId":31337,
            "signerAddress":"0x1111111111111111111111111111111111111111", "idempotencyKey":"stable-userop"},
        "params": [{"to":"0x1111111111111111111111111111111111111111"}]
    });
    let mut admitted_nonce = None;
    for _ in 0..2 {
        let response = http
            .post(format!("{base_url}/v1/write/transaction"))
            .header("x-thirdweb-client-id", "test")
            .header("x-thirdweb-service-key", "test")
            .header("x-wallet-access-token", "test")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        let persisted = queue
            .get_job("stable-userop")
            .await
            .unwrap()
            .unwrap()
            .data
            .pregenerated_nonce
            .expect("nonce persisted before workers can broadcast");
        assert_eq!(
            persisted.as_limbs()[0],
            0,
            "fresh ERC-4337 nonce lane starts at sequence zero"
        );
        if let Some(previous) = admitted_nonce {
            assert_eq!(persisted, previous);
        }
        admitted_nonce = Some(persisted);
    }
    assert_eq!(queue.count(JobStatus::Pending).await.unwrap(), 1);

    let eip_queue = state.queue_manager.eip7702_send_queue.clone();
    let eip_body = json!({
        "executionOptions": {"type":"EIP7702", "chainId":31337,
            "from":"0x1111111111111111111111111111111111111111", "idempotencyKey":"stable-calls"},
        "params": [{"to":"0x1111111111111111111111111111111111111111"}]
    });
    let mut admitted_uid = None;
    for _ in 0..2 {
        let response = http
            .post(format!("{base_url}/v1/write/transaction"))
            .header("x-thirdweb-client-id", "test")
            .header("x-thirdweb-service-key", "test")
            .header("x-wallet-access-token", "test")
            .json(&eip_body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        let persisted = eip_queue
            .get_job("stable-calls")
            .await
            .unwrap()
            .unwrap()
            .data
            .nonce
            .expect("UID persisted before broadcast");
        if let Some(previous) = admitted_uid {
            assert_eq!(persisted, previous);
        }
        admitted_uid = Some(persisted);
    }
    assert_eq!(eip_queue.count(JobStatus::Pending).await.unwrap(), 1);
    let mut legacy = eip_queue.get_job("stable-calls").await.unwrap().unwrap();
    legacy.data.nonce = None;
    assert!(matches!(
        eip_queue
            .handler
            .process(&BorrowedJob::new(legacy, "test-lease".into()))
            .await,
        Err(JobError::Fail(_))
    ));
    server.shutdown().await.unwrap();
}

#[test]
#[ignore = "requires redis-server; isolated credentials and actual loopback HTTP"]
fn configured_signer_routes_authenticate_and_queue_only_public_identity() {
    use solana_sdk::{signature::Keypair, signer::Signer};
    const TEST: &str = "configured_signer_routes_authenticate_and_queue_only_public_identity";
    const CHILD: &str = "ENGINE_CONFIGURED_SIGNER_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        use std::io::Write;
        let path = std::env::temp_dir().join(format!(
            "engine-http-solana-{}-{}.json",
            std::process::id(),
            rand::random::<u64>()
        ));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(&path)
            .unwrap()
            .write_all(&serde_json::to_vec(&Keypair::new().to_bytes().to_vec()).unwrap())
            .unwrap();
        let evm_key = alloy::signers::local::PrivateKeySigner::random();
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--ignored", "--nocapture"])
            .env(CHILD, "1")
            .env("ENGINE_SOLANA_KEYPAIR_FILE", &path)
            .env("ENGINE_PRIVATE_KEY", format!("{:#x}", evm_key.to_bytes()))
            .env(
                "ENGINE_SIGNING_TOKEN",
                "isolated-test-operator-token-with-32-bytes",
            )
            .output()
            .unwrap();
        std::fs::remove_file(path).unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use base64::{Engine, engine::general_purpose::STANDARD};
        use engine_solana_core::transaction::{decode_transaction_wire, encode_transaction_wire};
        use solana_sdk::{hash::Hash, message::{Message, VersionedMessage}, signature::Signature, transaction::VersionedTransaction};
        let (_redis, client) = start_redis().await;
        let config = thirdweb_engine::EvmRpcConfig { endpoints: [("31337".into(), engine_core::chain::RpcEndpointConfig { url: "http://127.0.0.1:1".into(), ..Default::default() })].into(), ..Default::default() };
        let state = state_with_rpc(client.clone(), config).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let mut server = EngineServer::new(state.clone()).await;
        server.start(listener).unwrap();
        let http = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
        let solana_credentials = SigningCredential::solana_environment().unwrap();
        let payer = solana_credentials.solana_keypair().unwrap().pubkey();
        let unsigned = VersionedTransaction { signatures: vec![Signature::default()], message: VersionedMessage::Legacy(Message::new_with_blockhash(&[], Some(&payer), &Hash::new_unique())) };
        let body = json!({"transaction": STANDARD.encode(encode_transaction_wire(&unsigned).unwrap()), "executionOptions": {"signerAddress": payer.to_string(), "chainId":"solana:local"}});
        for route in ["/v1/solana/sign/transaction", "/v1/solana/transaction"] {
            for token in [None, Some("wrong-token")] {
                let mut request = http.post(format!("{url}{route}")).json(&body);
                if let Some(token) = token { request = request.header("x-engine-signing-token", token); }
                assert!(request.send().await.unwrap().status().is_client_error());
            }
        }
        let response = http.post(format!("{url}/v1/solana/sign/transaction"))
            .header("x-engine-signing-token", "isolated-test-operator-token-with-32-bytes").json(&body).send().await.unwrap();
        let status = response.status();
        let response: serde_json::Value = response.json().await.unwrap();
        assert_eq!(status, reqwest::StatusCode::OK, "serialized signing must work with the RPC unavailable: {response}");
        let signed = decode_transaction_wire(&STANDARD.decode(response["result"]["signedTransaction"].as_str().unwrap()).unwrap()).unwrap();
        assert_eq!(signed.message.serialize(), unsigned.message.serialize());
        assert!(signed.signatures[0].verify(payer.as_ref(), &signed.message.serialize()));
        assert_eq!(signed.signatures[0].to_string(), response["result"]["signature"]);
        let mut queued = body.clone(); queued["idempotencyKey"] = json!("solana-authenticated");
        let response = http.post(format!("{url}/v1/solana/transaction"))
            .header("x-engine-signing-token", "isolated-test-operator-token-with-32-bytes").json(&queued).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        let retries = futures::future::join_all((0..16).map(|_| {
            http.post(format!("{url}/v1/solana/transaction"))
                .header("x-engine-signing-token", "isolated-test-operator-token-with-32-bytes")
                .json(&queued).send()
        })).await;
        assert!(retries.into_iter().all(|response| response.unwrap().status() == reqwest::StatusCode::ACCEPTED));
        assert_eq!(state.queue_manager.solana_executor_queue.count(JobStatus::Pending).await.unwrap(), 1);
        let mut changed_intent = queued.clone();
        changed_intent["executionOptions"]["commitment"] = json!("confirmed");
        let response = http.post(format!("{url}/v1/solana/transaction"))
            .header("x-engine-signing-token", "isolated-test-operator-token-with-32-bytes")
            .json(&changed_intent).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        assert!(response.text().await.unwrap().contains("different request"));
        let persisted = state.queue_manager.solana_executor_queue.get_job("solana-authenticated").await.unwrap().unwrap();
        assert!(matches!(persisted.data.signing_credential, SigningCredential::SolanaEnvironment { public_key } if public_key == payer));
        let persisted = serde_json::to_string(&persisted.data).unwrap();
        assert!(!persisted.contains("isolated-test-operator-token-with-32-bytes"));
        assert!(!persisted.contains(&std::env::var("ENGINE_SOLANA_KEYPAIR_FILE").unwrap()));
        for field in ["signerAddress", "computeUnitLimit"] {
            let mut invalid = body.clone();
            invalid["idempotencyKey"] = json!("invalid-solana");
            invalid["executionOptions"][field] = if field == "signerAddress" { json!(Keypair::new().pubkey().to_string()) } else { json!(1000) };
            for route in ["/v1/solana/sign/transaction", "/v1/solana/transaction"] {
                let response = http.post(format!("{url}{route}")).header("x-engine-signing-token", "isolated-test-operator-token-with-32-bytes").json(&invalid).send().await.unwrap();
                assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
            }
        }
        assert!(state.queue_manager.solana_executor_queue.get_job("invalid-solana").await.unwrap().is_none());
        let mut retry_request = queued.clone();
        retry_request["idempotencyKey"] = json!("unsupported-solana-retry");
        retry_request["executionOptions"]["maxBlockhashRetries"] = json!(1);
        let response = http.post(format!("{url}/v1/solana/transaction"))
            .header("x-engine-signing-token", "isolated-test-operator-token-with-32-bytes")
            .json(&retry_request).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        assert!(response.text().await.unwrap().contains("maxBlockhashRetries must be 0"));
        assert!(state.queue_manager.solana_executor_queue.get_job("unsupported-solana-retry").await.unwrap().is_none());
        let evm_credentials = SigningCredential::environment().unwrap();
        let evm_address = evm_credentials.local_signer().unwrap().address();
        let evm_body = json!({"executionOptions":{"type":"EOA", "chainId":31337, "from":evm_address, "idempotencyKey":"configured-eoa"}, "params":[{"to":evm_address,"value":"0"}]});
        for token in [None, Some("wrong-token")] {
            let mut request = http.post(format!("{url}/v1/write/transaction")).json(&evm_body);
            if let Some(token) = token { request = request.header("x-engine-signing-token", token); }
            assert!(request.send().await.unwrap().status().is_client_error());
        }
        // Caller-supplied Thirdweb headers cannot select the operator's paid RPC.
        let response = http.post(format!("{url}/v1/write/transaction"))
            .header("x-thirdweb-client-id", "fake").header("x-thirdweb-service-key", "fake").header("x-wallet-access-token", "fake")
            .json(&evm_body).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        let response = http.post(format!("{url}/v1/write/transaction"))
            .header("x-engine-signing-token", "isolated-test-operator-token-with-32-bytes").json(&evm_body).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
        let store = engine_executors::eoa::EoaExecutorStore::new(client.get_connection_manager().await.unwrap(), Some("api_safety".into()), evm_address, 31337, 3600);
        let stored = store.get_transaction_data("configured-eoa").await.unwrap().unwrap();
        let stored_json = serde_json::to_string(&stored).unwrap();
        assert!(stored_json.contains("Configured"));
        assert!(!stored_json.contains("isolated-test-operator-token-with-32-bytes"));
        assert!(!stored_json.contains(&std::env::var("ENGINE_PRIVATE_KEY").unwrap()));
        for token in [None, Some("wrong-token")] {
            let mut request = http.post(format!("{url}/v1/read/contract"))
                .json(&json!({"readOptions":{"chainId":31337}, "params":[]}));
            if let Some(token) = token { request = request.header("x-engine-signing-token", token); }
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
            assert!(response.text().await.unwrap().contains("Configured RPC access requires"));
        }
        // Smart-account signing can query a factory before remote wallet auth.
        // Reject fake IAW credentials before they can spend the configured RPC quota.
        for (route, params) in [
            ("/v1/sign/message", json!([{"message":"test", "format":"text"}])),
            ("/v1/sign/typed-data", json!([{"types":{"EIP712Domain":[],"Test":[{"name":"value","type":"uint256"}]},"primaryType":"Test","domain":{},"message":{"value":"1"}}])),
        ] {
            let response = http.post(format!("{url}{route}"))
                .header("x-thirdweb-client-id", "fake").header("x-thirdweb-service-key", "fake").header("x-wallet-access-token", "fake")
                .json(&json!({"signingOptions":{"type":"ERC4337","chainId":31337,"signerAddress":evm_address}, "params":params}))
                .send().await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let response = response.text().await.unwrap();
            assert!(response.contains("Configured RPC access requires"), "{route}: {response}");
        }
        let mut wrong_chain = evm_body.clone(); wrong_chain["executionOptions"]["chainId"] = json!(1);
        let response = http.post(format!("{url}/v1/write/transaction"))
            .header("x-engine-signing-token", "isolated-test-operator-token-with-32-bytes").json(&wrong_chain).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        server.shutdown().await.unwrap();
    });
}
