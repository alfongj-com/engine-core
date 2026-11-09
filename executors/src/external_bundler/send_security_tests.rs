use super::*;
use alloy::{
    primitives::B256,
    rpc::{client::RpcClient, types::UserOperation},
};
use engine_core::{chain::ThirdwebChainConfig, rpc_clients::BundlerClient};
use serde_json::{Value, json};
use std::sync::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn interrupted_cancellation_completes_only_proven_unsigned_work() {
    use engine_core::recovery::{RecoveryJournal, admission_fingerprint};
    use twmq::redis::AsyncCommands;
    let token = uuid::Uuid::new_v4().simple().to_string();
    let namespace = format!("cancelled_worker_{token}");
    let directory = std::env::temp_dir().join(&namespace);
    let path = directory.join("journal.sqlite");
    let url = std::env::var("TEST_REDIS_URL").expect("select disposable Redis");
    RecoveryJournal::initialize(&path, &url, Some(namespace.clone()))
        .await
        .unwrap();
    let journal = RecoveryJournal::open(&path, &url, Some(namespace.clone()))
        .await
        .unwrap();
    let payload = json!({"transactionId":"cancelled","pregeneratedNonce":"0x1"});
    journal
        .reserve_admission(
            "erc4337",
            "cancelled",
            &admission_fingerprint("erc4337", &payload).unwrap(),
            payload.clone(),
        )
        .await
        .unwrap();
    assert!(
        validate_send_admission(&journal, "cancelled", &payload)
            .await
            .is_ok()
    );
    journal.cancel_unbroadcast("cancelled").await.unwrap();
    drop(journal);
    let journal = RecoveryJournal::open(&path, &url, Some(namespace.clone()))
        .await
        .unwrap();
    assert!(matches!(
        validate_send_admission(&journal, "cancelled", &payload).await,
        Err(JobError::Fail(ExternalBundlerSendError::UserCancelled))
    ));
    assert!(matches!(
        validate_send_admission(
            &journal,
            "cancelled",
            &json!({"transactionId":"cancelled","pregeneratedNonce":"0x2"})
        )
        .await,
        Err(JobError::Nack { .. })
    ));
    let attempted = json!({"transactionId":"attempted","pregeneratedNonce":"0x1"});
    journal
        .reserve_admission(
            "erc4337",
            "attempted",
            &admission_fingerprint("erc4337", &attempted).unwrap(),
            attempted.clone(),
        )
        .await
        .unwrap();
    journal.before_broadcast("erc4337", "attempted", "erc4337:31337:0x1111111111111111111111111111111111111111:0x2222222222222222222222222222222222222222:0", json!({"wire":"retained"})).await.unwrap();
    assert!(!journal.cancel_unbroadcast("attempted").await.unwrap());
    assert!(
        validate_send_admission(&journal, "attempted", &attempted)
            .await
            .is_ok()
    );
    let client = twmq::redis::Client::open(url).unwrap();
    let mut redis = client.get_connection_manager().await.unwrap();
    let keys: Vec<String> = redis.keys(format!("{namespace}:*")).await.unwrap();
    let _: () = redis.del(keys).await.unwrap();
    assert!(
        matches!(
            validate_send_admission(&journal, "cancelled", &payload).await,
            Err(JobError::Nack { .. })
        ),
        "a continuity failure is never proof of cancellation"
    );
    drop(journal);
    std::fs::remove_dir_all(directory).unwrap();
}

fn operation() -> VersionedUserOp {
    VersionedUserOp::V0_6(UserOperation {
        sender: Address::repeat_byte(5),
        nonce: U256::from(7),
        init_code: Bytes::new(),
        call_data: Bytes::from_static(&[1, 2, 3, 4]),
        call_gas_limit: U256::from(100_000),
        verification_gas_limit: U256::from(200_000),
        pre_verification_gas: U256::from(30_000),
        max_fee_per_gas: U256::from(10),
        max_priority_fee_per_gas: U256::from(1),
        paymaster_and_data: Bytes::new(),
        signature: Bytes::from(vec![1; 65]),
    })
}

#[tokio::test]
async fn submitted_operation_survives_error_mismatched_hash_and_retry_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let operation = operation();
    let entrypoint = Address::repeat_byte(4);
    let expected = operation
        .hash_with_custom_entrypoint(31337, entrypoint)
        .unwrap();
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let observed = requests.clone();
    let task = tokio::spawn(async move {
        for result in [
            json!({"error":{"code":-32000,"message":"AA25 invalid account nonce"}}),
            json!({"result":B256::repeat_byte(99)}),
            json!({"result":expected}),
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            let (start, len) = loop {
                let mut buffer = [0; 4096];
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
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
                let mut buffer = [0; 4096];
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
            }
            let request: Value = serde_json::from_slice(&bytes[start..start + len]).unwrap();
            assert_eq!(request["method"], "eth_sendUserOperation");
            observed.lock().unwrap().push(request["params"].clone());
            let mut result = result;
            result["jsonrpc"] = json!("2.0");
            result["id"] = request["id"].clone();
            let body = result.to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
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
    chain.bundler_client = BundlerClient {
        inner: RpcClient::new_http(url.parse().unwrap()),
    };
    let account = DeterminedSmartAccount {
        address: Address::repeat_byte(5),
    };
    for attempts in [100, 1000] {
        assert!(
            matches!(
                send_built_userop(
                    &chain,
                    &account,
                    U256::from(7),
                    &operation,
                    entrypoint,
                    None,
                    attempts
                )
                .await,
                Err(JobError::Nack { .. })
            ),
            "even a nonce error after dispatch is not proof of failure"
        );
    }
    assert_eq!(
        send_built_userop(
            &chain,
            &account,
            U256::from(7),
            &operation,
            entrypoint,
            None,
            1001
        )
        .await
        .unwrap_or_else(|_| panic!("matching locally computed hash must succeed"))
        .as_ref(),
        expected.as_slice()
    );
    task.await.unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests.windows(2).all(|pair| pair[0] == pair[1]),
        "reconciliation retries keep the exact supplied operation and nonce"
    );
}
