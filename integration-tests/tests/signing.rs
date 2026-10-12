use alloy::{
    consensus::{SignableTransaction, TxEip1559, TypedTransaction},
    dyn_abi::TypedData,
    primitives::{Address, B256, Signature, U256, eip191_hash_message},
    rpc::types::{PackedUserOperation, UserOperation},
    signers::local::PrivateKeySigner,
};
use engine_aa_types::VersionedUserOp;
use engine_core::{
    credentials::SigningCredential,
    signer::{AccountSigner, EoaSigner, EoaSigningOptions, MessageFormat},
    userop::{UserOpSigner, UserOpSignerParams},
};
use thirdweb_core::iaw::IAWClient;

fn fixture() -> (EoaSigner, SigningCredential, EoaSigningOptions) {
    let key: PrivateKeySigner = "0000000000000000000000000000000000000000000000000000000000000001"
        .parse()
        .unwrap();
    let options = EoaSigningOptions {
        from: key.address(),
        chain_id: Some(1),
    };
    (
        EoaSigner::new(IAWClient::new("http://127.0.0.1:1").unwrap()),
        SigningCredential::PrivateKey(key),
        options,
    )
}

#[tokio::test]
async fn personal_sign_uses_message_bytes_and_eip191_domain() {
    let (signer, credential, options) = fixture();
    let text = signer
        .sign_message(options.clone(), "hello", MessageFormat::Text, &credential)
        .await
        .unwrap();
    let hex = signer
        .sign_message(
            options.clone(),
            "0x68656c6c6f",
            MessageFormat::Hex,
            &credential,
        )
        .await
        .unwrap();
    assert_eq!(text, hex);
    let signature: Signature = text.parse().unwrap();
    assert_eq!(
        signature
            .recover_address_from_prehash(&eip191_hash_message(b"hello"))
            .unwrap(),
        options.from
    );
    assert_ne!(
        signature
            .recover_address_from_prehash(&eip191_hash_message(b"different"))
            .unwrap(),
        options.from
    );
    assert!(
        signer
            .sign_message(options, "0xzz", MessageFormat::Hex, &credential)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn eoa_signing_rejects_mismatched_sender_for_every_operation() {
    let (signer, credential, mut options) = fixture();
    options.from = Address::repeat_byte(2);
    let typed: TypedData = serde_json::from_value(serde_json::json!({
        "types": {"EIP712Domain": [], "Mail": [{"name":"contents","type":"string"}]},
        "primaryType":"Mail", "domain":{}, "message":{"contents":"hello"}
    }))
    .unwrap();
    let transaction = TypedTransaction::Eip1559(TxEip1559 {
        chain_id: 1,
        ..Default::default()
    });
    assert!(
        signer
            .sign_message(options.clone(), "hello", MessageFormat::Text, &credential)
            .await
            .is_err()
    );
    assert!(
        signer
            .sign_typed_data(options.clone(), &typed, &credential)
            .await
            .is_err()
    );
    assert!(
        signer
            .sign_transaction(options.clone(), &transaction, &credential)
            .await
            .is_err()
    );
    assert!(
        signer
            .sign_authorization(options, 1, Address::repeat_byte(3), 0, &credential)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn transaction_signature_commits_to_nonce_chain_and_destination() {
    let (signer, credential, options) = fixture();
    let tx = TxEip1559 {
        chain_id: 1,
        nonce: 7,
        gas_limit: 21_000,
        max_fee_per_gas: 20_000_000_000,
        max_priority_fee_per_gas: 1_000_000_000,
        value: U256::from(123),
        to: Address::repeat_byte(9).into(),
        ..Default::default()
    };
    let signature: Signature = signer
        .sign_transaction(options.clone(), &tx.clone().into(), &credential)
        .await
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        signature
            .recover_address_from_prehash(&tx.signature_hash())
            .unwrap(),
        options.from
    );
    for changed in [
        TxEip1559 {
            nonce: 8,
            ..tx.clone()
        },
        TxEip1559 {
            chain_id: 42161,
            ..tx.clone()
        },
        TxEip1559 {
            to: Address::repeat_byte(8).into(),
            ..tx.clone()
        },
    ] {
        assert_ne!(
            signature
                .recover_address_from_prehash(&changed.signature_hash())
                .unwrap(),
            options.from
        );
    }
}

fn userop_fixtures() -> Vec<serde_json::Value> {
    serde_json::from_str::<serde_json::Value>(include_str!(
        "../fixtures/userop-signing/fixtures.json"
    ))
    .unwrap()["cases"]
        .as_array()
        .unwrap()
        .clone()
}

fn fixture_userop(fixture: &serde_json::Value) -> VersionedUserOp {
    match fixture["version"].as_str().unwrap() {
        "0.6" => VersionedUserOp::V0_6(
            serde_json::from_value::<UserOperation>(fixture["userop"].clone()).unwrap(),
        ),
        "0.7" => VersionedUserOp::V0_7(
            serde_json::from_value::<PackedUserOperation>(fixture["userop"].clone()).unwrap(),
        ),
        other => panic!("unknown fixture version {other}"),
    }
}

#[tokio::test]
async fn userop_signature_matches_reviewed_contract_digest_and_binds_chain_and_entrypoint() {
    let (_, credential, options) = fixture();
    let signer = UserOpSigner {
        iaw_client: IAWClient::new("http://127.0.0.1:1").unwrap(),
    };
    // The fixtures use manually assembled ABI words and independent Python
    // Keccak. Their verifier digest follows the reviewed AccountCore source.
    for fixture in userop_fixtures() {
        let userop = fixture_userop(&fixture);
        let entrypoint: Address = fixture["entrypoint"].as_str().unwrap().parse().unwrap();
        let factory_address: Address = fixture["factory"].as_str().unwrap().parse().unwrap();
        let raw_hash: B256 = fixture["userOpHash"].as_str().unwrap().parse().unwrap();
        let contract_digest: B256 = fixture["contractDigest"].as_str().unwrap().parse().unwrap();
        assert_eq!(
            userop
                .hash_with_custom_entrypoint(42161, entrypoint)
                .unwrap(),
            raw_hash
        );
        let bytes = signer
            .sign(UserOpSignerParams {
                credentials: credential.clone(),
                entrypoint,
                factory_address,
                userop: userop.clone(),
                signer_address: options.from,
                chain_id: 42161,
            })
            .await
            .unwrap();
        let signature = Signature::try_from(bytes.as_ref()).unwrap();
        assert_eq!(
            signature
                .recover_address_from_prehash(&contract_digest)
                .unwrap(),
            options.from
        );
        for wrong_digest in [
            raw_hash,
            fixture["otherChainDigest"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
            fixture["otherEntrypointDigest"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
            eip191_hash_message(contract_digest.as_slice()), // accidental double prefix
            eip191_hash_message(raw_hash.to_string().as_bytes()), // hex text, not bytes32
        ] {
            assert_ne!(
                signature
                    .recover_address_from_prehash(&wrong_digest)
                    .unwrap(),
                options.from
            );
        }
        assert!(
            signer
                .sign(UserOpSignerParams {
                    credentials: credential.clone(),
                    entrypoint,
                    factory_address,
                    userop,
                    signer_address: Address::repeat_byte(2),
                    chain_id: 42161,
                })
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn local_and_kms_userop_signers_reject_unknown_or_cross_version_profiles_before_signing() {
    use engine_core::{
        constants::{DEFAULT_FACTORY_ADDRESS_V0_6, DEFAULT_FACTORY_ADDRESS_V0_7},
        credentials::AwsKmsCredential,
    };
    let (_, credential, options) = fixture();
    let signer = UserOpSigner {
        iaw_client: IAWClient::new("http://127.0.0.1:1").unwrap(),
    };
    let credentials = [
        credential,
        SigningCredential::Environment {
            address: options.from,
        },
        SigningCredential::AwsKms(AwsKmsCredential {
            access_key_id: "must-not-reach-kms".into(),
            secret_access_key: "must-not-reach-kms".into(),
            key_id: "must-not-reach-kms".into(),
            region: "us-east-1".into(),
            kms_client_cache: None,
        }),
    ];
    for fixture in userop_fixtures() {
        let wrong_version_factory = if fixture["version"] == "0.6" {
            DEFAULT_FACTORY_ADDRESS_V0_7
        } else {
            DEFAULT_FACTORY_ADDRESS_V0_6
        };
        for factory_address in [Address::repeat_byte(0x22), wrong_version_factory] {
            for credential in &credentials {
                let error = signer
                    .sign(UserOpSignerParams {
                        credentials: credential.clone(),
                        entrypoint: fixture["entrypoint"].as_str().unwrap().parse().unwrap(),
                        factory_address,
                        userop: fixture_userop(&fixture),
                        signer_address: options.from,
                        chain_id: 42161,
                    })
                    .await
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("reviewed factory/version profile")
                );
            }
        }
    }
}

#[tokio::test]
async fn iaw_preserves_external_policy_and_request_envelope_for_unknown_factories() {
    use thirdweb_core::auth::ThirdwebAuth;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = Vec::new();
        let mut chunk = [0; 2048];
        let (header_end, content_length) = loop {
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            buffer.extend_from_slice(&chunk[..n]);
            if let Some(offset) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&buffer[..offset]);
                assert!(headers.starts_with("POST /api/v1/enclave-wallet/sign-message "));
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                break (offset + 4, length);
            }
        };
        while buffer.len() < header_end + content_length {
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            buffer.extend_from_slice(&chunk[..n]);
        }
        let request: serde_json::Value =
            serde_json::from_slice(&buffer[header_end..header_end + content_length]).unwrap();
        let response = r#"{"signature":"0x1234"}"#;
        socket.write_all(format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.len(), response
        ).as_bytes()).await.unwrap();
        request
    });
    let fixture = &userop_fixtures()[0];
    let signature = UserOpSigner {
        iaw_client: IAWClient::new(&endpoint).unwrap(),
    }
    .sign(UserOpSignerParams {
        credentials: SigningCredential::Iaw {
            auth_token: "test-token".into(),
            thirdweb_auth: ThirdwebAuth::SecretKey("test-secret".into()),
        },
        entrypoint: fixture["entrypoint"].as_str().unwrap().parse().unwrap(),
        factory_address: Address::repeat_byte(0x22),
        userop: fixture_userop(fixture),
        signer_address: fixture["admin"].as_str().unwrap().parse().unwrap(),
        chain_id: 42161,
    })
    .await
    .unwrap();
    assert_eq!(signature.as_ref(), &[0x12, 0x34]);
    let request = server.await.unwrap();
    assert_eq!(request["messagePayload"]["message"], fixture["userOpHash"]);
    assert_eq!(request["messagePayload"]["isRaw"], true);
    assert_eq!(request["messagePayload"]["chainId"], 42161);
}

#[test]
fn queued_environment_credentials_contain_only_a_public_address() {
    let (_, _, options) = fixture();
    let credential = SigningCredential::Environment {
        address: options.from,
    };
    let json = serde_json::to_string(&credential).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&json).unwrap(),
        serde_json::json!({"Environment":{"address":options.from}})
    );
    let restored: SigningCredential = serde_json::from_str(&json).unwrap();
    assert!(
        matches!(restored, SigningCredential::Environment { address } if address == options.from)
    );
}

// A child process owns its environment: Rust 2024 correctly disallows mutating
// process-wide environment concurrently with an async test runtime.
#[test]
fn environment_signing_survives_queue_roundtrip_and_rejects_rotation() {
    let exe = std::env::current_exe().unwrap();
    for mode in ["matching", "rotated", "missing", "invalid"] {
        let mut child = std::process::Command::new(&exe);
        child
            .args([
                "--exact",
                "environment_worker_child",
                "--ignored",
                "--nocapture",
            ])
            .env("ENGINE_TEST_MODE", mode)
            .env_remove("ENGINE_PRIVATE_KEY");
        match mode {
            "matching" => {
                child.env("ENGINE_PRIVATE_KEY", format!("{:064x}", 1));
            }
            "rotated" => {
                child.env("ENGINE_PRIVATE_KEY", format!("{:064x}", 2));
            }
            "invalid" => {
                child.env("ENGINE_PRIVATE_KEY", "invalid-test-key");
            }
            _ => {}
        }
        assert!(child.status().unwrap().success(), "worker scenario: {mode}");
    }
}

#[test]
#[ignore = "invoked by parent with isolated environment"]
fn environment_worker_child() {
    let mode = std::env::var("ENGINE_TEST_MODE").unwrap();
    let (signer, _, options) = fixture();
    let original = SigningCredential::Environment {
        address: options.from,
    };
    let credential = serde_json::from_slice(&serde_json::to_vec(&original).unwrap()).unwrap();
    let result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(signer.sign_message(
            options.clone(),
            "queued payload",
            MessageFormat::Text,
            &credential,
        ));
    if mode == "matching" {
        let signature: Signature = result.unwrap().parse().unwrap();
        assert_eq!(
            signature
                .recover_address_from_prehash(&eip191_hash_message(b"queued payload"))
                .unwrap(),
            options.from
        );
    } else {
        assert!(result.is_err());
    }
}

#[test]
fn credential_debug_output_redacts_all_secret_material() {
    use engine_core::credentials::AwsKmsCredential;
    use thirdweb_core::auth::ThirdwebAuth;
    let kms = AwsKmsCredential {
        access_key_id: "ACCESS_SENTINEL".into(),
        secret_access_key: "SECRET_SENTINEL".into(),
        key_id: "key-id".into(),
        region: "us-east-1".into(),
        kms_client_cache: None,
    };
    let iaw = SigningCredential::Iaw {
        auth_token: "TOKEN_SENTINEL".into(),
        thirdweb_auth: ThirdwebAuth::SecretKey("RPC_SECRET".into()),
    };
    let output = format!(
        "{:?} {:?} {:?}",
        SigningCredential::AwsKms(kms.clone()),
        kms,
        iaw
    );
    for secret in [
        "ACCESS_SENTINEL",
        "SECRET_SENTINEL",
        "TOKEN_SENTINEL",
        "RPC_SECRET",
    ] {
        assert!(!output.contains(secret));
    }
}
