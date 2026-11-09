//! Actual AWS SDK + Engine credential path against a loopback KMS protocol peer.
//! Known public development keys only. No AWS account, metadata or paid service.
use alloy::{
    consensus::{SignableTransaction, TxEnvelope, TxLegacy, TypedTransaction},
    eips::eip2718::Encodable2718,
    network::TxSigner,
    primitives::{B256, Bytes, TxKind, U256, address},
    signers::{Signer, SignerSync, k256, local::Secp256k1Signer},
};
use axum::{
    Router,
    body::Bytes as BodyBytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use engine_core::{
    credentials::{AwsKmsCredential, KmsClientCache, SigningCredential},
    error::{EngineError, SerialisableAwsSdkError, SerialisableAwsSignerError},
    signer::{AccountSigner, EoaSigner, EoaSigningOptions},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Default)]
struct Observed {
    gets: usize,
    signs: usize,
    invalid_requests: usize,
    by_key: BTreeMap<String, (usize, usize)>,
}

fn key(byte: u8) -> Secp256k1Signer {
    let mut bytes = [0u8; 32];
    bytes[31] = byte;
    Secp256k1Signer::from_bytes(&B256::from(bytes)).unwrap()
}
fn order() -> U256 {
    U256::from_str_radix(
        "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141",
        16,
    )
    .unwrap()
}
async fn kms(
    State(observed): State<Arc<Mutex<Observed>>>,
    headers: HeaderMap,
    bytes: BodyBytes,
) -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
    let request: Value = serde_json::from_slice(&bytes).unwrap();
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let authorization = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let id = request["KeyId"].as_str().unwrap_or("");
    let valid = authorization.starts_with("AWS4-HMAC-SHA256 ")
        && authorization.contains("Credential=fixture-access/")
        && matches!(
            id,
            "alias/low-s"
                | "alias/high-s"
                | "alias/wrong-key"
                | "alias/invalid-der"
                | "alias/invalid-spki"
                | "alias/sign-outage"
                | "alias/public-key-outage"
        );
    let response = if !valid {
        observed.lock().unwrap().invalid_requests += 1;
        json!({"__type":"ValidationException","message":"Invalid fixture request"})
    } else if target.ends_with(".GetPublicKey") {
        let calls = {
            let mut seen = observed.lock().unwrap();
            seen.gets += 1;
            let count = &mut seen.by_key.entry(id.into()).or_default().0;
            *count += 1;
            *count
        };
        if id == "alias/public-key-outage" && calls == 1 {
            return unavailable();
        }
        // SubjectPublicKeyInfo: id-ecPublicKey + secp256k1 + uncompressed point.
        let mut spki =
            alloy::hex::decode("3056301006072a8648ce3d020106052b8104000a03420004").unwrap();
        // Positive aliases have different public keys: swapping two valid key
        // IDs must fail the independently expected address/signature vectors.
        spki.extend_from_slice(
            key(if id == "alias/high-s" { 3 } else { 1 })
                .public_key()
                .as_slice(),
        );
        if id == "alias/invalid-spki" {
            spki = vec![0x30, 0];
        }
        json!({"KeyId":id,"KeySpec":"ECC_SECG_P256K1","KeyUsage":"SIGN_VERIFY",
            "SigningAlgorithms":["ECDSA_SHA_256"],"PublicKey":STANDARD.encode(spki)})
    } else if target.ends_with(".Sign") {
        let digest = request["Message"]
            .as_str()
            .and_then(|m| STANDARD.decode(m).ok())
            .unwrap_or_default();
        if digest.len() != 32
            || request["MessageType"] != "DIGEST"
            || request["SigningAlgorithm"] != "ECDSA_SHA_256"
        {
            observed.lock().unwrap().invalid_requests += 1;
            return (
                StatusCode::BAD_REQUEST,
                [("content-type", "application/x-amz-json-1.1")],
                serde_json::to_vec(
                    &json!({"__type":"ValidationException","message":"Expected a 32-byte digest"}),
                )
                .unwrap(),
            );
        }
        let calls = {
            let mut seen = observed.lock().unwrap();
            seen.signs += 1;
            let count = &mut seen.by_key.entry(id.into()).or_default().1;
            *count += 1;
            *count
        };
        if id == "alias/sign-outage" && calls == 1 {
            return unavailable();
        }
        // Independent libsecp256k1 produces the signature; RustCrypto only
        // serializes its public scalars into the DER representation used by KMS.
        let signature = key(match id {
            "alias/wrong-key" => 2,
            "alias/high-s" => 3,
            _ => 1,
        })
        .sign_hash_sync(&B256::from_slice(&digest))
        .unwrap();
        let s = if id == "alias/high-s" {
            order() - signature.s()
        } else {
            signature.s()
        };
        let der = k256::ecdsa::Signature::from_scalars(
            signature.r().to_be_bytes::<32>(),
            s.to_be_bytes::<32>(),
        )
        .unwrap()
        .to_der();
        let encoded = if id == "alias/invalid-der" {
            STANDARD.encode([0x30, 0])
        } else {
            STANDARD.encode(der.as_bytes())
        };
        json!({"KeyId":id,"SigningAlgorithm":"ECDSA_SHA_256","Signature":encoded})
    } else {
        observed.lock().unwrap().invalid_requests += 1;
        json!({"__type":"UnknownOperationException","message":"Unexpected fixture operation"})
    };
    (
        if valid {
            StatusCode::OK
        } else {
            StatusCode::BAD_REQUEST
        },
        [("content-type", "application/x-amz-json-1.1")],
        serde_json::to_vec(&response).unwrap(),
    )
}

fn unavailable() -> (StatusCode, [(&'static str, &'static str); 1], Vec<u8>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [("content-type", "application/x-amz-json-1.1")],
        serde_json::to_vec(
            &json!({"__type":"KMSInternalException","message":"Fixture service outage"}),
        )
        .unwrap(),
    )
}

fn configured_credential(id: &str) -> AwsKmsCredential {
    let cache: KmsClientCache = moka::future::Cache::new(4);
    AwsKmsCredential::new(
        "fixture-access".into(),
        "fixture-secret".into(),
        id.into(),
        "us-east-1".into(),
        cache,
    )
}

async fn credential(id: &str) -> alloy_signer_aws::AwsSigner {
    configured_credential(id).get_signer(Some(1)).await.unwrap()
}

async fn engine_error_and_recovery_vectors() {
    let engine = EoaSigner::new(thirdweb_core::iaw::IAWClient::new("http://127.0.0.1:1").unwrap());
    let native = key(1);
    let options = EoaSigningOptions {
        from: native.address(),
        chain_id: Some(1),
    };
    let transaction: TypedTransaction = TxLegacy {
        chain_id: Some(1),
        nonce: 9,
        gas_price: 20_000_000_000,
        gas_limit: 21000,
        to: TxKind::Call(address!("3535353535353535353535353535353535353535")),
        value: U256::from(1),
        input: Bytes::new(),
    }
    .into();
    for id in ["alias/sign-outage", "alias/public-key-outage"] {
        let credential = SigningCredential::AwsKms(configured_credential(id));
        let error = engine
            .sign_transaction(options.clone(), &transaction, &credential)
            .await
            .unwrap_err();
        match error {
            EngineError::AwsKmsSignerError {
                error:
                    SerialisableAwsSignerError::Sign {
                        aws_sdk_error: SerialisableAwsSdkError::ServiceError { .. },
                    },
            } => assert_eq!(id, "alias/sign-outage"),
            EngineError::AwsKmsSignerError {
                error:
                    SerialisableAwsSignerError::GetPublicKey {
                        aws_sdk_error: SerialisableAwsSdkError::ServiceError { .. },
                    },
            } => assert_eq!(id, "alias/public-key-outage"),
            other => panic!("SDK outage lost its typed cause: {other:?}"),
        }
        // No endpoint/key/input change and no hidden SDK retries. The same
        // original transaction succeeds once the protocol peer recovers.
        let signature = engine
            .sign_transaction(options.clone(), &transaction, &credential)
            .await
            .unwrap();
        assert_eq!(
            signature,
            native
                .sign_hash_sync(&transaction.signature_hash())
                .unwrap()
                .to_string()
        );
    }
    for id in ["alias/invalid-der", "alias/wrong-key"] {
        let credential = SigningCredential::AwsKms(configured_credential(id));
        let error = engine
            .sign_transaction(options.clone(), &transaction, &credential)
            .await
            .unwrap_err();
        match error {
            EngineError::AwsKmsSignerError {
                error: SerialisableAwsSignerError::K256 { .. },
            } => assert_eq!(id, "alias/invalid-der"),
            EngineError::AwsKmsSignerError {
                error: SerialisableAwsSignerError::SignatureRecoveryFailed,
            } => assert_eq!(id, "alias/wrong-key"),
            other => panic!("Invalid signature lost its permanent typed cause: {other:?}"),
        }
    }
    let credential = SigningCredential::AwsKms(configured_credential("alias/low-s"));
    let wrong_address = EoaSigningOptions {
        from: key(2).address(),
        chain_id: Some(1),
    };
    assert!(matches!(
        engine
            .sign_transaction(wrong_address, &transaction, &credential)
            .await,
        Err(EngineError::ValidationError { .. })
    ));
    let mut wrong_chain = transaction.clone();
    wrong_chain.set_chain_id(2);
    assert!(matches!(
        engine
            .sign_transaction(options, &wrong_chain, &credential)
            .await,
        Err(EngineError::ValidationError { .. })
    ));
}
async fn vectors() {
    for mode in ["alias/low-s", "alias/high-s"] {
        let native = key(if mode == "alias/high-s" { 3 } else { 1 });
        let signer = credential(mode).await;
        assert_eq!(Signer::address(&signer), native.address());
        assert_eq!(signer.chain_id(), Some(1));
        for number in [
            U256::ZERO,
            order() - U256::from(1),
            order(),
            order() + U256::from(1),
            U256::MAX,
        ] {
            let digest = B256::from(number.to_be_bytes::<32>());
            let signature = signer.sign_hash(&digest).await.unwrap();
            assert_eq!(signature, native.sign_hash_sync(&digest).unwrap());
            assert_eq!(
                signature.recover_address_from_prehash(&digest).unwrap(),
                native.address()
            );
            assert!(signature.s() <= order() / U256::from(2));
        }
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce: 9,
            gas_price: 20_000_000_000,
            gas_limit: 21000,
            to: TxKind::Call(address!("3535353535353535353535353535353535353535")),
            value: U256::from(1_000_000_000_000_000_000u64),
            input: Bytes::new(),
        };
        let signature = signer.sign_transaction(&mut tx).await.unwrap();
        let envelope: TxEnvelope = tx.clone().into_signed(signature).into();
        // Compare with independent native signing, including transaction chain ID.
        let reference: TxEnvelope = tx
            .clone()
            .into_signed(native.sign_hash_sync(&tx.signature_hash()).unwrap())
            .into();
        assert_eq!(envelope.encoded_2718(), reference.encoded_2718());
        assert_eq!(
            signer.sign_message(b"engine KMS protocol").await.unwrap(),
            native.sign_message_sync(b"engine KMS protocol").unwrap()
        );
    }
    for mode in ["alias/wrong-key", "alias/invalid-der"] {
        assert!(credential(mode).await.sign_hash(&B256::ZERO).await.is_err());
    }
    let cache: KmsClientCache = moka::future::Cache::new(4);
    let invalid = AwsKmsCredential::new(
        "fixture-access".into(),
        "fixture-secret".into(),
        "alias/invalid-spki".into(),
        "us-east-1".into(),
        cache,
    );
    assert!(invalid.get_signer(Some(1)).await.is_err());
    engine_error_and_recovery_vectors().await;
}

#[test]
fn aws_kms_protocol_vectors() {
    const CHILD: &str = "ENGINE_KMS_PROTOCOL_CHILD";
    if std::env::var_os(CHILD).is_some() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(vectors());
        return;
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let observed = Arc::new(Mutex::new(Observed::default()));
    let state = observed.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                axum::serve(
                    listener,
                    Router::new().route("/", post(kms)).with_state(state),
                )
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
            });
    });
    // A subprocess scopes endpoint overrides; parallel tests never observe them.
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "aws_kms_protocol_vectors", "--nocapture"])
        .env(CHILD, "1")
        .env("AWS_ENDPOINT_URL_KMS", &endpoint)
        .env("AWS_ENDPOINT_URL", &endpoint)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
        .env("AWS_DEFAULTS_MODE", "standard")
        .env("AWS_MAX_ATTEMPTS", "1")
        .env("AWS_CONFIG_FILE", "/dev/null")
        .env("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    let _ = stop.send(());
    server.join().unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let observed = observed.lock().unwrap();
    assert_eq!(observed.invalid_requests, 0);
    assert_eq!(observed.gets, 13);
    assert_eq!(observed.signs, 21);
    assert_eq!(
        observed.by_key,
        BTreeMap::from([
            ("alias/low-s".into(), (3, 7)),
            ("alias/high-s".into(), (1, 7)),
            ("alias/wrong-key".into(), (2, 2)),
            ("alias/invalid-der".into(), (2, 2)),
            ("alias/invalid-spki".into(), (1, 0)),
            ("alias/sign-outage".into(), (2, 2)),
            ("alias/public-key-outage".into(), (2, 1)),
        ])
    );
}
