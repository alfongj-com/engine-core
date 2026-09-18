use super::*;
use alloy::{providers::Provider, transports::http::reqwest::header::HeaderMap};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{ConnectInfo, State},
    http::{StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
};
use engine_core::{
    chain::{Chain, RpcEndpointConfig},
    error::AlloyRpcErrorToEngineError,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

#[derive(Clone)]
enum Mode {
    Success,
    Nonces,
    HttpError,
    RpcError,
    Malformed,
    MalformedResult,
    OversizedDeclared,
    OversizedChunked,
    Slow,
    Redirect(String),
}
struct Seen {
    request: Value,
    uri: String,
    headers: HeaderMap,
    peer: SocketAddr,
}
struct StubState {
    mode: Mode,
    seen: Mutex<Vec<Seen>>,
}
struct Stub {
    address: SocketAddr,
    state: Arc<StubState>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Stub {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Stub {
    async fn start(mode: Mode) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(StubState {
            mode,
            seen: Mutex::default(),
        });
        let app = Router::new().fallback(any(reply)).with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        Self {
            address,
            state,
            task,
        }
    }
    fn url(&self) -> String {
        format!("http://{}/secret-path?key=secret-query", self.address)
    }
}

async fn reply(
    State(state): State<Arc<StubState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request: Value = serde_json::from_slice(&body).unwrap();
    state.seen.lock().unwrap().push(Seen {
        request: request.clone(),
        uri: uri.to_string(),
        headers,
        peer,
    });
    match &state.mode {
        Mode::Nonces => Json(json!({"jsonrpc":"2.0","id":request["id"],"result":if request["params"][1] == "pending" {"0x63"} else {"0x7"}})).into_response(),
        Mode::Success => Json(json!({"jsonrpc":"2.0","id":request["id"],"result":"0x7"})).into_response(),
        Mode::Slow => { tokio::time::sleep(Duration::from_secs(5)).await; StatusCode::NO_CONTENT.into_response() }
        Mode::HttpError => (StatusCode::UNAUTHORIZED, "secret-path secret-query secret-header secret-token").into_response(),
        Mode::Malformed => Json(json!({"jsonrpc":"secret-token","id":{},"result":"secret-query"})).into_response(),
        Mode::MalformedResult => Json(json!({"jsonrpc":"2.0","id":request["id"],"result":"secret-query secret-token"})).into_response(),
        Mode::OversizedDeclared => Response::builder()
            .header("content-length", (16 * 1024 * 1024 + 1).to_string())
            .body(axum::body::Body::from_stream(futures::stream::pending::<Result<Bytes, std::io::Error>>()))
            .unwrap(),
        Mode::OversizedChunked => Response::new(axum::body::Body::from_stream(
            futures::stream::iter((0..17).map(|_| Ok::<_, std::io::Error>(vec![b'x'; 1024 * 1024])))
        )),
        Mode::RpcError => Json(json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"insufficient funds secret-path secret-query secret-header secret-token","data":{"secret-query":"secret-header"}}})).into_response(),
        Mode::Redirect(destination) => (StatusCode::TEMPORARY_REDIRECT, [("location", destination.as_str())], "secret-query").into_response(),
    }
}

fn thirdweb() -> ThirdwebConfig {
    ThirdwebConfig {
        client_id: "thirdweb-client-secret".into(),
        secret: "thirdweb-private-secret".into(),
        urls: crate::config::ThirdwebUrls {
            rpc: "rpc.invalid".into(),
            bundler: "bundler.invalid".into(),
            paymaster: "paymaster.invalid".into(),
            abi_service: "https://abi.invalid".into(),
            iaw_service: "https://iaw.invalid".into(),
        },
    }
}
fn config(url: String) -> EvmRpcConfig {
    EvmRpcConfig {
        endpoints: BTreeMap::from([(
            "11155111".into(),
            RpcEndpointConfig {
                url,
                headers: BTreeMap::from([
                    ("authorization".into(), "Bearer secret-token".into()),
                    ("x-api-key".into(), "secret-header".into()),
                ]),
                ..Default::default()
            },
        )]),
        ..Default::default()
    }
}
fn assert_redacted(text: &str) {
    for secret in [
        "secret-path",
        "secret-query",
        "secret-header",
        "secret-token",
        "thirdweb-private-secret",
        "thirdweb-client-secret",
    ] {
        assert!(
            !text.contains(secret),
            "credential present in diagnostic output"
        );
    }
}

#[tokio::test]
async fn configured_rpc_routes_headers_and_reuses_connection_across_wallet_cycles() {
    let first = Stub::start(Mode::Success).await;
    let second = Stub::start(Mode::Success).await;
    let mut rpc = config(first.url());
    rpc.endpoints.insert(
        "84532".into(),
        RpcEndpointConfig {
            url: second.url(),
            headers: BTreeMap::new(),
            ..Default::default()
        },
    );
    let service = ThirdwebChainService::new(&thirdweb(), &rpc).unwrap();
    assert!(service.is_configured(11155111));
    assert!(!service.is_configured(1));
    for _ in 0..3 {
        // Simulates separate worker cycles: no provider handle survives this iteration.
        let chain = service.get_chain(11155111).unwrap();
        assert_eq!(chain.provider().get_block_number().await.unwrap(), 7);
        assert_redacted(&format!("{:?}", chain.provider()));
    }
    service
        .get_chain(84532)
        .unwrap()
        .provider()
        .get_block_number()
        .await
        .unwrap();
    let calls = first.state.seen.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert!(
        calls.iter().all(|call| call.peer == calls[0].peer),
        "HTTP connection was not reused"
    );
    for call in calls.iter() {
        assert_eq!(call.uri, "/secret-path?key=secret-query");
        assert_eq!(call.headers["authorization"], "Bearer secret-token");
        assert_eq!(call.headers["x-api-key"], "secret-header");
        for header in ["x-client-id", "x-secret-key", "x-thirdweb-secret-key"] {
            assert!(!call.headers.contains_key(header));
        }
    }
    let calls = second.state.seen.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(!calls[0].headers.contains_key("authorization"));
    assert!(!calls[0].headers.contains_key("x-api-key"));
    assert!(!calls[0].headers.contains_key("x-secret-key"));
}

#[tokio::test]
async fn configured_rpc_redacts_http_protocol_and_json_rpc_errors() {
    for mode in [Mode::HttpError, Mode::Malformed, Mode::RpcError] {
        let stub = Stub::start(mode).await;
        let rpc = config(stub.url());
        assert_redacted(&format!("{rpc:?}"));
        let chain = ThirdwebChainService::new(&thirdweb(), &rpc)
            .unwrap()
            .get_chain(11155111)
            .unwrap();
        let error = chain.provider().get_block_number().await.unwrap_err();
        assert_redacted(&format!("{error:?} {error}"));
        let public = error.to_engine_error(&chain);
        assert_redacted(&format!("{public:?} {public}"));
        assert_redacted(&serde_json::to_string(&public).unwrap());
        if matches!(stub.state.mode, Mode::RpcError) {
            assert!(
                error.to_string().contains("insufficient funds"),
                "RPC classification text must survive redaction"
            );
            assert!(error.is_error_resp());
        }
    }
}

#[tokio::test]
async fn configured_rpc_does_not_follow_redirects_or_forward_credentials() {
    let destination = Stub::start(Mode::Success).await;
    let source = Stub::start(Mode::Redirect(destination.url())).await;
    let service = ThirdwebChainService::new(&thirdweb(), &config(source.url())).unwrap();
    let error = service
        .get_chain(11155111)
        .unwrap()
        .provider()
        .get_block_number()
        .await
        .unwrap_err();
    assert_redacted(&error.to_string());
    assert!(destination.state.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn configured_rpc_timeout_is_bounded_and_redacted() {
    let stub = Stub::start(Mode::Slow).await;
    let mut rpc = config(stub.url());
    rpc.request_timeout_ms = 40;
    let service = ThirdwebChainService::new(&thirdweb(), &rpc).unwrap();
    let chain = service.get_chain(11155111).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), chain.provider().get_block_number())
        .await
        .expect("request timeout not enforced")
        .unwrap_err();
    assert_redacted(&format!("{error:?} {error}"));
    assert_redacted(&serde_json::to_string(&error.to_engine_error(&chain)).unwrap());
}

#[test]
fn invalid_config_is_rejected_without_echoing_credentials() {
    for url in [
        "not a URL secret-query",
        "ftp://example.com/secret-path",
        "https://secret-token@example.com",
        "https://example.com/#secret-query",
    ] {
        let error = ThirdwebChainService::new(&thirdweb(), &config(url.into()))
            .err()
            .expect("invalid endpoint accepted");
        assert_redacted(&format!("{error:?} {error}"));
    }
    for (header, value) in [
        ("host", "secret-header"),
        ("x-api-key", "secret-header\ninvalid"),
        ("bad name", "secret-header"),
    ] {
        let mut rpc = config("https://example.com".into());
        rpc.endpoints
            .get_mut("11155111")
            .unwrap()
            .headers
            .insert(header.into(), value.into());
        let error = ThirdwebChainService::new(&thirdweb(), &rpc)
            .err()
            .expect("invalid header accepted");
        assert_redacted(&format!("{error:?} {error}"));
    }
}

#[test]
fn endpoint_configuration_accepts_environment_style_paths() {
    let source = HashMap::from([
        (
            "APP__EVM_RPC__ENDPOINTS__11155111__URL".into(),
            "http://127.0.0.1:8545".into(),
        ),
        (
            "APP__EVM_RPC__ENDPOINTS__11155111__HEADERS__AUTHORIZATION".into(),
            "Bearer secret-token".into(),
        ),
        (
            "APP__EVM_RPC__ENDPOINTS__11155111__USE_PENDING_FOR_PRECONFIRMATION".into(),
            "true".into(),
        ),
        ("APP__EVM_RPC__REQUEST_TIMEOUT_MS".into(), "1234".into()),
    ]);
    let parsed: EvmRpcConfig = ::config::Config::builder()
        .add_source(
            ::config::Environment::with_prefix("app")
                .separator("__")
                .source(Some(source)),
        )
        .build()
        .unwrap()
        .get("evm_rpc")
        .unwrap();
    assert!(parsed.endpoints["11155111"].use_pending_for_preconfirmation);
    assert_eq!(parsed.request_timeout_ms, 1234);
    assert_eq!(
        parsed.endpoints["11155111"].headers["authorization"],
        "Bearer secret-token"
    );
    assert_redacted(&format!("{parsed:?}"));
}

#[test]
fn legacy_provider_is_also_reused_and_local_override_wins() {
    let service = ThirdwebChainService::new(&thirdweb(), &EvmRpcConfig::default()).unwrap();
    let first = service.get_chain(1).unwrap();
    let second = service.get_chain(1).unwrap();
    assert!(std::ptr::eq(
        first.provider().client(),
        second.provider().client()
    ));
    let mut rpc = config("http://127.0.0.1:9876/custom".into());
    let endpoint = rpc.endpoints.remove("11155111").unwrap();
    rpc.endpoints.insert("31337".into(), endpoint);
    let service = ThirdwebChainService::new(&thirdweb(), &rpc).unwrap();
    assert_eq!(
        service.get_chain(31337).unwrap().rpc_url().port(),
        Some(9876)
    );
}

#[tokio::test]
async fn base_pending_mempool_count_requires_explicit_preconfirmation_capability() {
    use engine_executors::FlashblocksTransactionCount;
    let stub = Stub::start(Mode::Nonces).await;
    for enabled in [false, true] {
        let mut rpc = config(stub.url());
        let mut endpoint = rpc.endpoints.remove("11155111").unwrap();
        endpoint.use_pending_for_preconfirmation = enabled;
        rpc.endpoints.insert("84532".into(), endpoint);
        let service = ThirdwebChainService::new(&thirdweb(), &rpc).unwrap();
        let chain = service.get_chain(84532).unwrap();
        let counts = chain
            .provider()
            .get_transaction_counts_with_flashblocks_support(
                alloy::primitives::Address::ZERO,
                chain.use_pending_for_preconfirmation(),
            )
            .await
            .unwrap();
        assert_eq!(counts.latest, 7);
        assert_eq!(counts.preconfirmed, if enabled { 99 } else { 7 });
        let mut calls = stub.state.seen.lock().unwrap();
        assert_eq!(calls.len(), if enabled { 2 } else { 1 });
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.request["params"][1] == "pending")
                .count(),
            usize::from(enabled)
        );
        calls.clear();
    }
}

#[tokio::test]
async fn configured_rpc_withholds_post_transport_typed_result_errors() {
    let stub = Stub::start(Mode::MalformedResult).await;
    let chain = ThirdwebChainService::new(&thirdweb(), &config(stub.url()))
        .unwrap()
        .get_chain(11155111)
        .unwrap();
    let error = chain.provider().get_block_number().await.unwrap_err();
    // Envelope parsing succeeds. Alloy's later quantity decoder must fail,
    // without its raw result body reaching our serialized/logged error boundary.
    assert!(matches!(
        error,
        alloy::transports::RpcError::DeserError { .. }
    ));
    let public = error.to_engine_error(&chain);
    assert_redacted(&format!("{public:?} {public}"));
    assert_redacted(&serde_json::to_string(&public).unwrap());
    assert_redacted(&engine_core::error::rpc_error_diagnostic(&error));
}

#[tokio::test]
async fn configured_rpc_bounds_declared_and_chunked_response_bodies() {
    for mode in [Mode::OversizedDeclared, Mode::OversizedChunked] {
        let stub = Stub::start(mode).await;
        let chain = ThirdwebChainService::new(&thirdweb(), &config(stub.url()))
            .unwrap()
            .get_chain(11155111)
            .unwrap();
        let error =
            tokio::time::timeout(Duration::from_secs(2), chain.provider().get_block_number())
                .await
                .expect("oversized response waited for the request timeout or unbounded body")
                .unwrap_err();
        assert!(error.to_string().contains("16 MiB limit"), "{error}");
        assert_redacted(&format!("{error:?} {error}"));
        assert_redacted(&serde_json::to_string(&error.to_engine_error(&chain)).unwrap());
        assert_eq!(
            stub.state.seen.lock().unwrap().len(),
            1,
            "limit errors must not transparently retry"
        );
    }
}
