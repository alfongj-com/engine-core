use super::*;
use serde_json::json;
use std::sync::atomic::AtomicUsize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const SECRET: &str = "RPC_SECRET_SENTINEL";

async fn server(
    reply: impl Fn(Value) -> Vec<u8> + Send + Sync + 'static,
) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/private/{SECRET}?apiKey={SECRET}",
        listener.local_addr().unwrap()
    );
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (offset, length) = loop {
                let mut chunk = [0; 4096];
                let length = stream.read(&mut chunk).await.unwrap();
                assert!(length > 0);
                bytes.extend_from_slice(&chunk[..length]);
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
            while bytes.len() < offset + length {
                let mut chunk = [0; 4096];
                let length = stream.read(&mut chunk).await.unwrap();
                assert!(length > 0);
                bytes.extend_from_slice(&chunk[..length]);
            }
            let request = serde_json::from_slice(&bytes[offset..offset + length]).unwrap();
            seen.fetch_add(1, Ordering::SeqCst);
            // Oversize rejection is allowed to close the connection mid-response.
            let _ = stream.write_all(&reply(request)).await;
        }
    });
    (url, count, task)
}

fn response(status: &str, body: &str, extra: &str) -> Vec<u8> {
    format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n{body}", body.len()).into_bytes()
}

async fn call(sender: &BoundedRpcSender) -> ClientResult<Value> {
    sender.send(RpcRequest::GetBlockHeight, json!([])).await
}

fn assert_redacted(error: &ClientError) {
    assert!(!format!("{error:?} {error}").contains(SECRET));
    assert!(!format!("{error:?}").contains("127.0.0.1"));
}

#[tokio::test]
async fn rpc_metrics_count_real_http_outcomes_and_cancellation_without_secret_labels() {
    // The exporter is process-global. A dedicated child verifies exact counts
    // without resetting production globals or racing other transport fixtures.
    const CHILD: &str = "ENGINE_SOLANA_RPC_METRICS_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "solana_executor::rpc_cache::tests::rpc_metrics_count_real_http_outcomes_and_cancellation_without_secret_labels",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let params = || {
        json!([["SIGNATURE_SECRET_A", "SIGNATURE_SECRET_B", "SIGNATURE_SECRET_C"],
            {"searchTransactionHistory": true}])
    };
    let (url, seen, success_server) = server(|request| {
        response(
            "200 OK",
            &json!({"jsonrpc":"2.0","id":request["id"],
                "result":{"context":{"slot":1},"value":[null,null,null]}})
            .to_string(),
            "",
        )
    })
    .await;
    let sender = BoundedRpcSender::new(url).with_chain_id(SolanaChainId::SolanaMainnet);
    sender
        .send(RpcRequest::GetSignatureStatuses, params())
        .await
        .unwrap();
    sender
        .send(RpcRequest::Custom { method: SECRET }, json!([]))
        .await
        .unwrap();
    assert_eq!(seen.load(Ordering::SeqCst), 2);
    success_server.abort();

    let (url, seen, error_server) = server(|_| response("429 Too Many Requests", SECRET, "")).await;
    let sender = BoundedRpcSender::new(url).with_chain_id(SolanaChainId::SolanaMainnet);
    assert!(
        sender
            .send(RpcRequest::GetSignatureStatuses, params())
            .await
            .is_err()
    );
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "instrumentation cannot retry"
    );
    error_server.abort();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/{SECRET}", listener.local_addr().unwrap());
    let (arrived, received) = tokio::sync::oneshot::channel();
    let delayed_server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4096];
        assert!(stream.read(&mut bytes).await.unwrap() > 0);
        arrived.send(()).unwrap();
        std::future::pending::<()>().await;
        drop(stream);
    });
    let pending_params = params();
    let pending = tokio::spawn(async move {
        BoundedRpcSender::new(url)
            .with_chain_id(SolanaChainId::SolanaMainnet)
            .send(RpcRequest::GetSignatureStatuses, pending_params)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), received)
        .await
        .unwrap()
        .unwrap();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    delayed_server.abort();

    let exported = crate::metrics::export_default_metrics().unwrap();
    for secret in [SECRET, "SIGNATURE_SECRET", "127.0.0.1", "apiKey"] {
        assert!(
            !exported.contains(secret),
            "sensitive data in metric labels"
        );
    }
    let sample = |name: &str, labels: &[&str]| -> f64 {
        let values: Vec<f64> = exported
            .lines()
            .filter(|line| line.starts_with(&format!("{name}{{")))
            .filter(|line| labels.iter().all(|label| line.contains(label)))
            .map(|line| line.rsplit_once(' ').unwrap().1.parse().unwrap())
            .collect();
        assert_eq!(values.len(), 1, "missing or duplicated series: {name}");
        values[0]
    };
    for outcome in ["success", "error", "cancelled"] {
        assert_eq!(
            sample(
                "tw_engine_executor_operation_duration_seconds_count",
                &[
                    "executor_type=\"solana\"",
                    "chain_id=\"solana:mainnet\"",
                    "phase=\"rpc_get_signature_statuses\"",
                    &format!("outcome=\"{outcome}\""),
                ],
            ),
            1.0
        );
    }
    assert_eq!(
        sample(
            "tw_engine_executor_operation_duration_seconds_count",
            &["phase=\"rpc_other\"", "outcome=\"success\""],
        ),
        1.0
    );
    for (suffix, expected) in [("count", 3.0), ("sum", 9.0)] {
        assert_eq!(
            sample(
                &format!("tw_engine_executor_solana_status_request_signatures_{suffix}"),
                &["chain_id=\"solana:mainnet\""],
            ),
            expected
        );
    }
}

#[tokio::test]
async fn http_429_is_one_attempt_and_error_debug_withholds_url_headers_and_body() {
    let (url, count, task) = server(|_| {
        response(
            "429 Too Many Requests",
            SECRET,
            &format!("Retry-After: 0\r\nX-Provider-Token: {SECRET}\r\n"),
        )
    })
    .await;
    let sender = BoundedRpcSender::new(url);
    let error = call(&sender).await.unwrap_err();
    assert!(error.to_string().contains("429"));
    assert_redacted(&error);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(sender.get_transport_stats().request_count, 1);
    assert_eq!(
        sender.get_transport_stats().rate_limited_time,
        Duration::ZERO
    );
    assert!(!sender.url().contains(SECRET));
    task.abort();
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let location = format!("http://{}/{SECRET}", target.local_addr().unwrap());
    let (url, count, task) = server(move |_| {
        response(
            "307 Temporary Redirect",
            SECRET,
            &format!("Location: {location}\r\n"),
        )
    })
    .await;
    let error = call(&BoundedRpcSender::new(url)).await.unwrap_err();
    assert!(error.to_string().contains("307"));
    assert_redacted(&error);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), target.accept())
            .await
            .is_err()
    );
    task.abort();
}

#[tokio::test]
async fn oversized_chunked_body_is_bounded_without_content_length() {
    let (url, count, task) = server(|_| {
        let mut bytes =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
        let chunk = vec![b'x'; 64 * 1024];
        for _ in 0..MAX_RESPONSE_BYTES / chunk.len() {
            bytes.extend_from_slice(b"10000\r\n");
            bytes.extend_from_slice(&chunk);
            bytes.extend_from_slice(b"\r\n");
        }
        bytes.extend_from_slice(b"1\r\nx\r\n0\r\n\r\n");
        bytes
    })
    .await;
    let error = call(&BoundedRpcSender::new(url)).await.unwrap_err();
    assert!(error.to_string().contains("exceeds 16 MiB"), "{error}");
    assert_redacted(&error);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test]
async fn declared_oversize_is_rejected_before_waiting_for_body() {
    let (url, _, task) = server(|_| {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            MAX_RESPONSE_BYTES + 1
        )
        .into_bytes()
    })
    .await;
    let error = call(&BoundedRpcSender::new(url)).await.unwrap_err();
    assert!(error.to_string().contains("exceeds 16 MiB"), "{error}");
    task.abort();
}

#[tokio::test]
async fn response_envelope_and_provider_errors_are_validated_without_echoing_data() {
    for bad in [
        json!({"jsonrpc":"2.0","id":100,"result":42}),
        json!({"jsonrpc":"1.0","id":1,"result":42}),
        json!({"jsonrpc":"2.0","id":1,"result":42,"error":{"code":-32002,"message":SECRET}}),
        json!({"jsonrpc":"2.0","id":1,"error":{"message":SECRET}}),
    ] {
        let (url, _, task) = server(move |_| response("200 OK", &bad.to_string(), "")).await;
        let error = call(&BoundedRpcSender::new(url)).await.unwrap_err();
        assert!(error.to_string().contains("envelope"));
        assert_redacted(&error);
        task.abort();
    }
    let (url, _, task) = server(|request| response("200 OK", &json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32002,"message":SECRET,"data":{"logs":[SECRET]}}}).to_string(), "")).await;
    let error = call(&BoundedRpcSender::new(url)).await.unwrap_err();
    assert!(matches!(
        error.kind(),
        ErrorKind::RpcError(RpcError::RpcResponseError {
            code: -32002,
            data: RpcResponseErrorData::Empty,
            ..
        })
    ));
    assert_redacted(&error);
    task.abort();
}

#[tokio::test]
async fn invalid_json_and_connection_failures_are_secret_free() {
    let (url, _, task) = server(|_| response("200 OK", SECRET, "")).await;
    let error = call(&BoundedRpcSender::new(url.clone())).await.unwrap_err();
    assert!(error.to_string().contains("invalid JSON"));
    assert_redacted(&error);
    task.abort();
    let _ = task.await;
    let error = call(&BoundedRpcSender::new(url)).await.unwrap_err();
    assert!(error.to_string().contains("connection failed"));
    assert_redacted(&error);
}

#[tokio::test]
async fn typed_client_uses_bounded_sender_and_preserves_null_results() {
    let (url, count, task) = server(|request| {
        response(
            "200 OK",
            &json!({"jsonrpc":"2.0","id":request["id"],"result":null}).to_string(),
            "",
        )
    })
    .await;
    let client = RpcClient::new_sender(BoundedRpcSender::new(url), RpcClientConfig::default());
    let result: Option<Value> = client
        .send(RpcRequest::GetTransaction, json!([]))
        .await
        .unwrap();
    assert!(result.is_none());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test]
async fn genesis_validation_caches_only_success_by_endpoint_and_expected_cluster() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in = calls.clone();
    let (url, _, task) = server(move |request| {
        assert_eq!(request["method"], "getGenesisHash");
        let n = calls_in.fetch_add(1, Ordering::SeqCst);
        let body = match n {
            0 => {
                json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":SECRET}})
            }
            1 => json!({"jsonrpc":"2.0","id":request["id"],"result":MAINNET_GENESIS}),
            _ => json!({"jsonrpc":"2.0","id":request["id"],"result":DEVNET_GENESIS}),
        };
        response("200 OK", &body.to_string(), "")
    })
    .await;
    let mut cache = SolanaRpcCache::new(SolanaRpcUrls {
        devnet: url.clone(),
        mainnet: url.clone(),
        local: "http://127.0.0.1:1".into(),
    });
    for _ in 0..2 {
        let Err(error) = cache.get_verified(SolanaChainId::SolanaDevnet).await else {
            panic!("error/wrong genesis must not qualify")
        };
        assert!(!format!("{error:?}").contains(SECRET));
    }
    let results =
        futures::future::join_all((0..16).map(|_| cache.get_verified(SolanaChainId::SolanaDevnet)))
            .await;
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "failures retried; concurrent successes coalesce"
    );
    assert!(
        cache
            .get_verified(SolanaChainId::SolanaMainnet)
            .await
            .is_err(),
        "same endpoint cannot inherit devnet qualification for mainnet"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    let (wrong, other_calls, other_task) = server(|request| {
        response(
            "200 OK",
            &json!({"jsonrpc":"2.0","id":request["id"],"result":MAINNET_GENESIS}).to_string(),
            "",
        )
    })
    .await;
    cache.urls.devnet = wrong;
    assert!(
        cache
            .get_verified(SolanaChainId::SolanaDevnet)
            .await
            .is_err(),
        "changed endpoint cannot reuse old qualification"
    );
    assert_eq!(other_calls.load(Ordering::SeqCst), 1);
    assert!(
        cache.get_verified(SolanaChainId::SolanaLocal).await.is_ok(),
        "local profile is explicitly exempt"
    );
    task.abort();
    other_task.abort();
}
