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
