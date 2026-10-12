use super::*;
use crate::solana_executor::rpc_cache::{SolanaRpcCache, SolanaRpcUrls};
use engine_core::execution_options::solana::SolanaChainId;
use solana_sdk::signature::Signature;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::{JoinHandle, JoinSet},
};

const SECRET: &str = "BATCH_RPC_SECRET_SENTINEL";

struct Exchange {
    request: Value,
    response: oneshot::Sender<Value>,
}
impl Exchange {
    fn reply(self, value: Value) {
        let _ = self
            .response
            .send(json!({"jsonrpc":"2.0", "id":self.request["id"], "result":value}));
    }
    fn reject(self) {
        let _ = self
            .response
            .send(json!({"jsonrpc":"2.0", "id":self.request["id"],
            "error":{"code":-32005,"message":SECRET,"data":SECRET}}));
    }
}

struct Http {
    url: String,
    requests: mpsc::UnboundedReceiver<Exchange>,
    task: JoinHandle<()>,
}
impl Http {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/{SECRET}?apiKey={SECRET}",
            listener.local_addr().unwrap()
        );
        let (requests, receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.unwrap();
                        let requests = requests.clone();
                        clients.spawn(async move {
                            let mut bytes = Vec::new();
                            let (offset, length) = loop {
                                let mut chunk = [0; 4096];
                                let n = stream.read(&mut chunk).await.unwrap();
                                if n == 0 { return; }
                                bytes.extend_from_slice(&chunk[..n]);
                                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                                    let headers = String::from_utf8_lossy(&bytes[..end]);
                                    let length = headers.lines().find_map(|line| {
                                        let (name, value) = line.split_once(':')?;
                                        name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse::<usize>().unwrap())
                                    }).unwrap();
                                    break (end + 4, length);
                                }
                            };
                            while bytes.len() < offset + length {
                                let mut chunk = [0; 4096];
                                let n = stream.read(&mut chunk).await.unwrap();
                                if n == 0 { return; }
                                bytes.extend_from_slice(&chunk[..n]);
                            }
                            let request: Value = serde_json::from_slice(&bytes[offset..offset + length]).unwrap();
                            let (response, result) = oneshot::channel();
                            if requests.send(Exchange { request, response }).is_err() { return; }
                            if let Ok(body) = result.await {
                                let body = body.to_string();
                                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
                                let _ = stream.write_all(response.as_bytes()).await;
                            }
                        });
                    }
                    _ = clients.join_next(), if !clients.is_empty() => {}
                }
            }
        });
        Self {
            url,
            requests: receiver,
            task,
        }
    }
    async fn next(&mut self) -> Exchange {
        tokio::time::timeout(Duration::from_secs(3), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
    fn sender(&self) -> Arc<CoalescingRpcSender> {
        Arc::new(CoalescingRpcSender::new(
            BoundedRpcSender::new(self.url.clone()),
            MetricChain::Solana(SolanaChainId::SolanaLocal),
        ))
    }
}
impl Drop for Http {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn signature(index: u8) -> String {
    Signature::from([index; 64]).to_string()
}
fn params(signature: &str) -> Value {
    json!([[signature], {"searchTransactionHistory":true}])
}
fn statuses(values: Vec<Value>) -> Value {
    json!({"context":{"slot":150,"apiVersion":"3.1.8"},"value":values})
}
fn status(slot: u64, commitment: &str) -> Value {
    json!({"slot":slot,"confirmations":if commitment == "finalized" { None } else { Some(1) },
        "err":null,"status":{"Ok":null},"confirmationStatus":commitment})
}
fn spawn_polls(
    sender: Arc<CoalescingRpcSender>,
    signatures: Vec<String>,
) -> JoinHandle<Vec<ClientResult<Value>>> {
    tokio::spawn(async move {
        futures::future::join_all(
            signatures
                .into_iter()
                .map(|signature| sender.send(RpcRequest::GetSignatureStatuses, params(&signature))),
        )
        .await
    })
}
async fn finish<T>(task: JoinHandle<T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
}

// Enqueue without yielding so exact-boundary/cancellation tests do not depend on
// the host scheduling 256 spawned tasks inside a five-millisecond collection window.
fn enqueue(
    sender: &CoalescingRpcSender,
    signature: String,
) -> oneshot::Receiver<ClientResult<Value>> {
    let (reply, result) = oneshot::channel();
    sender
        .pending
        .try_send(Poll {
            signature,
            reply,
            _capacity: sender.capacity.clone().try_acquire_owned().unwrap(),
            wait: None,
        })
        .unwrap_or_else(|_| panic!("test coordinator queue unexpectedly full"));
    result
}

#[tokio::test]
async fn positional_mapping_retains_duplicates_nulls_and_provisional_statuses() {
    let mut http = Http::new().await;
    let sender = http.sender();
    let signatures = vec![
        signature(4),
        signature(2),
        signature(4),
        signature(7),
        signature(8),
    ];
    let pending = spawn_polls(sender.clone(), signatures.clone());
    let request = http.next().await;
    assert_eq!(
        request.request["params"],
        json!([signatures, {"searchTransactionHistory":true}])
    );
    let mut failed = status(84, "finalized");
    failed["err"] = json!("AccountNotFound");
    failed["status"] = json!({"Err":"AccountNotFound"});
    let values = vec![
        Value::Null,
        status(81, "confirmed"),
        status(82, "finalized"),
        status(83, "processed"),
        failed,
    ];
    request.reply(statuses(values.clone()));
    for (result, expected) in finish(pending).await.into_iter().zip(values) {
        let result = result.unwrap();
        assert_eq!(result["value"], json!([expected]));
        assert_eq!(result["context"], json!({"slot":150,"apiVersion":"3.1.8"}));
    }
    assert_eq!(sender.get_transport_stats().request_count, 1);
    assert!(http.requests.try_recv().is_err());
}

#[tokio::test]
async fn exact_256_limit_bounds_queued_and_inflight_polls_and_releases_capacity() {
    let mut http = Http::new().await;
    let sender = http.sender();
    let mut waiting: Vec<_> = (0..256)
        .map(|i| enqueue(&sender, signature(i as u8)))
        .collect();
    let error = sender
        .send(RpcRequest::GetSignatureStatuses, params(&signature(9)))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("full"));
    let request = http.next().await;
    assert_eq!(request.request["params"][0].as_array().unwrap().len(), 256);
    // After dispatch, cancelled consumers still occupy the bounded HTTP batch
    // until it completes; cancellation cannot start an unlimited replacement set.
    drop(waiting.drain(..128));
    assert_eq!(sender.capacity.available_permits(), 0);
    assert!(
        sender
            .send(RpcRequest::GetSignatureStatuses, params(&signature(9)))
            .await
            .is_err()
    );
    request.reply(statuses(vec![Value::Null; 256]));
    for receiver in waiting {
        assert_eq!(receiver.await.unwrap().unwrap()["value"], json!([null]));
    }
    assert_eq!(sender.capacity.available_permits(), MAX_OUTSTANDING);
    let followup = spawn_polls(sender.clone(), vec![signature(9)]);
    http.next().await.reply(statuses(vec![Value::Null]));
    assert!(finish(followup).await.remove(0).is_ok());
    assert_eq!(sender.get_transport_stats().request_count, 2);
}

#[tokio::test]
async fn cancellation_before_and_after_dispatch_does_not_shift_or_cancel_peers() {
    let mut http = Http::new().await;
    let sender = http.sender();
    let first = enqueue(&sender, signature(1));
    let cancelled = enqueue(&sender, signature(2));
    let last = enqueue(&sender, signature(3));
    drop(cancelled);
    let request = http.next().await;
    assert_eq!(
        request.request["params"][0],
        json!([signature(1), signature(3)])
    );
    drop(first);
    request.reply(statuses(vec![Value::Null, status(88, "confirmed")]));
    assert_eq!(
        last.await.unwrap().unwrap()["value"],
        json!([status(88, "confirmed")])
    );
    assert_eq!(sender.capacity.available_permits(), MAX_OUTSTANDING);
}

#[tokio::test]
async fn only_four_http_batches_run_while_later_polls_wait() {
    let mut http = Http::new().await;
    let sender = http.sender();
    let mut exchanges = Vec::new();
    let mut replies = Vec::new();
    for i in 0..MAX_IN_FLIGHT {
        replies.push(enqueue(&sender, signature(i as u8)));
        exchanges.push(http.next().await);
    }
    replies.push(enqueue(&sender, signature(9)));
    tokio::time::sleep(COLLECTION_DELAY * 3).await;
    assert!(
        http.requests.try_recv().is_err(),
        "fifth batch escaped the HTTP bound"
    );
    exchanges.remove(0).reply(statuses(vec![Value::Null]));
    exchanges.push(http.next().await);
    for exchange in exchanges {
        exchange.reply(statuses(vec![Value::Null]));
    }
    for reply in replies {
        assert!(reply.await.unwrap().is_ok());
    }
    assert_eq!(sender.get_transport_stats().request_count, 5);
}

#[tokio::test]
async fn malformed_length_context_or_status_fails_every_member_without_inventing_nulls() {
    let mut http = Http::new().await;
    let sender = http.sender();
    let mut bad_error = status(80, "finalized");
    bad_error["err"] = json!("AccountNotFound");
    for response in [
        statuses(vec![Value::Null]),
        statuses(vec![Value::Null; 3]),
        json!({"value":[null,null]}),
        json!({"context":{"slot":-1},"value":[null,null]}),
        json!({"context":{"slot":150,"apiVersion":"a".repeat(129)},"value":[null,null]}),
        statuses(vec![status(151, "finalized"), Value::Null]),
        statuses(vec![bad_error, Value::Null]),
        statuses(vec![json!({"slot":1,"garbage":SECRET}), Value::Null]),
    ] {
        let pending = spawn_polls(sender.clone(), vec![signature(1), signature(2)]);
        http.next().await.reply(response);
        for result in finish(pending).await {
            let error = result.unwrap_err();
            assert!(!format!("{error:?} {error}").contains(SECRET));
            assert!(!format!("{error:?}").contains("127.0.0.1"));
        }
    }
    assert_eq!(sender.get_transport_stats().request_count, 8);
    let pending = spawn_polls(sender.clone(), vec![signature(1), signature(2)]);
    http.next().await.reject();
    for result in finish(pending).await {
        let error = result.unwrap_err();
        assert!(matches!(
            error.kind(),
            ErrorKind::RpcError(RpcError::RpcResponseError { code: -32005, .. })
        ));
        assert!(!format!("{error:?} {error}").contains(SECRET));
    }
    assert_eq!(
        sender.get_transport_stats().request_count,
        9,
        "no hidden retry"
    );
}

#[tokio::test]
async fn timeout_and_actor_shutdown_fail_all_waiters() {
    let mut http = Http::new().await;
    let sender = Arc::new(CoalescingRpcSender::new(
        BoundedRpcSender::new_with_timeout(http.url.clone(), Duration::from_millis(30)),
        MetricChain::Unknown,
    ));
    let pending = spawn_polls(sender.clone(), vec![signature(1), signature(2)]);
    let unanswered = http.next().await;
    assert!(
        finish(pending)
            .await
            .into_iter()
            .all(|result| result.is_err())
    );
    assert_eq!(sender.capacity.available_permits(), MAX_OUTSTANDING);
    assert_eq!(sender.get_transport_stats().request_count, 1);
    drop(unanswered);

    let sender = http.sender();
    let inflight = spawn_polls(sender.clone(), vec![signature(3)]);
    let unanswered = http.next().await;
    let queued = enqueue(&sender, signature(4));
    sender.actor.abort();
    for result in finish(inflight).await {
        assert!(result.unwrap_err().to_string().contains("stopped"));
    }
    assert!(
        tokio::time::timeout(Duration::from_secs(3), queued)
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(sender.capacity.available_permits(), MAX_OUTSTANDING);
    assert!(
        sender
            .send(RpcRequest::GetSignatureStatuses, params(&signature(5)))
            .await
            .is_err()
    );
    drop(unanswered);
}

#[tokio::test]
async fn endpoint_cluster_and_history_configuration_never_share_batches() {
    let mut first = Http::new().await;
    let mut second = Http::new().await;
    let cache = SolanaRpcCache::new(SolanaRpcUrls {
        devnet: first.url.clone(),
        mainnet: first.url.clone(),
        local: second.url.clone(),
    });
    let devnet = cache.get_or_create(SolanaChainId::SolanaDevnet).await;
    let mainnet = cache.get_or_create(SolanaChainId::SolanaMainnet).await;
    let local = cache.get_or_create(SolanaChainId::SolanaLocal).await;
    let sig = Signature::from([5; 64]);
    let pending = tokio::spawn(async move {
        let signatures = [sig];
        tokio::join!(
            devnet.get_signature_statuses_with_history(&signatures),
            mainnet.get_signature_statuses_with_history(&signatures),
            local.get_signature_statuses_with_history(&signatures),
        )
    });
    for exchange in [first.next().await, first.next().await, second.next().await] {
        assert_eq!(exchange.request["params"][0].as_array().unwrap().len(), 1);
        exchange.reply(statuses(vec![Value::Null]));
    }
    let (a, b, c) = finish(pending).await;
    assert!(a.is_ok() && b.is_ok() && c.is_ok());

    let sender = first.sender();
    let pending = tokio::spawn(async move {
        tokio::join!(
            sender.send(RpcRequest::GetSignatureStatuses, params(&signature(1))),
            sender.send(
                RpcRequest::GetSignatureStatuses,
                json!([[signature(2)],{"searchTransactionHistory":false}])
            ),
            sender.send(
                RpcRequest::GetSignatureStatuses,
                json!([[signature(3)],{"searchTransactionHistory":true,"futureOption":1}])
            ),
        )
    });
    let mut seen = Vec::new();
    for _ in 0..3 {
        let exchange = first.next().await;
        assert_eq!(exchange.request["params"][0].as_array().unwrap().len(), 1);
        seen.push(exchange.request["params"][1].clone());
        exchange.reply(statuses(vec![Value::Null]));
    }
    assert!(seen.contains(&json!({"searchTransactionHistory":false})));
    assert!(seen.contains(&json!({"searchTransactionHistory":true,"futureOption":1})));
    let (a, b, c) = finish(pending).await;
    assert!(a.is_ok() && b.is_ok() && c.is_ok());
}

#[tokio::test]
async fn metrics_distinguish_logical_polls_wait_and_real_batches_without_identifiers() {
    const CHILD: &str = "ENGINE_SOLANA_BATCH_METRICS_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "solana_executor::status_batcher::tests::metrics_distinguish_logical_polls_wait_and_real_batches_without_identifiers", "--nocapture"])
            .env(CHILD,"1").output().unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let mut http = Http::new().await;
    let sender = http.sender();
    let pending = spawn_polls(sender, vec![signature(1), signature(2)]);
    http.next().await.reply(statuses(vec![Value::Null; 2]));
    assert!(
        finish(pending)
            .await
            .into_iter()
            .all(|result| result.is_ok())
    );
    let metrics = crate::metrics::export_default_metrics().unwrap();
    for forbidden in [
        SECRET.to_owned(),
        signature(1),
        signature(2),
        "127.0.0.1".into(),
        "apiKey".into(),
    ] {
        assert!(!metrics.contains(&forbidden), "identifier in metrics");
    }
    for (phase, expected) in [
        ("status_logical_poll", 2.0),
        ("status_coordinator_wait", 2.0),
        ("rpc_get_signature_statuses", 1.0),
    ] {
        let count: f64 = metrics
            .lines()
            .filter(|line| line.starts_with("tw_engine_executor_operation_duration_seconds_count{"))
            .filter(|line| {
                line.contains(&format!("phase=\"{phase}\"")) && line.contains("outcome=\"success\"")
            })
            .map(|line| line.rsplit_once(' ').unwrap().1.parse::<f64>().unwrap())
            .sum();
        assert_eq!(count, expected, "wrong count for {phase}");
    }
    let signature_count: f64 = metrics
        .lines()
        .filter(|line| line.starts_with("tw_engine_executor_solana_status_request_signatures_sum{"))
        .map(|line| line.rsplit_once(' ').unwrap().1.parse::<f64>().unwrap())
        .sum();
    assert_eq!(signature_count, 2.0);
}
