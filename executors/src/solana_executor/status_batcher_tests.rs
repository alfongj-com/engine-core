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
async fn exact_256_limit_splits_into_32_signature_batches_and_retains_inflight_capacity() {
    let mut http = Http::new().await;
    let sender = http.sender();
    let mut waiting: Vec<_> = (0..MAX_OUTSTANDING)
        .map(|i| enqueue(&sender, signature(i as u8)))
        .collect();
    let error = sender
        .send(RpcRequest::GetSignatureStatuses, params(&signature(9)))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("full"));
    let mut exchanges = Vec::new();
    for _ in 0..MAX_IN_FLIGHT {
        exchanges.push(http.next().await);
    }
    assert!(
        http.requests.try_recv().is_err(),
        "fifth HTTP batch escaped the bound"
    );
    assert!(
        exchanges
            .iter()
            .all(|exchange| exchange.request["params"][0].as_array().unwrap().len() == 32)
    );
    // All four initial cohorts are in flight. Cancelling one cohort's consumers
    // cannot free capacity while its HTTP request still occupies the sender.
    drop(waiting.drain(..MAX_SIGNATURES));
    assert_eq!(sender.capacity.available_permits(), 0);
    assert!(
        sender
            .send(RpcRequest::GetSignatureStatuses, params(&signature(9)))
            .await
            .is_err()
    );
    let mut observed = Vec::new();
    for exchange in exchanges {
        observed.extend(
            exchange.request["params"][0]
                .as_array()
                .unwrap()
                .iter()
                .map(|signature| signature.as_str().unwrap().to_owned()),
        );
        exchange.reply(statuses(vec![Value::Null; MAX_SIGNATURES]));
    }
    for _ in MAX_IN_FLIGHT..MAX_OUTSTANDING / MAX_SIGNATURES {
        let exchange = http.next().await;
        assert_eq!(exchange.request["params"][0].as_array().unwrap().len(), 32);
        observed.extend(
            exchange.request["params"][0]
                .as_array()
                .unwrap()
                .iter()
                .map(|signature| signature.as_str().unwrap().to_owned()),
        );
        exchange.reply(statuses(vec![Value::Null; MAX_SIGNATURES]));
    }
    for receiver in waiting {
        assert_eq!(receiver.await.unwrap().unwrap()["value"], json!([null]));
    }
    observed.sort();
    let mut expected: Vec<_> = (0..MAX_OUTSTANDING).map(|i| signature(i as u8)).collect();
    expected.sort();
    assert_eq!(
        observed, expected,
        "no admitted poll may be lost or duplicated during splitting"
    );
    assert_eq!(sender.capacity.available_permits(), MAX_OUTSTANDING);
    let followup = spawn_polls(sender.clone(), vec![signature(9)]);
    http.next().await.reply(statuses(vec![Value::Null]));
    assert!(finish(followup).await.remove(0).is_ok());
    assert_eq!(sender.get_transport_stats().request_count, 9);
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
async fn malformed_envelope_or_length_fails_every_member_without_inventing_nulls() {
    let mut http = Http::new().await;
    let sender = http.sender();
    let malformed = [
        statuses(vec![Value::Null]),
        statuses(vec![Value::Null; 3]),
        json!({"value":[null,null]}),
        json!({"context":{"slot":-1},"value":[null,null]}),
        json!({"context":{"slot":150,"apiVersion":"a".repeat(129)},"value":[null,null]}),
        json!({"context":{"slot":150},"value":{"providerSecret":SECRET}}),
    ];
    let expected_requests = malformed.len();
    for response in malformed {
        let pending = spawn_polls(sender.clone(), vec![signature(1), signature(2)]);
        http.next().await.reply(response);
        for result in finish(pending).await {
            let error = result.unwrap_err();
            assert!(!format!("{error:?} {error}").contains(SECRET));
            assert!(!format!("{error:?}").contains("127.0.0.1"));
        }
    }
    assert_eq!(
        sender.get_transport_stats().request_count,
        expected_requests
    );
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
        expected_requests + 1,
        "no hidden retry"
    );
}

#[tokio::test]
async fn invalid_statuses_fail_only_their_positions_among_healthy_null_and_duplicate_polls() {
    let mut http = Http::new().await;
    let sender = http.sender();
    let mut unknown_error = status(80, "finalized");
    unknown_error["err"] = json!({"FutureTransactionError":SECRET});
    unknown_error["status"] = json!({"Err":{"FutureTransactionError":SECRET}});
    let mut negative_slot = status(80, "confirmed");
    negative_slot["slot"] = json!(-1);
    let mut mismatched_error = status(80, "finalized");
    mismatched_error["err"] = json!("AccountNotFound");
    let malformed = [
        unknown_error,
        negative_slot,
        status(151, "finalized"),
        mismatched_error,
        json!({"slot":1,"garbage":SECRET}),
    ];
    let expected_requests = malformed.len();
    for poisoned in malformed {
        // The poisoned entry and both healthy duplicate entries deliberately
        // share a signature: each caller owns its original array position.
        let signatures = vec![
            signature(1),
            signature(1),
            signature(1),
            signature(2),
            signature(3),
        ];
        let pending = spawn_polls(sender.clone(), signatures.clone());
        let exchange = http.next().await;
        assert_eq!(exchange.request["params"][0], json!(signatures));
        let values = vec![
            status(90, "finalized"),
            poisoned,
            Value::Null,
            status(91, "confirmed"),
            status(92, "processed"),
        ];
        exchange.reply(statuses(values.clone()));
        for (position, (result, expected)) in
            finish(pending).await.into_iter().zip(values).enumerate()
        {
            if position == 1 {
                let error = result.unwrap_err();
                let rendered = format!("{error:?} {error}");
                assert!(!rendered.contains(SECRET));
                assert!(!rendered.contains("127.0.0.1"));
            } else {
                let response = result.unwrap();
                assert_eq!(
                    response["value"],
                    json!([expected]),
                    "wrong positional result at {position}"
                );
                assert_eq!(
                    response["context"],
                    json!({"slot":150,"apiVersion":"3.1.8"})
                );
            }
        }
    }
    assert_eq!(
        sender.get_transport_stats().request_count,
        expected_requests
    );
    assert!(
        http.requests.try_recv().is_err(),
        "entry errors must not add hidden retries"
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
async fn disabled_coalescing_keeps_singleton_history_requests_and_provisional_status() {
    let mut http = Http::new().await;
    let cache = SolanaRpcCache::new_with_status_batching(
        SolanaRpcUrls {
            devnet: http.url.clone(),
            mainnet: http.url.clone(),
            local: http.url.clone(),
        },
        false,
    );
    let client = cache.get_or_create(SolanaChainId::SolanaLocal).await;
    let polling = client.clone();
    let pending = tokio::spawn(async move {
        let first = [Signature::from([1; 64])];
        let second = [Signature::from([2; 64])];
        tokio::join!(
            polling.get_signature_statuses_with_history(&first),
            polling.get_signature_statuses_with_history(&second),
        )
    });
    // Both requests must reach HTTP independently before either is answered.
    // Re-enabling the coordinator by mistake would produce one two-entry request.
    let exchanges = [http.next().await, http.next().await];
    for exchange in exchanges {
        assert_eq!(exchange.request["method"], "getSignatureStatuses");
        assert_eq!(exchange.request["params"][0].as_array().unwrap().len(), 1);
        assert_eq!(
            exchange.request["params"][1],
            json!({"searchTransactionHistory":true})
        );
        let value = if exchange.request["params"][0][0] == signature(1) {
            status(90, "confirmed")
        } else {
            Value::Null
        };
        exchange.reply(statuses(vec![value]));
    }
    let (first, second) = finish(pending).await;
    assert_eq!(
        serde_json::to_value(first.unwrap().value[0].as_ref().unwrap()).unwrap(),
        status(90, "confirmed")
    );
    assert!(second.unwrap().value[0].is_none());
    assert_eq!(client.get_transport_stats().request_count, 2);
    assert!(http.requests.try_recv().is_err());
}

#[tokio::test]
async fn disabled_coalescing_retains_transport_timeout_redaction_and_no_hidden_retry() {
    let mut http = Http::new().await;
    let client = Arc::new(crate::solana_executor::rpc_cache::client_with_transport(
        BoundedRpcSender::new_with_timeout(http.url.clone(), Duration::from_millis(30)),
        SolanaChainId::SolanaLocal,
        false,
    ));
    let polling = client.clone();
    let pending = tokio::spawn(async move {
        let signatures = [Signature::from([1; 64])];
        polling
            .get_signature_statuses_with_history(&signatures)
            .await
    });
    let unanswered = http.next().await;
    assert_eq!(unanswered.request["params"], params(&signature(1)));
    let error = finish(pending).await.unwrap_err();
    let rendered = format!("{error:?} {error}");
    assert!(rendered.contains("timed out"));
    assert!(!rendered.contains(SECRET));
    assert!(!rendered.contains("127.0.0.1"));
    assert_eq!(client.get_transport_stats().request_count, 1);
    assert!(http.requests.try_recv().is_err());
    drop(unanswered);
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
