//! Coalesce concurrent history lookups without caching, deduplicating, or changing
//! the worker's reconciliation budget. Each sender belongs to one cached endpoint.
use super::rpc_cache::BoundedRpcSender;
use crate::metrics::{
    ExecutorFamily, ExecutorOperation, MetricChain, OperationOutcome, OperationTimer,
};
use futures::{StreamExt, stream::FuturesUnordered};
use serde_json::{Value, json};
use solana_rpc_client::rpc_sender::{RpcSender, RpcTransportStats};
use solana_rpc_client_api::{
    client_error::{Error as ClientError, ErrorKind, Result as ClientResult},
    request::{RpcError, RpcRequest, RpcResponseErrorData},
    response::Response,
};
use solana_transaction_status::TransactionStatus;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::AbortHandle,
    time::Instant,
};

const MAX_SIGNATURES: usize = 256;
const MAX_OUTSTANDING: usize = 256;
const MAX_IN_FLIGHT: usize = 4;
const COLLECTION_DELAY: Duration = Duration::from_millis(5);

pub(super) struct CoalescingRpcSender {
    transport: Arc<BoundedRpcSender>,
    pending: mpsc::Sender<Poll>,
    capacity: Arc<Semaphore>,
    actor: AbortHandle,
    chain: MetricChain,
}

struct Poll {
    signature: String,
    reply: oneshot::Sender<ClientResult<Value>>,
    // Held across queueing, HTTP and fanout, including a cancelled HTTP consumer.
    _capacity: OwnedSemaphorePermit,
    wait: Option<OperationTimer>,
}

impl CoalescingRpcSender {
    pub(super) fn new(transport: BoundedRpcSender, chain: MetricChain) -> Self {
        let transport = Arc::new(transport);
        let (pending, receiver) = mpsc::channel(MAX_OUTSTANDING);
        let actor = tokio::spawn(coordinate(transport.clone(), receiver)).abort_handle();
        Self {
            transport,
            pending,
            capacity: Arc::new(Semaphore::new(MAX_OUTSTANDING)),
            actor,
            chain,
        }
    }

    async fn poll(&self, signature: String) -> ClientResult<Value> {
        let timer = OperationTimer::start(
            ExecutorFamily::Solana,
            self.chain,
            ExecutorOperation::StatusLogicalPoll,
        );
        let result = async {
            let capacity = self
                .capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| error("Solana status coordinator is full; retry later"))?;
            let (reply, result) = oneshot::channel();
            self.pending
                .try_send(Poll {
                    signature,
                    reply,
                    _capacity: capacity,
                    wait: Some(OperationTimer::start(
                        ExecutorFamily::Solana,
                        self.chain,
                        ExecutorOperation::StatusCoordinatorWait,
                    )),
                })
                .map_err(|_| error("Solana status coordinator is unavailable; retry later"))?;
            result
                .await
                .map_err(|_| error("Solana status coordinator stopped; retry later"))?
        }
        .await;
        timer.finish_result(&result);
        result
    }
}

impl Drop for CoalescingRpcSender {
    fn drop(&mut self) {
        // There is no strong-reference cycle. Shutdown drops queued and in-flight
        // reply senders, waking consumers instead of leaving a detached actor.
        self.actor.abort();
    }
}

fn error(message: &'static str) -> ClientError {
    ErrorKind::Custom(message.into()).into()
}

// Only the exact singleton/history policy used by recovery is eligible. Other
// methods, multi-signature calls and future/unknown options keep their own RPC.
fn eligible_signature(request: &RpcRequest, params: &Value) -> Option<String> {
    if !matches!(request, RpcRequest::GetSignatureStatuses) {
        return None;
    }
    let params = params.as_array()?;
    if params.len() != 2 {
        return None;
    }
    let signatures = params[0].as_array()?;
    let config = params[1].as_object()?;
    if signatures.len() != 1
        || config.len() != 1
        || config.get("searchTransactionHistory") != Some(&Value::Bool(true))
    {
        return None;
    }
    let signature = signatures[0].as_str()?;
    // Bound queued input even for direct callers of the sender trait. The SDK
    // supplies validated signatures; invalid direct requests pass through.
    signature.parse::<solana_sdk::signature::Signature>().ok()?;
    Some(signature.to_owned())
}

#[async_trait::async_trait]
impl RpcSender for CoalescingRpcSender {
    async fn send(&self, request: RpcRequest, params: Value) -> ClientResult<Value> {
        match eligible_signature(&request, &params) {
            Some(signature) => self.poll(signature).await,
            None => self.transport.send(request, params).await,
        }
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        self.transport.get_transport_stats()
    }

    fn url(&self) -> String {
        self.transport.url()
    }
}

async fn coordinate(transport: Arc<BoundedRpcSender>, mut receiver: mpsc::Receiver<Poll>) {
    let mut pending = Vec::with_capacity(MAX_SIGNATURES);
    let mut deadline = None;
    let mut in_flight = FuturesUnordered::new();
    loop {
        // Preserve the surviving callers' FIFO positions, including duplicates.
        // A cancelled entry is removed before request construction, never replaced
        // by a synthetic null that could change another caller's positional result.
        pending.retain(|poll: &Poll| !poll.reply.is_closed());
        if pending.is_empty() {
            deadline = None;
        }
        if in_flight.len() < MAX_IN_FLIGHT
            && !pending.is_empty()
            && (pending.len() == MAX_SIGNATURES || deadline.is_some_and(|at| Instant::now() >= at))
        {
            let batch = std::mem::replace(&mut pending, Vec::with_capacity(MAX_SIGNATURES));
            deadline = None;
            in_flight.push(dispatch(transport.clone(), batch));
            continue;
        }
        tokio::select! {
            poll = receiver.recv(), if pending.len() < MAX_SIGNATURES => {
                let Some(poll) = poll else { break; };
                if !poll.reply.is_closed() {
                    if pending.is_empty() { deadline = Some(Instant::now() + COLLECTION_DELAY); }
                    pending.push(poll);
                }
            }
            _ = in_flight.next(), if !in_flight.is_empty() => {}
            _ = async {
                if let Some(at) = deadline { tokio::time::sleep_until(at).await; }
            }, if deadline.is_some() && in_flight.len() < MAX_IN_FLIGHT => {}
        }
    }
    // Dropping the vectors/receiver resolves outstanding oneshots with an error;
    // dropping the bounded HTTP futures cancels those requests during shutdown.
}

fn copy_batch_error(source: &ClientError) -> ClientError {
    // Preserve only the numeric RPC code. Never copy provider text, URLs, wire
    // bytes or serde diagnostics into a fanout error or observability label.
    if let ErrorKind::RpcError(RpcError::RpcResponseError { code, .. }) = source.kind() {
        return RpcError::RpcResponseError {
            code: *code,
            message: "Solana status batch rejected; provider details withheld".into(),
            data: RpcResponseErrorData::Empty,
        }
        .into();
    }
    error("Solana status batch failed; retry reconciliation")
}

fn validate_response(
    value: Value,
    count: usize,
) -> ClientResult<Response<Vec<Option<TransactionStatus>>>> {
    // Context is copied to each caller. Keep untrusted context metadata bounded
    // rather than multiplying a provider-controlled large string by batch size.
    if value
        .get("context")
        .and_then(|context| context.get("apiVersion"))
        .and_then(Value::as_str)
        .is_some_and(|version| version.len() > 128)
    {
        return Err(error("Solana status batch returned invalid context"));
    }
    let response: Response<Vec<Option<TransactionStatus>>> = serde_json::from_value(value)
        .map_err(|_| error("Solana status batch returned malformed statuses or context"))?;
    if response.value.len() != count
        || response.value.iter().flatten().any(|status| {
            status.slot > response.context.slot || status.err != status.status.clone().err()
        })
    {
        return Err(error(
            "Solana status batch returned inconsistent statuses or context",
        ));
    }
    Ok(response)
}

async fn dispatch(transport: Arc<BoundedRpcSender>, mut batch: Vec<Poll>) {
    // Cancellation can race the actor's decision to flush. Check again at the
    // actual dispatch boundary; thereafter one cancelled consumer cannot abort peers.
    batch.retain(|poll| !poll.reply.is_closed());
    if batch.is_empty() {
        return;
    }
    for poll in &mut batch {
        if let Some(timer) = poll.wait.take() {
            timer.finish(OperationOutcome::Success);
        }
    }
    let signatures: Vec<&str> = batch.iter().map(|poll| poll.signature.as_str()).collect();
    let result = transport
        .send(
            RpcRequest::GetSignatureStatuses,
            json!([signatures, {"searchTransactionHistory":true}]),
        )
        .await
        .and_then(|value| validate_response(value, batch.len()));
    match result {
        Ok(response) => {
            for (poll, status) in batch.into_iter().zip(response.value) {
                let _ = poll
                    .reply
                    .send(Ok(json!({"context": response.context, "value": [status]})));
            }
        }
        Err(source) => {
            for poll in batch {
                let _ = poll.reply.send(Err(copy_batch_error(&source)));
            }
        }
    }
}

#[cfg(test)]
#[path = "status_batcher_tests.rs"]
mod tests;
