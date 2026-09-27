//! Bounded-cardinality elapsed-time observations, not CPU or server-side timings.
use engine_core::execution_options::solana::SolanaChainId;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Instant,
};

pub const DURATION_BUCKETS: &[f64] = &[
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0,
];
const MAX_EVM_CHAINS: usize = 32;

#[derive(Clone, Copy)]
pub enum ExecutorFamily {
    Eoa,
    Solana,
}
impl ExecutorFamily {
    fn label(self) -> &'static str {
        match self {
            Self::Eoa => "eoa",
            Self::Solana => "solana",
        }
    }
}

#[derive(Clone, Copy)]
pub enum MetricChain {
    Evm(u64),
    Solana(SolanaChainId),
    Unknown,
}

#[derive(Clone, Copy)]
pub enum ExecutorOperation {
    Handler,
    WalletLockAcquire,
    CullPending,
    RecoverBorrowed,
    Confirm,
    Send,
    DurableAuthorization,
    RpcBroadcast,
    RpcReceipt,
    RedisConnection,
    RedisConnectionWait,
    RedisWatch,
    RedisOwnerRead,
    RedisValidation,
    RedisCommit,
    RedisBackoff,
    RedisCounts,
    RpcGetSignatureStatuses,
    RpcGetTransaction,
    RpcGetBlockHeight,
    RpcGetLatestBlockhash,
    RpcIsBlockhashValid,
    RpcGetRecentPrioritizationFees,
    RpcSendTransaction,
    RpcGetGenesisHash,
    RpcSimulateTransaction,
    RpcOther,
}
impl ExecutorOperation {
    fn label(self) -> &'static str {
        match self {
            Self::Handler => "handler",
            Self::WalletLockAcquire => "wallet_lock_acquire",
            Self::CullPending => "cull_pending",
            Self::RecoverBorrowed => "recover_borrowed",
            Self::Confirm => "confirm",
            Self::Send => "send",
            Self::DurableAuthorization => "durable_authorization",
            Self::RpcBroadcast => "rpc_broadcast",
            Self::RpcReceipt => "rpc_receipt",
            Self::RedisConnection => "redis_connection",
            Self::RedisConnectionWait => "redis_connection_wait",
            Self::RedisWatch => "redis_watch",
            Self::RedisOwnerRead => "redis_owner_read",
            Self::RedisValidation => "redis_validation",
            Self::RedisCommit => "redis_commit",
            Self::RedisBackoff => "redis_backoff",
            Self::RedisCounts => "redis_counts",
            Self::RpcGetSignatureStatuses => "rpc_get_signature_statuses",
            Self::RpcGetTransaction => "rpc_get_transaction",
            Self::RpcGetBlockHeight => "rpc_get_block_height",
            Self::RpcGetLatestBlockhash => "rpc_get_latest_blockhash",
            Self::RpcIsBlockhashValid => "rpc_is_blockhash_valid",
            Self::RpcGetRecentPrioritizationFees => "rpc_get_recent_prioritization_fees",
            Self::RpcSendTransaction => "rpc_send_transaction",
            Self::RpcGetGenesisHash => "rpc_get_genesis_hash",
            Self::RpcSimulateTransaction => "rpc_simulate_transaction",
            Self::RpcOther => "rpc_other",
        }
    }
}

#[derive(Clone, Copy)]
pub enum OperationOutcome {
    Success,
    Error,
    Cancelled,
    Conflict,
    Requeue,
}
impl OperationOutcome {
    fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
            Self::Conflict => "conflict",
            Self::Requeue => "requeue",
        }
    }
}

pub(super) struct OperationMetrics {
    duration: HistogramVec,
    status_signatures: HistogramVec,
    backlog: HistogramVec,
    progress: IntCounterVec,
    evm_chains: Mutex<HashSet<u64>>,
}
impl OperationMetrics {
    pub(super) fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let duration = HistogramVec::new(HistogramOpts::new("tw_engine_executor_operation_duration_seconds",
            "Monotonic elapsed time for a named executor phase; overlapping/nested phases must not be summed as exclusive time")
            .buckets(DURATION_BUCKETS.to_vec()), &["executor_type", "chain_id", "phase", "outcome"])?;
        let status_signatures = HistogramVec::new(HistogramOpts::new("tw_engine_executor_solana_status_request_signatures",
            "Signatures in each started status RPC request, including requests later cancelled or failed")
            .buckets(vec![1.,2.,4.,8.,16.,32.,64.,128.,256.]), &["chain_id"])?;
        let backlog = HistogramVec::new(HistogramOpts::new("tw_engine_executor_eoa_cycle_backlog",
            "Per-wallet backlog observed at completed cycle boundaries; samples are not aggregate per-chain gauges")
            .buckets(vec![0.,1.,8.,32.,128.,256.,1024.,4096.,25000.,100000.]), &["chain_id", "state"])?;
        let progress = IntCounterVec::new(
            Opts::new(
                "tw_engine_executor_eoa_cycle_transitions_total",
                "Redis state transitions reported by completed EOA cycles, not unique chain executions",
            ),
            &["chain_id", "transition"],
        )?;
        registry.register(Box::new(duration.clone()))?;
        registry.register(Box::new(status_signatures.clone()))?;
        registry.register(Box::new(backlog.clone()))?;
        registry.register(Box::new(progress.clone()))?;
        Ok(Self {
            duration,
            status_signatures,
            backlog,
            progress,
            evm_chains: Mutex::new(HashSet::new()),
        })
    }
    fn chain_label(&self, chain: MetricChain) -> String {
        match chain {
            MetricChain::Evm(id) => {
                let mut ids = self.evm_chains.lock().unwrap_or_else(|e| e.into_inner());
                if ids.contains(&id) || ids.len() < MAX_EVM_CHAINS {
                    ids.insert(id);
                    id.to_string()
                } else {
                    "other_evm".into()
                }
            }
            MetricChain::Solana(SolanaChainId::SolanaMainnet) => "solana:mainnet".into(),
            MetricChain::Solana(SolanaChainId::SolanaDevnet) => "solana:devnet".into(),
            MetricChain::Solana(SolanaChainId::SolanaLocal) => "solana:local".into(),
            MetricChain::Unknown => "unknown".into(),
        }
    }
    fn timer(
        self: Arc<Self>,
        family: ExecutorFamily,
        chain: MetricChain,
        operation: ExecutorOperation,
    ) -> OperationTimer {
        let chain = self.chain_label(chain);
        OperationTimer {
            metrics: self,
            started: Instant::now(),
            family,
            chain,
            operation,
            outcome: OperationOutcome::Cancelled,
        }
    }
}

/// Starts when constructed (normally immediately before polling the measured
/// future). Explicit completion records success/error; dropped pending work is
/// cancelled. Cancellation says nothing about remote execution or durable writes.
pub struct OperationTimer {
    metrics: Arc<OperationMetrics>,
    started: Instant,
    family: ExecutorFamily,
    chain: String,
    operation: ExecutorOperation,
    outcome: OperationOutcome,
}
impl OperationTimer {
    pub fn start(family: ExecutorFamily, chain: MetricChain, operation: ExecutorOperation) -> Self {
        super::get_metrics()
            .operations
            .clone()
            .timer(family, chain, operation)
    }
    pub fn finish(mut self, outcome: OperationOutcome) {
        self.outcome = outcome;
    }
    pub fn finish_result<T, E>(self, result: &Result<T, E>) {
        self.finish(if result.is_ok() {
            OperationOutcome::Success
        } else {
            OperationOutcome::Error
        });
    }
}
impl Drop for OperationTimer {
    fn drop(&mut self) {
        self.metrics
            .duration
            .with_label_values(&[
                self.family.label(),
                &self.chain,
                self.operation.label(),
                self.outcome.label(),
            ])
            .observe(self.started.elapsed().as_secs_f64());
    }
}

pub async fn measure_eoa<T, E>(
    chain_id: u64,
    operation: ExecutorOperation,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let timer = OperationTimer::start(ExecutorFamily::Eoa, MetricChain::Evm(chain_id), operation);
    let result = future.await;
    timer.finish_result(&result);
    result
}

pub fn record_status_request_signatures(chain: MetricChain, count: usize) {
    let metrics = &super::get_metrics().operations;
    metrics
        .status_signatures
        .with_label_values(&[&metrics.chain_label(chain)])
        .observe(count as f64);
}

pub(crate) fn record_eoa_cycle(chain: u64, result: &crate::eoa::worker::EoaExecutorWorkerResult) {
    let metrics = &super::get_metrics().operations;
    let chain = metrics.chain_label(MetricChain::Evm(chain));
    for (state, count) in [
        ("pending", result.pending_transactions),
        ("borrowed", result.borrowed_transactions),
        ("submitted", result.submitted_transactions),
        ("recycled", result.recycled_nonces),
    ] {
        metrics
            .backlog
            .with_label_values(&[&chain, state])
            .observe(count as f64);
    }
    for (phase, count) in [
        ("sent", result.sent_transactions),
        ("recovered", result.recovered_transactions),
        ("confirmed", result.confirmed_transactions),
        ("failed", result.failed_transactions),
    ] {
        metrics
            .progress
            .with_label_values(&[&chain, phase])
            .inc_by(count as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;
    #[test]
    fn bounded_chain_labels_do_not_expand_after_limit() {
        let metrics = OperationMetrics::new(&Registry::new()).unwrap();
        for id in 0..MAX_EVM_CHAINS as u64 {
            assert_eq!(metrics.chain_label(MetricChain::Evm(id)), id.to_string());
        }
        for id in 100..1000 {
            assert_eq!(metrics.chain_label(MetricChain::Evm(id)), "other_evm");
        }
        assert_eq!(metrics.evm_chains.lock().unwrap().len(), MAX_EVM_CHAINS);
        assert_eq!(metrics.chain_label(MetricChain::Evm(0)), "0");
        assert_eq!(
            metrics.chain_label(MetricChain::Solana(SolanaChainId::SolanaLocal)),
            "solana:local"
        );
    }
    #[tokio::test]
    async fn cancelled_pending_future_and_errors_are_recorded_once() {
        let registry = Registry::new();
        let metrics = Arc::new(OperationMetrics::new(&registry).unwrap());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let handle = {
            let metrics = metrics.clone();
            tokio::spawn(async move {
                let _timer = metrics.timer(
                    ExecutorFamily::Eoa,
                    MetricChain::Evm(1),
                    ExecutorOperation::RedisCommit,
                );
                started_tx.send(()).unwrap();
                pending::<()>().await;
            })
        };
        started_rx.await.unwrap();
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
        metrics
            .clone()
            .timer(
                ExecutorFamily::Eoa,
                MetricChain::Evm(1),
                ExecutorOperation::RedisCommit,
            )
            .finish_result(&Err::<(), _>(()));
        metrics
            .clone()
            .timer(
                ExecutorFamily::Eoa,
                MetricChain::Evm(1),
                ExecutorOperation::RedisCommit,
            )
            .finish_result(&Ok::<_, ()>(()));
        for outcome in ["success", "error", "cancelled"] {
            assert_eq!(
                metrics
                    .duration
                    .with_label_values(&["eoa", "1", "redis_commit", outcome])
                    .get_sample_count(),
                1
            );
        }
        assert_eq!(
            registry
                .gather()
                .iter()
                .find(|m| m.get_name() == "tw_engine_executor_operation_duration_seconds")
                .unwrap()
                .get_metric()
                .len(),
            3
        );
    }
}
