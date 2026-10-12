//! Queue timing uses only fixed labels. Durations are client-observed elapsed
//! time, including socket/Redis scheduling; they are not Redis CPU measurements.
use prometheus::{HistogramOpts, HistogramVec, Registry};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Instant;

#[derive(Clone, Copy)]
pub(crate) enum QueueType {
    Single,
    Multilane,
}
impl QueueType {
    fn label(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Multilane => "multilane",
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) enum Phase {
    DispatchWait,
    PopBatch,
    RedisPop,
    RedisPrune,
    Handler,
    Complete,
    Connection,
    Watch,
    OwnerRead,
    Exec,
    Unwatch,
}
impl Phase {
    fn label(self) -> &'static str {
        match self {
            Self::DispatchWait => "dispatch_wait",
            Self::PopBatch => "pop_batch",
            Self::RedisPop => "redis_pop",
            Self::RedisPrune => "redis_prune",
            Self::Handler => "handler",
            Self::Complete => "complete",
            Self::Connection => "connection",
            Self::Watch => "watch",
            Self::OwnerRead => "owner_read",
            Self::Exec => "exec",
            Self::Unwatch => "unwatch",
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) enum Outcome {
    Success,
    Error,
    Cancelled,
    Conflict,
    Requeue,
    Fail,
}
impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
            Self::Conflict => "conflict",
            Self::Requeue => "requeue",
            Self::Fail => "fail",
        }
    }
}

pub struct QueueMetrics {
    duration: HistogramVec,
}
impl QueueMetrics {
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let duration=HistogramVec::new(HistogramOpts::new("tw_engine_queue_operation_duration_seconds",
            "Queue elapsed time by fixed phase; handler/pop/complete totals contain nested Redis observations and must not be summed")
            .buckets(vec![0.0001,0.0005,0.001,0.005,0.01,0.05,0.1,0.5,1.,5.,15.,60.,300.]),
            &["queue_type","phase","outcome"])?;
        registry.register(Box::new(duration.clone()))?;
        Ok(Self { duration })
    }
}
static METRICS: RwLock<Option<Arc<QueueMetrics>>> = RwLock::new(None);
static DEFAULT: LazyLock<Arc<QueueMetrics>> = LazyLock::new(|| {
    Arc::new(QueueMetrics::new(&Registry::new()).expect("fixed queue metric definition"))
});
pub fn initialize_metrics(metrics: QueueMetrics) {
    *METRICS.write().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(metrics));
}
fn metrics() -> Arc<QueueMetrics> {
    METRICS
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_else(|| DEFAULT.clone())
}

pub(crate) struct Timer {
    metrics: Arc<QueueMetrics>,
    started: Instant,
    queue: QueueType,
    phase: Phase,
    outcome: Outcome,
}
impl Timer {
    pub(crate) fn start(queue: QueueType, phase: Phase) -> Self {
        Self {
            metrics: metrics(),
            started: Instant::now(),
            queue,
            phase,
            outcome: Outcome::Cancelled,
        }
    }
    pub(crate) fn finish(mut self, outcome: Outcome) {
        self.outcome = outcome;
    }
    pub(crate) fn finish_result<T, E>(self, result: &Result<T, E>) {
        self.finish(if result.is_ok() {
            Outcome::Success
        } else {
            Outcome::Error
        });
    }
    pub(crate) fn finish_job<T, E>(self, result: &crate::job::JobResult<T, E>) {
        self.finish(match result {
            Ok(_) => Outcome::Success,
            Err(crate::job::JobError::Nack { .. }) => Outcome::Requeue,
            Err(crate::job::JobError::Fail(_)) => Outcome::Fail,
        });
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        self.metrics
            .duration
            .with_label_values(&[self.queue.label(), self.phase.label(), self.outcome.label()])
            .observe(self.started.elapsed().as_secs_f64());
    }
}
pub(crate) async fn measure<T, E>(
    queue: QueueType,
    phase: Phase,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let timer = Timer::start(queue, phase);
    let result = future.await;
    timer.finish_result(&result);
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn requeue_error_and_cancel_are_distinct_without_job_labels() {
        let registry = Registry::new();
        let metrics = Arc::new(QueueMetrics::new(&registry).unwrap());
        let timer = || Timer {
            metrics: metrics.clone(),
            started: Instant::now(),
            queue: QueueType::Single,
            phase: Phase::Handler,
            outcome: Outcome::Cancelled,
        };
        timer().finish_job(&Err::<(), _>(crate::job::JobError::Nack {
            error: (),
            delay: None,
            position: crate::job::RequeuePosition::Last,
        }));
        timer().finish_result(&Err::<(), _>(()));
        drop(timer());
        for outcome in ["requeue", "error", "cancelled"] {
            assert_eq!(
                metrics
                    .duration
                    .with_label_values(&["single", "handler", outcome])
                    .get_sample_count(),
                1
            );
        }
        let family = registry.gather().pop().unwrap();
        for metric in family.get_metric() {
            assert_eq!(metric.get_label().len(), 3);
        }
    }
}
