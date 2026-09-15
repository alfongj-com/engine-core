//! Queue safety regressions using explicit transitions, not sleeps or live RPCs.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TestError;
impl From<TwmqError> for TestError {
    fn from(_: TwmqError) -> Self {
        Self
    }
}
impl UserCancellable for TestError {
    fn user_cancelled() -> Self {
        Self
    }
}
pub(crate) struct Handler {
    pub counter: String,
}
impl DurableExecution for Handler {
    type Output = u64;
    type ErrorData = TestError;
    type JobData = u64;
    async fn process(&self, job: &BorrowedJob<u64>) -> JobResult<u64, TestError> {
        Ok(*job.data())
    }
    async fn on_success(
        &self,
        _: &BorrowedJob<u64>,
        _: SuccessHookData<'_, u64>,
        tx: &mut TransactionContext<'_>,
    ) {
        tx.pipeline().incr(&self.counter, 1);
    }
}
pub(crate) fn redis_url() -> String {
    std::env::var("TEST_REDIS_URL").expect("set TEST_REDIS_URL to a disposable Redis")
}
pub(crate) async fn queue() -> Arc<Queue<Handler>> {
    let name = format!("lease-regression:{}", nanoid::nanoid!());
    Arc::new(
        Queue::new(
            &redis_url(),
            &name,
            Some(QueueOptions {
                idempotency_mode: IdempotencyMode::Active,
                ..Default::default()
            }),
            Handler {
                counter: format!("twmq:{name}:hook-count"),
            },
        )
        .await
        .unwrap(),
    )
}
pub(crate) async fn cleanup(conn: &mut ConnectionManager, prefix: &str) {
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}:*"))
        .query_async(conn)
        .await
        .unwrap();
    if !keys.is_empty() {
        let _: usize = conn.del(keys).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancelled_expired_job_is_not_reborrowed_in_the_same_batch() {
    let queue = queue().await;
    queue
        .push(JobOptions::new(7).with_id("cancel-me"))
        .await
        .unwrap();
    let borrowed = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
    assert!(matches!(
        queue.cancel_job(borrowed.id()).await.unwrap(),
        CancelResult::CancellationPending
    ));
    let _: () = queue
        .redis
        .clone()
        .del(queue.lease_key_name(borrowed.id(), &borrowed.lease_token))
        .await
        .unwrap();
    assert!(queue.pop_batch_jobs(1).await.unwrap().is_empty());
    assert_eq!(queue.count(JobStatus::Pending).await.unwrap(), 0);
    assert_eq!(queue.count(JobStatus::Active).await.unwrap(), 0);
    assert_eq!(queue.count(JobStatus::Failed).await.unwrap(), 1);
    cleanup(&mut queue.redis.clone(), &format!("twmq:{}", queue.name())).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn old_completion_cannot_ack_a_reborrowed_job_or_commit_hooks() {
    let queue = queue().await;
    queue
        .push(JobOptions::new(7).with_id("reuse"))
        .await
        .unwrap();
    let old = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
    let _: () = queue
        .redis
        .clone()
        .del(queue.lease_key_name(old.id(), &old.lease_token))
        .await
        .unwrap();
    let current = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
    assert_ne!(old.lease_token, current.lease_token);
    queue.complete_job(&old, Ok(9)).await.unwrap();
    assert_eq!(queue.count(JobStatus::Active).await.unwrap(), 1);
    assert_eq!(queue.count(JobStatus::Success).await.unwrap(), 0);
    assert_eq!(
        queue
            .redis
            .clone()
            .get::<_, Option<u64>>(&queue.handler.counter)
            .await
            .unwrap(),
        None
    );
    queue.complete_job(&current, Ok(7)).await.unwrap();
    assert_eq!(queue.count(JobStatus::Success).await.unwrap(), 1);
    assert_eq!(
        queue
            .redis
            .clone()
            .get::<_, u64>(&queue.handler.counter)
            .await
            .unwrap(),
        1
    );
    cleanup(&mut queue.redis.clone(), &format!("twmq:{}", queue.name())).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn concurrent_acknowledgements_commit_one_outcome_and_one_hook() {
    let queue = queue().await;
    queue.push(JobOptions::new(7)).await.unwrap();
    let borrowed = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
    let acknowledgements = (0..32).map(|_| queue.complete_job(&borrowed, Ok(7)));
    for result in futures::future::join_all(acknowledgements).await {
        result.unwrap();
    }
    assert_eq!(queue.count(JobStatus::Success).await.unwrap(), 1);
    assert_eq!(queue.count(JobStatus::Active).await.unwrap(), 0);
    assert_eq!(
        queue
            .redis
            .clone()
            .get::<_, u64>(&queue.handler.counter)
            .await
            .unwrap(),
        1
    );
    cleanup(&mut queue.redis.clone(), &format!("twmq:{}", queue.name())).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancellation_after_nack_removes_delayed_work() {
    let queue = queue().await;
    queue.push(JobOptions::new(7)).await.unwrap();
    let borrowed = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
    queue.cancel_job(borrowed.id()).await.unwrap();
    queue
        .complete_job(
            &borrowed,
            Err(JobError::Nack {
                error: TestError,
                delay: Some(Duration::from_secs(60)),
                position: RequeuePosition::Last,
            }),
        )
        .await
        .unwrap();
    assert!(queue.pop_batch_jobs(1).await.unwrap().is_empty());
    assert_eq!(queue.count(JobStatus::Delayed).await.unwrap(), 0);
    assert_eq!(queue.count(JobStatus::Failed).await.unwrap(), 1);
    cleanup(&mut queue.redis.clone(), &format!("twmq:{}", queue.name())).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn completion_error_does_not_replay_partially_executed_commands() {
    let queue = queue().await;
    let lease = format!("twmq:{}:test-lease", queue.name());
    let invalid = format!("twmq:{}:wrong-type", queue.name());
    let effects = format!("twmq:{}:effects", queue.name());
    let mut observer = queue.redis.clone();
    let _: () = observer.set_ex(&lease, 1, 30).await.unwrap();
    let _: () = observer.set(&invalid, "string").await.unwrap();
    let mut pipeline = redis::pipe();
    pipeline.incr(&effects, 1).hset(&invalid, "field", "value");
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        queue
            .transaction_connections
            .commit_if_leased(&lease, &pipeline),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    assert_eq!(observer.get::<_, u64>(&effects).await.unwrap(), 1);
    cleanup(&mut observer, &format!("twmq:{}", queue.name())).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn completed_permits_refill_backlog_without_waiting_for_poll_timer() {
    let name = format!("refill:{}", nanoid::nanoid!());
    let queue = Arc::new(
        Queue::new(
            &redis_url(),
            &name,
            Some(QueueOptions {
                local_concurrency: 2,
                polling_interval: Duration::from_secs(3600),
                ..Default::default()
            }),
            Handler {
                counter: format!("twmq:{name}:hook-count"),
            },
        )
        .await
        .unwrap(),
    );
    for id in 0..20 {
        queue.push(JobOptions::new(id)).await.unwrap();
    }
    let worker = queue.work();
    tokio::time::timeout(Duration::from_secs(5), async {
        while queue.count(JobStatus::Success).await.unwrap() != 20 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("backlog must refill on completion, before the next hourly poll");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        queue.poll_count.load(std::sync::atomic::Ordering::Relaxed) <= 21,
        "a drained backlog must stop waking the worker"
    );
    assert_eq!(
        queue
            .redis
            .clone()
            .get::<_, u64>(&queue.handler.counter)
            .await
            .unwrap(),
        20
    );
    tokio::time::timeout(Duration::from_secs(2), worker.shutdown())
        .await
        .unwrap()
        .unwrap();
    cleanup(&mut queue.redis.clone(), &format!("twmq:{name}")).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn empty_worker_does_not_spin_and_shutdown_does_not_wait_for_poll() {
    let name = format!("idle:{}", nanoid::nanoid!());
    let queue = Arc::new(
        Queue::new(
            &redis_url(),
            &name,
            Some(QueueOptions {
                polling_interval: Duration::from_secs(3600),
                ..Default::default()
            }),
            Handler {
                counter: format!("twmq:{name}:hook-count"),
            },
        )
        .await
        .unwrap(),
    );
    let worker = queue.work();
    tokio::time::timeout(Duration::from_secs(2), async {
        while queue.poll_count.load(std::sync::atomic::Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Observation window for absence of activity, not synchronization with a job.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        queue.poll_count.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    tokio::time::timeout(Duration::from_secs(2), worker.shutdown())
        .await
        .unwrap()
        .unwrap();
    cleanup(&mut queue.redis.clone(), &format!("twmq:{name}")).await;
}

pub(crate) struct Gate {
    pub current: std::sync::atomic::AtomicUsize,
    pub peak: std::sync::atomic::AtomicUsize,
    pub started: std::sync::atomic::AtomicUsize,
    pub releases: tokio::sync::Semaphore,
}
impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            current: 0.into(),
            peak: 0.into(),
            started: 0.into(),
            releases: tokio::sync::Semaphore::new(0),
        })
    }
    pub async fn wait_for_started(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.started.load(std::sync::atomic::Ordering::SeqCst) < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker should refill after a permit is released");
    }
}
pub(crate) struct GatedHandler {
    pub gate: Arc<Gate>,
}
impl DurableExecution for GatedHandler {
    type Output = u64;
    type ErrorData = TestError;
    type JobData = u64;
    async fn process(&self, job: &BorrowedJob<u64>) -> JobResult<u64, TestError> {
        use std::sync::atomic::Ordering::SeqCst;
        let current = self.gate.current.fetch_add(1, SeqCst) + 1;
        self.gate.peak.fetch_max(current, SeqCst);
        self.gate.started.fetch_add(1, SeqCst);
        self.gate.releases.acquire().await.unwrap().forget();
        self.gate.current.fetch_sub(1, SeqCst);
        Ok(*job.data())
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn immediate_refill_preserves_configured_concurrency() {
    let name = format!("bounded-refill:{}", nanoid::nanoid!());
    let gate = Gate::new();
    let queue = Arc::new(
        Queue::new(
            &redis_url(),
            &name,
            Some(QueueOptions {
                local_concurrency: 2,
                polling_interval: Duration::from_secs(3600),
                ..Default::default()
            }),
            GatedHandler { gate: gate.clone() },
        )
        .await
        .unwrap(),
    );
    for id in 0..8 {
        queue.push(JobOptions::new(id)).await.unwrap();
    }
    let worker = queue.work();
    gate.wait_for_started(2).await;
    gate.releases.add_permits(2);
    gate.wait_for_started(4).await;
    assert_eq!(gate.current.load(std::sync::atomic::Ordering::SeqCst), 2);
    gate.releases.add_permits(8);
    gate.wait_for_started(8).await;
    tokio::time::timeout(Duration::from_secs(2), worker.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gate.peak.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(queue.count(JobStatus::Success).await.unwrap(), 8);
    cleanup(&mut queue.redis.clone(), &format!("twmq:{name}")).await;
}
