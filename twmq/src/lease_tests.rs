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
async fn pruning_preserves_delayed_reused_id_and_deletes_finished_jobs() {
    for success in [true, false] {
        let mut queue = queue().await;
        let options = &mut Arc::get_mut(&mut queue).unwrap().options;
        options.max_success = 1;
        options.max_failed = 1;
        queue
            .push(JobOptions::new(1).with_id("reused"))
            .await
            .unwrap();
        let old = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        let outcome = || {
            if success {
                Ok(1)
            } else {
                Err(JobError::Fail(TestError))
            }
        };
        queue.complete_job(&old, outcome()).await.unwrap();
        queue
            .push(
                JobOptions::new(2)
                    .with_id("reused")
                    .with_delay(crate::job::DelayOptions {
                        delay: Duration::from_secs(600),
                        position: RequeuePosition::Last,
                    }),
            )
            .await
            .unwrap();
        queue
            .push(JobOptions::new(3).with_id("trigger-prune"))
            .await
            .unwrap();
        let trigger = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        assert_eq!(trigger.id(), "trigger-prune");
        queue.complete_job(&trigger, outcome()).await.unwrap();
        assert_eq!(queue.get_job("reused").await.unwrap().unwrap().data, 2);
        let mut conn = queue.redis.clone();
        assert!(
            conn.exists::<_, bool>(queue.job_meta_hash_name("reused"))
                .await
                .unwrap()
        );
        assert!(
            conn.sismember::<_, _, bool>(queue.dedupe_set_name(), "reused")
                .await
                .unwrap()
        );
        assert_eq!(queue.count(JobStatus::Delayed).await.unwrap(), 1);
        // Advance only this test-owned delay; do not rely on wall-clock sleeps.
        let _: () = conn
            .zadd(queue.delayed_zset_name(), "reused", 0)
            .await
            .unwrap();
        let current = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        assert_eq!(*current.data(), 2);
        queue.complete_job(&current, outcome()).await.unwrap();
        assert!(queue.get_job("trigger-prune").await.unwrap().is_none());
        assert!(
            !conn
                .exists::<_, bool>(queue.job_meta_hash_name("trigger-prune"))
                .await
                .unwrap()
        );
        cleanup(&mut conn, &format!("twmq:{}", queue.name())).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancellation_of_reused_id_is_not_overruled_by_historical_success() {
    for nack in [false, true] {
        let queue = queue().await;
        queue
            .push(JobOptions::new(1).with_id("reused"))
            .await
            .unwrap();
        let old = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue.complete_job(&old, Ok(1)).await.unwrap();
        queue
            .push(JobOptions::new(2).with_id("reused"))
            .await
            .unwrap();
        let current = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue.cancel_job(current.id()).await.unwrap();
        // A live owner is still allowed to finish. Cancellation must remain
        // pending, even though the success list contains an older generation.
        assert!(queue.pop_batch_jobs(1).await.unwrap().is_empty());
        assert_eq!(queue.count(JobStatus::Active).await.unwrap(), 1);
        let mut conn = queue.redis.clone();
        assert!(
            conn.sismember::<_, _, bool>(queue.pending_cancellation_set_name(), "reused")
                .await
                .unwrap()
        );
        if nack {
            queue
                .complete_job(
                    &current,
                    Err(JobError::Nack {
                        error: TestError,
                        delay: Some(Duration::from_secs(600)),
                        position: RequeuePosition::Last,
                    }),
                )
                .await
                .unwrap();
        } else {
            let _: () = conn
                .del(queue.lease_key_name(current.id(), &current.lease_token))
                .await
                .unwrap();
        }
        assert!(queue.pop_batch_jobs(1).await.unwrap().is_empty());
        assert_eq!(queue.count(JobStatus::Active).await.unwrap(), 0);
        assert_eq!(queue.count(JobStatus::Pending).await.unwrap(), 0);
        assert_eq!(queue.count(JobStatus::Delayed).await.unwrap(), 0);
        assert_eq!(queue.count(JobStatus::Failed).await.unwrap(), 1);
        assert!(
            !conn
                .sismember::<_, _, bool>(queue.pending_cancellation_set_name(), "reused")
                .await
                .unwrap()
        );
        // The expired/nacked owner's late ack cannot run its success hook.
        queue.complete_job(&current, Ok(2)).await.unwrap();
        assert_eq!(conn.get::<_, u64>(&queue.handler.counter).await.unwrap(), 1);
        cleanup(&mut conn, &format!("twmq:{}", queue.name())).await;
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

async fn retained_terminal_history(first_success: bool, last_success: bool) {
    let mut queue = queue().await;
    let options = &mut Arc::get_mut(&mut queue).unwrap().options;
    options.max_success = 1;
    options.max_failed = 1;
    let outcomes = [("reused", 1, first_success), ("reused", 2, last_success)];
    for (id, payload, success) in outcomes {
        queue
            .push(JobOptions::new(payload).with_id(id))
            .await
            .unwrap();
        let job = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue
            .complete_job(
                &job,
                if success {
                    Ok(payload)
                } else {
                    Err(JobError::Fail(TestError))
                },
            )
            .await
            .unwrap();
    }
    let mut conn = queue.redis.clone();
    assert_eq!(
        queue
            .get_job("reused")
            .await
            .unwrap()
            .expect("retained newest entry must keep shared data")
            .data,
        2
    );
    let retained_meta: std::collections::HashMap<String, String> = conn
        .hgetall(queue.job_meta_hash_name("reused"))
        .await
        .unwrap();
    assert!(retained_meta.contains_key("finished_at"));
    if first_success != last_success {
        // Remove the first generation's reference from the OTHER terminal list.
        queue
            .push(JobOptions::new(3).with_id("prune-old-kind"))
            .await
            .unwrap();
        let job = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue
            .complete_job(
                &job,
                if first_success {
                    Ok(3)
                } else {
                    Err(JobError::Fail(TestError))
                },
            )
            .await
            .unwrap();
        assert_eq!(
            queue
                .get_job("reused")
                .await
                .unwrap()
                .expect("other terminal list still retains this ID")
                .data,
            2
        );
        assert_eq!(
            conn.hgetall::<_, std::collections::HashMap<String, String>>(
                queue.job_meta_hash_name("reused")
            )
            .await
            .unwrap(),
            retained_meta
        );
    }
    // Once the newest reference also falls outside retention, delete its records.
    queue
        .push(JobOptions::new(4).with_id("prune-final-reference"))
        .await
        .unwrap();
    let job = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
    queue
        .complete_job(
            &job,
            if last_success {
                Ok(4)
            } else {
                Err(JobError::Fail(TestError))
            },
        )
        .await
        .unwrap();
    assert!(queue.get_job("reused").await.unwrap().is_none());
    assert!(
        !conn
            .exists::<_, bool>(queue.job_meta_hash_name("reused"))
            .await
            .unwrap()
    );
    assert!(
        !conn
            .hexists::<_, _, bool>(queue.job_result_hash_name(), "reused")
            .await
            .unwrap()
    );
    assert!(
        !conn
            .exists::<_, bool>(queue.job_errors_list_name("reused"))
            .await
            .unwrap()
    );
    cleanup(&mut conn, &format!("twmq:{}", queue.name())).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn same_kind_terminal_history_retains_reused_id_until_last_reference() {
    retained_terminal_history(true, true).await;
    retained_terminal_history(false, false).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cross_kind_terminal_history_retains_reused_id_until_last_reference() {
    retained_terminal_history(true, false).await;
    retained_terminal_history(false, true).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn zero_terminal_retention_removes_index_and_records() {
    for success in [true, false] {
        let mut queue = queue().await;
        let options = &mut Arc::get_mut(&mut queue).unwrap().options;
        options.max_success = 0;
        options.max_failed = 0;
        queue
            .push(JobOptions::new(1).with_id("zero-retention"))
            .await
            .unwrap();
        let job = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue
            .complete_job(
                &job,
                if success {
                    Ok(1)
                } else {
                    Err(JobError::Fail(TestError))
                },
            )
            .await
            .unwrap();
        let mut conn = queue.redis.clone();
        assert_eq!(
            conn.llen::<_, usize>(queue.success_list_name())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            conn.llen::<_, usize>(queue.failed_list_name())
                .await
                .unwrap(),
            0
        );
        assert!(queue.get_job("zero-retention").await.unwrap().is_none());
        cleanup(&mut conn, &format!("twmq:{}", queue.name())).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn pruning_final_reference_clears_deferred_cancellation() {
    for success in [true, false] {
        let mut queue = queue().await;
        let options = &mut Arc::get_mut(&mut queue).unwrap().options;
        options.max_success = 0;
        options.max_failed = 0;
        queue
            .push(JobOptions::new(1).with_id("cancel-then-prune"))
            .await
            .unwrap();
        let job = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue.cancel_job(job.id()).await.unwrap();
        queue
            .complete_job(
                &job,
                if success {
                    Ok(1)
                } else {
                    Err(JobError::Fail(TestError))
                },
            )
            .await
            .unwrap();
        let mut conn = queue.redis.clone();
        assert!(
            !conn
                .sismember::<_, _, bool>(queue.pending_cancellation_set_name(), job.id())
                .await
                .unwrap(),
            "pruned terminal record must not leave an orphan cancellation"
        );
        assert!(queue.pop_batch_jobs(1).await.unwrap().is_empty());
        assert_eq!(
            conn.llen::<_, usize>(queue.failed_list_name())
                .await
                .unwrap(),
            0
        );
        assert!(
            !conn
                .exists::<_, bool>(queue.job_meta_hash_name(job.id()))
                .await
                .unwrap()
        );
        assert!(queue.get_job(job.id()).await.unwrap().is_none());
        cleanup(&mut conn, &format!("twmq:{}", queue.name())).await;
    }
}
