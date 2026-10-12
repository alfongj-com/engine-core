//! Exercise the production cycle decision through real TWMQ lease completion.
use super::*;
use std::sync::Mutex;
use tokio::sync::{Notify, mpsc};
use twmq::{job::JobOptions, queue::QueueOptions, redis::AsyncCommands};

fn idle() -> EoaExecutorWorkerResult {
    EoaExecutorWorkerResult {
        recovered_transactions: 0,
        confirmed_transactions: 0,
        failed_transactions: 0,
        sent_transactions: 0,
        replaced_transactions: 0,
        submitted_transactions: 0,
        pending_transactions: 0,
        borrowed_transactions: 0,
        recycled_nonces: 0,
    }
}

struct SchedulingHandler {
    result: EoaExecutorWorkerResult,
    delegation: Option<bool>,
    fail_workflow: bool,
    first: Mutex<bool>,
    observations: mpsc::UnboundedSender<String>,
    release_blocker: Notify,
}

impl DurableExecution for SchedulingHandler {
    type Output = EoaExecutorWorkerResult;
    type ErrorData = EoaExecutorWorkerError;
    type JobData = ();

    async fn process(&self, job: &BorrowedJob<()>) -> JobResult<Self::Output, Self::ErrorData> {
        self.observations.send(job.id().to_owned()).unwrap();
        if job.id() == "blocker" {
            self.release_blocker.notified().await;
        }
        let first = if job.id() == "target" {
            std::mem::replace(&mut *self.first.lock().unwrap(), false)
        } else {
            false
        };
        if first {
            if self.fail_workflow {
                // Production process propagates a workflow failure before the
                // successful-cycle scheduling decision, even after some work.
                return Err(EoaExecutorWorkerError::InternalError {
                    message: "injected workflow failure".into(),
                }
                .handle());
            }
            self.result.clone().into_job_result(self.delegation)
        } else {
            idle().into_job_result(self.delegation)
        }
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn progress_requeues_at_tail_while_unknown_and_poll_only_work_stays_delayed() {
    let url = std::env::var("TEST_REDIS_URL").unwrap();
    // Expected delay seconds are TWMQ's real persisted schedule, not the
    // application's subsecond Duration. The blocker freezes queue consumption
    // after the target's lease commit, eliminating sleep/second-boundary races.
    for (name, sent, recovered, pending, borrowed, confirmed, delegation, failure, delay) in [
        ("send_progress", 1, 0, 1, 0, 0, Some(false), false, None),
        (
            "recovered_progress",
            0,
            1,
            1,
            0,
            0,
            Some(false),
            false,
            None,
        ),
        ("no_progress", 0, 0, 1, 0, 0, Some(false), false, Some(1)),
        ("unknown_only", 0, 0, 1, 1, 0, Some(false), false, Some(1)),
        ("finality_only", 0, 0, 1, 0, 1, Some(false), false, Some(1)),
        (
            "no_unsigned_work",
            1,
            0,
            0,
            0,
            0,
            Some(false),
            false,
            Some(1),
        ),
        ("delegated", 1, 0, 1, 0, 0, Some(true), false, Some(2)),
        ("unknown_delegation", 1, 0, 1, 0, 0, None, false, Some(1)),
        (
            "workflow_failure",
            1,
            0,
            1,
            0,
            0,
            Some(false),
            true,
            Some(10),
        ),
    ] {
        let mut result = idle();
        result.sent_transactions = sent;
        result.recovered_transactions = recovered;
        result.pending_transactions = pending;
        result.borrowed_transactions = borrowed;
        result.confirmed_transactions = confirmed;
        result.submitted_transactions = 1;
        let (observations, mut observed) = mpsc::unbounded_channel();
        let queue = Arc::new(
            Queue::new(
                &url,
                &format!("eoa-scheduling-{name}-{}", uuid::Uuid::new_v4()),
                Some(QueueOptions {
                    local_concurrency: 1,
                    polling_interval: Duration::from_secs(3600),
                    ..Default::default()
                }),
                SchedulingHandler {
                    result,
                    delegation,
                    fail_workflow: failure,
                    first: Mutex::new(true),
                    observations,
                    release_blocker: Notify::new(),
                },
            )
            .await
            .unwrap(),
        );
        for id in ["target", "blocker", "tail"] {
            queue.push(JobOptions::new(()).with_id(id)).await.unwrap();
        }
        let worker = queue.work();
        for expected in ["target", "blocker"] {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), observed.recv())
                    .await
                    .expect("worker did not advance")
                    .unwrap(),
                expected,
                "{name}: target must yield its queue slot"
            );
        }
        let mut conn = queue.redis.clone();
        let queued: Vec<String> = conn.lrange(queue.pending_list_name(), 0, -1).await.unwrap();
        let scheduled: Option<u64> = conn
            .zscore(queue.delayed_zset_name(), "target")
            .await
            .unwrap();
        let serialized_error: String = conn
            .lindex(queue.job_errors_list_name("target"), 0)
            .await
            .unwrap();
        let record: twmq::job::JobErrorRecord<EoaExecutorWorkerError> =
            serde_json::from_str(&serialized_error).unwrap();
        if let Some(seconds) = delay {
            assert_eq!(queued, ["tail"], "{name}: must not become immediately due");
            assert_eq!(scheduled, Some(record.created_at + seconds), "{name}");
        } else {
            assert_eq!(queued, ["tail", "target"], "{name}: progress goes to tail");
            assert_eq!(scheduled, None, "{name}: no rounded delay");
        }
        // The old lease has committed; no target remains active during requeue.
        assert!(
            !conn
                .hexists::<_, _, bool>(queue.active_hash_name(), "target")
                .await
                .unwrap()
        );
        queue.handler.release_blocker.notify_one();
        if delay.is_none() {
            for expected in ["tail", "target"] {
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(5), observed.recv())
                        .await
                        .expect("immediate backlog incorrectly waits for the one-hour timer")
                        .unwrap(),
                    expected,
                    "{name}"
                );
            }
        }
        tokio::time::timeout(Duration::from_secs(5), worker.shutdown())
            .await
            .unwrap()
            .unwrap();
        let keys: Vec<String> = conn.keys(format!("twmq:{}:*", queue.name())).await.unwrap();
        if !keys.is_empty() {
            let _: usize = conn.del(keys).await.unwrap();
        }
    }
}
