use super::*;
use crate::lease_tests::{Handler, cleanup, redis_url};
use crate::{IdempotencyMode, job::JobStatus};

async fn queue() -> Arc<MultilaneQueue<Handler>> {
    let name = format!("multilane-regression:{}", nanoid::nanoid!());
    Arc::new(
        MultilaneQueue::new(
            &redis_url(),
            &name,
            Some(QueueOptions {
                idempotency_mode: IdempotencyMode::Active,
                ..Default::default()
            }),
            Handler {
                counter: format!("twmq_multilane:{name}:hook-count"),
            },
        )
        .await
        .unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn pruning_preserves_delayed_reused_id_and_deletes_finished_lane_jobs() {
    for success in [true, false] {
        let mut queue = queue().await;
        let options = &mut Arc::get_mut(&mut queue).unwrap().options;
        options.max_success = 1;
        options.max_failed = 1;
        queue
            .push_to_lane("lane", JobOptions::new(1).with_id("reused"))
            .await
            .unwrap();
        let (_, old) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        let outcome = || {
            if success {
                Ok(1)
            } else {
                Err(JobError::Fail(crate::lease_tests::TestError))
            }
        };
        queue.complete_job(&old, outcome()).await.unwrap();
        queue
            .push_to_lane(
                "lane",
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
            .push_to_lane("lane", JobOptions::new(3).with_id("trigger-prune"))
            .await
            .unwrap();
        let (_, trigger) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
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
        assert_eq!(queue.count(JobStatus::Delayed, None).await.unwrap(), 1);
        let _: () = conn
            .zadd(queue.lane_delayed_zset_name("lane"), "reused", 0)
            .await
            .unwrap();
        // Make this lane eligible for housekeeping in the same wall-clock second.
        let _: () = conn.zadd(queue.lanes_zset_name(), "lane", 0).await.unwrap();
        let (_, current) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        assert_eq!(*current.data(), 2);
        queue.complete_job(&current, outcome()).await.unwrap();
        assert!(queue.get_job("trigger-prune").await.unwrap().is_none());
        assert!(
            !conn
                .exists::<_, bool>(queue.job_meta_hash_name("trigger-prune"))
                .await
                .unwrap()
        );
        cleanup(&mut conn, &format!("twmq_multilane:{}", queue.queue_id())).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancellation_of_reused_lane_id_is_not_overruled_by_historical_success() {
    for nack in [false, true] {
        let queue = queue().await;
        queue
            .push_to_lane("lane", JobOptions::new(1).with_id("reused"))
            .await
            .unwrap();
        let (_, old) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue.complete_job(&old, Ok(1)).await.unwrap();
        queue
            .push_to_lane("lane", JobOptions::new(2).with_id("reused"))
            .await
            .unwrap();
        let (_, current) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue.cancel_job(current.id()).await.unwrap();
        assert!(queue.pop_batch_jobs(1).await.unwrap().is_empty());
        assert_eq!(queue.count(JobStatus::Active, None).await.unwrap(), 1);
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
                        error: crate::lease_tests::TestError,
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
        assert_eq!(queue.count(JobStatus::Active, None).await.unwrap(), 0);
        assert_eq!(queue.count(JobStatus::Pending, None).await.unwrap(), 0);
        assert_eq!(queue.count(JobStatus::Delayed, None).await.unwrap(), 0);
        assert_eq!(queue.count(JobStatus::Failed, None).await.unwrap(), 1);
        assert!(
            !conn
                .sismember::<_, _, bool>(queue.pending_cancellation_set_name(), "reused")
                .await
                .unwrap()
        );
        queue.complete_job(&current, Ok(2)).await.unwrap();
        assert_eq!(conn.get::<_, u64>(&queue.handler.counter).await.unwrap(), 1);
        cleanup(&mut conn, &format!("twmq_multilane:{}", queue.queue_id())).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn cancelled_expired_lane_job_is_not_reborrowed() {
    let queue = queue().await;
    queue
        .push_to_lane("lane", JobOptions::new(7))
        .await
        .unwrap();
    let (_, borrowed) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
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
    assert_eq!(queue.count(JobStatus::Pending, None).await.unwrap(), 0);
    assert_eq!(queue.count(JobStatus::Active, None).await.unwrap(), 0);
    assert_eq!(queue.count(JobStatus::Failed, None).await.unwrap(), 1);
    cleanup(
        &mut queue.redis.clone(),
        &format!("twmq_multilane:{}", queue.queue_id()),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn same_id_reuse_creates_a_new_lease_generation() {
    let queue = queue().await;
    queue
        .push_to_lane("lane", JobOptions::new(7).with_id("reused"))
        .await
        .unwrap();
    let (_, old) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
    queue.complete_job(&old, Ok(7)).await.unwrap();
    queue
        .push_to_lane("lane", JobOptions::new(8).with_id("reused"))
        .await
        .unwrap();
    let (_, current) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
    assert_ne!(old.lease_token, current.lease_token);
    queue.complete_job(&old, Ok(7)).await.unwrap();
    assert_eq!(queue.count(JobStatus::Active, None).await.unwrap(), 1);
    assert_eq!(
        queue
            .redis
            .clone()
            .get::<_, u64>(&queue.handler.counter)
            .await
            .unwrap(),
        1
    );
    queue.complete_job(&current, Ok(8)).await.unwrap();
    assert_eq!(
        queue
            .redis
            .clone()
            .get::<_, u64>(&queue.handler.counter)
            .await
            .unwrap(),
        2
    );
    cleanup(
        &mut queue.redis.clone(),
        &format!("twmq_multilane:{}", queue.queue_id()),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn concurrent_lane_acknowledgements_commit_once() {
    let queue = queue().await;
    queue
        .push_to_lane("lane", JobOptions::new(7))
        .await
        .unwrap();
    let (_, borrowed) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
    for result in
        futures::future::join_all((0..32).map(|_| queue.complete_job(&borrowed, Ok(7)))).await
    {
        result.unwrap();
    }
    assert_eq!(queue.count(JobStatus::Success, None).await.unwrap(), 1);
    assert_eq!(queue.count(JobStatus::Active, None).await.unwrap(), 0);
    assert_eq!(
        queue
            .redis
            .clone()
            .get::<_, u64>(&queue.handler.counter)
            .await
            .unwrap(),
        1
    );
    cleanup(
        &mut queue.redis.clone(),
        &format!("twmq_multilane:{}", queue.queue_id()),
    )
    .await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn completed_lane_permits_refill_without_waiting_for_poll_timer() {
    let name = format!("lane-refill:{}", nanoid::nanoid!());
    let queue = Arc::new(
        MultilaneQueue::new(
            &redis_url(),
            &name,
            Some(QueueOptions {
                local_concurrency: 2,
                polling_interval: std::time::Duration::from_secs(3600),
                ..Default::default()
            }),
            Handler {
                counter: format!("twmq_multilane:{name}:hook-count"),
            },
        )
        .await
        .unwrap(),
    );
    for id in 0..20 {
        queue
            .push_to_lane(&format!("lane-{}", id % 3), JobOptions::new(id))
            .await
            .unwrap();
    }
    let worker = queue.work();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while queue.count(JobStatus::Success, None).await.unwrap() != 20 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("lane backlog must refill before the next hourly poll");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        queue.poll_count.load(std::sync::atomic::Ordering::Relaxed) <= 21,
        "a drained lane backlog must stop waking the worker"
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
    tokio::time::timeout(std::time::Duration::from_secs(2), worker.shutdown())
        .await
        .unwrap()
        .unwrap();
    cleanup(&mut queue.redis.clone(), &format!("twmq_multilane:{name}")).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn empty_lane_worker_does_not_spin_and_shuts_down_promptly() {
    let name = format!("lane-idle:{}", nanoid::nanoid!());
    let queue = Arc::new(
        MultilaneQueue::new(
            &redis_url(),
            &name,
            Some(QueueOptions {
                polling_interval: std::time::Duration::from_secs(3600),
                ..Default::default()
            }),
            Handler {
                counter: format!("twmq_multilane:{name}:hook-count"),
            },
        )
        .await
        .unwrap(),
    );
    let worker = queue.work();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while queue.poll_count.load(std::sync::atomic::Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(
        queue.poll_count.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), worker.shutdown())
        .await
        .unwrap()
        .unwrap();
    cleanup(&mut queue.redis.clone(), &format!("twmq_multilane:{name}")).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn immediate_lane_refill_preserves_configured_concurrency() {
    use crate::lease_tests::{Gate, GatedHandler};
    let name = format!("bounded-lane-refill:{}", nanoid::nanoid!());
    let gate = Gate::new();
    let queue = Arc::new(
        MultilaneQueue::new(
            &redis_url(),
            &name,
            Some(QueueOptions {
                local_concurrency: 2,
                polling_interval: std::time::Duration::from_secs(3600),
                ..Default::default()
            }),
            GatedHandler { gate: gate.clone() },
        )
        .await
        .unwrap(),
    );
    for id in 0..8 {
        queue
            .push_to_lane(&format!("lane-{}", id % 3), JobOptions::new(id))
            .await
            .unwrap();
    }
    let worker = queue.work();
    gate.wait_for_started(2).await;
    gate.releases.add_permits(2);
    gate.wait_for_started(4).await;
    assert_eq!(gate.current.load(std::sync::atomic::Ordering::SeqCst), 2);
    gate.releases.add_permits(8);
    gate.wait_for_started(8).await;
    tokio::time::timeout(std::time::Duration::from_secs(2), worker.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gate.peak.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(queue.count(JobStatus::Success, None).await.unwrap(), 8);
    cleanup(&mut queue.redis.clone(), &format!("twmq_multilane:{name}")).await;
}

async fn retained_terminal_history(first_success: bool, last_success: bool) {
    let mut queue = queue().await;
    let options = &mut Arc::get_mut(&mut queue).unwrap().options;
    options.max_success = 1;
    options.max_failed = 1;
    let outcomes = [("reused", 1, first_success), ("reused", 2, last_success)];
    for (id, payload, success) in outcomes {
        queue
            .push_to_lane("lane", JobOptions::new(payload).with_id(id))
            .await
            .unwrap();
        let (_, job) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue
            .complete_job(
                &job,
                if success {
                    Ok(payload)
                } else {
                    Err(JobError::Fail(crate::lease_tests::TestError))
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
            .push_to_lane("lane", JobOptions::new(3).with_id("prune-old-kind"))
            .await
            .unwrap();
        let (_, job) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue
            .complete_job(
                &job,
                if first_success {
                    Ok(3)
                } else {
                    Err(JobError::Fail(crate::lease_tests::TestError))
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
        .push_to_lane("lane", JobOptions::new(4).with_id("prune-final-reference"))
        .await
        .unwrap();
    let (_, job) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
    queue
        .complete_job(
            &job,
            if last_success {
                Ok(4)
            } else {
                Err(JobError::Fail(crate::lease_tests::TestError))
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
    cleanup(&mut conn, &format!("twmq_multilane:{}", queue.queue_id())).await;
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
            .push_to_lane("lane", JobOptions::new(1).with_id("zero-retention"))
            .await
            .unwrap();
        let (_, job) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue
            .complete_job(
                &job,
                if success {
                    Ok(1)
                } else {
                    Err(JobError::Fail(crate::lease_tests::TestError))
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
        cleanup(&mut conn, &format!("twmq_multilane:{}", queue.queue_id())).await;
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
            .push_to_lane("lane", JobOptions::new(1).with_id("cancel-then-prune"))
            .await
            .unwrap();
        let (_, job) = queue.pop_batch_jobs(1).await.unwrap().pop().unwrap();
        queue.cancel_job(job.id()).await.unwrap();
        queue
            .complete_job(
                &job,
                if success {
                    Ok(1)
                } else {
                    Err(JobError::Fail(crate::lease_tests::TestError))
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
        cleanup(&mut conn, &format!("twmq_multilane:{}", queue.queue_id())).await;
    }
}
