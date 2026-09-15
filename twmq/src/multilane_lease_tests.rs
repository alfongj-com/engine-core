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
