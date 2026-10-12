use super::*;
use crate::IdempotencyMode;
use crate::diagnostic_tests::{DiagnosticError, Handler};
use crate::diagnostics::{MAX_JOB_ERROR_RECORD_BYTES, MAX_JOB_ERROR_RECORDS};
use crate::lease_tests::{cleanup, redis_url};
async fn queue() -> Arc<MultilaneQueue<Handler>> {
    let name = format!("diagnostic-regression:{}", nanoid::nanoid!());
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
    .unwrap()
    .arc()
}
async fn pop(queue: &Arc<MultilaneQueue<Handler>>) -> BorrowedJob<u64> {
    queue.pop_batch_jobs(1).await.unwrap().pop().unwrap().1
}
fn nack(cycle: usize) -> JobResult<u64, DiagnosticError> {
    let error = if cycle % 2 == 0 {
        DiagnosticError::WorkRemaining { cycle }
    } else {
        DiagnosticError::Remote {
            cycle,
            message: "actual RPC failure".into(),
        }
    };
    Err(JobError::Nack {
        error,
        delay: None,
        position: RequeuePosition::Last,
    })
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn diagnostics_bound_mixed_nacks_keep_order_attempts_and_existing_expiry() {
    let queue = queue().await;
    queue
        .push_to_lane("lane", JobOptions::new(1).with_id("target"))
        .await
        .unwrap();
    let mut conn = queue.redis.clone();
    let key = queue.job_errors_list_name("target");
    let mut seed = redis::pipe();
    for _ in 0..MAX_JOB_ERROR_RECORDS + 20 {
        seed.rpush(&key, "legacy").ignore();
    }
    seed.expire(&key, 600).ignore();
    seed.query_async::<()>(&mut conn).await.unwrap();
    let mut job = pop(&queue).await;
    let first_attempt = job.job.attempts;
    let count = MAX_JOB_ERROR_RECORDS + 7;
    for cycle in 0..count {
        assert_eq!(job.job.attempts, first_attempt + cycle as u32);
        queue.complete_job(&job, nack(cycle)).await.unwrap();
        assert_eq!(
            conn.llen::<_, usize>(&key).await.unwrap(),
            MAX_JOB_ERROR_RECORDS
        );
        job = pop(&queue).await;
    }
    assert_eq!(
        job.job.attempts,
        first_attempt + count as u32,
        "retention must not reset retry metadata"
    );
    let rows: Vec<String> = conn.lrange(&key, 0, -1).await.unwrap();
    for (offset, row) in rows.iter().enumerate() {
        let record: JobErrorRecord<DiagnosticError> = serde_json::from_str(row).unwrap();
        let cycle = count - 1 - offset;
        match record.error {
            DiagnosticError::WorkRemaining { cycle: observed }
            | DiagnosticError::Remote {
                cycle: observed, ..
            } => assert_eq!(observed, cycle),
        }
        assert_eq!(record.attempt, first_attempt + cycle as u32);
    }
    let ttl: i64 = conn.ttl(&key).await.unwrap();
    assert!(
        ttl > 0 && ttl <= 600,
        "append/trim must preserve existing expiry"
    );
    queue
        .push_to_lane("lane", JobOptions::new(2).with_id("tail"))
        .await
        .unwrap();
    queue.complete_job(&job, nack(count)).await.unwrap();
    assert_eq!(
        conn.lrange::<_, Vec<String>>(queue.lane_pending_list_name("lane"), 0, -1)
            .await
            .unwrap(),
        ["tail", "target"]
    );
    // Preserve existing multilane RPOP semantics; this diagnostic-only patch
    // does not repair its pre-existing Last/RPUSH priority reversal.
    let target = pop(&queue).await;
    assert_eq!(target.id(), "target");
    queue.complete_job(&target, Ok(1)).await.unwrap();
    let tail = pop(&queue).await;
    assert_eq!(tail.id(), "tail");
    queue.complete_job(&tail, Ok(2)).await.unwrap();
    assert_eq!(
        conn.get::<_, usize>(&queue.handler.counter).await.unwrap(),
        count + 1
    );
    cleanup(&mut conn, &format!("twmq_multilane:{}", queue.queue_id())).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn diagnostics_stale_nack_cannot_append_trim_or_change_new_owner() {
    let queue = queue().await;
    queue
        .push_to_lane("lane", JobOptions::new(1).with_id("target"))
        .await
        .unwrap();
    let old = pop(&queue).await;
    let mut conn = queue.redis.clone();
    let key = queue.job_errors_list_name("target");
    let mut seed = redis::pipe();
    for n in 0..MAX_JOB_ERROR_RECORDS + 2 {
        seed.rpush(&key, n).ignore();
    }
    seed.query_async::<()>(&mut conn).await.unwrap();
    let before: Vec<String> = conn.lrange(&key, 0, -1).await.unwrap();
    let _: usize = conn
        .del(queue.lease_key_name(old.id(), &old.lease_token))
        .await
        .unwrap();
    let _: usize = conn.zadd(queue.lanes_zset_name(), "lane", 0).await.unwrap();
    let current = pop(&queue).await;
    assert_ne!(old.lease_token, current.lease_token);
    queue.complete_job(&old, nack(999)).await.unwrap();
    assert_eq!(
        conn.lrange::<_, Vec<String>>(&key, 0, -1).await.unwrap(),
        before
    );
    assert_eq!(
        conn.hget::<_, _, String>(queue.job_meta_hash_name("target"), "lease_token")
            .await
            .unwrap(),
        current.lease_token
    );
    assert!(
        !conn
            .exists::<_, bool>(&queue.handler.counter)
            .await
            .unwrap()
    );
    queue.complete_job(&current, nack(1)).await.unwrap();
    assert_eq!(
        conn.llen::<_, usize>(&key).await.unwrap(),
        MAX_JOB_ERROR_RECORDS
    );
    assert_eq!(
        conn.get::<_, usize>(&queue.handler.counter).await.unwrap(),
        1
    );
    let next = pop(&queue).await;
    queue.complete_job(&next, Ok(1)).await.unwrap();
    cleanup(&mut conn, &format!("twmq_multilane:{}", queue.queue_id())).await;
}

#[tokio::test]
#[ignore = "requires disposable Redis in TEST_REDIS_URL"]
async fn diagnostics_oversize_nack_fail_and_queue_error_preserve_transitions() {
    let queue = queue().await;
    let mut conn = queue.redis.clone();
    for site in ["nack", "fail", "deserialize"] {
        queue
            .push_to_lane("lane", JobOptions::new(1).with_id(site))
            .await
            .unwrap();
        let job = pop(&queue).await;
        let key = queue.job_errors_list_name(site);
        let mut seed = redis::pipe();
        for _ in 0..MAX_JOB_ERROR_RECORDS + 1 {
            seed.rpush(&key, "legacy").ignore();
        }
        seed.query_async::<()>(&mut conn).await.unwrap();
        let error = DiagnosticError::Remote {
            cycle: 77,
            message: "\0".repeat(MAX_JOB_ERROR_RECORD_BYTES),
        };
        if site == "deserialize" {
            let malformed = Job {
                id: job.job.id.clone(),
                data: None,
                attempts: job.job.attempts,
                created_at: job.job.created_at,
                processed_at: job.job.processed_at,
                finished_at: None,
            };
            queue
                .complete_job_queue_error(&malformed, &job.lease_token, &error)
                .await
                .unwrap();
        } else if site == "fail" {
            queue
                .complete_job(&job, Err(JobError::Fail(error)))
                .await
                .unwrap();
        } else {
            queue
                .complete_job(
                    &job,
                    Err(JobError::Nack {
                        error,
                        delay: Some(Duration::from_secs(600)),
                        position: RequeuePosition::First,
                    }),
                )
                .await
                .unwrap();
            assert!(
                conn.zscore::<_, _, Option<u64>>(queue.lane_delayed_zset_name("lane"), site)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                conn.hget::<_, _, String>(queue.job_meta_hash_name(site), "reentry_position")
                    .await
                    .unwrap(),
                RequeuePosition::First.to_string()
            );
        }
        assert_eq!(
            conn.llen::<_, usize>(&key).await.unwrap(),
            MAX_JOB_ERROR_RECORDS
        );
        let encoded: String = conn.lindex(&key, 0).await.unwrap();
        assert!(encoded.len() <= MAX_JOB_ERROR_RECORD_BYTES);
        let marker: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(marker["attempt"], job.job.attempts);
        assert_eq!(
            marker["diagnosticOmitted"]["reason"],
            "serialized_record_exceeds_limit"
        );
        assert!(marker.get("error").is_none());
        assert_eq!(conn.ttl::<_, i64>(&key).await.unwrap(), -1, "no new expiry");
        assert!(
            !conn
                .exists::<_, bool>(queue.lease_key_name(job.id(), &job.lease_token))
                .await
                .unwrap()
        );
        if site != "nack" {
            assert!(
                conn.lpos::<_, _, Option<usize>>(
                    queue.failed_list_name(),
                    site,
                    redis::LposOptions::default()
                )
                .await
                .unwrap()
                .is_some()
            );
            assert!(
                !conn
                    .sismember::<_, _, bool>(queue.dedupe_set_name(), site)
                    .await
                    .unwrap()
            );
        }
    }
    assert_eq!(
        conn.get::<_, usize>(&queue.handler.counter).await.unwrap(),
        3
    );
    cleanup(&mut conn, &format!("twmq_multilane:{}", queue.queue_id())).await;
}
