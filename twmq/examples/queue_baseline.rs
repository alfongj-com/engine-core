//! Reproducible queue-only load experiment. Requires a dedicated Redis instance.
//! Completion is observed from a Redis hook committed in the same transaction as
//! success, then independently reconciled with success IDs and stored results.
//! This measures Redis-committed completion, not disk durability or blockchain TPS.

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Semaphore, task::JoinSet};
use twmq::{
    BorrowedJob, DurableExecution, IdempotencyMode, Queue, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{JobError, JobResult, JobStatus, RequeuePosition},
    queue::QueueOptions,
};

#[derive(Clone, Serialize, Deserialize)]
struct Payload {
    sequence: usize,
    scheduled_us: u64,
    started_us: u64,
    body: String,
}

#[derive(Serialize, Deserialize)]
struct Output {
    sequence: usize,
    checksum: u64,
    bytes: usize,
}

#[derive(Serialize, Deserialize)]
struct JobFailure(String);

impl From<TwmqError> for JobFailure {
    fn from(error: TwmqError) -> Self {
        Self(error.to_string())
    }
}

impl UserCancellable for JobFailure {
    fn user_cancelled() -> Self {
        Self("cancelled".into())
    }
}

struct Handler {
    attempts: Arc<Vec<AtomicU32>>,
    retry_every: usize,
    completion_key: String,
    effects_key: String,
}

fn checksum(body: &str) -> u64 {
    body.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}

impl DurableExecution for Handler {
    type JobData = Payload;
    type Output = Output;
    type ErrorData = JobFailure;

    async fn process(&self, job: &BorrowedJob<Payload>) -> JobResult<Output, JobFailure> {
        let data = job.data();
        let attempt = self.attempts[data.sequence].fetch_add(1, Ordering::Relaxed);
        if self.retry_every != 0 && data.sequence % self.retry_every == 0 && attempt == 0 {
            return Err(JobError::Nack {
                error: JobFailure("one deterministic retry".into()),
                delay: None,
                position: RequeuePosition::Last,
            });
        }
        Ok(Output {
            sequence: data.sequence,
            checksum: checksum(&data.body),
            bytes: data.body.len(),
        })
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Payload>,
        _: SuccessHookData<'_, Output>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Two real Redis writes are included in the measured completion path.
        // HINCRBY exposes duplicate committed effects; RPUSH provides a cursor.
        let data = job.data();
        tx.pipeline()
            .cmd("HINCRBY")
            .arg(&self.effects_key)
            .arg(job.id())
            .arg(1)
            .ignore()
            .cmd("RPUSH")
            .arg(&self.completion_key)
            .arg(format!(
                "{}:{}:{}",
                data.sequence, data.scheduled_us, data.started_us
            ))
            .ignore();
    }
}

fn number(name: &str, default: usize) -> Result<usize, Box<dyn Error>> {
    Ok(match std::env::var(name) {
        Ok(value) => value.parse()?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    })
}

fn quantiles(mut values: Vec<u64>) -> serde_json::Value {
    values.sort_unstable();
    if values.is_empty() {
        return json!({"samples": 0});
    }
    let percentile = |p: usize| values[(values.len() * p).div_ceil(100).saturating_sub(1)];
    json!({
        "samples": values.len(), "unit": "microseconds",
        "p50": percentile(50), "p95": percentile(95),
        "p99": percentile(99), "max": values[values.len() - 1],
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    let redis_url = std::env::var("BENCH_REDIS_URL")
        .map_err(|_| "Set BENCH_REDIS_URL to a dedicated disposable Redis instance")?;
    let rate = number("BENCH_RATE", 1000)?;
    let seconds = number("BENCH_SECONDS", 5)?;
    let concurrency = number("BENCH_CONCURRENCY", 100)?;
    let producer_concurrency = number("BENCH_PRODUCER_CONCURRENCY", 128)?;
    let retry_every = number("BENCH_RETRY_EVERY", 0)?;
    let drain_seconds = number("BENCH_DRAIN_SECONDS", 30)?;
    if rate == 0 || seconds == 0 || concurrency == 0 || producer_concurrency == 0 {
        return Err("rate, seconds and concurrency must be positive".into());
    }
    let offered = rate.checked_mul(seconds).ok_or("workload is too large")?;
    if offered > 1_000_000 {
        return Err("limit this local experiment to 1,000,000 jobs".into());
    }
    let started_epoch_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let name = format!("baseline_{}", nanoid::nanoid!(12));
    let completion_key = format!("twmq:{name}:benchmark:completions");
    let effects_key = format!("twmq:{name}:benchmark:effects");
    let attempts = Arc::new((0..offered).map(|_| AtomicU32::new(0)).collect::<Vec<_>>());
    let queue = Arc::new(
        Queue::new(
            &redis_url,
            &name,
            Some(QueueOptions {
                max_success: offered + 1,
                max_failed: offered + 1,
                lease_duration: Duration::from_secs(30),
                local_concurrency: concurrency,
                polling_interval: Duration::from_millis(10),
                always_poll: true,
                idempotency_mode: IdempotencyMode::Permanent,
            }),
            Handler {
                attempts: attempts.clone(),
                retry_every,
                completion_key: completion_key.clone(),
                effects_key: effects_key.clone(),
            },
        )
        .await?,
    );

    let origin = Instant::now();
    let observer_stop = Arc::new(AtomicBool::new(false));
    let observer = {
        let mut connection = queue.redis.clone();
        let queue = queue.clone();
        let stop = observer_stop.clone();
        let key = completion_key.clone();
        tokio::spawn(async move {
            let mut cursor = 0usize;
            let mut records = Vec::new();
            let mut depths = Vec::new();
            let mut next_depth = Instant::now();
            loop {
                let batch: Vec<String> = redis::cmd("LRANGE")
                    .arg(&key)
                    .arg(cursor)
                    .arg(-1)
                    .query_async(&mut connection)
                    .await?;
                let observed_us = origin.elapsed().as_micros() as u64;
                cursor += batch.len();
                for entry in batch {
                    let fields: Vec<u64> = entry
                        .split(':')
                        .map(str::parse)
                        .collect::<Result<_, _>>()
                        .map_err(|_| {
                            redis::RedisError::from((
                                redis::ErrorKind::TypeError,
                                "malformed observation",
                            ))
                        })?;
                    if fields.len() != 3 {
                        return Err(redis::RedisError::from((
                            redis::ErrorKind::TypeError,
                            "malformed observation",
                        )));
                    }
                    records.push(json!({
                        "id": fields[0], "scheduled_us": fields[1], "push_started_us": fields[2],
                        "completion_observed_us": observed_us,
                        "submit_to_completion_us": observed_us.saturating_sub(fields[2]),
                        "offered_to_completion_us": observed_us.saturating_sub(fields[1]),
                    }));
                }
                if Instant::now() >= next_depth {
                    let (pending, active, success): (usize, usize, usize) = redis::pipe()
                        .cmd("LLEN")
                        .arg(queue.pending_list_name())
                        .cmd("HLEN")
                        .arg(queue.active_hash_name())
                        .cmd("LLEN")
                        .arg(queue.success_list_name())
                        .query_async(&mut connection)
                        .await?;
                    depths.push(json!({"elapsed_us": observed_us, "pending": pending, "active": active, "success": success}));
                    next_depth = Instant::now() + Duration::from_millis(100);
                }
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, redis::RedisError>((records, depths))
        })
    };
    let worker = queue.work();
    let permits = Arc::new(Semaphore::new(producer_concurrency));
    let mut producers = JoinSet::new();
    let mut generator_dropped = 0usize;
    let mut generator_lag = Vec::with_capacity(offered);
    let body = "0123456789abcdef".repeat(16); // 256 bytes, deterministic and compressible.
    let mut admission_records = Vec::with_capacity(offered);
    for sequence in 0..offered {
        let scheduled = Duration::from_secs_f64(sequence as f64 / rate as f64);
        tokio::time::sleep_until(tokio::time::Instant::from_std(origin + scheduled)).await;
        generator_lag.push(origin.elapsed().saturating_sub(scheduled).as_micros() as u64);
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            generator_dropped += 1;
            continue;
        };
        let queue = queue.clone();
        let body = body.clone();
        producers.spawn(async move {
            let _permit = permit;
            let started_us = origin.elapsed().as_micros() as u64;
            let result = queue
                .job(Payload {
                    sequence,
                    scheduled_us: scheduled.as_micros() as u64,
                    started_us,
                    body,
                })
                .with_id(sequence.to_string())
                .push()
                .await;
            let returned_us = origin.elapsed().as_micros() as u64;
            (
                sequence,
                started_us,
                returned_us,
                result.map(|_| ()).map_err(|error| error.to_string()),
            )
        });
        // Reap completed tasks without delaying future scheduled arrivals.
        while let Some(result) = producers.try_join_next() {
            admission_records.push(result?);
        }
    }
    while let Some(result) = producers.join_next().await {
        admission_records.push(result?);
    }
    let producer_finished_us = origin.elapsed().as_micros() as u64;
    let accepted_ids: HashSet<String> = admission_records
        .iter()
        .filter(|item| item.3.is_ok())
        .map(|item| item.0.to_string())
        .collect();
    let accepted = accepted_ids.len();
    let deadline = Instant::now() + Duration::from_secs(drain_seconds as u64);
    loop {
        let completed =
            queue.count(JobStatus::Success).await? + queue.count(JobStatus::Failed).await?;
        if completed >= accepted || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let drained_us = origin.elapsed().as_micros() as u64;
    worker.shutdown().await?;
    observer_stop.store(true, Ordering::Relaxed);
    let (records, depths) = observer.await??;

    let mut connection = queue.redis.clone();
    let success_ids: Vec<String> = redis::cmd("LRANGE")
        .arg(queue.success_list_name())
        .arg(0)
        .arg(-1)
        .query_async(&mut connection)
        .await?;
    let failed_ids: Vec<String> = redis::cmd("LRANGE")
        .arg(queue.failed_list_name())
        .arg(0)
        .arg(-1)
        .query_async(&mut connection)
        .await?;
    let results: HashMap<String, String> = redis::cmd("HGETALL")
        .arg(queue.job_result_hash_name())
        .query_async(&mut connection)
        .await?;
    let effects: HashMap<String, u32> = redis::cmd("HGETALL")
        .arg(&effects_key)
        .query_async(&mut connection)
        .await?;
    let unique_success: HashSet<String> = success_ids.iter().cloned().collect();
    let unique_failed: HashSet<String> = failed_ids.iter().cloned().collect();
    let terminal: HashSet<String> = unique_success.union(&unique_failed).cloned().collect();
    let unresolved: Vec<String> = accepted_ids.difference(&terminal).cloned().collect();
    let unexpected_terminal: Vec<String> = terminal.difference(&accepted_ids).cloned().collect();
    let missing_results: Vec<String> = unique_success
        .iter()
        .filter(|id| !results.contains_key(*id))
        .cloned()
        .collect();
    let mut corrupt_results = Vec::new();
    for (id, result) in &results {
        let parsed = serde_json::from_str::<Output>(result);
        if !matches!(parsed, Ok(output) if output.sequence.to_string() == *id && output.bytes == body.len() && output.checksum == checksum(&body))
        {
            corrupt_results.push(id.clone());
        }
    }
    let duplicate_effects: u64 = effects
        .values()
        .map(|count| u64::from(count.saturating_sub(1)))
        .sum();
    let missing_effects: Vec<String> = unique_success
        .iter()
        .filter(|id| effects.get(*id) != Some(&1))
        .cloned()
        .collect();
    let observed_unique: HashSet<String> = records
        .iter()
        .map(|record| record["id"].as_u64().unwrap().to_string())
        .collect();
    let observations_match =
        observed_unique == unique_success && records.len() == unique_success.len();
    let attempts_total: u64 = attempts
        .iter()
        .map(|count| u64::from(count.load(Ordering::Relaxed)))
        .sum();
    let expected_retries = if retry_every == 0 {
        0
    } else {
        accepted_ids
            .iter()
            .filter(|id| id.parse::<usize>().unwrap() % retry_every == 0)
            .count()
    };
    let pending = queue.count(JobStatus::Pending).await?;
    let active = queue.count(JobStatus::Active).await?;
    let delayed = queue.count(JobStatus::Delayed).await?;
    let healthy = unresolved.is_empty()
        && unexpected_terminal.is_empty()
        && missing_results.is_empty()
        && corrupt_results.is_empty()
        && duplicate_effects == 0
        && missing_effects.is_empty()
        && success_ids.len() == unique_success.len()
        && unique_failed.is_empty()
        && observations_match
        && pending + active + delayed == 0
        && attempts_total == (accepted + expected_retries) as u64;
    let report = json!({
        "schema_version": 1, "started_epoch_ms": started_epoch_ms,
        "scope": "queue-only; Redis-committed completion; persistence depends on Redis configuration",
        "workload": {"rate_per_second": rate, "seconds": seconds, "body_bytes": body.len(),
            "worker_processes": 1, "tokio_threads": 4, "local_concurrency": concurrency,
            "producer_concurrency": producer_concurrency, "poll_interval_ms": 10,
            "observation_interval_ms": 10, "retry_every": retry_every, "idempotency": "Permanent",
            "retained_successes": offered + 1, "completion_hook_writes": 2},
        "accounting": {"offered": offered, "generator_dropped": generator_dropped,
            "accepted": accepted, "push_errors": admission_records.len() - accepted,
            "unique_success": unique_success.len(), "failed": unique_failed.len(),
            "handler_attempts": attempts_total, "expected_retries": expected_retries,
            "duplicate_success_entries": success_ids.len() - unique_success.len(),
            "duplicate_committed_effects": duplicate_effects, "unresolved_ids": unresolved,
            "unexpected_terminal_ids": unexpected_terminal, "missing_results": missing_results,
            "corrupt_results": corrupt_results, "missing_or_duplicate_effects": missing_effects,
            "observations_match_successes": observations_match,
            "pending": pending, "active": active, "delayed": delayed, "healthy": healthy},
        "timing": {"producer_finished_us": producer_finished_us, "drained_us": drained_us,
            "admission_per_second": accepted as f64 / (producer_finished_us as f64 / 1e6),
            "unique_completion_per_second_including_drain": unique_success.len() as f64 / (drained_us as f64 / 1e6)},
        "latency": {
            "submit_to_observed_completion": quantiles(records.iter().map(|record| record["submit_to_completion_us"].as_u64().unwrap()).collect()),
            "offered_to_observed_completion": quantiles(records.iter().map(|record| record["offered_to_completion_us"].as_u64().unwrap()).collect()),
            "push_response": quantiles(admission_records.iter().filter(|item| item.3.is_ok()).map(|item| item.2 - item.1).collect()),
            "generator_lag": quantiles(generator_lag)},
        "queue_depth_samples": depths, "completion_samples": records,
        "admission_errors": admission_records.iter().filter_map(|item| item.3.as_ref().err().map(|error| json!({"id": item.0, "error": error}))).collect::<Vec<_>>()
    });

    // SCAN and delete only this unique namespace, never flush shared data.
    let mut cursor = 0u64;
    let mut keys = Vec::<String>::new();
    loop {
        let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(format!("twmq:{name}:*"))
            .arg("COUNT")
            .arg(1000)
            .query_async(&mut connection)
            .await?;
        keys.extend(batch);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    for batch in keys.chunks(1000) {
        redis::cmd("DEL")
            .arg(batch)
            .query_async::<usize>(&mut connection)
            .await?;
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !healthy {
        return Err("queue result reconciliation failed; inspect JSON accounting".into());
    }
    Ok(())
}
