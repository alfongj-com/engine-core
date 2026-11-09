//! Offline projection repair. The server's journal owner lock excludes workers.
//! Rebuild never opens an RPC connection or grants broadcast permission.
use engine_core::recovery::RecoveryJournal;
use engine_executors::{
    eoa::{EoaExecutorStore, EoaExecutorWorkerJobData, EoaTransactionRequest},
    external_bundler::send::ExternalBundlerSendJobData,
    solana_executor::{
        storage::{SolanaTransactionStorage, solana_admission_fingerprint},
        worker::SolanaExecutorJobData,
    },
};
use std::sync::Arc;
use thirdweb_engine::queue::manager::QueueManager;
use twmq::{
    DurableExecution, IdempotencyMode, Queue,
    redis::{self, AsyncCommands},
};

#[derive(Default, serde::Serialize)]
pub(crate) struct RebuildSummary {
    eoa: usize,
    solana: usize,
    erc4337_unsent: usize,
    /// Unsupported legacy kinds/attempted AA need their original reconciliation path.
    requires_reconciliation: usize,
}

pub(crate) async fn rebuild(
    journal: &Arc<RecoveryJournal>,
    queues: &QueueManager,
    namespace: Option<String>,
    completed_ttl: u64,
) -> anyhow::Result<RebuildSummary> {
    let mut summary = RebuildSummary::default();
    let mut after = None;
    loop {
        let page = journal.rebuild_page(after.take()).await?;
        if page.is_empty() {
            break;
        }
        for record in page {
            let id = &record.admission.id;
            after = Some(id.clone());
            journal.ensure_healthy().await?;
            let queue_name = match record.admission.kind.as_str() {
                "eoa" => {
                    let data: EoaTransactionRequest =
                        serde_json::from_value(record.admission.payload).map_err(|_| {
                            anyhow::anyhow!("Journal payload is incompatible with queue repair")
                        })?;
                    anyhow::ensure!(data.transaction_id == *id, "EOA admission ID mismatch");
                    let store = EoaExecutorStore::new(
                        queues.eoa_executor_queue.redis.clone(),
                        namespace.clone(),
                        data.from,
                        data.chain_id,
                        completed_ttl,
                    );
                    let job_id = format!("eoa_{}_{}", data.from, data.chain_id);
                    let existing: Option<String> = queues
                        .eoa_executor_queue
                        .redis
                        .clone()
                        .hget(queues.eoa_executor_queue.job_data_hash_name(), &job_id)
                        .await?;
                    let job = match existing {
                        Some(encoded) => {
                            let job: EoaExecutorWorkerJobData = serde_json::from_str(&encoded)
                                .map_err(|_| anyhow::anyhow!("Invalid EOA worker projection"))?;
                            anyhow::ensure!(
                                job.eoa_address == data.from && job.chain_id == data.chain_id,
                                "EOA worker identity conflicts with journal"
                            );
                            job
                        }
                        None => EoaExecutorWorkerJobData {
                            eoa_address: data.from,
                            chain_id: data.chain_id,
                            noop_signing_credential: data.signing_credential.clone(),
                        },
                    };
                    restore_job(&queues.eoa_executor_queue, &job_id, &job).await?;
                    let owner = format!("offline-rebuild-{}", rand::random::<u128>());
                    let atomic = store
                        .acquire_eoa_lock_aggressively(
                            &owner,
                            queues.eoa_executor_queue.handler.eoa_metrics.clone(),
                            &queues.eoa_executor_queue.handler.redis_client,
                        )
                        .await?;
                    let result = atomic
                        .rebuild_missing_intent(
                            &data,
                            record.admission.replay_key.as_deref(),
                            &record.attempts,
                        )
                        .await;
                    atomic.release_eoa_lock().await?;
                    result?;
                    summary.eoa += 1;
                    "eoa_executor"
                }
                "solana" => {
                    let data: SolanaExecutorJobData =
                        serde_json::from_value(record.admission.payload).map_err(|_| {
                            anyhow::anyhow!("Journal payload is incompatible with queue repair")
                        })?;
                    anyhow::ensure!(data.transaction_id == *id, "Solana admission ID mismatch");
                    let storage = SolanaTransactionStorage::new(
                        queues.solana_executor_queue.redis.clone(),
                        namespace.clone(),
                    );
                    let schema: Option<String> = queues
                        .solana_executor_queue
                        .redis
                        .clone()
                        .get(format!(
                            "twmq:{}:solana_admission_schema",
                            queues.solana_executor_queue.name()
                        ))
                        .await?;
                    anyhow::ensure!(
                        schema.as_deref().is_none_or(|s| s == "1"),
                        "Unsupported Solana admission schema"
                    );
                    // Worker restoration validates signed bytes and activates its durable
                    // recovery budget before any possible replay. No signing happens here.
                    restore_solana_admission(&queues.solana_executor_queue.redis, &storage, &data)
                        .await?;
                    restore_job(&queues.solana_executor_queue, id, &data).await?;
                    summary.solana += 1;
                    "solana_executor"
                }
                "erc4337" if record.attempts.is_empty() => {
                    let data: ExternalBundlerSendJobData =
                        serde_json::from_value(record.admission.payload).map_err(|_| {
                            anyhow::anyhow!("Journal payload is incompatible with queue repair")
                        })?;
                    anyhow::ensure!(
                        data.transaction_id == *id && data.pregenerated_nonce.is_some(),
                        "ERC4337 admission has no retained nonce"
                    );
                    restore_job(&queues.external_bundler_send_queue, id, &data).await?;
                    summary.erc4337_unsent += 1;
                    "external_bundler_send"
                }
                _ => {
                    summary.requires_reconciliation += 1;
                    continue;
                }
            };
            let key = queues.transaction_registry.registry_key();
            let result: i32 = redis::Script::new(
                r#"
                local t=redis.call('TYPE',KEYS[1]).ok
                if t~='none' and t~='hash' then return -1 end
                local old=redis.call('HGET',KEYS[1],ARGV[1])
                if old and old~=ARGV[2] then return -1 end
                redis.call('HSET',KEYS[1],ARGV[1],ARGV[2]);return 1
            "#,
            )
            .key(key)
            .arg(id)
            .arg(queue_name)
            .invoke_async(&mut queues.eoa_executor_queue.redis.clone())
            .await?;
            anyhow::ensure!(result > 0, "Transaction registry conflicts with journal");
            journal.ensure_healthy().await?;
        }
    }
    Ok(summary)
}

async fn restore_solana_admission(
    redis: &redis::aio::ConnectionManager,
    storage: &SolanaTransactionStorage,
    data: &SolanaExecutorJobData,
) -> anyhow::Result<()> {
    let fingerprint = solana_admission_fingerprint(data)?;
    let result: i32 = redis::Script::new(
        r#"
        local t=redis.call('TYPE',KEYS[1]).ok
        if t~='none' and t~='hash' then return -1 end
        local f=redis.call('HGET',KEYS[1],'fingerprint')
        local state=redis.call('HGET',KEYS[1],'state')
        if (f and f~=ARGV[1]) or (state and state~='active') then return -1 end
        if not f and redis.call('EXISTS',KEYS[1])==1 then return -1 end
        redis.call('HSET',KEYS[1],'fingerprint',ARGV[1],'state','active');return 1
    "#,
    )
    .key(storage.admission_key(&data.transaction_id))
    .arg(fingerprint)
    .invoke_async(&mut redis.clone())
    .await?;
    anyhow::ensure!(result > 0, "Solana admission conflicts with journal");
    Ok(())
}

/// Preserve a live job's counters/order; restore missing scheduling only after
/// every type, payload, terminal/cancellation and live-lease check succeeds.
async fn restore_job<H: DurableExecution>(
    queue: &Queue<H>,
    id: &str,
    data: &H::JobData,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(data)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let result: i32 = redis::Script::new(
        r#"
        local types={'hash','hash','list','hash','zset','set','set','list','list','hash'}
        for i,t in ipairs(types) do
            local actual=redis.call('TYPE',KEYS[i]).ok
            if actual~='none' and actual~=t then return -1 end
        end
        local id=ARGV[1]
        local old=redis.call('HGET',KEYS[1],id)
        if old and old~=ARGV[2] then return -2 end
        if redis.call('SISMEMBER',KEYS[7],id)==1 then return -3 end
        local pending=redis.call('LPOS',KEYS[3],id,'COUNT',2)
        if #pending>1 then return -3 end
        local active=redis.call('HEXISTS',KEYS[4],id)==1
        local delayed=redis.call('ZSCORE',KEYS[5],id)~=false
        local token=redis.call('HGET',KEYS[2],'lease_token')
        if token and redis.call('EXISTS',ARGV[5]..token)==1 then return -4 end
        local finished=redis.call('HEXISTS',KEYS[2],'finished_at')==1
            or redis.call('LPOS',KEYS[8],id)~=false
            or redis.call('LPOS',KEYS[9],id)~=false
            or redis.call('HEXISTS',KEYS[10],id)==1
        -- EOA worker IDs are intentionally reusable after successful completion.
        -- Permanent transaction jobs never revive a terminal/cancelled ID.
        local reuse=ARGV[4]=='1' and not active and not delayed and #pending==0
            and redis.call('SISMEMBER',KEYS[6],id)==0
        if finished and not reuse then return -3 end
        if (#pending>0 and delayed) or (active and (#pending>0 or delayed)) then return -3 end
        local attempts=redis.call('HGET',KEYS[2],'attempts')
        local created=redis.call('HGET',KEYS[2],'created_at')
        if (attempts and (not string.match(attempts,'^%d+$') or #attempts>10))
            or (created and (not string.match(created,'^%d+$') or #created>12)) then return -3 end
        -- No runtime-error-prone validation remains after the first write.
        redis.call('HSET',KEYS[1],id,ARGV[2])
        if reuse then
            redis.call('DEL',KEYS[2])
            redis.call('LREM',KEYS[8],0,id);redis.call('LREM',KEYS[9],0,id)
            redis.call('HDEL',KEYS[10],id)
        end
        redis.call('HSETNX',KEYS[2],'created_at',ARGV[3])
        redis.call('HSETNX',KEYS[2],'attempts',0)
        redis.call('HDEL',KEYS[2],'lease_token')
        redis.call('HDEL',KEYS[4],id)
        redis.call('SADD',KEYS[6],id)
        if #pending==0 and not delayed then redis.call('RPUSH',KEYS[3],id) end
        return 1
    "#,
    )
    .key(queue.job_data_hash_name())
    .key(queue.job_meta_hash_name(id))
    .key(queue.pending_list_name())
    .key(queue.active_hash_name())
    .key(queue.delayed_zset_name())
    .key(queue.dedupe_set_name())
    .key(queue.pending_cancellation_set_name())
    .key(queue.success_list_name())
    .key(queue.failed_list_name())
    .key(queue.job_result_hash_name())
    .arg(id)
    .arg(payload)
    .arg(now)
    .arg(
        if queue.options.idempotency_mode == IdempotencyMode::Active {
            1
        } else {
            0
        },
    )
    .arg(format!("twmq:{}:job:{id}:lease:", queue.name()))
    .invoke_async(&mut queue.redis.clone())
    .await?;
    anyhow::ensure!(
        result > 0,
        "Queue projection conflicts with journal or has a live lease"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_executors::solana_executor::worker::SolanaExecutorError;
    use serde_json::{Value, json};
    use twmq::{
        job::{BorrowedJob, JobResult},
        queue::QueueOptions,
    };

    struct Offline;
    impl DurableExecution for Offline {
        type JobData = Value;
        type Output = ();
        type ErrorData = SolanaExecutorError;
        async fn process(&self, _: &BorrowedJob<Value>) -> JobResult<(), Self::ErrorData> {
            panic!("offline repair must never execute a job")
        }
    }
    async fn fixture(active: bool) -> Queue<Offline> {
        Queue::builder()
            .redis_url(std::env::var("TEST_REDIS_URL").unwrap())
            .name(format!("repair_{}", rand::random::<u64>()))
            .handler(Offline)
            .options(QueueOptions {
                idempotency_mode: if active {
                    IdempotencyMode::Active
                } else {
                    IdempotencyMode::Permanent
                },
                ..Default::default()
            })
            .build()
            .await
            .unwrap()
    }
    async fn cleanup(q: Queue<Offline>) {
        let keys: Vec<String> = q
            .redis
            .clone()
            .keys(format!("twmq:{}:*", q.name()))
            .await
            .unwrap();
        if !keys.is_empty() {
            let _: usize = q.redis.clone().del(keys).await.unwrap();
        }
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn repair_is_idempotent_preserves_delays_counters_and_fences_live_leases() {
        let q = fixture(false).await;
        let id = "intent";
        let data = json!({"intent":"original"});
        let mut redis = q.redis.clone();
        restore_job(&q, id, &data).await.unwrap();
        restore_job(&q, id, &data).await.unwrap();
        assert_eq!(
            redis.llen::<_, usize>(q.pending_list_name()).await.unwrap(),
            1
        );
        let _: () = redis
            .hset_multiple(
                q.job_meta_hash_name(id),
                &[("attempts", 17), ("created_at", 123)],
            )
            .await
            .unwrap();
        let _: () = redis.lrem(q.pending_list_name(), 0, id).await.unwrap();
        let _: () = redis.zadd(q.delayed_zset_name(), id, 9999).await.unwrap();
        restore_job(&q, id, &data).await.unwrap();
        assert_eq!(
            redis
                .hget::<_, _, u32>(q.job_meta_hash_name(id), "attempts")
                .await
                .unwrap(),
            17
        );
        assert_eq!(
            redis
                .zscore::<_, _, u64>(q.delayed_zset_name(), id)
                .await
                .unwrap(),
            9999
        );
        assert_eq!(
            redis.llen::<_, usize>(q.pending_list_name()).await.unwrap(),
            0
        );
        let _: () = redis.zrem(q.delayed_zset_name(), id).await.unwrap();
        let _: () = redis.hset(q.active_hash_name(), id, 1).await.unwrap();
        let _: () = redis
            .hset(q.job_meta_hash_name(id), "lease_token", "old")
            .await
            .unwrap();
        let lease = q.lease_key_name(id, "old");
        let _: () = redis.set(&lease, "1").await.unwrap();
        assert!(restore_job(&q, id, &data).await.is_err());
        assert!(
            redis
                .hexists::<_, _, bool>(q.active_hash_name(), id)
                .await
                .unwrap()
        );
        let _: () = redis.del(lease).await.unwrap();
        restore_job(&q, id, &data).await.unwrap();
        assert!(
            !redis
                .hexists::<_, _, bool>(q.active_hash_name(), id)
                .await
                .unwrap()
        );
        assert_eq!(
            redis.llen::<_, usize>(q.pending_list_name()).await.unwrap(),
            1
        );
        assert_eq!(
            redis
                .hget::<_, _, u32>(q.job_meta_hash_name(id), "attempts")
                .await
                .unwrap(),
            17
        );
        cleanup(q).await;
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn terminal_cancelled_conflicting_and_wrong_type_records_are_not_overwritten() {
        let q = fixture(false).await;
        let mut redis = q.redis.clone();
        let data = json!({"intent":"original"});
        let _: () = redis
            .hset(q.job_meta_hash_name("terminal"), "finished_at", 1)
            .await
            .unwrap();
        assert!(restore_job(&q, "terminal", &data).await.is_err());
        assert!(
            !redis
                .hexists::<_, _, bool>(q.job_data_hash_name(), "terminal")
                .await
                .unwrap()
        );
        let _: () = redis
            .sadd(q.pending_cancellation_set_name(), "cancelled")
            .await
            .unwrap();
        assert!(restore_job(&q, "cancelled", &data).await.is_err());
        let _: () = redis
            .hset(q.job_data_hash_name(), "conflict", "different")
            .await
            .unwrap();
        assert!(restore_job(&q, "conflict", &data).await.is_err());
        assert_eq!(
            redis
                .hget::<_, _, String>(q.job_data_hash_name(), "conflict")
                .await
                .unwrap(),
            "different"
        );
        let _: () = redis.set(q.failed_list_name(), "wrong-type").await.unwrap();
        assert!(restore_job(&q, "new", &data).await.is_err());
        assert!(
            !redis
                .exists::<_, bool>(q.job_meta_hash_name("new"))
                .await
                .unwrap()
        );
        assert_eq!(
            redis.llen::<_, usize>(q.pending_list_name()).await.unwrap(),
            0
        );
        cleanup(q).await;
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn completed_eoa_worker_can_be_reused_without_reviving_permanent_transaction_ids() {
        let q = fixture(true).await;
        let mut redis = q.redis.clone();
        let id = "worker";
        let data = json!({"chain":1});
        let _: () = redis
            .hset_multiple(
                q.job_meta_hash_name(id),
                &[("finished_at", 1), ("attempts", 17)],
            )
            .await
            .unwrap();
        let _: () = redis.rpush(q.success_list_name(), id).await.unwrap();
        let _: () = redis
            .hset(q.job_result_hash_name(), id, "old-result")
            .await
            .unwrap();
        restore_job(&q, id, &data).await.unwrap();
        assert_eq!(
            redis
                .hget::<_, _, u32>(q.job_meta_hash_name(id), "attempts")
                .await
                .unwrap(),
            0
        );
        assert!(
            !redis
                .hexists::<_, _, bool>(q.job_meta_hash_name(id), "finished_at")
                .await
                .unwrap()
        );
        assert_eq!(
            redis.llen::<_, usize>(q.success_list_name()).await.unwrap(),
            0
        );
        assert_eq!(
            redis.llen::<_, usize>(q.pending_list_name()).await.unwrap(),
            1
        );
        cleanup(q).await;
    }
}
