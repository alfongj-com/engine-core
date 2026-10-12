//! Immutable Solana request identity, retained independently of queue history.
use engine_core::error::EngineError;
use engine_executors::{
    solana_executor::{
        storage::{SolanaTransactionStorage, solana_admission_fingerprint},
        worker::SolanaExecutorJobData,
    },
    transaction_registry::TransactionRegistry,
};
use twmq::{
    DurableExecution, Queue,
    redis::{Script, aio::ConnectionManager},
};

/// No delay is supported here: admission creates one pending Solana job. This
/// mirrors TWMQ's initial metadata schema, while atomically reserving the intent.
pub(crate) async fn admit<H: DurableExecution<JobData = SolanaExecutorJobData>>(
    redis: &ConnectionManager,
    queue: &Queue<H>,
    registry: &TransactionRegistry,
    storage: &SolanaTransactionStorage,
    data: &SolanaExecutorJobData,
) -> Result<(), EngineError> {
    let fingerprint =
        solana_admission_fingerprint(data).map_err(|_| EngineError::InternalError {
            message: "Failed to fingerprint Solana request".into(),
        })?;
    let payload = serde_json::to_string(data).map_err(|_| EngineError::InternalError {
        message: "Failed to encode Solana request".into(),
    })?;
    let id = &data.transaction_id;
    let result: i32 = Script::new(r#"
        -- Redis scripts are atomic, but runtime errors do not roll back writes.
        -- Validate every key type before the first mutation.
        local types = {'hash','string','hash','hash','hash','set','list','hash','zset','hash','list','list','set','list','string'}
        for i, expected in ipairs(types) do
            local actual = redis.call('TYPE', KEYS[i]).ok
            if actual ~= 'none' and actual ~= expected then return -3 end
        end
        local id, fingerprint, payload, now = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
        local schema = redis.call('GET', KEYS[15])
        if schema and schema ~= '1' then return -5 end
        if not schema then
            -- One migration check, bounded before fetching any lists. Old workers
            -- must be stopped before enabling this admission schema.
            local count = redis.call('LLEN', KEYS[7]) + redis.call('HLEN', KEYS[8])
                + redis.call('ZCARD', KEYS[9]) + redis.call('LLEN', KEYS[11]) + redis.call('LLEN', KEYS[12])
            if count > 20000 then return -6 end
            local seen = {}
            local function check_records(ids, terminal)
                for _, other_id in ipairs(ids) do
                    if seen[other_id] then return false end
                    seen[other_id] = true
                    local meta = ARGV[5] .. other_id .. ':meta'
                    if redis.call('TYPE', meta).ok ~= 'hash'
                        or redis.call('HEXISTS', KEYS[4], other_id) ~= 1
                        or redis.call('HEXISTS', meta, 'created_at') ~= 1
                        or redis.call('HEXISTS', meta, 'attempts') ~= 1 then return false end
                    local finished = redis.call('HEXISTS', meta, 'finished_at') == 1
                    if finished ~= terminal then return false end
                    if not terminal and redis.call('SISMEMBER', KEYS[6], other_id) ~= 1 then return false end
                end
                return true
            end
            if not check_records(redis.call('LRANGE', KEYS[7], 0, -1), false)
                or not check_records(redis.call('HKEYS', KEYS[8]), false)
                or not check_records(redis.call('ZRANGE', KEYS[9], 0, -1), false)
                or not check_records(redis.call('LRANGE', KEYS[11], 0, -1), true)
                or not check_records(redis.call('LRANGE', KEYS[12], 0, -1), true) then return -2 end
        end
        local existing = redis.call('HGET', KEYS[1], 'fingerprint')
        if existing then
            if existing ~= fingerprint then return -1 end
            local state = redis.call('HGET', KEYS[1], 'state')
            if state == 'completed' or state == 'failed' then return 0 end
            if state ~= 'active' then return -2 end
            if redis.call('HEXISTS', KEYS[4], id) == 1
                and redis.call('HEXISTS', KEYS[5], 'created_at') == 1
                and redis.call('HEXISTS', KEYS[5], 'attempts') == 1
                and redis.call('HEXISTS', KEYS[5], 'finished_at') == 0
                and redis.call('SISMEMBER', KEYS[6], id) == 1 then
                return 0
            end
            -- A cancelled or lost job cannot be resumed by resubmitting intent.
            return -4
        end
        if redis.call('EXISTS', KEYS[1]) == 1 or redis.call('EXISTS', KEYS[2]) == 1
            or redis.call('HEXISTS', KEYS[3], id) == 1
            or redis.call('HEXISTS', KEYS[4], id) == 1 or redis.call('EXISTS', KEYS[5]) == 1
            or redis.call('SISMEMBER', KEYS[6], id) == 1
            or redis.call('HEXISTS', KEYS[8], id) == 1
            or redis.call('ZSCORE', KEYS[9], id) ~= false
            or redis.call('HEXISTS', KEYS[10], id) == 1
            or redis.call('SISMEMBER', KEYS[13], id) == 1
            or redis.call('EXISTS', KEYS[14]) == 1 then return -2 end
        redis.call('SET', KEYS[15], '1')
        redis.call('HSET', KEYS[1], 'fingerprint', fingerprint, 'state', 'active')
        redis.call('HSET', KEYS[4], id, payload)
        redis.call('HSET', KEYS[5], 'created_at', now, 'attempts', 0)
        redis.call('SADD', KEYS[6], id)
        redis.call('RPUSH', KEYS[7], id)
        redis.call('HSET', KEYS[3], id, 'solana_executor')
        return 1
    "#)
    .key(storage.admission_key(id)).key(storage.attempt_key(id))
    .key(registry.registry_key()).key(queue.job_data_hash_name()).key(queue.job_meta_hash_name(id))
    .key(queue.dedupe_set_name()).key(queue.pending_list_name()).key(queue.active_hash_name())
    .key(queue.delayed_zset_name()).key(queue.job_result_hash_name())
    .key(queue.success_list_name()).key(queue.failed_list_name())
    .key(queue.pending_cancellation_set_name()).key(queue.job_errors_list_name(id))
    .key(format!("twmq:{}:solana_admission_schema", queue.name()))
    .arg(id).arg(fingerprint).arg(payload)
    .arg(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|_| EngineError::InternalError { message: "System clock precedes Unix epoch".into() })?.as_secs())
    .arg(format!("twmq:{}:job:", queue.name()))
    .invoke_async(&mut redis.clone()).await.map_err(|_| EngineError::InternalError {
        message: "Solana admission storage unavailable".into(),
    })?;
    match result {
        0 | 1 => Ok(()),
        -1 => Err(EngineError::ValidationError { message: "Solana idempotency key already belongs to a different request".into() }),
        -5 => Err(EngineError::ValidationError { message: "Unknown Solana admission schema; migration is required".into() }),
        -6 => Err(EngineError::ValidationError { message: "Solana queue exceeds the 20000-record online migration limit; stop old workers and reconcile or migrate offline".into() }),
        -4 => Err(EngineError::ValidationError { message: "Solana request is unresolved without a live queue job; operator reconciliation is required".into() }),
        _ => Err(EngineError::ValidationError { message: "Existing or incomplete Solana recovery state requires operator reconciliation; request was not queued".into() }),
    }
}

#[cfg(test)]
#[path = "solana_admission_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "solana_admission_bench.rs"]
mod bench;
