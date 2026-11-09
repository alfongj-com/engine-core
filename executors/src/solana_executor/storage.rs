use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use solana_hash::Hash;
use solana_signature::Signature;
use twmq::{
    job::QueueLease,
    redis,
    redis::{AsyncCommands, aio::ConnectionManager},
};

/// Stable identity for a normalized queued Solana request, independent of map
/// insertion order. Store only this digest and the already-public signer reference.
pub fn solana_admission_fingerprint(value: &impl Serialize) -> Result<String, serde_json::Error> {
    use sha2::{Digest, Sha256};
    fn canonicalize(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(fields) => {
                for value in fields.values_mut() {
                    canonicalize(value);
                }
                fields.sort_keys();
            }
            serde_json::Value::Array(items) => items.iter_mut().for_each(canonicalize),
            _ => {}
        }
    }
    let mut value = serde_json::to_value(value)?;
    canonicalize(&mut value);
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&value)?)))
}

/// Represents a single attempt to send a Solana transaction
/// This is stored in Redis BEFORE sending to prevent duplicate transactions
#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SolanaTransactionAttempt {
    /// The transaction signature
    #[serde_as(as = "DisplayFromStr")]
    pub signature: Signature,
    /// The blockhash used for this attempt
    #[serde_as(as = "DisplayFromStr")]
    pub blockhash: Hash,
    /// The block height at which this blockhash expires
    pub blockhash_last_valid_height: Option<u64>,
    /// Canonical signed wire bytes. Missing on legacy records: reconcile only, never rebuild.
    #[serde(default)]
    pub signed_transaction: Option<String>,
    #[serde(default)]
    pub broadcast_attempts: u32,
    #[serde(default)]
    pub last_broadcast_at: u64,
    #[serde(default)]
    pub reconciliation_checks: u32,
    /// Restored projections dispatch only through the cumulative journal budget.
    /// Omit false to preserve the canonical payload of older journaled attempts.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub restored_from_journal: bool,
    /// Timestamp when the transaction was sent (milliseconds)
    pub sent_at: u64,
    /// Resubmission attempt number (1 = first submission, 2 = first resubmission, etc.)
    pub submission_attempt_number: u32,
}

impl SolanaTransactionAttempt {
    pub fn new(
        signature: Signature,
        blockhash: Hash,
        blockhash_last_valid_height: Option<u64>,
        submission_attempt_number: u32,
        signed_transaction: String,
    ) -> Self {
        Self {
            signature,
            blockhash,
            blockhash_last_valid_height,
            signed_transaction: Some(signed_transaction),
            broadcast_attempts: 0,
            last_broadcast_at: 0,
            reconciliation_checks: 0,
            restored_from_journal: false,
            sent_at: crate::metrics::current_timestamp_ms(),
            submission_attempt_number,
        }
    }
}

// Shared by every attempt mutation/read and the final pre-RPC lock check.
// Legacy UUID locks keep their original semantics until release/expiry.
const PROCESSING_LOCK_OWNERSHIP: &str = r#"
local function owner_is_well_formed(owner)
    return type(owner) == 'table'
        and type(owner.metadata_key) == 'string' and owner.metadata_key ~= ''
        and type(owner.lease_key) == 'string' and owner.lease_key ~= ''
        and type(owner.token) == 'string' and owner.token ~= ''
        and string.sub(owner.metadata_key, -5) == ':meta'
        and owner.lease_key == string.sub(owner.metadata_key, 1, -6) .. ':lease:' .. owner.token
end
local function owner_is_live(owner)
    return owner_is_well_formed(owner)
        and redis.call('HGET', owner.metadata_key, 'lease_token') == owner.token
        and redis.call('EXISTS', owner.lease_key) == 1
end
local function owns_processing_lock(key, value)
    if redis.call('GET', key) ~= value then return false end
    local ok, record = pcall(cjson.decode, value)
    if ok then
        return type(record) == 'table' and record.version == 1 and owner_is_live(record.owner)
    end
    return #value == 36 and string.match(value, '^%x%x%x%x%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%x%x%x%x%x%x%x%x$') ~= nil
end
"#;

fn processing_script(body: &str) -> redis::Script {
    redis::Script::new(&format!("{PROCESSING_LOCK_OWNERSHIP}\n{body}"))
}

/// Represents a lock held on a transaction
/// When dropped, the lock is automatically released
pub struct TransactionLock {
    redis: ConnectionManager,
    lock_key: String,
    lock_value: String,
}

impl TransactionLock {
    /// Check if we still hold the lock
    pub async fn still_held(&self) -> Result<bool, redis::RedisError> {
        processing_script("return owns_processing_lock(KEYS[1], ARGV[1]) and 1 or 0")
            .key(&self.lock_key)
            .arg(&self.lock_value)
            .invoke_async::<i32>(&mut self.redis.clone())
            .await
            .map(|owned| owned == 1)
    }

    /// Explicitly release the lock
    pub async fn release(self) -> Result<(), redis::RedisError> {
        // Use Lua script to atomically check and delete only if we own the lock
        let script = r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
        "#;

        let _: i32 = redis::Script::new(script)
            .key(&self.lock_key)
            .arg(&self.lock_value)
            .invoke_async(&mut self.redis.clone())
            .await?;

        Ok(())
    }
}

impl Drop for TransactionLock {
    fn drop(&mut self) {
        // Best effort release on drop (fire and forget)
        let redis = self.redis.clone();
        let lock_key = self.lock_key.clone();
        let lock_value = self.lock_value.clone();

        tokio::spawn(async move {
            let script = r#"
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
            "#;

            let _: Result<i32, _> = redis::Script::new(script)
                .key(&lock_key)
                .arg(&lock_value)
                .invoke_async(&mut redis.clone())
                .await;
        });
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum LockError {
    #[error("Failed to acquire lock: another worker is processing this transaction")]
    AlreadyLocked,

    #[error("Queue lease is no longer owned")]
    QueueLeaseLost,

    #[error("Redis error: {0}")]
    RedisError(String),
}

impl From<redis::RedisError> for LockError {
    fn from(e: redis::RedisError) -> Self {
        LockError::RedisError(e.to_string())
    }
}

/// Storage for Solana transaction attempts
/// Provides atomic operations to prevent duplicate transactions
pub struct SolanaTransactionStorage {
    redis: ConnectionManager,
    namespace: Option<String>,
    completed_transaction_ttl_seconds: u64,
}

impl SolanaTransactionStorage {
    pub fn new(redis: ConnectionManager, namespace: Option<String>) -> Self {
        Self {
            redis,
            namespace,
            completed_transaction_ttl_seconds: 86_400,
        }
    }

    pub fn with_completed_transaction_ttl_seconds(mut self, seconds: u64) -> Self {
        self.completed_transaction_ttl_seconds = seconds.max(1);
        self
    }

    /// Admission identity is independent of bounded queue history and pruning.
    pub fn admission_key(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:solana_admission:{transaction_id}"),
            None => format!("solana_admission:{transaction_id}"),
        }
    }

    /// Only a terminal queue commit may begin completed-intent retention. Legacy
    /// jobs without a fingerprint cannot create a trustworthy tombstone.
    pub fn add_terminal_admission_command(
        &self,
        pipeline: &mut redis::Pipeline,
        transaction_id: &str,
        fingerprint: &str,
        status: &str,
        require_no_attempt: bool,
    ) {
        pipeline
            .cmd("EVAL")
            .arg(
                r#"
            if redis.call('HGET', KEYS[1], 'fingerprint') == ARGV[3]
                and (ARGV[4] == '0' or redis.call('EXISTS', KEYS[2]) == 0) then
                redis.call('HSET', KEYS[1], 'state', ARGV[1])
                redis.call('EXPIRE', KEYS[1], ARGV[2])
            end
            return 1
        "#,
            )
            .arg(2)
            .arg(self.admission_key(transaction_id))
            .arg(self.attempt_key(transaction_id))
            .arg(status)
            .arg(self.completed_transaction_ttl_seconds)
            .arg(fingerprint)
            .arg(if require_no_attempt { 1 } else { 0 });
    }

    /// Get the Redis key for a transaction's attempt
    pub fn attempt_key(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:solana_tx_attempt:{transaction_id}"),
            None => format!("solana_tx_attempt:{transaction_id}"),
        }
    }

    /// Get the Redis key for a transaction's lock
    fn lock_key(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:solana_tx_lock:{transaction_id}"),
            None => format!("solana_tx_lock:{transaction_id}"),
        }
    }

    /// Try to acquire a distributed lock on a transaction
    ///
    /// Returns Ok(Some(lock)) if lock acquired
    /// Returns Ok(None) if lock is held by another worker
    /// Returns Err on Redis errors
    pub async fn try_acquire_lock(
        &self,
        transaction_id: &str,
    ) -> Result<TransactionLock, LockError> {
        let lock_key = self.lock_key(transaction_id);
        // Use a unique value (random UUID) so we can safely release only our lock
        let lock_value = uuid::Uuid::new_v4().to_string();

        // Try to acquire lock with SET NX EX (set if not exists with expiry)
        // Lock expires after 120 seconds to prevent deadlocks from crashes
        let result: Option<String> = redis::cmd("SET")
            .arg(&lock_key)
            .arg(&lock_value)
            .arg("NX") // Only set if key doesn't exist
            .arg("EX")
            .arg(120) // Expire after 120 seconds
            .arg("GET") // Return old value if any
            .query_async(&mut self.redis.clone())
            .await?;

        if result.is_some() {
            // Another worker holds the lock
            Err(LockError::AlreadyLocked)
        } else {
            // We acquired the lock!
            Ok(TransactionLock {
                redis: self.redis.clone(),
                lock_key,
                lock_value,
            })
        }
    }

    /// Reclaim only a lock whose recorded queue owner is no longer live. The
    /// claimant's lease and the prior owner's lease are checked in the same CAS.
    /// Opaque/legacy values retain their TTL; they carry no ownership evidence.
    pub async fn try_acquire_lock_with_lease(
        &self,
        transaction_id: &str,
        lease: &QueueLease,
    ) -> Result<TransactionLock, LockError> {
        let lock_key = self.lock_key(transaction_id);
        let lock_value = serde_json::json!({
            "version": 1,
            "token": uuid::Uuid::new_v4().to_string(),
            "owner": {
                "metadata_key": lease.metadata_key(),
                "lease_key": lease.lease_key(),
                "token": lease.token(),
            },
        })
        .to_string();
        let result: i32 = processing_script(
            r#"
            local owner = {metadata_key=KEYS[2], lease_key=KEYS[3], token=ARGV[1]}
            if not owner_is_live(owner) then return -1 end
            local old = redis.call('GET', KEYS[1])
            if old then
                local ok, record = pcall(cjson.decode, old)
                if not ok or type(record) ~= 'table' or record.version ~= 1
                    or not owner_is_well_formed(record.owner) then return 0 end
                if owner_is_live(record.owner) then return 0 end
            end
            redis.call('SET', KEYS[1], ARGV[2], 'EX', 120)
            return 1
        "#,
        )
        .key(&lock_key)
        .key(lease.metadata_key())
        .key(lease.lease_key())
        .arg(lease.token())
        .arg(&lock_value)
        .invoke_async(&mut self.redis.clone())
        .await?;
        match result {
            1 => Ok(TransactionLock {
                redis: self.redis.clone(),
                lock_key,
                lock_value,
            }),
            -1 => Err(LockError::QueueLeaseLost),
            _ => Err(LockError::AlreadyLocked),
        }
    }

    /// Store an attempt ATOMICALLY before sending to RPC (must hold lock)
    /// This is critical to prevent duplicate transactions from crashes
    /// Uses Lua script to atomically check lock and store attempt
    ///
    /// Returns Ok(true) if stored successfully (first time)
    /// Returns Ok(false) if attempt already exists (duplicate prevention)
    /// Returns Err if lock was lost or other error occurred
    pub async fn store_attempt_if_not_exists(
        &self,
        transaction_id: &str,
        attempt: &SolanaTransactionAttempt,
        lock: &TransactionLock,
    ) -> Result<bool, redis::RedisError> {
        let attempt_key = self.attempt_key(transaction_id);
        let json = serde_json::to_string(attempt).map_err(|e| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "serialization", e.to_string()))
        })?;

        // Lua script to atomically verify lock and store attempt
        let script = processing_script(
            r#"
            local lock_key = KEYS[1]
            local lock_value = ARGV[1]
            local attempt_key = KEYS[2]
            local attempt_json = ARGV[2]
            
            -- Check if we still hold the lock
            if not owns_processing_lock(lock_key, lock_value) then
                return redis.error_reply('ERR lock lost')
            end
            
            -- Try to store attempt if it doesn't exist
            local result = redis.call('SET', attempt_key, attempt_json, 'NX')
            if result then
                return 1
            else
                return 0
            end
            "#,
        );

        let result: Result<i32, redis::RedisError> = script
            .key(&lock.lock_key)
            .key(&attempt_key)
            .arg(&lock.lock_value)
            .arg(&json)
            .invoke_async(&mut self.redis.clone())
            .await;

        match result {
            Ok(1) => Ok(true),
            Ok(0) => Ok(false),
            Ok(_) => Err(redis::RedisError::from((
                redis::ErrorKind::IoError,
                "unexpected result",
                "Unexpected result from Lua script".to_string(),
            ))),
            Err(e) => {
                if e.to_string().contains("lock lost") {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "lock lost",
                        "Lost lock while storing attempt".to_string(),
                    )))
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Install an independently validated retained wire only while this exact
    /// transaction lock is owned. A concurrently present value always wins:
    /// restoration must never overwrite it, even if it looks malformed.
    pub async fn restore_attempt_if_absent(
        &self,
        transaction_id: &str,
        attempt: &SolanaTransactionAttempt,
        lock: &TransactionLock,
    ) -> Result<bool, redis::RedisError> {
        if lock.lock_key != self.lock_key(transaction_id) || !attempt.restored_from_journal {
            return Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Invalid retained-attempt restoration ownership",
            )));
        }
        self.store_attempt_if_not_exists(transaction_id, attempt, lock)
            .await
    }

    /// Get the current attempt for a transaction (must hold lock)
    /// Uses Lua script to atomically verify lock and read attempt
    pub async fn get_attempt(
        &self,
        transaction_id: &str,
        lock: &TransactionLock,
    ) -> Result<Option<SolanaTransactionAttempt>, redis::RedisError> {
        let attempt_key = self.attempt_key(transaction_id);

        // Lua script to atomically verify lock and get attempt
        let script = processing_script(
            r#"
            local lock_key = KEYS[1]
            local lock_value = ARGV[1]
            local attempt_key = KEYS[2]
            
            -- Check if we still hold the lock
            if not owns_processing_lock(lock_key, lock_value) then
                return redis.error_reply('ERR lock lost')
            end
            
            -- Get the attempt
            redis.call('PERSIST', attempt_key)
            return redis.call('GET', attempt_key)
            "#,
        );

        let result: Result<Option<String>, redis::RedisError> = script
            .key(&lock.lock_key)
            .key(&attempt_key)
            .arg(&lock.lock_value)
            .invoke_async(&mut self.redis.clone())
            .await;

        match result {
            Ok(Some(json)) => {
                let attempt = serde_json::from_str(&json).map_err(|e| {
                    redis::RedisError::from((
                        redis::ErrorKind::TypeError,
                        "deserialization",
                        e.to_string(),
                    ))
                })?;
                Ok(Some(attempt))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                if e.to_string().contains("lock lost") {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "lock lost",
                        "Lost lock while reading attempt".to_string(),
                    )))
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Update an existing attempt (for retry scenarios, must hold lock)
    /// This will overwrite the existing attempt
    /// Uses Lua script to atomically verify lock and update attempt
    pub async fn update_attempt(
        &self,
        transaction_id: &str,
        attempt: &SolanaTransactionAttempt,
        lock: &TransactionLock,
    ) -> Result<(), redis::RedisError> {
        let attempt_key = self.attempt_key(transaction_id);
        let json = serde_json::to_string(attempt).map_err(|e| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "serialization", e.to_string()))
        })?;

        // Lua script to atomically verify lock and update attempt
        let script = processing_script(
            r#"
            local lock_key = KEYS[1]
            local lock_value = ARGV[1]
            local attempt_key = KEYS[2]
            local attempt_json = ARGV[2]
            
            -- Check if we still hold the lock
            if not owns_processing_lock(lock_key, lock_value) then
                return redis.error_reply('ERR lock lost')
            end
            
            -- Update the attempt
            redis.call('SET', attempt_key, attempt_json)
            return 1
            "#,
        );

        let result: Result<i32, redis::RedisError> = script
            .key(&lock.lock_key)
            .key(&attempt_key)
            .arg(&lock.lock_value)
            .arg(&json)
            .invoke_async(&mut self.redis.clone())
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.to_string().contains("lock lost") {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "lock lost",
                        "Lost lock while updating attempt".to_string(),
                    )))
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Cleanup belongs to the queue's fenced terminal transaction, never a side effect
    /// during process(). An aborted or stale queue completion retains recovery evidence.
    pub fn add_delete_attempt_command(&self, pipeline: &mut redis::Pipeline, transaction_id: &str) {
        pipeline.del(self.attempt_key(transaction_id));
    }

    /// Explicit operator recovery: replenish read checks without changing signed bytes,
    /// blockhash, signature, or the lifetime broadcast limit. Caller must own the lock.
    pub async fn resume_reconciliation(
        &self,
        transaction_id: &str,
        lock: &TransactionLock,
    ) -> Result<bool, redis::RedisError> {
        let Some(mut attempt) = self.get_attempt(transaction_id, lock).await? else {
            return Ok(false);
        };
        attempt.reconciliation_checks = 0;
        self.update_attempt(transaction_id, &attempt, lock).await?;
        Ok(true)
    }

    /// Check if an attempt exists without retrieving it
    pub async fn has_attempt(&self, transaction_id: &str) -> Result<bool, redis::RedisError> {
        let key = self.attempt_key(transaction_id);
        let exists: bool = self.redis.clone().exists(&key).await?;
        Ok(exists)
    }
}
