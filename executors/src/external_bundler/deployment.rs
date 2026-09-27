use alloy::primitives::Address;
use engine_aa_core::userop::deployment::{
    AcquireLockResult, DeploymentCache, DeploymentLock, LockId,
};
use engine_core::error::EngineError;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use twmq::{
    error::TwmqError,
    redis::{AsyncCommands, Pipeline, SetExpiry, SetOptions, aio::ConnectionManager},
};
use uuid::Uuid;

const CACHE_PREFIX: &str = "deployment_cache";
const LOCK_PREFIX: &str = "deployment_lock";

/// Fallback TTL so a lock that's never explicitly released (e.g. worker crash)
/// can't block the account forever.
const LOCK_TTL_SECONDS: u64 = 300;

#[derive(Clone)]
pub struct RedisDeploymentCache {
    connection_manager: twmq::redis::aio::ConnectionManager,
    namespace: Option<String>,
}

#[derive(Clone)]
pub struct RedisDeploymentLock {
    connection_manager: twmq::redis::aio::ConnectionManager,
    namespace: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct LockData {
    lock_id: String,
    acquired_at: u64,
}

impl RedisDeploymentCache {
    pub async fn new(client: twmq::redis::Client) -> Result<Self, TwmqError> {
        Ok(Self {
            connection_manager: ConnectionManager::new(client).await?,
            namespace: None,
        })
    }

    pub fn with_namespace(mut self, namespace: Option<String>) -> Self {
        self.namespace = namespace;
        self
    }

    pub fn conn(&self) -> &ConnectionManager {
        &self.connection_manager
    }

    fn cache_key(&self, chain_id: u64, account_address: &Address) -> String {
        deployment_key(
            self.namespace.as_deref(),
            CACHE_PREFIX,
            chain_id,
            account_address,
        )
    }
}

impl DeploymentCache for RedisDeploymentCache {
    async fn is_deployed(&self, chain_id: u64, account_address: &Address) -> Option<bool> {
        let mut conn = self.conn().clone();
        let key = self.cache_key(chain_id, account_address);

        match conn.get::<_, Option<String>>(&key).await {
            Ok(Some(value)) if value == "deployed" => Some(true),
            Ok(Some(value)) if value == "not_deployed" => Some(false),
            _ => None,
        }
    }
}

impl RedisDeploymentLock {
    pub async fn new(client: twmq::redis::Client) -> Result<Self, TwmqError> {
        Ok(Self {
            connection_manager: ConnectionManager::new(client).await?,
            namespace: None,
        })
    }

    pub fn with_namespace(mut self, namespace: Option<String>) -> Self {
        self.namespace = namespace;
        self
    }

    pub fn conn(&self) -> &ConnectionManager {
        &self.connection_manager
    }

    fn lock_key(&self, chain_id: u64, account_address: &Address) -> String {
        deployment_key(
            self.namespace.as_deref(),
            LOCK_PREFIX,
            chain_id,
            account_address,
        )
    }

    fn cache_key(&self, chain_id: u64, account_address: &Address) -> String {
        deployment_key(
            self.namespace.as_deref(),
            CACHE_PREFIX,
            chain_id,
            account_address,
        )
    }

    /// Schedule an atomic compare-and-delete. A stale worker cannot release a
    /// newer worker's lock, even if its own lease expired during submission.
    pub fn release_lock_with_pipeline(
        &self,
        pipeline: &mut Pipeline,
        chain_id: u64,
        account_address: &Address,
        lock_id: &str,
    ) {
        self.release_owned_with_pipeline(pipeline, chain_id, account_address, lock_id, None);
    }

    /// Cache updates are fenced by the same ownership check as lock release.
    pub fn release_lock_and_update_cache_with_pipeline(
        &self,
        pipeline: &mut Pipeline,
        chain_id: u64,
        account_address: &Address,
        lock_id: &str,
        is_deployed: bool,
    ) {
        self.release_owned_with_pipeline(
            pipeline,
            chain_id,
            account_address,
            lock_id,
            Some(is_deployed),
        );
    }

    fn release_owned_with_pipeline(
        &self,
        pipeline: &mut Pipeline,
        chain_id: u64,
        account_address: &Address,
        lock_id: &str,
        is_deployed: Option<bool>,
    ) {
        let cache_value = match is_deployed {
            Some(true) => "deployed",
            Some(false) => "not_deployed",
            None => "",
        };
        pipeline
            .cmd("EVAL")
            .arg(RELEASE_OWNED_LOCK)
            .arg(2)
            .arg(self.lock_key(chain_id, account_address))
            .arg(self.cache_key(chain_id, account_address))
            .arg(lock_id)
            .arg(cache_value)
            .ignore();
    }
}

impl DeploymentLock for RedisDeploymentLock {
    async fn check_lock(
        &self,
        chain_id: u64,
        account_address: &Address,
    ) -> Option<(LockId, Duration)> {
        let mut conn = self.conn().clone();
        let key = self.lock_key(chain_id, account_address);

        let lock_data_str: Option<String> = conn.get(key).await.ok()?;
        let lock_data_str = lock_data_str?;

        let lock_data: LockData = serde_json::from_str(&lock_data_str).ok()?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        let duration = Duration::from_secs(now.saturating_sub(lock_data.acquired_at));

        Some((lock_data.lock_id, duration))
    }

    async fn acquire_lock(
        &self,
        chain_id: u64,
        account_address: &Address,
    ) -> Result<AcquireLockResult, EngineError> {
        let mut conn = self.conn().clone();

        let key = self.lock_key(chain_id, account_address);
        let lock_id = Uuid::new_v4().to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| EngineError::InternalError {
                message: format!("System time error: {e}"),
            })?
            .as_secs();

        let lock_data = LockData {
            lock_id: lock_id.clone(),
            acquired_at: now,
        };

        let lock_data_str =
            serde_json::to_string(&lock_data).map_err(|e| EngineError::InternalError {
                message: format!("Serialization failed: {e}"),
            })?;

        // SET NX EX: atomic acquire with a fallback expiry.
        let opts = SetOptions::default()
            .conditional_set(twmq::redis::ExistenceCheck::NX)
            .with_expiration(SetExpiry::EX(LOCK_TTL_SECONDS));

        let result: Option<String> =
            conn.set_options(&key, &lock_data_str, opts)
                .await
                .map_err(|e| EngineError::InternalError {
                    message: format!("Lock acquire failed: {e}"),
                })?;

        match result {
            Some(_) => Ok(AcquireLockResult::Acquired(lock_id)),
            None => {
                // Lock already exists, get the lock_id
                let existing_data: Option<String> =
                    conn.get(&key)
                        .await
                        .map_err(|e| EngineError::InternalError {
                            message: format!("Failed to read existing lock: {e}"),
                        })?;

                let existing_lock_id = existing_data
                    .and_then(|data| serde_json::from_str::<LockData>(&data).ok())
                    .map(|data| data.lock_id)
                    .unwrap_or_else(|| "unknown".to_string());

                Ok(AcquireLockResult::AlreadyLocked(existing_lock_id))
            }
        }
    }

    async fn release_lock_if_owner(
        &self,
        chain_id: u64,
        account_address: &Address,
        lock_id: &str,
    ) -> Result<bool, EngineError> {
        let mut conn = self.conn().clone();
        let key = self.lock_key(chain_id, account_address);

        // Atomic compare-and-delete: only DEL if the stored lock's lock_id matches.
        let script = twmq::redis::Script::new(
            r#"
            local v = redis.call('GET', KEYS[1])
            if not v then return 0 end
            local ok, data = pcall(cjson.decode, v)
            if ok and type(data) == 'table' and data.lock_id == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            "#,
        );

        let deleted: i64 = script
            .key(&key)
            .arg(lock_id)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| EngineError::InternalError {
                message: format!("Failed to release lock for account {account_address}: {e}"),
            })?;

        Ok(deleted > 0)
    }
}

fn deployment_key(
    namespace: Option<&str>,
    prefix: &str,
    chain_id: u64,
    address: &Address,
) -> String {
    match namespace {
        Some(namespace) => format!("{namespace}:{prefix}:{chain_id}:{address}"),
        None => format!("{prefix}:{chain_id}:{address}"),
    }
}

const RELEASE_OWNED_LOCK: &str = r#"
local value = redis.call('GET', KEYS[1])
if not value then return 0 end
local ok, data = pcall(cjson.decode, value)
if not ok or type(data) ~= 'table' or data.lock_id ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
if ARGV[2] ~= '' then redis.call('SETEX', KEYS[2], 3600, ARGV[2]) end
return 1
"#;

#[cfg(test)]
#[path = "deployment_tests.rs"]
mod tests;
