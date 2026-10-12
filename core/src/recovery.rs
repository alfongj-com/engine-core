//! Independent, single-host recovery authority. Redis is a queue projection.
//! Never initialize a missing ledger from normal server startup.
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex as AsyncMutex, RwLock, Semaphore};
use twmq::redis::{self, aio::ConnectionManager};

pub mod fairness;
pub mod metrics;

struct SerialGuard<'a> {
    _guard: tokio::sync::MutexGuard<'a, ()>,
    _timer: prometheus::HistogramTimer,
    // Drop after the global mutex so a chain never has two global contenders.
    _lane: tokio::sync::OwnedSemaphorePermit,
}

static GLOBAL: OnceLock<Arc<RecoveryJournal>> = OnceLock::new();
const SCHEMA: i64 = 2;
#[path = "recovery/projection.rs"]
mod projection;
// Additive physical indexes: no journal-schema/token or evidence changes.
const HISTORY_INDEX_SQL: &str = "
    CREATE INDEX IF NOT EXISTS terminal_evidence_id_sequence ON terminal_evidence(id,sequence);
    CREATE INDEX IF NOT EXISTS attempts_id_sequence ON attempts(id,sequence);
";
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
// Admission waits are bounded; reconciliation and existing broadcasts do not
// consume these permits and can drain work while intake is overloaded.
const MAX_CONCURRENT_ADMISSIONS: usize = 64;
// Health checks may acknowledge pending publication and therefore outlive a
// cancelled caller. Bound those retained tasks; waiting for a slot is cancellable.
const MAX_CONCURRENT_HEALTH_CHECKS: usize = 32;

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("Redis checkpoint publication is pending; retry the same transaction ID")]
    PublicationPending,
    #[error(
        "Recovery admission is busy; retry the same transaction ID after capacity is available"
    )]
    Busy,
    #[error("Recovery ledger storage is unavailable or invalid; transaction execution is blocked")]
    Storage,
    #[error("Recovery ledger is already owned by an active process")]
    Locked,
    #[error("Recovery ledger is missing; initialize a fresh deployment explicitly")]
    Missing,
    #[error("Recovery required: {0}")]
    RecoveryRequired(&'static str),
    #[error("Recovery request conflicts with immutable admitted intent or replay identity")]
    Conflict,
    #[error("Invalid recovery configuration: {0}")]
    Invalid(&'static str),
}
impl From<rusqlite::Error> for RecoveryError {
    fn from(error: rusqlite::Error) -> Self {
        tracing::error!(code = ?error.sqlite_error_code(), "Recovery SQLite operation failed");
        Self::Storage
    }
}
impl From<std::io::Error> for RecoveryError {
    fn from(error: std::io::Error) -> Self {
        tracing::error!(kind = ?error.kind(), "Recovery filesystem operation failed");
        Self::Storage
    }
}
impl From<redis::RedisError> for RecoveryError {
    fn from(error: redis::RedisError) -> Self {
        tracing::error!(kind = ?error.kind(), "Recovery Redis operation failed");
        Self::Storage
    }
}
type Result<T> = std::result::Result<T, RecoveryError>;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionState {
    Admitted,
    Terminal,
    Quarantined,
}
impl AdmissionState {
    fn parse(s: &str) -> Result<Self> {
        match s {
            "admitted" => Ok(Self::Admitted),
            "terminal" => Ok(Self::Terminal),
            "quarantined" => Ok(Self::Quarantined),
            _ => Err(RecoveryError::Storage),
        }
    }
}
/// Deliberately no Debug: payload can contain credentials.
pub struct AdmissionReservation {
    pub payload: Value,
    pub terminal: bool,
}
#[derive(Serialize, Deserialize)]
pub struct AdmissionRecord {
    pub id: String,
    pub kind: String,
    pub fingerprint: String,
    pub payload: Value,
    pub state: AdmissionState,
    pub replay_key: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RecoveryStatus {
    pub schema: i64,
    pub deployment: String,
    pub epoch: i64,
    pub checkpoint: i64,
    pub projection_pending: bool,
    pub namespace: Option<String>,
    pub halted: bool,
    pub halt_reason: Option<String>,
    pub admissions: u64,
    pub attempts: u64,
    pub terminal: u64,
    pub quarantined: u64,
}
#[derive(Clone)]
struct Control {
    schema: i64,
    deployment: String,
    epoch: i64,
    checkpoint: i64,
    namespace: Option<String>,
    run_id: String,
    halted: bool,
    reason: Option<String>,
}
impl Control {
    fn token(&self) -> String {
        format!(
            "v{}:{}:{}:{}",
            self.schema, self.deployment, self.epoch, self.checkpoint
        )
    }
    fn key(&self) -> String {
        marker_key(&self.namespace)
    }
}

/// Holding this object owns the exclusive OS lock until process shutdown.
/// SQLite and its WAL must live on one durable local filesystem, never NFS.
pub struct RecoveryJournal {
    db: Arc<Mutex<Connection>>,
    _owner: Arc<File>,
    redis: ConnectionManager,
    serial: AsyncMutex<()>,
    fairness: fairness::JournalFairness,
    admission_slots: Arc<Semaphore>,
    health_slots: Arc<Semaphore>,
    owned_operations: Arc<RwLock<()>>,
    failed: AtomicBool,
    #[cfg(test)]
    before_ack: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
    #[cfg(test)]
    before_mirror: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

pub fn global() -> Option<Arc<RecoveryJournal>> {
    GLOBAL.get().cloned()
}
pub fn install(journal: Arc<RecoveryJournal>) -> Result<()> {
    GLOBAL
        .set(journal)
        .map_err(|_| RecoveryError::Invalid("journal already installed"))
}
/// Optional for embedded library tests. The actual server must install a journal.
pub async fn ensure_healthy() -> Result<()> {
    if let Some(journal) = global() {
        journal.ensure_healthy().await?;
    }
    Ok(())
}

fn canonical(value: &Value) -> Result<String> {
    fn sort(v: &mut Value) {
        match v {
            Value::Object(m) => {
                for x in m.values_mut() {
                    sort(x);
                }
                m.sort_keys();
            }
            Value::Array(a) => a.iter_mut().for_each(sort),
            _ => {}
        }
    }
    let mut value = value.clone();
    sort(&mut value);
    let encoded = serde_json::to_string(&value)
        .map_err(|_| RecoveryError::Invalid("cannot encode record"))?;
    if encoded.len() > MAX_RECORD_BYTES {
        return Err(RecoveryError::Invalid("record exceeds 16 MiB"));
    }
    Ok(encoded)
}
pub fn admission_fingerprint(kind: &str, payload: &Value) -> Result<String> {
    let mut intent = payload.clone();
    if let Some(fields) = intent.as_object_mut() {
        if kind == "erc4337" {
            fields.remove("pregeneratedNonce");
        }
        if kind == "eip7702" {
            fields.remove("nonce");
        }
    }
    Ok(format!(
        "{:x}",
        Sha256::digest(format!("{kind}\0{}", canonical(&intent)?))
    ))
}
fn identity(kind: &str, id: &str) -> Result<()> {
    if !matches!(kind, "eoa" | "eoa_noop" | "solana" | "erc4337" | "eip7702")
        || id.is_empty()
        || id.len() > 1024
    {
        return Err(RecoveryError::Invalid(
            "unsupported kind or invalid transaction ID",
        ));
    }
    Ok(())
}
fn namespace_valid(namespace: &Option<String>) -> Result<()> {
    if let Some(ns) = namespace {
        if ns.is_empty()
            || ns.len() > 128
            || !ns
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(RecoveryError::Invalid(
                "namespace must use ASCII letters, digits, hyphens or underscores",
            ));
        }
    }
    Ok(())
}
fn marker_key(namespace: &Option<String>) -> String {
    format!(
        "{}:recovery:checkpoint",
        namespace.as_deref().unwrap_or("engine")
    )
}

#[cfg(unix)]
fn check_private(path: &Path, directory: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || if directory {
            !metadata.is_dir()
        } else {
            !metadata.is_file()
        }
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(RecoveryError::Invalid(
            "ledger directory must be private (0700), files private (0600), and not symlinks",
        ));
    }
    Ok(())
}
#[cfg(not(unix))]
fn check_private(_: &Path, _: bool) -> Result<()> {
    Err(RecoveryError::Invalid(
        "local recovery requires a supported Unix filesystem",
    ))
}
fn private_new_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    file.sync_all()?;
    Ok(file)
}
fn prepare_directory(path: &Path, create: bool) -> Result<()> {
    let parent =
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .ok_or(RecoveryError::Invalid(
                "ledger path needs a parent directory",
            ))?;
    if !parent.exists() && create {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(parent)?;
        let ancestor = parent
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(ancestor)?.sync_all()?;
    }
    check_private(parent, true)
}
fn lock_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".lock");
    PathBuf::from(name)
}
fn owner_lock(path: &Path, create: bool) -> Result<File> {
    prepare_directory(path, create)?;
    let lock = lock_path(path);
    let file = if create && !lock.exists() {
        private_new_file(&lock)?
    } else {
        check_private(&lock, false)?;
        OpenOptions::new().read(true).write(true).open(&lock)?
    };
    FileExt::try_lock_exclusive(&file).map_err(|_| RecoveryError::Locked)?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(file)
}
fn connection(path: &Path, readonly: bool) -> Result<Connection> {
    if !path.exists() {
        return Err(RecoveryError::Missing);
    }
    prepare_directory(path, false)?;
    check_private(path, false)?;
    let flags = if readonly {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE
    };
    let conn = Connection::open_with_flags(path, flags)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    if !readonly {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "fullfsync", "ON")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let mode: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
        let sync: i64 = conn.pragma_query_value(None, "synchronous", |r| r.get(0))?;
        if mode != "wal" || sync != 2 {
            return Err(RecoveryError::Storage);
        }
    }
    Ok(conn)
}
/// One crash-atomic DDL transaction; an error rolls back both indexes.
/// Only explicit initialization and healthy owned startup may invoke this.
fn ensure_history_indexes(conn: &mut Connection) -> Result<()> {
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    tx.execute_batch(HISTORY_INDEX_SQL)?;
    commit_transaction(tx)?;
    Ok(())
}

fn commit_transaction(tx: rusqlite::Transaction<'_>) -> Result<()> {
    let _timer = metrics::timer("database", "sql_commit");
    tx.commit()?;
    Ok(())
}

fn control(conn: &Connection) -> Result<Control> {
    let schema: i64 = conn.query_row(
        "SELECT schema_version FROM control WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    if !matches!(schema, 1 | SCHEMA) {
        return Err(RecoveryError::Invalid("unknown ledger schema"));
    }
    Ok(conn.query_row(
        "SELECT deployment,epoch,checkpoint,namespace,run_id,halted,reason FROM control WHERE singleton=1",
        [],
        |row| Ok(Control {
            schema,
            deployment: row.get(0)?,
            epoch: row.get(1)?,
            checkpoint: row.get(2)?,
            namespace: row.get(3)?,
            run_id: row.get(4)?,
            halted: row.get(5)?,
            reason: row.get(6)?,
        }),
    )?)
}
fn lookup(conn: &Connection, id: &str) -> Result<Option<AdmissionRecord>> {
    let row = conn
        .query_row(
            "SELECT id,kind,fingerprint,payload,state,replay_key FROM admissions WHERE id=?",
            [id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()?;
    row.map(|(id, kind, fingerprint, payload, state, replay_key)| {
        Ok(AdmissionRecord {
            id,
            kind,
            fingerprint,
            payload: serde_json::from_str(&payload).map_err(|_| RecoveryError::Storage)?,
            state: AdmissionState::parse(&state)?,
            replay_key,
        })
    })
    .transpose()
}
fn advance(conn: &Connection) -> Result<Control> {
    let old = control(conn)?;
    if old.checkpoint == i64::MAX {
        return Err(RecoveryError::Invalid("checkpoint exhausted"));
    }
    conn.execute(
        "UPDATE control SET checkpoint=checkpoint+1 WHERE singleton=1",
        [],
    )?;
    let target = control(conn)?;
    projection::record(conn, &old, &target)?;
    Ok(old)
}
async fn redis_connection(url: &str) -> Result<ConnectionManager> {
    let config = redis::aio::ConnectionManagerConfig::new()
        .set_connection_timeout(Duration::from_secs(5))
        .set_response_timeout(Duration::from_secs(5))
        .set_number_of_retries(0);
    Ok(redis::Client::open(url)?
        .get_connection_manager_with_config(config)
        .await?)
}
async fn observation(
    redis: &ConnectionManager,
    key: &str,
) -> Result<(String, String, Option<String>)> {
    let _timer = metrics::timer("continuity", "redis_observation");
    let (server, replication, marker): (String, String, Option<String>) = redis::pipe()
        .atomic()
        .cmd("INFO")
        .arg("server")
        .cmd("INFO")
        .arg("replication")
        .cmd("GET")
        .arg(key)
        .query_async(&mut redis.clone())
        .await?;
    fn field(info: &str, key: &str) -> Result<String> {
        info.lines()
            .find_map(|l| l.strip_prefix(key))
            .map(str::to_owned)
            .ok_or(RecoveryError::Storage)
    }
    Ok((
        field(&server, "run_id:")?,
        field(&replication, "role:")?,
        marker,
    ))
}
async fn empty_namespace(redis: &ConnectionManager, namespace: &Option<String>) -> Result<()> {
    namespace_valid(namespace)?;
    let patterns = match namespace {
        Some(ns) => vec![format!("{ns}:*"), format!("twmq:{ns}_*")],
        None => vec!["*".into()],
    };
    for pattern in patterns {
        let mut cursor = 0u64;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut redis.clone())
                .await?;
            if !keys.is_empty() {
                return Err(RecoveryError::Invalid(
                    "destination Redis namespace is not empty",
                ));
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
    }
    Ok(())
}

impl RecoveryJournal {
    /// Explicit offline initialization, never called as a fallback from open().
    pub async fn initialize(
        path: impl AsRef<Path>,
        redis_url: &str,
        namespace: Option<String>,
    ) -> Result<()> {
        let path = path.as_ref();
        namespace_valid(&namespace)?;
        if path.exists() {
            return Err(RecoveryError::Invalid("ledger already exists"));
        }
        let _owner = owner_lock(path, true)?;
        let redis = redis_connection(redis_url).await?;
        empty_namespace(&redis, &namespace).await?;
        let (run_id, role, _) = observation(&redis, &marker_key(&namespace)).await?;
        if role != "master" {
            return Err(RecoveryError::Invalid("Redis must be a writable primary"));
        }
        private_new_file(path)?;
        let conn = connection(path, false)?;
        conn.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE control(singleton INTEGER PRIMARY KEY CHECK(singleton=1),schema_version INTEGER NOT NULL,deployment TEXT NOT NULL,epoch INTEGER NOT NULL,checkpoint INTEGER NOT NULL,namespace TEXT,run_id TEXT NOT NULL,halted INTEGER NOT NULL,reason TEXT);
            CREATE TABLE admissions(id TEXT PRIMARY KEY,kind TEXT NOT NULL,fingerprint TEXT NOT NULL,payload TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('admitted','terminal','quarantined')),replay_key TEXT UNIQUE);
            CREATE TABLE attempts(sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL REFERENCES admissions(id),replay_key TEXT NOT NULL,digest TEXT NOT NULL,payload TEXT NOT NULL,UNIQUE(id,digest));
            CREATE TABLE terminal_evidence(sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL REFERENCES admissions(id),evidence TEXT NOT NULL);
            CREATE TABLE chain_checkpoints(chain_id TEXT PRIMARY KEY,evidence TEXT NOT NULL);
            CREATE TABLE chain_halts(chain_id TEXT PRIMARY KEY,reason TEXT NOT NULL);
            CREATE TABLE recovery_events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,event TEXT NOT NULL);")?;
        conn.execute_batch(HISTORY_INDEX_SQL)?;
        conn.execute_batch(projection::TABLE_SQL)?;
        conn.execute(
            "INSERT INTO control VALUES(1,?, ?,1,0,?,?,1,'initializing')",
            params![SCHEMA, uuid::Uuid::new_v4().to_string(), namespace, run_id],
        )?;
        conn.execute_batch("COMMIT")?;
        File::open(path.parent().unwrap())?.sync_all()?;
        let c = control(&conn)?;
        let result: Option<String> = redis::cmd("SET")
            .arg(c.key())
            .arg(c.token())
            .arg("NX")
            .query_async(&mut redis.clone())
            .await?;
        if result.is_none() {
            return Err(RecoveryError::RecoveryRequired("initialization collision"));
        }
        conn.execute(
            "UPDATE control SET halted=0,reason=NULL WHERE singleton=1",
            [],
        )?;
        Ok(())
    }
    pub async fn open(
        path: impl AsRef<Path>,
        redis_url: &str,
        namespace: Option<String>,
    ) -> Result<Arc<Self>> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(RecoveryError::Missing);
        }
        namespace_valid(&namespace)?;
        let owner = owner_lock(path, false)?;
        let conn = connection(path, false)?;
        let check: String = conn.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
        if check != "ok" {
            return Err(RecoveryError::Storage);
        }
        if control(&conn)?.namespace != namespace {
            return Err(RecoveryError::Invalid(
                "configured namespace does not match ledger",
            ));
        }
        let this = Arc::new(Self {
            db: Arc::new(Mutex::new(conn)),
            _owner: Arc::new(owner),
            redis: redis_connection(redis_url).await?,
            serial: AsyncMutex::new(()),
            fairness: fairness::JournalFairness::new(),
            admission_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_ADMISSIONS)),
            health_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_HEALTH_CHECKS)),
            owned_operations: Arc::new(RwLock::new(())),
            failed: AtomicBool::new(false),
            #[cfg(test)]
            before_ack: Mutex::new(None),
            #[cfg(test)]
            before_mirror: Mutex::new(None),
        });
        Self::prepare_owned_startup(this.clone(), || {})
            .await
            .map_err(|_| RecoveryError::Storage)??;
        Ok(this)
    }
    /// The callback is a local synchronization seam for ownership tests; normal
    /// startup uses a no-op. It never changes the DDL or its safety checks.
    fn prepare_owned_startup(
        preparing: Arc<Self>,
        before_ddl: impl FnOnce() + Send + 'static,
    ) -> tokio::task::JoinHandle<Result<Control>> {
        // Retain ownership even if the caller cancels while SQLite is working.
        tokio::spawn(async move {
            let _guard = preparing.serial_guard("startup").await;
            preparing.healthy_locked().await?;
            // Blocking work may outlive its async task during runtime shutdown.
            let ddl_owner = preparing.clone();
            preparing
                .db(move |conn| {
                    let _owner = ddl_owner;
                    before_ddl();
                    ensure_history_indexes(conn)
                })
                .await?;
            let current = preparing.healthy_locked().await?;
            if current.schema == 1 {
                let result = preparing.db(projection::migrate).await;
                preparing.storage_result(result).await?;
                #[cfg(test)]
                projection_tests::crash_cut("after_migration_commit").await;
                preparing.healthy_locked().await
            } else {
                Ok(current)
            }
        })
    }
    async fn lane_permit(&self, operation: &'static str) -> tokio::sync::OwnedSemaphorePermit {
        let lane_wait = metrics::timer(operation, "lane_wait");
        let lane = self.fairness.acquire().await;
        drop(lane_wait);
        lane
    }
    async fn serial_guard(&self, operation: &'static str) -> SerialGuard<'_> {
        let lane = self.lane_permit(operation).await;
        self.serial_guard_with_lane(operation, lane).await
    }
    async fn serial_guard_with_lane(
        &self,
        operation: &'static str,
        lane: tokio::sync::OwnedSemaphorePermit,
    ) -> SerialGuard<'_> {
        let wait = metrics::timer(operation, "serial_wait");
        let guard = self.serial.lock().await;
        drop(wait);
        SerialGuard {
            _guard: guard,
            _timer: metrics::timer(operation, "serial_hold"),
            _lane: lane,
        }
    }
    /// Once a mutation begins, caller cancellation must not split its FULL
    /// commit from the Redis mirror. The owned task retains serialization and
    /// admission capacity through completion. After a process crash, a recorded
    /// pending transition can be repaired without recreating the caller's permit.
    async fn run_owned<T, F, Fut>(self: &Arc<Self>, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Self>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
    {
        // Register before spawn, so shutdown also sees tasks not yet polled.
        let registered = self.owned_operations.clone().read_owned().await;
        let journal = self.clone();
        let chain = fairness::current();
        tokio::spawn(fairness::scope(chain, async move {
            let _registered = registered;
            operation(journal).await
        }))
        .await
        .map_err(|_| RecoveryError::Storage)?
    }
    /// Stop HTTP/workers first, then wait for mutations whose callers were
    /// cancelled. Returning from the async runtime before this barrier could
    /// abort a detached task between its SQL commit and Redis publication.
    pub async fn drain_mutations(&self) {
        let _drained = self.owned_operations.write().await;
    }
    async fn db<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let db = self.db.clone();
        let owner = self._owner.clone();
        let submitted = Instant::now();
        tokio::task::spawn_blocking(move || {
            let _owner = owner;
            metrics::observe("database", "blocking_queue", submitted.elapsed());
            let wait = metrics::timer("database", "db_mutex_wait");
            let mut db = db.lock().map_err(|_| RecoveryError::Storage)?;
            drop(wait);
            let _execution = metrics::timer("database", "db_execution");
            f(&mut db)
        })
        .await
        .map_err(|_| RecoveryError::Storage)?
    }
    async fn halt(&self, reason: &'static str) -> RecoveryError {
        self.failed.store(true, Ordering::SeqCst);
        tracing::error!(reason, "Recovery safety gate latched");
        // Never overwrite an earlier cause: reattach may clear only continuity loss.
        let _ = self
            .db(move |c| {
                c.execute(
                    "UPDATE control SET halted=1,reason=COALESCE(reason,?) WHERE singleton=1",
                    [reason],
                )?;
                Ok(())
            })
            .await;
        RecoveryError::RecoveryRequired(reason)
    }
    async fn healthy_locked(&self) -> Result<Control> {
        if self.failed.load(Ordering::SeqCst) {
            return Err(RecoveryError::RecoveryRequired(
                "process safety gate is latched",
            ));
        }
        let state = self
            .db(|db| {
                let current = control(db)?;
                let pending = projection::load(db, &current)?;
                Ok((current, pending))
            })
            .await;
        let (current, pending) = match state {
            Ok(state) => state,
            Err(_) => return Err(self.halt("ledger unavailable").await),
        };
        if current.halted {
            tracing::error!(reason = ?current.reason, "Recovery ledger is durably halted");
            self.failed.store(true, Ordering::SeqCst);
            return Err(RecoveryError::RecoveryRequired(
                "durable safety gate is halted",
            ));
        }
        self.observe_checkpoint(&current, pending.as_ref()).await?;
        if let Some(pending) = pending {
            self.complete_pending(&current, pending).await?;
        }
        Ok(current)
    }

    /// Only a recorded, unacknowledged transition permits its predecessor.
    /// Transport uncertainty is retryable only while that exact record exists.
    async fn observe_checkpoint(
        &self,
        current: &Control,
        pending: Option<&projection::PendingUpdate>,
    ) -> Result<()> {
        let (run, role, marker) = match observation(&self.redis, &current.key()).await {
            Ok(observed) => observed,
            Err(_) if pending.is_some() => return Err(RecoveryError::PublicationPending),
            Err(_) => return Err(self.halt("Redis continuity unavailable").await),
        };
        let matches_target = marker.as_deref() == Some(current.token().as_str());
        let matches_previous =
            pending.is_some_and(|p| marker.as_deref() == Some(p.previous_token.as_str()));
        if !matches_target && !matches_previous {
            return Err(self.halt("Redis checkpoint mismatch").await);
        }
        if role != "master" {
            return Err(self.halt("Redis primary role changed").await);
        }
        if run != current.run_id {
            return Err(self.halt("Redis process changed").await);
        }
        Ok(())
    }

    pub async fn ensure_healthy(self: &Arc<Self>) -> Result<()> {
        // Wait cancellably in this chain's lane before reserving global health
        // capacity. A hot chain must not fill all slots with its lane waiters.
        // Both permits transfer to the owned task before any repair can begin.
        let lane = self.lane_permit("health").await;
        let capacity_wait = metrics::timer("health", "health_capacity_wait");
        let permit = self
            .health_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RecoveryError::Storage)?;
        drop(capacity_wait);
        self.run_owned(move |journal| async move {
            let _permit = permit;
            let _guard = journal.serial_guard_with_lane("health", lane).await;
            journal.healthy_locked().await.map(|_| ())
        })
        .await
    }

    async fn complete_pending(
        &self,
        current: &Control,
        pending: projection::PendingUpdate,
    ) -> Result<()> {
        const PUBLISH_CHECKPOINT: &str = r#"
            local current = redis.call('GET', KEYS[1])
            if current == ARGV[2] then return 1 end
            if current ~= ARGV[1] then return 0 end
            redis.call('SET', KEYS[1], ARGV[2])
            return 1
        "#;
        let mirror_timer = metrics::timer("checkpoint", "redis_mirror");
        let result: redis::RedisResult<i32> = redis::Script::new(PUBLISH_CHECKPOINT)
            .key(&pending.redis_key)
            .arg(&pending.previous_token)
            .arg(&pending.target_token)
            .invoke_async(&mut self.redis.clone())
            .await;
        drop(mirror_timer);
        match result {
            Ok(1) => {}
            Ok(_) => return Err(self.halt("Redis checkpoint mismatch").await),
            Err(_) => return Err(RecoveryError::PublicationPending),
        }
        #[cfg(test)]
        projection_tests::crash_cut("after_redis_apply").await;
        // The CAS response alone cannot authorize a send: bind the applied
        // marker to the same Redis process acting as writable primary.
        let (run, role, marker) = observation(&self.redis, &current.key())
            .await
            .map_err(|_| RecoveryError::PublicationPending)?;
        if marker.as_deref() != Some(pending.target_token.as_str()) {
            return Err(self.halt("Redis checkpoint mismatch").await);
        }
        if role != "master" {
            return Err(self.halt("Redis primary role changed").await);
        }
        if run != pending.run_id {
            return Err(self.halt("Redis process changed").await);
        }
        #[cfg(test)]
        {
            projection_tests::crash_cut("before_ack").await;
            let pause = self.before_ack.lock().unwrap().take();
            if let Some((entered, resume)) = pause {
                let _ = entered.send(());
                let _ = resume.await;
            }
        }
        let result = self
            .db(move |conn| projection::acknowledge(conn, &pending))
            .await;
        self.storage_result(result).await?;
        #[cfg(test)]
        projection_tests::crash_cut("after_ack").await;
        // No pending exception remains after acknowledgement. A subsequent
        // rollback or unavailable observation follows the ordinary hard fence.
        self.observe_checkpoint(current, None).await
    }

    async fn mirror(&self, old: Control) -> Result<()> {
        #[cfg(test)]
        {
            projection_tests::crash_cut("after_authority_commit").await;
            let pause = self.before_mirror.lock().unwrap().take();
            if let Some((entered, resume)) = pause {
                let _ = entered.send(());
                let _ = resume.await;
            }
        }
        let state = self
            .db(move |conn| {
                let current = control(conn)?;
                let pending = projection::load(conn, &current)?.ok_or(RecoveryError::Storage)?;
                if pending.previous_token != old.token() {
                    return Err(RecoveryError::Storage);
                }
                Ok((current, pending))
            })
            .await;
        let (current, pending) = self.storage_result(state).await?;
        self.observe_checkpoint(&current, Some(&pending)).await?;
        self.complete_pending(&current, pending).await
    }
    pub async fn reserve_admission(
        self: &Arc<Self>,
        kind: &str,
        id: &str,
        fingerprint: &str,
        payload: Value,
    ) -> Result<AdmissionReservation> {
        let _permit = self
            .admission_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecoveryError::Busy)?;
        identity(kind, id)?;
        if admission_fingerprint(kind, &payload)? != fingerprint {
            return Err(RecoveryError::Conflict);
        }
        let encoded = canonical(&payload)?;
        let (kind, id, fingerprint) = (kind.to_owned(), id.to_owned(), fingerprint.to_owned());
        self.run_owned(move |journal| async move {
            let _permit = _permit;
            let _guard = journal.serial_guard("admission").await;
            journal.healthy_locked().await?;
            let result = journal.db(move |conn| {
                let tx = conn.transaction()?;
                if let Some(old) = lookup(&tx, &id)? {
                    if old.kind != kind || old.fingerprint != fingerprint {
                        return Err(RecoveryError::Conflict);
                    }
                    if old.state == AdmissionState::Quarantined {
                        return Err(RecoveryError::RecoveryRequired("transaction is quarantined"));
                    }
                    return Ok((AdmissionReservation {
                        payload: old.payload,
                        terminal: old.state == AdmissionState::Terminal,
                    }, None));
                }
                tx.execute(
                    "INSERT INTO admissions(id,kind,fingerprint,payload,state) VALUES(?,?,?,?,'admitted')",
                    params![id, kind, fingerprint, encoded],
                )?;
                let old = advance(&tx)?;
                commit_transaction(tx)?;
                Ok((AdmissionReservation { payload, terminal: false }, Some(old)))
        }).await;
        let (reservation, old) = journal.storage_result(result).await?;
        if let Some(old) = old {
            journal.mirror(old).await?;
        }
        Ok(reservation)
        }).await
    }
    async fn storage_result<T>(&self, result: Result<T>) -> Result<T> {
        match result {
            Err(RecoveryError::Storage) => Err(self.halt("ledger unavailable").await),
            other => other,
        }
    }
    pub async fn admission(&self, kind: &str, id: &str) -> Result<Option<AdmissionRecord>> {
        let (kind, id) = (kind.to_owned(), id.to_owned());
        self.db(move |c| {
            let result = lookup(c, &id)?;
            if result.as_ref().is_some_and(|x| x.kind != kind) {
                return Err(RecoveryError::Conflict);
            }
            Ok(result)
        })
        .await
    }
    pub async fn admission_state(&self, kind: &str, id: &str) -> Result<Option<AdmissionState>> {
        Ok(self.admission(kind, id).await?.map(|r| r.state))
    }
    /// First committed terminal witness. Later contradictory observations remain
    /// in the audit history and halt execution; they never replace this witness.
    pub async fn terminal_evidence(&self, kind: &str, id: &str) -> Result<Option<Value>> {
        let (kind, id) = (kind.to_owned(), id.to_owned());
        self.db(move |c| {
            let Some(record) = lookup(c, &id)? else {
                return Ok(None);
            };
            if record.kind != kind {
                return Err(RecoveryError::Conflict);
            }
            if record.state != AdmissionState::Terminal {
                return Ok(None);
            }
            let body: String = c.query_row(
                "SELECT evidence FROM terminal_evidence WHERE id=? ORDER BY sequence LIMIT 1",
                [id],
                |r| r.get(0),
            )?;
            Ok(Some(
                serde_json::from_str(&body).map_err(|_| RecoveryError::Storage)?,
            ))
        })
        .await
    }
    pub async fn validate_admission(
        self: &Arc<Self>,
        kind: &str,
        id: &str,
        fingerprint: &str,
    ) -> Result<()> {
        self.ensure_healthy().await?;
        let record = self
            .admission(kind, id)
            .await?
            .ok_or(RecoveryError::RecoveryRequired("untracked transaction"))?;
        if record.fingerprint != fingerprint {
            return Err(RecoveryError::Conflict);
        }
        if record.state != AdmissionState::Admitted {
            return Err(RecoveryError::RecoveryRequired(
                "transaction is terminal or quarantined",
            ));
        }
        Ok(())
    }
    pub async fn validate_payload(
        self: &Arc<Self>,
        kind: &str,
        id: &str,
        payload: &Value,
    ) -> Result<()> {
        let fingerprint = admission_fingerprint(kind, payload)?;
        self.validate_admission(kind, id, &fingerprint).await?;
        let record = self
            .admission(kind, id)
            .await?
            .ok_or(RecoveryError::RecoveryRequired("untracked transaction"))?;
        if canonical(&record.payload)? != canonical(payload)? {
            return Err(RecoveryError::Conflict);
        }
        Ok(())
    }
    /// Read-only pre-sign check. Network emission still requires before_broadcast,
    /// which atomically binds the identity and commits the exact attempted wire.
    pub async fn validate_replay_binding(
        self: &Arc<Self>,
        kind: &str,
        id: &str,
        replay_key: &str,
    ) -> Result<()> {
        identity(kind, id)?;
        validate_replay_key(kind, replay_key)?;
        let (kind, id, key) = (kind.to_owned(), id.to_owned(), replay_key.to_owned());
        self.run_owned(move |journal| async move {
            let _guard = journal.serial_guard("replay_validation").await;
            journal.healthy_locked().await?;
            journal
                .db(move |c| {
                    let record = lookup(c, &id)?
                        .ok_or(RecoveryError::RecoveryRequired("untracked signing intent"))?;
                    if record.kind != kind || record.replay_key.as_ref().is_some_and(|k| k != &key)
                    {
                        return Err(RecoveryError::Conflict);
                    }
                    if record.state != AdmissionState::Admitted {
                        return Err(RecoveryError::RecoveryRequired(
                            "transaction is terminal or quarantined",
                        ));
                    }
                    let owner: Option<String> = c
                        .query_row(
                            "SELECT id FROM admissions WHERE replay_key=?",
                            [&key],
                            |r| r.get(0),
                        )
                        .optional()?;
                    if owner.as_ref().is_some_and(|owner| owner != &id) {
                        return Err(RecoveryError::Conflict);
                    }
                    if kind != "solana" {
                        let chain = key
                            .split(':')
                            .nth(1)
                            .ok_or(RecoveryError::Invalid("missing replay chain"))?;
                        let halted: bool = c.query_row(
                            "SELECT EXISTS(SELECT 1 FROM chain_halts WHERE chain_id=?)",
                            [chain],
                            |r| r.get(0),
                        )?;
                        if halted {
                            return Err(RecoveryError::RecoveryRequired(
                                "chain finality conflict requires reconciliation",
                            ));
                        }
                    }
                    Ok(())
                })
                .await
        })
        .await
    }
    /// Commit exact attempted wire/request BEFORE network I/O. A committed record
    /// is conservatively possibly broadcast even if the process dies before send.
    pub async fn before_broadcast(
        self: &Arc<Self>,
        kind: &str,
        id: &str,
        replay_key: &str,
        attempt: Value,
    ) -> Result<()> {
        identity(kind, id)?;
        validate_replay_key(kind, replay_key)?;
        let encoded = canonical(&attempt)?;
        let digest = format!("{:x}", Sha256::digest(encoded.as_bytes()));
        let (kind, id, replay_key) = (kind.to_owned(), id.to_owned(), replay_key.to_owned());
        self.run_owned(move |journal| async move {
            let _guard = journal.serial_guard("broadcast").await;
            journal.healthy_locked().await?;
            let result = journal
                .db(move |conn| {
                    let tx = conn.transaction()?;
                    let record = lookup(&tx, &id)?
                        .ok_or(RecoveryError::RecoveryRequired("untracked broadcast"))?;
                    if record.kind != kind {
                        return Err(RecoveryError::Conflict);
                    }
                    if record.state != AdmissionState::Admitted {
                        return Err(RecoveryError::RecoveryRequired(
                            "transaction is terminal or quarantined",
                        ));
                    }
                    if kind != "solana" {
                        let chain = replay_key
                            .split(':')
                            .nth(1)
                            .ok_or(RecoveryError::Invalid("replay key has no chain identity"))?;
                        let halted: bool = tx.query_row(
                            "SELECT EXISTS(SELECT 1 FROM chain_halts WHERE chain_id=?)",
                            [chain],
                            |r| r.get(0),
                        )?;
                        if halted {
                            return Err(RecoveryError::RecoveryRequired(
                                "chain finality conflict requires reconciliation",
                            ));
                        }
                    }
                    if record
                        .replay_key
                        .as_ref()
                        .is_some_and(|key| key != &replay_key)
                    {
                        return Err(RecoveryError::Conflict);
                    }
                    let owner: Option<String> = tx
                        .query_row(
                            "SELECT id FROM admissions WHERE replay_key=?",
                            [&replay_key],
                            |r| r.get(0),
                        )
                        .optional()?;
                    if owner.as_ref().is_some_and(|owner| owner != &id) {
                        return Err(RecoveryError::Conflict);
                    }
                    // An exact retry already has FULL-committed evidence. Check its
                    // current owner/state/chain above, but do not fsync the same wire
                    // again or advance the checkpoint merely for a retransmission.
                    let existing: Option<(String, String)> = tx
                        .query_row(
                            "SELECT replay_key,payload FROM attempts WHERE id=? AND digest=?",
                            params![id, digest],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .optional()?;
                    if let Some((stored_key, stored_payload)) = existing {
                        if stored_key != replay_key
                            || stored_payload != encoded
                            || record.replay_key.as_deref() != Some(replay_key.as_str())
                        {
                            return Err(RecoveryError::Storage);
                        }
                        return Ok(None);
                    }
                    tx.execute(
                        "UPDATE admissions SET replay_key=? WHERE id=?",
                        params![replay_key, id],
                    )?;
                    tx.execute(
                        "INSERT INTO attempts(id,replay_key,digest,payload) VALUES(?,?,?,?)",
                        params![id, replay_key, digest, encoded],
                    )?;
                    let old = advance(&tx)?;
                    commit_transaction(tx)?;
                    Ok(Some(old))
                })
                .await;
            let old = journal.storage_result(result).await?;
            if let Some(old) = old {
                journal.mirror(old).await?;
            }
            Ok(())
        })
        .await
    }
    /// Read one already persisted attempt for exact-wire gap recovery. This is
    /// not broadcast authorization: callers must decode/validate the wire and
    /// pass before_broadcast again immediately before dispatch.
    pub async fn latest_eoa_attempt(
        self: &Arc<Self>,
        id: &str,
        expected_replay_key: &str,
    ) -> Result<Option<Value>> {
        identity("eoa", id)?;
        validate_replay_key("eoa", expected_replay_key)?;
        self.ensure_healthy().await?;
        let (id, expected) = (id.to_owned(), expected_replay_key.to_owned());
        self.db(move |conn| {
            let record = lookup(conn, &id)?
                .ok_or(RecoveryError::RecoveryRequired("untracked gap recovery"))?;
            if record.kind != "eoa" || record.replay_key.as_deref() != Some(expected.as_str()) {
                return Err(RecoveryError::Conflict);
            }
            if record.state != AdmissionState::Admitted {
                return Err(RecoveryError::RecoveryRequired(
                    "gap recovery is terminal or quarantined",
                ));
            }
            let chain = expected.split(':').nth(1).ok_or(RecoveryError::Storage)?;
            ensure_chain_not_halted(conn, chain)?;
            let row: Option<(String, String)> = conn.query_row(
                "SELECT replay_key,payload FROM attempts WHERE id=? ORDER BY sequence DESC LIMIT 1",
                [&id], |r| Ok((r.get(0)?, r.get(1)?)),
            ).optional()?;
            let Some((key, payload)) = row else {
                return Ok(None);
            };
            if key != expected {
                return Err(RecoveryError::Storage);
            }
            Ok(Some(
                serde_json::from_str(&payload).map_err(|_| RecoveryError::Storage)?,
            ))
        })
        .await
    }

    /// Verify that confirmation identity belongs to this admission's durable
    /// attempt history. This can run before network queries, and is repeated in
    /// the terminal transaction so no earlier validation can bypass a halt.
    pub async fn validate_attempt_identity(
        self: &Arc<Self>,
        kind: &str,
        id: &str,
        identity: &Value,
    ) -> Result<()> {
        self.ensure_healthy().await?;
        let (kind, id, identity) = (kind.to_owned(), id.to_owned(), identity.clone());
        self.db(move |conn| {
            let record = lookup(conn, &id)?
                .ok_or(RecoveryError::RecoveryRequired("untracked confirmation"))?;
            if record.kind != kind {
                return Err(RecoveryError::Conflict);
            }
            validate_attempt_identity_in_db(conn, &record, &identity)
        })
        .await
    }
    /// Only a trusted chain reconciliation path may supply terminal evidence.
    /// A null receipt or nonce progress cannot supply a terminal outcome.
    pub async fn record_terminal(
        self: &Arc<Self>,
        kind: &str,
        id: &str,
        evidence: Value,
    ) -> Result<()> {
        identity(kind, id)?;
        let normalized = terminal_identity(&evidence)?;
        let encoded = canonical(&evidence)?;
        let (kind, id) = (kind.to_owned(), id.to_owned());
        self.run_owned(move |journal| async move {
            let _guard = journal.serial_guard("terminal").await;
            journal.healthy_locked().await?;
            let result = journal.db(move |conn| {
                let tx = conn.transaction()?;
                let record = lookup(&tx, &id)?
                    .ok_or(RecoveryError::RecoveryRequired("untracked terminal observation"))?;
                if record.kind != kind { return Err(RecoveryError::Conflict); }
                validate_attempt_identity_in_db(&tx, &record, &evidence)?;
                if record.state == AdmissionState::Terminal {
                    let previous: String = tx.query_row(
                        "SELECT evidence FROM terminal_evidence WHERE id=? ORDER BY sequence LIMIT 1",
                        [&id], |row| row.get(0),
                    )?;
                    let previous: Value = serde_json::from_str(&previous).map_err(|_| RecoveryError::Storage)?;
                    if terminal_identity(&previous)? == normalized { return Ok((None, false)); }
                    tx.execute("INSERT INTO terminal_evidence(id,evidence) VALUES(?,?)", params![id, encoded])?;
                    tx.execute("UPDATE control SET halted=1,reason='terminal evidence conflict' WHERE singleton=1", [])?;
                    commit_transaction(tx)?;
                    return Ok((None, true));
                }
                tx.execute("INSERT INTO terminal_evidence(id,evidence) VALUES(?,?)", params![id, encoded])?;
                tx.execute("UPDATE admissions SET state='terminal' WHERE id=?", [id])?;
                let old = advance(&tx)?;
                commit_transaction(tx)?;
                Ok((Some(old), false))
        }).await;
        let (old, conflict) = journal.storage_result(result).await?;
        if conflict {
            journal.failed.store(true, Ordering::SeqCst);
            return Err(RecoveryError::RecoveryRequired(
                "terminal evidence conflict",
            ));
        }
        if let Some(old) = old {
            journal.mirror(old).await?;
        }
        Ok(())
        }).await
    }

    pub async fn check_chain_healthy(self: &Arc<Self>, chain_id: u64) -> Result<()> {
        self.ensure_healthy().await?;
        self.db(move |c| {
            let halted: bool = c.query_row(
                "SELECT EXISTS(SELECT 1 FROM chain_halts WHERE chain_id=?)",
                [chain_id.to_string()],
                |r| r.get(0),
            )?;
            if halted {
                Err(RecoveryError::RecoveryRequired(
                    "chain finality conflict requires reconciliation",
                ))
            } else {
                Ok(())
            }
        })
        .await
    }
    pub async fn load_checkpoint(
        self: &Arc<Self>,
        chain_id: u64,
    ) -> Result<Option<crate::finality::FinalityEvidence>> {
        self.check_chain_healthy(chain_id).await?;
        self.db(move |c| load_chain_checkpoint(c, chain_id)).await
    }
    /// Compare-and-swap the prior evidence whose canonicality the caller checked.
    /// A concurrent advancement requires another chain check, never a blind overwrite.
    pub async fn commit_checkpoint(
        self: &Arc<Self>,
        chain_id: u64,
        expected: Option<crate::finality::FinalityEvidence>,
        evidence: crate::finality::FinalityEvidence,
    ) -> Result<bool> {
        self.run_owned(move |journal| async move {
            let _guard = journal.serial_guard("checkpoint").await;
            journal.healthy_locked().await?;
            let result = journal.db(move |conn| {
                let tx = conn.transaction()?;
                ensure_chain_not_halted(&tx, &chain_id.to_string())?;
                let current = load_chain_checkpoint(&tx, chain_id)?;
                if current != expected {
                    return Ok((false, None, false));
                }
                if let Some(previous) = current {
                    if previous.policy != evidence.policy
                        || (previous.checkpoint_number == evidence.checkpoint_number
                            && previous.checkpoint_hash != evidence.checkpoint_hash) {
                        tx.execute(
                            "INSERT OR IGNORE INTO chain_halts VALUES(?,'finality checkpoint conflict')",
                            [chain_id.to_string()],
                        )?;
                        let old = advance(&tx)?;
                        commit_transaction(tx)?;
                        return Ok((false, Some(old), true));
                    }
                    if evidence.checkpoint_number < previous.checkpoint_number {
                        return Ok((false, None, false));
                    }
                    if evidence.checkpoint_number == previous.checkpoint_number {
                        // Receipt block identity belongs to each terminal witness.
                        // The chain fence is unchanged when policy/head/hash match.
                        return Ok((true, None, false));
                    }
                }
                let encoded = serde_json::to_string(&evidence).map_err(|_| RecoveryError::Storage)?;
                tx.execute(
                    "INSERT INTO chain_checkpoints(chain_id,evidence) VALUES(?,?) ON CONFLICT(chain_id) DO UPDATE SET evidence=excluded.evidence",
                    params![chain_id.to_string(), encoded],
                )?;
                let old = advance(&tx)?;
                commit_transaction(tx)?;
                Ok((true, Some(old), false))
        }).await;
        let (accepted, old, conflict) = journal.storage_result(result).await?;
        if let Some(old) = old {
            journal.mirror(old).await?;
        }
        if conflict {
            return Err(RecoveryError::RecoveryRequired(
                "chain finality checkpoint conflicts",
            ));
        }
        Ok(accepted)
        }).await
    }
    pub async fn halt_chain(self: &Arc<Self>, chain_id: u64, _reason: &str) -> Result<()> {
        self.run_owned(move |journal| async move {
            let _guard = journal.serial_guard("chain_halt").await;
            journal.healthy_locked().await?;
            let result = journal
                .db(move |conn| {
                    let tx = conn.transaction()?;
                    tx.execute(
                    "INSERT OR IGNORE INTO chain_halts VALUES(?,'finality checkpoint conflict')",
                    [chain_id.to_string()],
                )?;
                    let old = advance(&tx)?;
                    commit_transaction(tx)?;
                    Ok(old)
                })
                .await;
            let old = journal.storage_result(result).await?;
            journal.mirror(old).await
        })
        .await
    }

    /// Read-only inventory remains available while the server holds the owner lock.
    pub fn status(path: impl AsRef<Path>) -> Result<RecoveryStatus> {
        let conn = connection(path.as_ref(), true)?;
        conn.execute_batch("BEGIN")?;
        status(&conn)
    }
    /// Full export contains admitted credentials/wire data: only a private, new
    /// output file is allowed. Never print this structure in normal diagnostics.
    pub fn export(path: impl AsRef<Path>, output: impl AsRef<Path>) -> Result<()> {
        let conn = connection(path.as_ref(), true)?;
        conn.execute_batch("BEGIN")?;
        export::publish_private(output.as_ref(), |file| export::write_snapshot(&conn, file))
    }
    /// Offline durable stop. There is deliberately no force-clear command.
    pub fn quarantine(path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let _owner = owner_lock(path, false)?;
        let conn = connection(path, false)?;
        control(&conn)?;
        conn.execute(
            "UPDATE control SET halted=1,reason=COALESCE(reason,'operator quarantine') WHERE singleton=1",
            [],
        )?;
        Ok(())
    }
    /// Reattach an intact persisted Redis dataset after a Redis process restart.
    /// Exact marker identity is mandatory; no rollback or arbitrary halt is cleared.
    pub async fn reattach(
        path: impl AsRef<Path>,
        redis_url: &str,
        namespace: Option<String>,
    ) -> Result<RecoveryStatus> {
        let path = path.as_ref();
        let _owner = owner_lock(path, false)?;
        let conn = connection(path, false)?;
        let c = control(&conn)?;
        if projection::load(&conn, &c)?.is_some() {
            return Err(RecoveryError::RecoveryRequired(
                "pending checkpoint requires recovery, not reattach",
            ));
        }
        if c.namespace != namespace {
            return Err(RecoveryError::Invalid(
                "configured namespace does not match ledger",
            ));
        }
        if c.halted
            && !matches!(
                c.reason.as_deref(),
                Some(
                    "Redis process changed"
                        | "Redis continuity unavailable"
                        | "Redis primary role changed"
                )
            )
        {
            return Err(RecoveryError::RecoveryRequired(
                "halt cannot be cleared by reattach",
            ));
        }
        let redis = redis_connection(redis_url).await?;
        let (run, role, marker) = observation(&redis, &c.key()).await?;
        if role != "master" || marker.as_deref() != Some(c.token().as_str()) {
            return Err(RecoveryError::RecoveryRequired(
                "reattach requires exact Redis checkpoint and primary role",
            ));
        }
        conn.execute_batch("BEGIN IMMEDIATE")?;
        conn.execute(
            "UPDATE control SET run_id=?,halted=0,reason=NULL WHERE singleton=1",
            [run],
        )?;
        conn.execute(
            "INSERT INTO recovery_events(event) VALUES('reattached exact persisted checkpoint')",
            [],
        )?;
        conn.execute_batch("COMMIT")?;
        status(&conn)
    }
    /// Restore availability into an EMPTY NEW namespace. All possibly broadcast
    /// nonterminal records remain permanently quarantined. No transaction is sent
    /// or requeued by this operation; retrying unsent IDs returns original payload.
    pub async fn recover(
        path: impl AsRef<Path>,
        redis_url: &str,
        new_namespace: Option<String>,
    ) -> Result<RecoveryStatus> {
        namespace_valid(&new_namespace)?;
        if new_namespace.is_none() {
            return Err(RecoveryError::Invalid(
                "recovery requires a new explicit namespace",
            ));
        }
        let path = path.as_ref();
        let _owner = owner_lock(path, false)?;
        let conn = connection(path, false)?;
        let old = control(&conn)?;
        let discarded_pending = projection::load(&conn, &old)?;
        // Repeated equivalent evidence is a no-op. A second retained witness
        // therefore means an unresolved contradiction, not a Redis-only disaster.
        let terminal_conflict: bool = conn.query_row(
            "SELECT EXISTS(SELECT id FROM terminal_evidence GROUP BY id HAVING count(*)>1)",
            [],
            |r| r.get(0),
        )?;
        if terminal_conflict {
            return Err(RecoveryError::RecoveryRequired(
                "conflicting terminal evidence requires dedicated reconciliation",
            ));
        }
        if old.namespace == new_namespace {
            return Err(RecoveryError::Invalid(
                "recovery must use a different empty namespace",
            ));
        }
        if old.epoch == i64::MAX {
            return Err(RecoveryError::Invalid("deployment epoch exhausted"));
        }
        let redis = redis_connection(redis_url).await?;
        empty_namespace(&redis, &new_namespace).await?;
        let (run, role, _) = observation(&redis, &marker_key(&new_namespace)).await?;
        if role != "master" {
            return Err(RecoveryError::Invalid("Redis must be a writable primary"));
        }
        conn.execute_batch("BEGIN IMMEDIATE")?;
        if old.schema == 1 {
            conn.execute_batch(projection::TABLE_SQL)?;
        }
        conn.execute("DELETE FROM projection_pending", [])?;
        conn.execute(
            "UPDATE control SET schema_version=? WHERE singleton=1",
            [SCHEMA],
        )?;
        conn.execute(
            "UPDATE admissions SET state='quarantined' WHERE state!='terminal' AND EXISTS(SELECT 1 FROM attempts WHERE attempts.id=admissions.id)",
            [],
        )?;
        conn.execute(
            "UPDATE control SET namespace=?,epoch=epoch+1,checkpoint=0,run_id=?,halted=1,reason='recovery incomplete' WHERE singleton=1",
            params![new_namespace, run],
        )?;
        let event = serde_json::json!({
            "action": "recover",
            "previous_namespace": old.namespace,
            "new_namespace": new_namespace,
            "previous_epoch": old.epoch,
            "discarded_pending": discarded_pending,
        });
        conn.execute(
            "INSERT INTO recovery_events(event) VALUES(?)",
            [event.to_string()],
        )?;
        conn.execute_batch("COMMIT")?;
        let c = control(&conn)?;
        let set: Option<String> = redis::cmd("SET")
            .arg(c.key())
            .arg(c.token())
            .arg("NX")
            .query_async(&mut redis.clone())
            .await?;
        if set.is_none() {
            return Err(RecoveryError::RecoveryRequired(
                "recovery namespace collision",
            ));
        }
        let (current_run, current_role, current_marker) = observation(&redis, &c.key()).await?;
        if current_run != c.run_id
            || current_role != "master"
            || current_marker.as_deref() != Some(c.token().as_str())
        {
            return Err(RecoveryError::RecoveryRequired(
                "Redis changed during recovery",
            ));
        }
        conn.execute(
            "UPDATE control SET halted=0,reason=NULL WHERE singleton=1",
            [],
        )?;
        status(&conn)
    }
}

fn ensure_chain_not_halted(conn: &Connection, chain: &str) -> Result<()> {
    let halted: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM chain_halts WHERE chain_id=?)",
        [chain],
        |row| row.get(0),
    )?;
    if halted {
        return Err(RecoveryError::RecoveryRequired(
            "chain finality conflict requires reconciliation",
        ));
    }
    Ok(())
}

fn validate_attempt_identity_in_db(
    conn: &Connection,
    record: &AdmissionRecord,
    identity: &Value,
) -> Result<()> {
    use alloy::primitives::{Address, B256, U256};
    fn field<T: serde::de::DeserializeOwned>(value: &Value, name: &str) -> Result<T> {
        serde_json::from_value(value.get(name).cloned().ok_or(RecoveryError::Conflict)?)
            .map_err(|_| RecoveryError::Conflict)
    }
    if record.state == AdmissionState::Quarantined {
        return Err(RecoveryError::RecoveryRequired(
            "transaction is quarantined",
        ));
    }
    let key = record
        .replay_key
        .as_deref()
        .ok_or(RecoveryError::RecoveryRequired(
            "confirmation has no recorded broadcast attempt",
        ))?;
    let chain = key.split(':').nth(1).ok_or(RecoveryError::Storage)?;
    if record.kind != "solana" {
        let supplied_chain: u64 = field(identity, "chainId")?;
        if chain.parse::<u64>().ok() != Some(supplied_chain) {
            return Err(RecoveryError::Conflict);
        }
        ensure_chain_not_halted(conn, chain)?;
    }
    if record.kind == "eip7702" {
        return Err(RecoveryError::RecoveryRequired(
            "bundled EIP-7702 lacks a qualified canonical UID witness",
        ));
    }
    // The id prefix of UNIQUE(id,digest) bounds the query to this request's
    // history. Iterate rows instead of materializing every signed payload.
    let mut statement =
        conn.prepare("SELECT payload FROM attempts WHERE id=? ORDER BY sequence")?;
    let mut rows = statement.query([&record.id])?;
    while let Some(row) = rows.next()? {
        let payload: String = row.get(0)?;
        let attempt: Value = serde_json::from_str(&payload).map_err(|_| RecoveryError::Storage)?;
        let matches = match record.kind.as_str() {
            "eoa" | "eoa_noop" => {
                let wanted: B256 = field(identity, "transactionHash")?;
                field::<B256>(&attempt, "transactionHash").ok() == Some(wanted)
            }
            "solana" => {
                let signature_text: String = field(identity, "signature")?;
                let signature: solana_sdk::signature::Signature = signature_text
                    .parse()
                    .map_err(|_| RecoveryError::Conflict)?;
                let supplied_chain: String = field(identity, "chainId")?;
                key == format!("solana:{supplied_chain}:{signature}")
                    && attempt
                        .get("signature")
                        .and_then(Value::as_str)
                        .and_then(|value| value.parse::<solana_sdk::signature::Signature>().ok())
                        == Some(signature)
            }
            "erc4337" => {
                let chain_id: u64 = field(identity, "chainId")?;
                let sender: Address = field(identity, "sender")?;
                let entrypoint: Address = field(identity, "entrypoint")?;
                let nonce: U256 = field(identity, "nonce")?;
                let wanted: B256 = field(identity, "userOperationHash")?;
                if key != format!("erc4337:{chain_id}:{entrypoint:#x}:{sender:#x}:{nonce}") {
                    return Err(RecoveryError::Conflict);
                }
                let operation: engine_aa_types::VersionedUserOp = field(&attempt, "userOperation")?;
                field::<u64>(&attempt, "chainId").ok() == Some(chain_id)
                    && field::<Address>(&attempt, "sender").ok() == Some(sender)
                    && field::<Address>(&attempt, "entrypoint").ok() == Some(entrypoint)
                    && field::<U256>(&attempt, "nonce").ok() == Some(nonce)
                    && operation
                        .hash_with_custom_entrypoint(chain_id, entrypoint)
                        .ok()
                        == Some(wanted)
            }
            _ => return Err(RecoveryError::Invalid("unsupported confirmation kind")),
        };
        if matches {
            return Ok(());
        }
    }
    Err(RecoveryError::RecoveryRequired(
        "confirmation identity is not a recorded broadcast attempt",
    ))
}

fn load_chain_checkpoint(
    conn: &Connection,
    chain_id: u64,
) -> Result<Option<crate::finality::FinalityEvidence>> {
    let body: Option<String> = conn
        .query_row(
            "SELECT evidence FROM chain_checkpoints WHERE chain_id=?",
            [chain_id.to_string()],
            |r| r.get(0),
        )
        .optional()?;
    body.map(|body| serde_json::from_str(&body).map_err(|_| RecoveryError::Storage))
        .transpose()
}
fn terminal_identity(evidence: &Value) -> Result<String> {
    let mut identity = evidence.clone();
    if let Some(finality) = identity.get_mut("finality").and_then(Value::as_object_mut) {
        finality.remove("checkpointNumber");
        finality.remove("checkpointHash");
    }
    canonical(&identity)
}

fn status(conn: &Connection) -> Result<RecoveryStatus> {
    let c = control(conn)?;
    fn count(c: &Connection, sql: &str) -> Result<u64> {
        let n: i64 = c.query_row(sql, [], |r| r.get(0))?;
        n.try_into().map_err(|_| RecoveryError::Storage)
    }
    let projection_pending = projection::load(conn, &c)?.is_some();
    Ok(RecoveryStatus {
        schema: c.schema,
        projection_pending,
        deployment: c.deployment,
        epoch: c.epoch,
        checkpoint: c.checkpoint,
        namespace: c.namespace,
        halted: c.halted,
        halt_reason: c.reason,
        admissions: count(conn, "SELECT count(*) FROM admissions")?,
        attempts: count(conn, "SELECT count(*) FROM attempts")?,
        terminal: count(
            conn,
            "SELECT count(*) FROM admissions WHERE state='terminal'",
        )?,
        quarantined: count(
            conn,
            "SELECT count(*) FROM admissions WHERE state='quarantined'",
        )?,
    })
}
/// Canonical keys prevent textual aliases from evading wallet+nonce uniqueness.
fn validate_replay_key(kind: &str, key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 2048 {
        return Err(RecoveryError::Invalid("invalid replay key"));
    }
    if matches!(kind, "eoa" | "eoa_noop") {
        let fields: Vec<_> = key.split(':').collect();
        if fields.len() != 4 || fields[0] != "evm" {
            return Err(RecoveryError::Invalid(
                "EOA replay key must be evm:chain:lowercase-address:nonce",
            ));
        }
        let chain = fields[1]
            .parse::<u64>()
            .map_err(|_| RecoveryError::Invalid("invalid replay chain"))?;
        let nonce = fields[3]
            .parse::<u64>()
            .map_err(|_| RecoveryError::Invalid("invalid replay nonce"))?;
        let address = fields[2];
        if fields[1] != chain.to_string()
            || fields[3] != nonce.to_string()
            || address.len() != 42
            || !address.starts_with("0x")
            || !address[2..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(RecoveryError::Invalid("EOA replay key is not canonical"));
        }
    } else if !key.starts_with(&format!("{kind}:")) {
        return Err(RecoveryError::Invalid("replay key kind mismatch"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "recovery/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "recovery/benchmark.rs"]
mod benchmark;

#[path = "recovery/export.rs"]
mod export;

#[cfg(test)]
#[path = "recovery/history_index_tests.rs"]
mod history_index_tests;

#[cfg(test)]
mod cancellation_tests;

#[cfg(test)]
#[path = "recovery/projection_tests.rs"]
mod projection_tests;
