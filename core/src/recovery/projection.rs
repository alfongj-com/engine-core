//! One exact, durable Redis checkpoint transition. This is not a queue outbox:
//! repairing the marker never enqueues work or revives a crashed caller's permit.
use super::{Control, RecoveryError, Result, SCHEMA, commit_transaction, control};
use rusqlite::{Connection, OptionalExtension, params};

pub(super) const TABLE_SQL: &str = "
    CREATE TABLE projection_pending(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        previous_token TEXT NOT NULL,
        target_token TEXT NOT NULL,
        redis_key TEXT NOT NULL,
        run_id TEXT NOT NULL
    );
";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PendingUpdate {
    pub previous_token: String,
    pub target_token: String,
    pub redis_key: String,
    pub run_id: String,
}

impl PendingUpdate {
    fn between(previous: &Control, target: &Control) -> Self {
        Self {
            previous_token: previous.token(),
            target_token: target.token(),
            redis_key: target.key(),
            run_id: target.run_id.clone(),
        }
    }

    fn validate(&self, current: &Control) -> Result<()> {
        let mut predecessor = current.clone();
        let sequential = if current.checkpoint > 0 {
            predecessor.checkpoint -= 1;
            self.previous_token == predecessor.token()
        } else {
            false
        };
        let mut legacy = current.clone();
        legacy.schema = 1;
        let migration = self.previous_token == legacy.token();
        if current.schema != SCHEMA
            || self.target_token != current.token()
            || self.redis_key != current.key()
            || self.run_id != current.run_id
            || !(sequential || migration)
        {
            return Err(RecoveryError::Storage);
        }
        Ok(())
    }
}

pub(super) fn load(conn: &Connection, current: &Control) -> Result<Option<PendingUpdate>> {
    if current.schema == 1 {
        return Ok(None);
    }
    let pending = conn
        .query_row(
            "SELECT previous_token,target_token,redis_key,run_id FROM projection_pending WHERE singleton=1",
            [],
            |r| Ok(PendingUpdate {
                previous_token: r.get(0)?,
                target_token: r.get(1)?,
                redis_key: r.get(2)?,
                run_id: r.get(3)?,
            }),
        )
        .optional()?;
    if let Some(pending) = &pending {
        pending.validate(current)?;
    }
    Ok(pending)
}

/// Caller must be inside the same SQLite transaction as the authority mutation.
pub(super) fn record(conn: &Connection, previous: &Control, target: &Control) -> Result<()> {
    let pending = PendingUpdate::between(previous, target);
    pending.validate(target)?;
    conn.execute(
        "INSERT INTO projection_pending VALUES(1,?,?,?,?)",
        params![pending.previous_token, pending.target_token, pending.redis_key, pending.run_id],
    )?;
    Ok(())
}

/// Called only after observing the exact legacy marker under exclusive ownership.
/// The schema bump excludes old binaries even when Redis already has the target.
pub(super) fn migrate(conn: &mut Connection) -> Result<()> {
    let old = control(conn)?;
    if old.schema == SCHEMA {
        return Ok(());
    }
    if old.schema != 1 || old.halted {
        return Err(RecoveryError::RecoveryRequired("legacy journal is not healthy"));
    }
    let tx = conn.transaction()?;
    tx.execute_batch(TABLE_SQL)?;
    tx.execute("UPDATE control SET schema_version=? WHERE singleton=1", [SCHEMA])?;
    let new = control(&tx)?;
    record(&tx, &old, &new)?;
    commit_transaction(tx)
}

/// The final FULL commit is the permission boundary. A canceled caller never
/// reconstructs its permission; subsequent health checks may finish this write.
pub(super) fn acknowledge(conn: &mut Connection, expected: &PendingUpdate) -> Result<()> {
    let tx = conn.transaction()?;
    let current = control(&tx)?;
    expected.validate(&current)?;
    if current.halted || load(&tx, &current)?.as_ref() != Some(expected) {
        return Err(RecoveryError::RecoveryRequired("pending checkpoint changed before acknowledgement"));
    }
    let changed = tx.execute(
        "DELETE FROM projection_pending WHERE singleton=1 AND previous_token=? AND target_token=? AND redis_key=? AND run_id=?",
        params![expected.previous_token, expected.target_token, expected.redis_key, expected.run_id],
    )?;
    if changed != 1 {
        return Err(RecoveryError::Storage);
    }
    commit_transaction(tx)
}
