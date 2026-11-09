//! Offline journal administration and read-only inventory.
//! Offline mutations retain exclusive ownership for the complete operation.
#[cfg(test)]
use super::projection_tests;
use super::{
    RecoveryError, RecoveryJournal, RecoveryStatus, Result, SCHEMA, commit_transaction, connection,
    control, empty_namespace, export, marker_key, namespace_valid, observation, owner_lock,
    projection, redis_connection, solana_projection, status,
};
use rusqlite::params;
use std::path::Path;
use twmq::redis;

impl RecoveryJournal {
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
        let mut conn = connection(path, false)?;
        let c = control(&conn)?;
        solana_projection::validate_schema(&conn)?;
        let pending = projection::load(&conn, &c)?;
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
        #[cfg(test)]
        projection_tests::crash_cut("before_reattach_commit").await;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        if control(&tx)? != c || projection::load(&tx, &c)? != pending {
            return Err(RecoveryError::RecoveryRequired(
                "journal changed during reattach",
            ));
        }
        if let Some(pending) = &pending {
            // Only the exact target was observed above. Removing the witness and
            // adopting its new Redis process are one FULL authority commit.
            projection::remove_exact(&tx, pending)?;
        }
        tx.execute(
            "UPDATE control SET run_id=?,halted=0,reason=NULL WHERE singleton=1",
            [&run],
        )?;
        tx.execute(
            "INSERT INTO recovery_events(event) VALUES(?)",
            [serde_json::json!({
                "action": "reattach", "previous_run_id": c.run_id,
                "run_id": run, "acknowledged_pending": pending,
            })
            .to_string()],
        )?;
        commit_transaction(tx)?;
        #[cfg(test)]
        projection_tests::crash_cut("after_reattach_commit").await;
        let failure = match observation(&redis, &c.key()).await {
            Err(_) => Some("Redis continuity unavailable"),
            Ok((_, _, marker)) if marker.as_deref() != Some(c.token().as_str()) => {
                Some("Redis checkpoint mismatch")
            }
            Ok((_, role, _)) if role != "master" => Some("Redis primary role changed"),
            Ok((observed_run, _, _)) if observed_run != run => Some("Redis process changed"),
            Ok(_) => None,
        };
        if let Some(reason) = failure {
            // Preserve any prior cause; post-commit uncertainty never reports a
            // healthy reattach. Startup/send still perform fresh normal gates.
            conn.execute(
                "UPDATE control SET halted=1,reason=COALESCE(reason,?) WHERE singleton=1",
                [reason],
            )?;
            return Err(RecoveryError::RecoveryRequired(reason));
        }
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
        if old.schema < SCHEMA {
            conn.execute_batch(solana_projection::TABLE_SQL)?;
        } else {
            solana_projection::validate_schema(&conn)?;
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
