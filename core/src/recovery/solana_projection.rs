//! Restore only retained Solana wire; bound all later recovery send permissions
//! in the independent journal so deleting Redis cannot reset their budget.
use super::*;

pub const MAX_SOLANA_RECOVERY_BROADCASTS: u32 = 20;
const RECOVERY_BROADCAST_INTERVAL_MS: u64 = 2_000;
pub(super) const TABLE_SQL: &str = "
    CREATE TABLE solana_recovery(
        id TEXT PRIMARY KEY REFERENCES admissions(id),
        attempt_digest TEXT NOT NULL,
        broadcasts INTEGER NOT NULL CHECK(broadcasts BETWEEN 0 AND 20),
        last_broadcast_at INTEGER NOT NULL CHECK(last_broadcast_at >= 0),
        CHECK((broadcasts=0 AND last_broadcast_at=0) OR (broadcasts>0 AND last_broadcast_at>0))
    );
";

/// Inventory only. Returning this record never grants permission to send it.
pub struct RetainedSolanaAttempt {
    pub replay_key: String,
    pub payload: Value,
    pub state: AdmissionState,
    pub recovery_broadcasts: u32,
    pub last_recovery_broadcast_at: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SolanaRecoveryBroadcast {
    Granted { broadcasts: u32, at_ms: u64 },
    Wait { retry_after_ms: u64 },
    Exhausted,
}

pub(super) fn counts(conn: &Connection, schema: i64) -> Result<(u64, u64)> {
    if schema < 3 {
        return Ok((0, 0));
    }
    let (intents, broadcasts): (i64, i64) = conn.query_row(
        "SELECT count(*),COALESCE(sum(broadcasts),0) FROM solana_recovery",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok((
        intents.try_into().map_err(|_| RecoveryError::Storage)?,
        broadcasts.try_into().map_err(|_| RecoveryError::Storage)?,
    ))
}

pub(super) fn validate_schema(conn: &Connection) -> Result<()> {
    if control(conn)?.schema >= 3 {
        // Check columns even for an empty table. A missing authority table must
        // never be recreated with zeroed budgets on ordinary startup.
        conn.prepare(
            "SELECT id,attempt_digest,broadcasts,last_broadcast_at FROM solana_recovery LIMIT 0",
        )?;
    }
    Ok(())
}

pub(super) fn recovery_mode(conn: &Connection, id: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM solana_recovery WHERE id=?)",
        [id],
        |row| row.get(0),
    )?)
}

struct Retained {
    public: RetainedSolanaAttempt,
    digest: String,
    recovery_mode: bool,
}

fn retained(conn: &Connection, id: &str, expected_payload: &Value) -> Result<Option<Retained>> {
    let admission = lookup(conn, id)?.ok_or(RecoveryError::RecoveryRequired(
        "untracked Solana projection recovery",
    ))?;
    if admission.kind != "solana"
        || admission.fingerprint != admission_fingerprint("solana", expected_payload)?
        || canonical(&admission.payload)? != canonical(expected_payload)?
    {
        return Err(RecoveryError::Conflict);
    }
    if admission.state == AdmissionState::Quarantined {
        return Err(RecoveryError::RecoveryRequired(
            "transaction is quarantined",
        ));
    }
    let mode: Option<(String, u32, i64)> = conn
        .query_row(
            "SELECT attempt_digest,broadcasts,last_broadcast_at FROM solana_recovery WHERE id=?",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let mut statement = conn.prepare(
        "SELECT replay_key,digest,payload FROM attempts WHERE id=? ORDER BY sequence LIMIT 2",
    )?;
    let mut rows = statement.query([id])?;
    let Some(row) = rows.next()? else {
        if admission.replay_key.is_some()
            || mode.is_some()
            || admission.state != AdmissionState::Admitted
        {
            return Err(RecoveryError::Storage);
        }
        return Ok(None);
    };
    let key: String = row.get(0)?;
    let digest: String = row.get(1)?;
    let encoded: String = row.get(2)?;
    if rows.next()?.is_some() || admission.replay_key.as_deref() != Some(key.as_str()) {
        return Err(RecoveryError::Storage);
    }
    let payload: Value = serde_json::from_str(&encoded).map_err(|_| RecoveryError::Storage)?;
    if canonical(&payload)? != encoded
        || format!("{:x}", Sha256::digest(encoded.as_bytes())) != digest
    {
        return Err(RecoveryError::Storage);
    }
    let signature = payload
        .get("signature")
        .and_then(Value::as_str)
        .ok_or(RecoveryError::Storage)?;
    let signature: solana_sdk::signature::Signature =
        signature.parse().map_err(|_| RecoveryError::Storage)?;
    let chain = payload
        .get("chainId")
        .and_then(Value::as_str)
        .ok_or(RecoveryError::Storage)?;
    if key != format!("solana:{chain}:{signature}") {
        return Err(RecoveryError::Storage);
    }
    let attempt = payload.get("attempt").ok_or(RecoveryError::Storage)?;
    if attempt.get("signature").and_then(Value::as_str) != Some(signature.to_string().as_str()) {
        return Err(RecoveryError::Storage);
    }
    if attempt
        .get("signed_transaction")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
    {
        return Err(RecoveryError::RecoveryRequired(
            "retained Solana attempt has no signed wire",
        ));
    }
    let (broadcasts, last_broadcast_at) = match &mode {
        None => (0, 0),
        Some((stored_digest, broadcasts, last)) => {
            if stored_digest != &digest
                || *broadcasts > MAX_SOLANA_RECOVERY_BROADCASTS
                || *last < 0
                || (*broadcasts == 0) != (*last == 0)
            {
                return Err(RecoveryError::Storage);
            }
            (
                *broadcasts,
                (*last).try_into().map_err(|_| RecoveryError::Storage)?,
            )
        }
    };
    Ok(Some(Retained {
        public: RetainedSolanaAttempt {
            replay_key: key,
            payload,
            state: admission.state,
            recovery_broadcasts: broadcasts,
            last_recovery_broadcast_at: last_broadcast_at,
        },
        digest,
        recovery_mode: mode.is_some(),
    }))
}

impl RecoveryJournal {
    /// Validate retained intent and activate permanent recovery mode before
    /// rebuilding Redis. The normal send path cannot bypass its durable budget.
    pub async fn prepare_solana_restore(
        self: &Arc<Self>,
        id: &str,
        expected_payload: &Value,
    ) -> Result<Option<RetainedSolanaAttempt>> {
        identity("solana", id)?;
        let (id, payload) = (id.to_owned(), expected_payload.clone());
        self.run_owned(move |journal| async move {
            let _guard = journal.serial_guard("solana_restore").await;
            journal.healthy_locked().await?;
            let result = journal
                .db(move |conn| {
                    let tx = conn.transaction()?;
                    let Some(stored) = retained(&tx, &id, &payload)? else {
                        return Ok((None, None));
                    };
                    let old = if stored.recovery_mode {
                        None
                    } else {
                        tx.execute(
                            "INSERT INTO solana_recovery VALUES(?,?,0,0)",
                            params![id, stored.digest],
                        )?;
                        let old = advance(&tx)?;
                        commit_transaction(tx)?;
                        Some(old)
                    };
                    Ok((Some(stored.public), old))
                })
                .await;
            let (record, old) = journal.storage_result(result).await?;
            if let Some(old) = old {
                journal.mirror(old).await?;
            }
            Ok(record)
        })
        .await
    }

    /// Only Granted is a new send permission. Every granted count/time is FULL
    /// committed and acknowledged before return; uncertainty burns the allowance.
    pub async fn before_solana_recovery_broadcast(
        self: &Arc<Self>,
        id: &str,
        expected_payload: &Value,
        expected_attempt: &Value,
    ) -> Result<SolanaRecoveryBroadcast> {
        identity("solana", id)?;
        let (id, payload, attempt) = (
            id.to_owned(),
            expected_payload.clone(),
            expected_attempt.clone(),
        );
        self.run_owned(move |journal| async move {
            let _guard = journal.serial_guard("solana_recovery_broadcast").await;
            journal.healthy_locked().await?;
            let result = journal
                .db(move |conn| {
                    let tx = conn.transaction()?;
                    let stored = retained(&tx, &id, &payload)?.ok_or(
                        RecoveryError::RecoveryRequired("missing retained Solana attempt"),
                    )?;
                    if !stored.recovery_mode || stored.public.state != AdmissionState::Admitted {
                        return Err(RecoveryError::RecoveryRequired(
                            "Solana recovery broadcast is not allowed",
                        ));
                    }
                    if canonical(&stored.public.payload)? != canonical(&attempt)? {
                        return Err(RecoveryError::Conflict);
                    }
                    let now: u64 = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|_| RecoveryError::Storage)?
                        .as_millis()
                        .try_into()
                        .map_err(|_| RecoveryError::Storage)?;
                    let result = claim(&tx, &id, &stored.public, now)?;
                    if !matches!(result, SolanaRecoveryBroadcast::Granted { .. }) {
                        return Ok((result, None));
                    }
                    let old = advance(&tx)?;
                    commit_transaction(tx)?;
                    Ok((result, Some(old)))
                })
                .await;
            let (permission, old) = journal.storage_result(result).await?;
            if let Some(old) = old {
                journal.mirror(old).await?;
            }
            Ok(permission)
        })
        .await
    }
}

fn claim(
    conn: &Connection,
    id: &str,
    stored: &RetainedSolanaAttempt,
    now: u64,
) -> Result<SolanaRecoveryBroadcast> {
    if stored.recovery_broadcasts >= MAX_SOLANA_RECOVERY_BROADCASTS {
        return Ok(SolanaRecoveryBroadcast::Exhausted);
    }
    let elapsed = now.saturating_sub(stored.last_recovery_broadcast_at);
    if stored.last_recovery_broadcast_at != 0 && elapsed < RECOVERY_BROADCAST_INTERVAL_MS {
        return Ok(SolanaRecoveryBroadcast::Wait {
            retry_after_ms: RECOVERY_BROADCAST_INTERVAL_MS - elapsed,
        });
    }
    if now == 0 || now > i64::MAX as u64 {
        return Err(RecoveryError::Storage);
    }
    let broadcasts = stored.recovery_broadcasts + 1;
    let now_sql: i64 = now.try_into().map_err(|_| RecoveryError::Storage)?;
    let previous_sql: i64 = stored
        .last_recovery_broadcast_at
        .try_into()
        .map_err(|_| RecoveryError::Storage)?;
    let changed = conn.execute(
        "UPDATE solana_recovery SET broadcasts=?,last_broadcast_at=? WHERE id=? AND broadcasts=? AND last_broadcast_at=?",
        params![broadcasts, now_sql, id, stored.recovery_broadcasts, previous_sql],
    )?;
    if changed != 1 {
        return Err(RecoveryError::Storage);
    }
    Ok(SolanaRecoveryBroadcast::Granted {
        broadcasts,
        at_ms: now,
    })
}

#[cfg(test)]
#[path = "solana_projection_tests.rs"]
mod tests;
