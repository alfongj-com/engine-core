//! Negative terminal evidence before any wire has been authorized. This does
//! not replace the independent receipt/finality checks for broadcast attempts.
use super::*;

const WITNESS_KIND: &str = "eoa_preparation_failure";

fn exact_record(conn: &Connection, id: &str, payload: &Value) -> Result<AdmissionRecord> {
    let record = lookup(conn, id)?.ok_or(RecoveryError::RecoveryRequired("untracked failure"))?;
    if record.kind != "eoa"
        || record.fingerprint != admission_fingerprint("eoa", payload)?
        || canonical(&record.payload)? != canonical(payload)?
    {
        return Err(RecoveryError::Conflict);
    }
    Ok(record)
}

fn attempted(conn: &Connection, id: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM attempts WHERE id=?)",
        [id],
        |row| row.get(0),
    )?)
}

fn retained_failure(conn: &Connection, record: &AdmissionRecord) -> Result<Option<Value>> {
    if record.state != AdmissionState::Terminal
        || record.replay_key.is_some()
        || attempted(conn, &record.id)?
    {
        return Ok(None);
    }
    let encoded: String = conn.query_row(
        "SELECT evidence FROM terminal_evidence WHERE id=? ORDER BY sequence LIMIT 1",
        [&record.id],
        |row| row.get(0),
    )?;
    let evidence: Value = serde_json::from_str(&encoded).map_err(|_| RecoveryError::Storage)?;
    if evidence["kind"] != WITNESS_KIND || !evidence["error"].is_object() {
        return Err(RecoveryError::Storage);
    }
    Ok(Some(evidence["error"].clone()))
}

impl RecoveryJournal {
    /// Commit a trusted executor's permanent preparation failure before its
    /// Redis status/webhook. None means an attempt or another terminal state won:
    /// the caller must retain reconciliation, never publish this failure.
    /// Repeats return the first failure unchanged, including after a crash.
    pub async fn record_eoa_preparation_failure(
        self: &Arc<Self>,
        id: &str,
        payload: &Value,
        error: Value,
    ) -> Result<Option<Value>> {
        identity("eoa", id)?;
        if !error.is_object() {
            return Err(RecoveryError::Invalid(
                "preparation failure needs structured evidence",
            ));
        }
        let evidence = canonical(&serde_json::json!({"kind":WITNESS_KIND,"error":error}))?;
        let (id, payload) = (id.to_owned(), payload.clone());
        self.run_owned(move |journal| async move {
            let _guard = journal.serial_guard("preparation_failure").await;
            journal.healthy_locked().await?;
            let result = journal
                .db(move |conn| {
                    let tx = conn.transaction()?;
                    let record = exact_record(&tx, &id, &payload)?;
                    if let Some(error) = retained_failure(&tx, &record)? {
                        return Ok((Some(error), None));
                    }
                    if record.state != AdmissionState::Admitted
                        || record.replay_key.is_some()
                        || attempted(&tx, &id)?
                    {
                        return Ok((None, None));
                    }
                    tx.execute(
                        "INSERT INTO terminal_evidence(id,evidence) VALUES(?,?)",
                        params![id, evidence],
                    )?;
                    tx.execute("UPDATE admissions SET state='terminal' WHERE id=?", [&id])?;
                    let old = advance(&tx)?;
                    commit_transaction(tx)?;
                    Ok((Some(error), Some(old)))
                })
                .await;
            let (error, old) = journal.storage_result(result).await?;
            if let Some(old) = old {
                journal.mirror(old).await?;
            }
            Ok(error)
        })
        .await
    }

    /// Retry only the matching no-attempt failure projection. A chain receipt,
    /// quarantined attempt, corrupt witness or unhealthy journal is not this proof.
    pub async fn eoa_preparation_failure(
        self: &Arc<Self>,
        id: &str,
        payload: &Value,
    ) -> Result<Option<Value>> {
        identity("eoa", id)?;
        let (id, payload) = (id.to_owned(), payload.clone());
        self.run_owned(move |journal| async move {
            let _guard = journal.serial_guard("preparation_failure_projection").await;
            journal.healthy_locked().await?;
            let result = journal
                .db(move |conn| {
                    let record = exact_record(conn, &id, &payload)?;
                    retained_failure(conn, &record)
                })
                .await;
            journal.storage_result(result).await
        })
        .await
    }
}

#[cfg(test)]
#[path = "preparation_failure_tests.rs"]
mod tests;
