//! Final settlement shared by all EVM executors. Redis is a working queue;
//! checkpoint continuity and terminal evidence live in the external journal.
use alloy::{consensus::TxReceipt, primitives::B256, rpc::types::TransactionReceipt};
use engine_core::{
    chain::Chain,
    error::EngineError,
    finality::{self, CheckpointContinuity, FinalityAssessment, FinalityEvidence},
    recovery,
};
use serde_json::json;

fn unavailable(message: impl std::fmt::Display) -> EngineError {
    EngineError::InternalError {
        message: message.to_string(),
    }
}

/// Active polling checks previously accepted history even when the new receipt
/// is absent/provisional/orphaned. Absence is unknown; a positive hash conflict
/// permanently fences new emissions on this chain.
pub(crate) async fn check_continuity(
    chain: &impl Chain,
) -> Result<Option<FinalityEvidence>, EngineError> {
    let Some(journal) = recovery::global() else {
        return Ok(None);
    };
    let previous = journal
        .load_checkpoint(chain.chain_id())
        .await
        .map_err(unavailable)?;
    if let Some(ref previous) = previous {
        if previous.policy != chain.finality_policy() {
            journal
                .halt_chain(
                    chain.chain_id(),
                    "Finality policy changed after accepted checkpoint",
                )
                .await
                .map_err(unavailable)?;
            return Err(unavailable(
                "Finality policy changed after accepted checkpoint; chain halted",
            ));
        }
        match finality::assess_checkpoint_continuity(chain, previous).await? {
            CheckpointContinuity::Unavailable => {
                return Err(unavailable("Prior finality checkpoint is unavailable"));
            }
            CheckpointContinuity::Conflict => {
                journal
                    .halt_chain(chain.chain_id(), "Previously settled checkpoint changed")
                    .await
                    .map_err(unavailable)?;
                return Err(unavailable(
                    "Previously settled checkpoint changed; chain halted",
                ));
            }
            CheckpointContinuity::Consistent => {}
        }
    }
    Ok(previous)
}

pub(crate) async fn assess<R: TxReceipt>(
    chain: &impl Chain,
    hash: B256,
    receipt: &TransactionReceipt<R>,
) -> Result<FinalityAssessment, EngineError> {
    // Do not postpone deep-reorg detection until a new receipt reaches finality.
    let mut previous = check_continuity(chain).await?;
    let assessment = finality::assess_receipt_finality(chain, hash, receipt).await?;
    let FinalityAssessment::Finalized(ref evidence) = assessment else {
        return Ok(assessment);
    };
    let Some(journal) = recovery::global() else {
        return Ok(assessment);
    };
    // CAS avoids accepting a checkpoint that raced another worker's observation.
    // A race is transient; never spin indefinitely or downgrade the proof.
    for retry in 0..3 {
        if retry > 0 {
            previous = check_continuity(chain).await?;
        }
        if previous
            .as_ref()
            .is_some_and(|old| evidence.checkpoint_number < old.checkpoint_number)
        {
            return Ok(FinalityAssessment::Pending { canonical: true });
        }
        if journal
            .commit_checkpoint(chain.chain_id(), previous.clone(), evidence.clone())
            .await
            .map_err(unavailable)?
        {
            return Ok(assessment);
        }
    }
    Ok(FinalityAssessment::Pending { canonical: true })
}

pub(crate) async fn record_evm_terminal(
    kind: &str,
    id: &str,
    chain_id: u64,
    hash: B256,
    succeeded: bool,
    evidence: &FinalityEvidence,
) -> Result<(), EngineError> {
    if let Some(journal) = recovery::global() {
        journal
            .check_chain_healthy(chain_id)
            .await
            .map_err(unavailable)?;
        journal
            .record_terminal(
                kind,
                id,
                json!({
                    "chainId": chain_id, "transactionHash": hash,
                    "outcome": if succeeded {"success"} else {"reverted"}, "finality": evidence,
                }),
            )
            .await
            .map_err(unavailable)?;
    }
    Ok(())
}

pub(crate) async fn record_solana_terminal(
    id: &str,
    chain_id: &str,
    signature: &str,
    slot: u64,
    succeeded: bool,
) -> Result<(), EngineError> {
    if let Some(journal) = recovery::global() {
        journal.record_terminal("solana", id, json!({
            "chainId": chain_id, "signature": signature, "slot": slot,
            "commitment": "finalized", "outcome": if succeeded {"success"} else {"reverted"},
        })).await.map_err(unavailable)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "finality_tests.rs"]
mod tests;

/// Resume a terminal intent whose Redis projection survived at the borrowed stage.
/// The old borrowed hash may be a superseded fee bump; use the durable winner hash
/// with the same reserved nonce, and let normal finality polling verify it again.
pub(crate) async fn terminal_eoa_projection(
    journal: &recovery::RecoveryJournal,
    request: &crate::eoa::EoaTransactionRequest,
    nonce: u64,
) -> Result<Option<B256>, EngineError> {
    let payload = serde_json::to_value(request).map_err(unavailable)?;
    validate_reconciliation_payload(journal, "eoa", &request.transaction_id, &payload).await?;
    let Some(admission) = journal
        .admission("eoa", &request.transaction_id)
        .await
        .map_err(unavailable)?
    else {
        return Err(unavailable("Missing durable admission"));
    };
    if admission.state != recovery::AdmissionState::Terminal {
        return Ok(None);
    }
    let expected = format!("evm:{}:{:#x}:{nonce}", request.chain_id, request.from);
    if admission.replay_key.as_deref() != Some(expected.as_str()) {
        return Err(unavailable(
            "Terminal intent has a different nonce reservation",
        ));
    }
    let proof = journal
        .terminal_evidence("eoa", &request.transaction_id)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| unavailable("Terminal intent has no durable proof"))?;
    if proof["chainId"].as_u64() != Some(request.chain_id) {
        return Err(unavailable("Terminal proof belongs to another chain"));
    }
    let hash = proof["transactionHash"]
        .as_str()
        .ok_or_else(|| unavailable("Missing terminal transaction hash"))?
        .parse()
        .map_err(|_| unavailable("Invalid terminal transaction hash"))?;
    Ok(Some(hash))
}

/// Exact admission identity is required for reconciliation too, but a terminal
/// journal entry must remain readable until its Redis completion commits.
pub(crate) async fn validate_reconciliation_payload(
    journal: &recovery::RecoveryJournal,
    kind: &str,
    id: &str,
    payload: &serde_json::Value,
) -> Result<(), EngineError> {
    journal.ensure_healthy().await.map_err(unavailable)?;
    let admission = journal
        .admission(kind, id)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| unavailable("Missing durable admission"))?;
    if admission.payload != *payload || admission.state == recovery::AdmissionState::Quarantined {
        return Err(unavailable(
            "Reconciliation payload is changed or quarantined",
        ));
    }
    Ok(())
}
