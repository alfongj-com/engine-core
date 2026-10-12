//! Durable authorization at the last boundary before transaction broadcast.

use crate::eoa::{EoaTransactionRequest, worker::error::EoaExecutorWorkerError};
use alloy::{
    consensus::{Signed, Transaction, TxEnvelope, TypedTransaction},
    eips::eip2718::Encodable2718,
};
use engine_core::{error::EngineError, recovery};
use serde::Serialize;
use serde_json::json;
use std::time::Duration;
use twmq::job::{JobError, RequeuePosition};

pub(crate) async fn validate<T: Serialize>(
    kind: &str,
    id: &str,
    payload: &T,
) -> Result<(), EngineError> {
    if let Some(journal) = recovery::global() {
        let value =
            serde_json::to_value(payload).map_err(|_| error("Cannot encode recovery payload"))?;
        journal
            .validate_payload(kind, id, &value)
            .await
            .map_err(from_journal_error)?;
    }
    Ok(())
}

pub(crate) fn error(message: &str) -> EngineError {
    EngineError::InternalError {
        message: message.into(),
    }
}

/// Preserve the one recoverable journal error through executor adapters. A
/// pending publication is not broadcast permission: retry all original gates.
pub(crate) fn from_journal_error(error: recovery::RecoveryError) -> EngineError {
    match error {
        recovery::RecoveryError::PublicationPending => EngineError::RecoveryPending {
            message: error.to_string(),
        },
        _ => self::error(&error.to_string()),
    }
}

/// Keep the executor's existing delay for every other error. Only the typed
/// publication state may bypass a long reconciliation polling delay.
pub(crate) fn nack<T>(
    error: EngineError,
    fallback_delay: Duration,
    wrap: impl FnOnce(EngineError) -> T,
) -> JobError<T> {
    let delay = if matches!(error, EngineError::RecoveryPending { .. }) {
        Duration::from_secs(2)
    } else {
        fallback_delay
    };
    JobError::Nack {
        error: wrap(error),
        delay: Some(delay),
        position: RequeuePosition::Last,
    }
}

pub(crate) fn eoa_error(error: EngineError) -> EoaExecutorWorkerError {
    match error {
        EngineError::RecoveryPending { message } => {
            EoaExecutorWorkerError::RecoveryPending { message }
        }
        _ => eoa_required(error),
    }
}

pub(crate) fn eoa_journal_error(error: recovery::RecoveryError) -> EoaExecutorWorkerError {
    eoa_error(from_journal_error(error))
}

pub(crate) fn eoa_required(error: impl std::fmt::Display) -> EoaExecutorWorkerError {
    EoaExecutorWorkerError::RecoveryRequired {
        message: error.to_string(),
    }
}

pub(crate) async fn validate_eoa_nonce(
    request: &EoaTransactionRequest,
    nonce: u64,
) -> Result<(), EoaExecutorWorkerError> {
    if let Some(journal) = recovery::global() {
        validate("eoa", &request.transaction_id, request)
            .await
            .map_err(eoa_error)?;
        let key = format!("evm:{}:{:#x}:{nonce}", request.chain_id, request.from);
        journal
            .validate_replay_binding("eoa", &request.transaction_id, &key)
            .await
            .map_err(eoa_journal_error)?;
    }
    Ok(())
}

pub(crate) async fn before_eoa(
    request: &EoaTransactionRequest,
    tx: &Signed<TypedTransaction>,
) -> Result<(), EoaExecutorWorkerError> {
    let Some(journal) = recovery::global() else {
        return Ok(());
    };
    validate("eoa", &request.transaction_id, request)
        .await
        .map_err(eoa_error)?;
    validate_eoa_wire(request, tx)?;
    let transaction = tx.tx();
    let envelope: TxEnvelope = tx.clone().into();
    let replay_key = format!(
        "evm:{}:{:#x}:{}",
        request.chain_id,
        request.from,
        transaction.nonce()
    );
    journal
        .before_broadcast(
            "eoa",
            &request.transaction_id,
            &replay_key,
            json!({
                "transactionHash": tx.hash().to_string(), "chainId": request.chain_id,
                "sender": request.from, "nonce": transaction.nonce(),
                "signedTransaction": format!("0x{}", hex::encode(envelope.encoded_2718())),
            }),
        )
        .await
        .map_err(eoa_journal_error)
}

fn validate_eoa_wire(
    request: &EoaTransactionRequest,
    tx: &Signed<TypedTransaction>,
) -> Result<(), EoaExecutorWorkerError> {
    validate_signed_hash(tx)?;
    let transaction = tx.tx();
    let expected_authorizations = match &request.transaction_type_data {
        Some(engine_core::transaction::TransactionTypeData::Eip7702(data)) => {
            data.authorization_list.as_deref().unwrap_or_default()
        }
        _ => &[],
    };
    if transaction.chain_id() != Some(request.chain_id)
        || transaction.to() != request.to
        || transaction.value() != request.value
        || transaction.input() != &request.data
        || transaction.authorization_list().unwrap_or_default() != expected_authorizations
        || tx
            .signature()
            .recover_address_from_prehash(&tx.signature_hash())
            .ok()
            != Some(request.from)
    {
        return Err(eoa_required(
            "Signed transaction does not match its durable admission",
        ));
    }
    Ok(())
}

pub(crate) async fn reserve_noop(
    chain_id: u64,
    sender: alloy::primitives::Address,
    nonce: u64,
) -> Result<String, EoaExecutorWorkerError> {
    let id = format!("__engine_noop:{chain_id}:{sender:#x}:{nonce}");
    if let Some(journal) = recovery::global() {
        let payload = json!({"chainId":chain_id, "sender":sender, "nonce":nonce});
        let fingerprint =
            recovery::admission_fingerprint("eoa_noop", &payload).map_err(eoa_journal_error)?;
        let reserved = journal
            .reserve_admission("eoa_noop", &id, &fingerprint, payload)
            .await
            .map_err(eoa_journal_error)?;
        if reserved.terminal {
            return Err(eoa_required("Finalized NOOP cannot be broadcast again"));
        }
        journal
            .validate_replay_binding(
                "eoa_noop",
                &id,
                &format!("evm:{chain_id}:{sender:#x}:{nonce}"),
            )
            .await
            .map_err(eoa_journal_error)?;
    }
    Ok(id)
}

pub(crate) async fn before_noop(
    id: &str,
    chain_id: u64,
    sender: alloy::primitives::Address,
    tx: &Signed<TypedTransaction>,
) -> Result<(), EoaExecutorWorkerError> {
    let Some(journal) = recovery::global() else {
        return Ok(());
    };
    validate_noop_wire(chain_id, sender, tx)?;
    let expected_id = format!("__engine_noop:{chain_id}:{sender:#x}:{}", tx.tx().nonce());
    if id != expected_id {
        return Err(eoa_required("NOOP ID does not match its nonce reservation"));
    }
    journal
        .validate_payload(
            "eoa_noop",
            id,
            &json!({"chainId":chain_id,"sender":sender,"nonce":tx.tx().nonce()}),
        )
        .await
        .map_err(eoa_journal_error)?;
    let envelope: TxEnvelope = tx.clone().into();
    let replay_key = format!("evm:{chain_id}:{sender:#x}:{}", tx.tx().nonce());
    journal
        .before_broadcast(
            "eoa_noop",
            id,
            &replay_key,
            json!({
                "transactionHash": tx.hash().to_string(), "chainId":chain_id,
                "sender":sender, "nonce":tx.tx().nonce(),
                "signedTransaction":format!("0x{}", hex::encode(envelope.encoded_2718())),
            }),
        )
        .await
        .map_err(eoa_journal_error)
}

fn validate_noop_wire(
    chain_id: u64,
    sender: alloy::primitives::Address,
    tx: &Signed<TypedTransaction>,
) -> Result<(), EoaExecutorWorkerError> {
    validate_signed_hash(tx)?;
    let transaction = tx.tx();
    if transaction.chain_id() != Some(chain_id)
        || transaction.to() != Some(sender)
        || !transaction.value().is_zero()
        || !transaction.input().is_empty()
        || !transaction
            .authorization_list()
            .unwrap_or_default()
            .is_empty()
        || tx
            .signature()
            .recover_address_from_prehash(&tx.signature_hash())
            .ok()
            != Some(sender)
    {
        return Err(eoa_required(
            "Signed NOOP does not match its durable reservation",
        ));
    }
    Ok(())
}

/// Alloy's Signed deserializer accepts a cached hash without recomputing it.
/// The durable identity must refer to the bytes we actually authorize/broadcast.
fn validate_signed_hash(tx: &Signed<TypedTransaction>) -> Result<(), EoaExecutorWorkerError> {
    let envelope: TxEnvelope = tx.clone().into();
    if alloy::primitives::keccak256(envelope.encoded_2718()) != *tx.hash() {
        return Err(eoa_required(
            "Cached transaction hash does not match signed wire bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
