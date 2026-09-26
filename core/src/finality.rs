//! Finality evidence from a qualified RPC. This is not a consensus/light-client
//! proof. Callers must durably fence checkpoint advancement and terminal effects.

use alloy::{
    consensus::TxReceipt,
    primitives::B256,
    providers::Provider,
    rpc::types::{BlockNumberOrTag, TransactionReceipt},
};
use serde::{Deserialize, Serialize};

use crate::{
    chain::Chain,
    error::{AlloyRpcErrorToEngineError, EngineError},
};

/// Depth is an explicit probabilistic policy, never a fallback for finalized.
/// `confirmations` counts blocks *after* the receipt's block.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum FinalityPolicy {
    #[default]
    Finalized,
    Depth {
        confirmations: u64,
    },
}

impl<'de> Deserialize<'de> for FinalityPolicy {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // A unit variant silently ignores extra fields even with Serde's
        // deny_unknown_fields. An empty struct variant rejects ambiguous config.
        #[derive(Deserialize)]
        #[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
        enum Policy {
            Finalized {},
            Depth {
                #[serde(deserialize_with = "deserialize_confirmations")]
                confirmations: u64,
            },
        }
        Ok(match Policy::deserialize(deserializer)? {
            Policy::Finalized {} => Self::Finalized,
            Policy::Depth { confirmations } => Self::Depth { confirmations },
        })
    }
}

// config::Environment represents nested enum fields as strings unless global
// coercion is enabled. Accept decimal strings without broadening the policy to
// floats, negatives, booleans, or overflow; serialization remains numeric.
fn deserialize_confirmations<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumberOrString {
        Number(u64),
        Text(String),
    }
    match NumberOrString::deserialize(deserializer)? {
        NumberOrString::Number(value) => Ok(value),
        NumberOrString::Text(value)
            if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            value.parse().map_err(serde::de::Error::custom)
        }
        NumberOrString::Text(_) => Err(serde::de::Error::custom(
            "Finality confirmations must be an unsigned decimal integer",
        )),
    }
}

impl FinalityPolicy {
    pub fn validate(self, chain_id: u64) -> Result<(), EngineError> {
        if matches!(self, Self::Depth { confirmations: 0 }) && chain_id != 31337 {
            return Err(EngineError::RpcConfigError {
                message: "Finality depth must be positive; zero is restricted to local chain 31337"
                    .into(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FinalityEvidence {
    pub block_number: u64,
    pub block_hash: B256,
    pub checkpoint_number: u64,
    pub checkpoint_hash: B256,
    /// A depth policy must be displayed as probabilistic, not consensus finality.
    pub policy: FinalityPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalityAssessment {
    Pending {
        canonical: bool,
    },
    Orphaned,
    /// The configured policy is satisfied; inspect `policy` before describing
    /// the guarantee. Explicit depth policies remain probabilistic.
    Finalized(FinalityEvidence),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointContinuity {
    Consistent,
    Unavailable,
    Conflict,
}

/// Check a retained checkpoint against the provider's current canonical view.
/// A null/failed lookup is not proof of a reorg. The caller must persist/fence
/// monotonic checkpoint progress separately and quarantine positive conflicts.
pub async fn assess_checkpoint_continuity(
    chain: &impl Chain,
    previous: &FinalityEvidence,
) -> Result<CheckpointContinuity, EngineError> {
    let Some(block) = chain
        .provider()
        .get_block_by_number(BlockNumberOrTag::Number(previous.checkpoint_number))
        .await
        .map_err(|error| error.to_engine_error(chain))?
    else {
        return Ok(CheckpointContinuity::Unavailable);
    };
    if block.header.number != previous.checkpoint_number {
        return Err(inconsistent(
            "RPC returned the wrong checkpoint block number",
        ));
    }
    Ok(if block.header.hash == previous.checkpoint_hash {
        CheckpointContinuity::Consistent
    } else {
        CheckpointContinuity::Conflict
    })
}

fn inconsistent(message: &'static str) -> EngineError {
    EngineError::InternalError {
        message: message.into(),
    }
}

/// Validate a receipt against canonical block identity and a covering policy
/// checkpoint. Both success and reverted receipts pass the same finality gate.
/// Null blocks are unknown, not evidence authorizing resubmission/new identity.
pub async fn assess_receipt_finality<R: TxReceipt>(
    chain: &impl Chain,
    expected_hash: B256,
    receipt: &TransactionReceipt<R>,
) -> Result<FinalityAssessment, EngineError> {
    let policy = chain.finality_policy();
    policy.validate(chain.chain_id())?;
    if receipt.transaction_hash != expected_hash {
        return Err(inconsistent(
            "Receipt transaction hash does not match stored attempt",
        ));
    }
    // Pre-EIP-658 post-state roots must not be coerced to successful execution.
    if receipt.inner.status_or_post_state().as_eip658().is_none() {
        return Err(inconsistent("Receipt does not contain an execution status"));
    }
    let (Some(block_number), Some(block_hash)) = (receipt.block_number, receipt.block_hash) else {
        return Ok(FinalityAssessment::Pending { canonical: false });
    };
    let provider = chain.provider();
    let Some(block) = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number))
        .await
        .map_err(|error| error.to_engine_error(chain))?
    else {
        return Ok(FinalityAssessment::Pending { canonical: false });
    };
    if block.header.number != block_number {
        return Err(inconsistent(
            "RPC returned the wrong canonical block number",
        ));
    }
    if block.header.hash != block_hash {
        return Ok(FinalityAssessment::Orphaned);
    }

    let tag = match policy {
        FinalityPolicy::Finalized => BlockNumberOrTag::Finalized,
        FinalityPolicy::Depth { .. } => BlockNumberOrTag::Latest,
    };
    let Some(checkpoint) = provider
        .get_block_by_number(tag)
        .await
        .map_err(|error| error.to_engine_error(chain))?
    else {
        return Ok(FinalityAssessment::Pending { canonical: true });
    };
    let required_height = match policy {
        FinalityPolicy::Finalized => block_number,
        FinalityPolicy::Depth { confirmations } => {
            let Some(required) = block_number.checked_add(confirmations) else {
                return Err(inconsistent(
                    "Finality depth exceeds representable block height",
                ));
            };
            required
        }
    };
    if checkpoint.header.number < required_height {
        return Ok(FinalityAssessment::Pending { canonical: true });
    }

    // The receipt can be orphaned between the first read and the head read.
    let Some(rechecked_block) = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_number))
        .await
        .map_err(|error| error.to_engine_error(chain))?
    else {
        return Ok(FinalityAssessment::Pending { canonical: false });
    };
    if rechecked_block.header.number != block_number {
        return Err(inconsistent(
            "RPC returned the wrong canonical block number",
        ));
    }
    if rechecked_block.header.hash != block_hash {
        return Ok(FinalityAssessment::Orphaned);
    }
    // Re-read the selected checkpoint by number rather than accepting height
    // alone. A mixed-backend/fork response must not silently complete the job.
    let Some(rechecked_checkpoint) = provider
        .get_block_by_number(BlockNumberOrTag::Number(checkpoint.header.number))
        .await
        .map_err(|error| error.to_engine_error(chain))?
    else {
        return Ok(FinalityAssessment::Pending { canonical: true });
    };
    if rechecked_checkpoint.header.number != checkpoint.header.number
        || rechecked_checkpoint.header.hash != checkpoint.header.hash
        || (checkpoint.header.number == block_number && checkpoint.header.hash != block_hash)
    {
        return Err(inconsistent(
            "Finality checkpoint changed during receipt reconciliation",
        ));
    }

    Ok(FinalityAssessment::Finalized(FinalityEvidence {
        block_number,
        block_hash,
        checkpoint_number: checkpoint.header.number,
        checkpoint_hash: checkpoint.header.hash,
        policy,
    }))
}

#[cfg(test)]
#[path = "finality_tests.rs"]
mod tests;
