use std::time::Duration;

use alloy::{
    consensus::Transaction,
    primitives::U256,
    transports::{RpcError, TransportErrorKind},
};
use engine_core::{
    chain::Chain,
    error::{EngineError, RpcErrorKind},
};
use serde::{Deserialize, Serialize};
use thirdweb_core::iaw::IAWError;
use twmq::{
    UserCancellable,
    error::TwmqError,
    job::{JobError, RequeuePosition},
};

use crate::eoa::{
    store::{BorrowedTransaction, SubmissionResult, SubmissionResultType, TransactionStoreError},
    worker::EoaExecutorWorkerResult,
};

#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum EoaExecutorWorkerError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Store error: {message}")]
    StoreError {
        message: String,
        inner_error: TransactionStoreError,
    },

    #[error("Transaction not found: {transaction_id}")]
    TransactionNotFound { transaction_id: String },

    #[error("Transaction simulation failed: {message}")]
    TransactionSimulationFailed {
        message: String,
        inner_error: EngineError,
    },

    #[error("Transaction build failed: {message}")]
    TransactionBuildFailed { message: String },

    #[error("RPC error encountered during generic operation: {message}")]
    RpcError {
        message: String,
        inner_error: EngineError,
    },

    #[error("Error encountered when broadcasting transaction: {message}")]
    TransactionSendError {
        message: String,
        inner_error: EngineError,
    },

    #[error("Signature parsing failed: {message}")]
    SignatureParsingFailed { message: String },

    #[error("Transaction signing failed: {message}")]
    SigningError {
        message: String,
        inner_error: EngineError,
    },

    #[error("Work still remaining: {result:?}")]
    WorkRemaining { result: EoaExecutorWorkerResult },

    #[error("EOA out of funds: {balance} < {balance_threshold} wei")]
    EoaOutOfFunds {
        balance: U256,
        balance_threshold: U256,
    },

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("Recovery required: {message}")]
    RecoveryRequired { message: String },

    #[error("User cancelled")]
    UserCancelled,
}

impl EoaExecutorWorkerError {
    pub fn handle(self) -> JobError<EoaExecutorWorkerError> {
        match &self {
            EoaExecutorWorkerError::EoaOutOfFunds { .. } => JobError::Nack {
                error: self,
                delay: Some(Duration::from_secs(60)),
                position: RequeuePosition::Last,
            },
            EoaExecutorWorkerError::StoreError {
                inner_error: TransactionStoreError::LockLost { .. },
                ..
            } => JobError::Fail(self),
            EoaExecutorWorkerError::RpcError { .. } => {
                if is_retryable_preparation_error(&self) {
                    JobError::Nack {
                        error: self,
                        delay: Some(Duration::from_secs(10)),
                        position: RequeuePosition::Last,
                    }
                } else {
                    JobError::Fail(self)
                }
            }
            _ => JobError::Nack {
                error: self,
                delay: Some(Duration::from_secs(10)),
                position: RequeuePosition::Last,
            },
        }
    }
}

impl From<TwmqError> for EoaExecutorWorkerError {
    fn from(error: TwmqError) -> Self {
        EoaExecutorWorkerError::InternalError {
            message: format!("Queue error: {error}"),
        }
    }
}

impl From<TransactionStoreError> for EoaExecutorWorkerError {
    fn from(error: TransactionStoreError) -> Self {
        EoaExecutorWorkerError::StoreError {
            message: error.to_string(),
            inner_error: error,
        }
    }
}

impl UserCancellable for EoaExecutorWorkerError {
    fn user_cancelled() -> Self {
        EoaExecutorWorkerError::UserCancelled
    }
}

// Once dispatch starts, neither provider error text nor a transport failure can
// revoke the durable nonce reservation. Reconcile or retry the same signed bytes.
#[derive(PartialEq, Eq, Debug)]
pub enum SendContext {
    Rebroadcast,
    InitialBroadcast,
}

pub fn should_update_balance_threshold(error: &EngineError) -> bool {
    match error {
        EngineError::RpcError { kind, .. }
        | EngineError::PaymasterError { kind, .. }
        | EngineError::BundlerError { kind, .. } => match kind {
            RpcErrorKind::ErrorResp(resp) => {
                let message = resp.message.to_lowercase();
                message.contains("insufficient funds")
                    || message.contains("insufficient balance")
                    || message.contains("out of gas")
                    || message.contains("insufficient eth")
                    || message.contains("balance too low")
                    || message.contains("not enough funds")
                    || message.contains("insufficient native token")
                    || message.contains("transaction execution error: user cant pay the bills") // chain 106
            }
            _ => false,
        },
        _ => false,
    }
}

pub fn is_retryable_rpc_error(kind: &RpcErrorKind) -> bool {
    match kind {
        RpcErrorKind::TransportHttpError { status, .. } if *status >= 400 && *status < 500 => false,
        RpcErrorKind::UnsupportedFeature { .. } => false,
        RpcErrorKind::ErrorResp(resp) => {
            let message = resp.message.to_lowercase();
            // if the error message contains "invalid chain", it's not retryable
            !(message.contains("invalid chain") || message.contains("invalid opcode"))
        }
        _ => true,
    }
}

pub fn is_retryable_preparation_error(error: &EoaExecutorWorkerError) -> bool {
    match error {
        EoaExecutorWorkerError::RpcError { inner_error, .. } => {
            // extract the RpcErrorKind from the inner error
            if let EngineError::RpcError { kind, .. } = inner_error {
                is_retryable_rpc_error(kind)
            } else {
                false
            }
        }
        EoaExecutorWorkerError::ChainServiceError { .. } => true, // Network related
        EoaExecutorWorkerError::StoreError { inner_error, .. } => {
            matches!(inner_error, TransactionStoreError::RedisError { .. })
        }
        EoaExecutorWorkerError::TransactionSimulationFailed { .. } => false, // Deterministic
        EoaExecutorWorkerError::TransactionBuildFailed { .. } => false,      // Deterministic
        EoaExecutorWorkerError::SigningError { inner_error, .. } => match inner_error {
            // if iaw error, it's retryable only if it's a network error
            EngineError::IawError { error, .. } => matches!(error, IAWError::NetworkError { .. }),
            _ => false,
        },
        EoaExecutorWorkerError::TransactionNotFound { .. } => false, // Deterministic
        EoaExecutorWorkerError::InternalError { .. } => false,       // Deterministic
        EoaExecutorWorkerError::RecoveryRequired { .. } => true, // Never classify uncertainty as chain failure.
        EoaExecutorWorkerError::UserCancelled => false,          // Deterministic
        EoaExecutorWorkerError::TransactionSendError { .. } => false, // Different context
        EoaExecutorWorkerError::SignatureParsingFailed { .. } => false, // Deterministic
        EoaExecutorWorkerError::WorkRemaining { .. } => false,   // Different context
        EoaExecutorWorkerError::EoaOutOfFunds { .. } => false,   // Deterministic
    }
}

impl SubmissionResult {
    /// Convert a send result to a SubmissionResult for batch processing
    /// This handles the specific RpcError<TransportErrorKind> type from alloy
    pub fn from_send_result<SR>(
        borrowed_transaction: &BorrowedTransaction,
        send_result: Result<SR, RpcError<TransportErrorKind>>,
        send_context: SendContext,
        _chain: &impl Chain,
    ) -> Self {
        let result = match send_result {
            Ok(_) => SubmissionResultType::Success,
            Err(error) => {
                tracing::warn!(
                    context = ?send_context,
                    transaction_id = %borrowed_transaction.transaction_id,
                    nonce = borrowed_transaction.signed_transaction.tx().nonce(),
                    error = %engine_core::error::rpc_error_diagnostic(&error),
                    "Broadcast outcome is unknown; retaining signed attempt for recovery"
                );
                SubmissionResultType::Uncertain
            }
        };
        SubmissionResult {
            result,
            transaction: borrowed_transaction.clone().into(),
        }
    }
}

pub fn is_unsupported_eip1559_error(error: &RpcError<TransportErrorKind>) -> bool {
    if let RpcError::UnsupportedFeature(_) = error {
        return true;
    }

    if let RpcError::ErrorResp(resp) = error {
        let message = resp.message.to_lowercase();
        return message.contains("method not found");
    }

    false
}
