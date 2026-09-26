//! Admission bridge between the durable recovery ledger and Redis queues.

use engine_core::{error::EngineError, recovery};
use serde::{Serialize, de::DeserializeOwned};

/// Persist the exact admitted payload before Redis sees it. A matching retry
/// uses the original generated replay identity; a terminal record creates no work.
pub(crate) async fn reserve<T: Serialize + DeserializeOwned>(
    kind: &str,
    id: &str,
    payload: T,
) -> Result<Option<T>, EngineError> {
    let Some(journal) = recovery::global() else {
        // Embedded library tests supply their own disposable state. The server
        // binary always opens and installs its journal before starting workers.
        return Ok(Some(payload));
    };
    let value =
        serde_json::to_value(payload).map_err(|_| internal("Cannot encode recovery admission"))?;
    let fingerprint = recovery::admission_fingerprint(kind, &value).map_err(recovery_error)?;
    let reservation = journal
        .reserve_admission(kind, id, &fingerprint, value)
        .await
        .map_err(recovery_error)?;
    if reservation.terminal {
        return Ok(None);
    }
    serde_json::from_value(reservation.payload)
        .map(Some)
        .map_err(|_| internal("Recovery admission payload does not match this engine version"))
}

fn internal(message: &str) -> EngineError {
    EngineError::InternalError {
        message: message.into(),
    }
}

fn recovery_error(error: recovery::RecoveryError) -> EngineError {
    if matches!(error, recovery::RecoveryError::Conflict) {
        EngineError::ValidationError {
            message: error.to_string(),
        }
    } else {
        EngineError::RecoveryRequired {
            message: error.to_string(),
        }
    }
}
