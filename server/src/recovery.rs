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

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RetryAdmission {
    Missing,
    Active,
    Terminal,
}

/// A full Redis queue must distinguish a fresh rejection from an existing
/// durable intent. Completed retries need no work; active IDs may still execute.
pub(crate) async fn retry_admission<T: Serialize>(
    kind: &str,
    id: &str,
    payload: &T,
) -> Result<RetryAdmission, EngineError> {
    let Some(journal) = recovery::global() else {
        return Ok(RetryAdmission::Missing);
    };
    retry_admission_with_journal(&journal, kind, id, payload).await
}

async fn retry_admission_with_journal<T: Serialize>(
    journal: &std::sync::Arc<recovery::RecoveryJournal>,
    kind: &str,
    id: &str,
    payload: &T,
) -> Result<RetryAdmission, EngineError> {
    journal.ensure_healthy().await.map_err(recovery_error)?;
    let Some(record) = journal.admission(kind, id).await.map_err(recovery_error)? else {
        return Ok(RetryAdmission::Missing);
    };
    let value =
        serde_json::to_value(payload).map_err(|_| internal("Cannot encode recovery retry"))?;
    let fingerprint = recovery::admission_fingerprint(kind, &value).map_err(recovery_error)?;
    if record.fingerprint != fingerprint {
        return Err(recovery_error(recovery::RecoveryError::Conflict));
    }
    if record.state == recovery::AdmissionState::Quarantined {
        return Err(recovery_error(recovery::RecoveryError::RecoveryRequired(
            "transaction is quarantined",
        )));
    }
    Ok(if record.state == recovery::AdmissionState::Terminal {
        RetryAdmission::Terminal
    } else {
        RetryAdmission::Active
    })
}

pub(crate) fn admission_pending(id: &str) -> EngineError {
    EngineError::AdmissionPending {
        transaction_id: id.into(),
        message: "Intent is durably reserved and may execute through recovery. Queue capacity is unavailable; retry the original request with this transactionId as its idempotency key.".into(),
    }
}

/// Called only after reserve returned the original payload. Embedded tests may
/// omit the journal; their ordinary capacity rejection still has no admission.
pub(crate) fn eoa_enqueue_error(
    error: engine_executors::eoa::store::TransactionStoreError,
    id: &str,
    durably_reserved: bool,
) -> EngineError {
    use engine_executors::eoa::store::TransactionStoreError;
    match error {
        TransactionStoreError::CapacityExceeded if durably_reserved => admission_pending(id),
        TransactionStoreError::CapacityExceeded => EngineError::Overloaded { message: error.to_string() },
        TransactionStoreError::TransactionConflict { .. } => EngineError::ValidationError {
            message: "The idempotency key already belongs to a different or incomplete transaction. Use the original request or reconcile its status.".into(),
        },
        other => EngineError::from(twmq::error::TwmqError::Runtime {
            message: format!("Failed to add transaction to EOA store: {other}"),
        }),
    }
}

fn internal(message: &str) -> EngineError {
    EngineError::InternalError {
        message: message.into(),
    }
}

pub(crate) fn recovery_error(error: recovery::RecoveryError) -> EngineError {
    if matches!(error, recovery::RecoveryError::PublicationPending) {
        EngineError::RecoveryPending {
            message: error.to_string(),
        }
    } else if matches!(error, recovery::RecoveryError::Busy) {
        EngineError::Overloaded {
            message: error.to_string(),
        }
    } else if matches!(error, recovery::RecoveryError::Conflict) {
        EngineError::ValidationError {
            message: error.to_string(),
        }
    } else {
        EngineError::RecoveryRequired {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::error::ApiEngineError;
    use axum::{http::StatusCode, response::IntoResponse};

    #[test]
    fn admission_pressure_is_retryable_429_and_does_not_request_disaster_recovery() {
        let error = recovery_error(recovery::RecoveryError::Busy);
        assert!(matches!(error, EngineError::Overloaded { .. }));
        assert_eq!(
            ApiEngineError(error).into_response().status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            ApiEngineError(recovery_error(recovery::RecoveryError::Conflict))
                .into_response()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ApiEngineError(recovery_error(recovery::RecoveryError::Storage))
                .into_response()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn publication_pending_is_typed_retryable_503_while_halts_remain_recovery_required() {
        for (error, expected) in [
            (
                recovery::RecoveryError::PublicationPending,
                "RECOVERY_PENDING",
            ),
            (
                recovery::RecoveryError::RecoveryRequired("Redis checkpoint mismatch"),
                "RECOVERY_REQUIRED",
            ),
        ] {
            let response = ApiEngineError(recovery_error(error)).into_response();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            let bytes = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["error"]["details"]["type"], expected);
        }
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn full_queue_accepts_only_exact_terminal_retry_without_recreating_work() {
        use alloy::primitives::{Address, B256, Bytes, U256};
        use engine_core::{chain::RpcCredentials, credentials::SigningCredential};
        use engine_executors::eoa::{
            EoaExecutorStore, EoaTransactionRequest,
            store::{MAX_PENDING_TRANSACTIONS, TransactionStoreError},
        };
        use twmq::redis::AsyncCommands;
        let token = uuid_for_test();
        let directory = std::env::temp_dir().join(format!("engine-terminal-cap-{token}"));
        let path = directory.join("journal.sqlite");
        let namespace = format!("terminal_cap_{token}");
        let url = std::env::var("TEST_REDIS_URL").expect("select disposable Redis");
        recovery::RecoveryJournal::initialize(&path, &url, Some(namespace.clone()))
            .await
            .unwrap();
        let journal = recovery::RecoveryJournal::open(&path, &url, Some(namespace.clone()))
            .await
            .unwrap();
        let redis_client = twmq::redis::Client::open(url.as_str()).unwrap();
        let mut redis = redis_client.get_connection_manager().await.unwrap();
        let sender = Address::repeat_byte(1);
        let request = EoaTransactionRequest {
            transaction_id: "terminal-intent".into(),
            chain_id: 31337,
            from: sender,
            to: Some(Address::repeat_byte(2)),
            value: U256::from(1),
            data: Bytes::new(),
            gas_limit: Some(21000),
            webhook_options: vec![],
            signing_credential: SigningCredential::Environment { address: sender },
            rpc_credentials: RpcCredentials::Configured,
            transaction_type_data: None,
        };
        let value = serde_json::to_value(&request).unwrap();
        journal
            .reserve_admission(
                "eoa",
                &request.transaction_id,
                &recovery::admission_fingerprint("eoa", &value).unwrap(),
                value,
            )
            .await
            .unwrap();
        let hash = B256::repeat_byte(9);
        journal
            .before_broadcast(
                "eoa",
                &request.transaction_id,
                &format!("evm:31337:{sender:#x}:0"),
                serde_json::json!({"transactionHash":hash}),
            )
            .await
            .unwrap();
        journal
            .record_terminal(
                "eoa",
                &request.transaction_id,
                serde_json::json!({"chainId":31337,"transactionHash":hash,"outcome":"success"}),
            )
            .await
            .unwrap();
        let store =
            EoaExecutorStore::new(redis.clone(), Some(namespace.clone()), sender, 31337, 3600);
        let _: () = twmq::redis::Script::new(
            "for i=1,tonumber(ARGV[1]) do redis.call('ZADD',KEYS[1],i,'queued-'..i) end return nil",
        )
        .key(store.pending_transactions_zset_name())
        .arg(MAX_PENDING_TRANSACTIONS)
        .invoke_async(&mut redis)
        .await
        .unwrap();
        assert!(
            matches!(
                store
                    .check_admission_capacity(&request.transaction_id)
                    .await,
                Err(TransactionStoreError::CapacityExceeded)
            ),
            "terminal Redis payload intentionally absent, as after TTL expiry"
        );
        let before = recovery::RecoveryJournal::status(&path).unwrap();
        assert_eq!(
            retry_admission_with_journal(&journal, "eoa", &request.transaction_id, &request)
                .await
                .unwrap(),
            RetryAdmission::Terminal,
        );
        let after = recovery::RecoveryJournal::status(&path).unwrap();
        assert_eq!(before.checkpoint, after.checkpoint);
        assert_eq!(
            (after.admissions, after.attempts, after.terminal),
            (1, 1, 1)
        );
        let queued: u64 = redis
            .zcard(store.pending_transactions_zset_name())
            .await
            .unwrap();
        assert_eq!(queued, MAX_PENDING_TRANSACTIONS);
        let mut conflicting = request.clone();
        conflicting.value = U256::from(2);
        let error =
            retry_admission_with_journal(&journal, "eoa", &request.transaction_id, &conflicting)
                .await
                .err()
                .unwrap();
        assert_eq!(
            ApiEngineError(error).into_response().status(),
            StatusCode::BAD_REQUEST
        );
        let mut fresh = request;
        fresh.transaction_id = "new-intent".into();
        assert_eq!(
            retry_admission_with_journal(&journal, "eoa", &fresh.transaction_id, &fresh)
                .await
                .unwrap(),
            RetryAdmission::Missing,
        );
        assert!(
            journal
                .admission("eoa", "new-intent")
                .await
                .unwrap()
                .is_none()
        );
        let keys: Vec<String> = redis.keys(format!("{namespace}:*")).await.unwrap();
        let _: () = redis.del(keys).await.unwrap();
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn uuid_for_test() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        format!(
            "{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    #[tokio::test]
    #[ignore = "requires disposable Redis in TEST_REDIS_URL"]
    async fn capacity_race_after_durable_reservation_returns_retryable_503_with_original_id() {
        use alloy::primitives::{Address, Bytes, U256};
        use engine_core::{
            chain::RpcCredentials, credentials::SigningCredential,
            execution_options::SendTransactionRequest,
        };
        use engine_executors::eoa::{
            EoaExecutorStore, EoaTransactionRequest,
            store::{MAX_PENDING_TRANSACTIONS, TransactionStoreError},
        };
        use twmq::redis::AsyncCommands;
        let token = uuid_for_test();
        let namespace = format!("admission_race_{token}");
        let directory = std::env::temp_dir().join(&namespace);
        let path = directory.join("journal.sqlite");
        let url = std::env::var("TEST_REDIS_URL").expect("select disposable Redis");
        recovery::RecoveryJournal::initialize(&path, &url, Some(namespace.clone()))
            .await
            .unwrap();
        let journal = recovery::RecoveryJournal::open(&path, &url, Some(namespace.clone()))
            .await
            .unwrap();
        let client = twmq::redis::Client::open(url).unwrap();
        let mut redis = client.get_connection_manager().await.unwrap();
        let sender = Address::repeat_byte(1);
        // Exercise the real default ID generator: the caller did not supply an
        // idempotency key, so the error response must return the generated one.
        let envelope: SendTransactionRequest = serde_json::from_value(serde_json::json!({
            "executionOptions":{"type":"EOA","chainId":31337,"from":sender},
            "params":[{"to":Address::repeat_byte(2),"value":"0x1"}]
        }))
        .unwrap();
        let request = EoaTransactionRequest {
            transaction_id: envelope.execution_options.base.idempotency_key,
            chain_id: 31337,
            from: sender,
            to: Some(Address::repeat_byte(2)),
            value: U256::from(1),
            data: Bytes::new(),
            gas_limit: Some(21000),
            webhook_options: vec![],
            signing_credential: SigningCredential::Environment { address: sender },
            rpc_credentials: RpcCredentials::Configured,
            transaction_type_data: None,
        };
        assert!(!request.transaction_id.is_empty());
        let store =
            EoaExecutorStore::new(redis.clone(), Some(namespace.clone()), sender, 31337, 3600);
        store
            .check_admission_capacity(&request.transaction_id)
            .await
            .unwrap();
        let payload = serde_json::to_value(&request).unwrap();
        let fingerprint = recovery::admission_fingerprint("eoa", &payload).unwrap();
        journal
            .reserve_admission(
                "eoa",
                &request.transaction_id,
                &fingerprint,
                payload.clone(),
            )
            .await
            .unwrap();
        // A competing intake fills capacity after preflight and journal commit.
        let _: () = twmq::redis::Script::new(
            "for i=1,tonumber(ARGV[1]) do redis.call('ZADD',KEYS[1],i,'queued-'..i) end return nil",
        )
        .key(store.pending_transactions_zset_name())
        .arg(MAX_PENDING_TRANSACTIONS)
        .invoke_async(&mut redis)
        .await
        .unwrap();
        let error = store.add_transaction(request.clone()).await.unwrap_err();
        assert!(matches!(error, TransactionStoreError::CapacityExceeded));
        let response =
            ApiEngineError(eoa_enqueue_error(error, &request.transaction_id, true)).into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["details"]["type"], "ADMISSION_PENDING");
        assert_eq!(
            body["error"]["details"]["transactionId"],
            request.transaction_id
        );
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("retry the original request")
        );
        assert_eq!(
            retry_admission_with_journal(&journal, "eoa", &request.transaction_id, &request)
                .await
                .unwrap(),
            RetryAdmission::Active
        );
        assert!(
            store
                .get_transaction_data(&request.transaction_id)
                .await
                .unwrap()
                .is_none()
        );
        let retained = journal
            .admission("eoa", &request.transaction_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retained.state, recovery::AdmissionState::Admitted);
        assert_eq!(retained.payload, payload);
        assert!(retained.replay_key.is_none());
        let status = recovery::RecoveryJournal::status(&path).unwrap();
        assert_eq!((status.admissions, status.attempts), (1, 0));
        // A true pre-reservation capacity refusal remains429, and the same
        // generated ID can finish queue publication when a slot becomes free.
        assert_eq!(
            ApiEngineError(eoa_enqueue_error(
                TransactionStoreError::CapacityExceeded,
                "fresh",
                false
            ))
            .into_response()
            .status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        let _: () = redis
            .zrem(store.pending_transactions_zset_name(), "queued-1")
            .await
            .unwrap();
        let retry = journal
            .reserve_admission("eoa", &request.transaction_id, &fingerprint, payload)
            .await
            .unwrap();
        let request: EoaTransactionRequest = serde_json::from_value(retry.payload).unwrap();
        store.add_transaction(request.clone()).await.unwrap();
        store.add_transaction(request.clone()).await.unwrap();
        assert_eq!(
            redis
                .zcard::<_, u64>(store.pending_transactions_zset_name())
                .await
                .unwrap(),
            MAX_PENDING_TRANSACTIONS
        );
        assert_eq!(
            recovery::RecoveryJournal::status(&path).unwrap().admissions,
            1
        );
        let keys: Vec<String> = redis.keys(format!("{namespace}:*")).await.unwrap();
        let _: () = redis.del(keys).await.unwrap();
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
