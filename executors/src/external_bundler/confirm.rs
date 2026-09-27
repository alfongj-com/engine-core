use alloy::{
    primitives::{Address, B256, Bytes, U256, keccak256},
    providers::Provider,
};
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    error::{AlloyRpcErrorToEngineError, EngineError},
    execution_options::WebhookOptions,
    finality::{FinalityAssessment, FinalityEvidence},
    rpc_clients::UserOperationReceipt,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use twmq::{
    DurableExecution, FailHookData, NackHookData, Queue, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, JobResult, RequeuePosition, ToJobResult},
};

use crate::{
    metrics::{
        calculate_duration_seconds_from_twmq, current_timestamp_ms,
        record_transaction_queued_to_confirmed,
    },
    transaction_registry::TransactionRegistry,
    webhook::{
        WebhookJobHandler,
        envelope::{ExecutorStage, HasWebhookOptions, WebhookCapable},
    },
};

use super::deployment::RedisDeploymentLock;

const MAX_CONFIRMATION_JOB_AGE_SECONDS: u64 = 24 * 60 * 60;

fn job_age_seconds<T: Clone>(job: &BorrowedJob<T>) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    now.saturating_sub(job.job.created_at)
}

// --- Job Payload ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserOpConfirmationJobData {
    pub transaction_id: String,
    pub chain_id: u64,
    pub account_address: Address,
    pub user_op_hash: Bytes,
    pub nonce: U256,
    /// Persist the intended EntryPoint; legacy confirmation jobs require reconciliation.
    #[serde(default)]
    pub entrypoint_address: Option<Address>,
    pub deployment_lock_acquired: bool,
    /// Ownership token from the send attempt. Legacy jobs cannot release newer locks.
    #[serde(default)]
    pub deployment_lock_id: Option<String>,
    pub webhook_options: Vec<WebhookOptions>,
    pub rpc_credentials: RpcCredentials,
    /// Original timestamp when the transaction was first queued (unix timestamp in milliseconds)
    #[serde(default)]
    pub original_queued_timestamp: Option<u64>,
}

// --- Success Result ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserOpConfirmationResult {
    pub user_op_hash: Bytes,
    pub receipt: UserOperationReceipt,
    #[serde(default)]
    pub finality: Option<FinalityEvidence>,
    /// Unknown until the ownership-checked Redis completion hook commits.
    /// Historical boolean results still deserialize.
    #[serde(default)]
    pub deployment_lock_released: Option<bool>,
}

// --- Error Types ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum UserOpConfirmationError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    #[serde(rename_all = "camelCase")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Receipt not yet available for user operation {user_op_hash}")]
    #[serde(rename_all = "camelCase")]
    ReceiptNotAvailable {
        user_op_hash: Bytes,
        attempt_number: u32,
    },

    #[error(
        "Confirmation job aged out after {age_seconds}s (attempt {attempt_number}) for user operation {user_op_hash}"
    )]
    #[serde(rename_all = "camelCase")]
    StaleJob {
        user_op_hash: Bytes,
        attempt_number: u32,
        age_seconds: u64,
    },

    #[error("Failed to query user operation receipt: {message}")]
    #[serde(rename_all = "camelCase")]
    ReceiptQueryFailed {
        user_op_hash: Bytes,
        message: String,
        inner_error: Option<EngineError>,
    },

    #[error("Awaiting verified finality: {message}")]
    FinalityPending { message: String },

    #[error("User operation reverted after final settlement")]
    TransactionFailed {
        receipt: Box<UserOperationReceipt>,
        finality: FinalityEvidence,
    },

    #[error("Internal error: {message}")]
    #[serde(rename_all = "camelCase")]
    InternalError { message: String },

    #[error("Transaction cancelled by user")]
    UserCancelled,
}

impl From<TwmqError> for UserOpConfirmationError {
    fn from(error: TwmqError) -> Self {
        UserOpConfirmationError::InternalError {
            message: format!("Deserialization error for job data: {error}"),
        }
    }
}

impl UserCancellable for UserOpConfirmationError {
    fn user_cancelled() -> Self {
        UserOpConfirmationError::UserCancelled
    }
}

fn durable_userop_identity(job: &UserOpConfirmationJobData) -> serde_json::Value {
    serde_json::json!({
        "chainId": job.chain_id, "userOperationHash": job.user_op_hash,
        "entrypoint": job.entrypoint_address, "sender": job.account_address, "nonce": job.nonce,
    })
}

async fn validate_durable_confirmation(job: &UserOpConfirmationJobData) -> Result<(), EngineError> {
    let Some(journal) = engine_core::recovery::global() else {
        return Ok(());
    };
    let invalid = || EngineError::InternalError {
        message: "Confirmation does not match its durable admission and broadcast identity".into(),
    };
    let admission = journal
        .admission("erc4337", &job.transaction_id)
        .await
        .map_err(|_| invalid())?
        .ok_or_else(invalid)?;
    let admitted: super::send::ExternalBundlerSendJobData =
        serde_json::from_value(admission.payload).map_err(|_| invalid())?;
    if admission.state == engine_core::recovery::AdmissionState::Quarantined
        || admitted.transaction_id != job.transaction_id
        || admitted.chain_id != job.chain_id
        || admitted.pregenerated_nonce != Some(job.nonce)
        || job.entrypoint_address
            != Some(
                admitted
                    .execution_options
                    .entrypoint_details
                    .entrypoint_address,
            )
        || serde_json::to_value(&admitted.rpc_credentials).map_err(|_| invalid())?
            != serde_json::to_value(&job.rpc_credentials).map_err(|_| invalid())?
        || serde_json::to_value(&admitted.webhook_options).map_err(|_| invalid())?
            != serde_json::to_value(&job.webhook_options).map_err(|_| invalid())?
    {
        return Err(invalid());
    }
    journal
        .validate_attempt_identity(
            "erc4337",
            &job.transaction_id,
            &durable_userop_identity(job),
        )
        .await
        .map_err(|_| invalid())
}

fn userop_identity_matches(
    job: &UserOpConfirmationJobData,
    receipt: &UserOperationReceipt,
) -> bool {
    job.user_op_hash.len() == 32
        && receipt.user_op_hash == job.user_op_hash
        && receipt.sender == job.account_address
        && receipt.nonce == job.nonce
        && job.entrypoint_address == Some(receipt.entry_point)
}

fn userop_event_matches(
    job: &UserOpConfirmationJobData,
    receipt: &UserOperationReceipt,
    canonical: &alloy::rpc::types::TransactionReceipt,
) -> bool {
    let topic =
        keccak256("UserOperationEvent(bytes32,address,address,uint256,bool,uint256,uint256)");
    let hash = B256::from_slice(&job.user_op_hash);
    let matching: Vec<_> = canonical
        .inner
        .logs()
        .iter()
        .filter(|log| {
            let topics = log.topics();
            !log.removed
                && Some(log.address()) == job.entrypoint_address
                && topics.len() == 4
                && topics[0] == topic
                && topics[1] == hash
                && topics[2].as_slice()[..12] == [0; 12]
                && Address::from_slice(&topics[2].as_slice()[12..]) == job.account_address
        })
        .collect();
    if matching.len() != 1 {
        return false;
    }
    let data = matching[0].data().data.as_ref();
    data.len() == 128
        && U256::from_be_slice(&data[..32]) == job.nonce
        && U256::from_be_slice(&data[32..64]) == U256::from(u8::from(receipt.success))
        && U256::from_be_slice(&data[64..96]) == receipt.actual_gas_cost
        && U256::from_be_slice(&data[96..]) == receipt.actual_gas_used
}

// --- Handler ---
pub struct UserOpConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub deployment_lock: RedisDeploymentLock,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub transaction_registry: Arc<TransactionRegistry>,
    pub max_confirmation_attempts: u32,
    pub confirmation_retry_delay: Duration,
}

impl<CS> UserOpConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub fn new(
        chain_service: Arc<CS>,
        deployment_lock: RedisDeploymentLock,
        webhook_queue: Arc<Queue<WebhookJobHandler>>,
        transaction_registry: Arc<TransactionRegistry>,
    ) -> Self {
        Self {
            chain_service,
            deployment_lock,
            webhook_queue,
            transaction_registry,
            max_confirmation_attempts: 50, // ~100 seconds with 2 second delays
            confirmation_retry_delay: Duration::from_secs(2),
        }
    }

    pub fn with_retry_config(mut self, max_attempts: u32, retry_delay: Duration) -> Self {
        self.max_confirmation_attempts = max_attempts;
        self.confirmation_retry_delay = retry_delay;
        self
    }
}

impl<CS> DurableExecution for UserOpConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    type Output = UserOpConfirmationResult;
    type ErrorData = UserOpConfirmationError;
    type JobData = UserOpConfirmationJobData;

    #[tracing::instrument(skip(self, job), fields(transaction_id = job.job.id, stage = Self::stage_name(), executor = Self::executor_name()))]
    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        let job_data = &job.job.data;

        // A deadline is a polling budget, not evidence of an on-chain failure.
        let retry_delay = if job_age_seconds(job) > MAX_CONFIRMATION_JOB_AGE_SECONDS {
            Duration::from_secs(3600)
        } else if job.job.attempts >= self.max_confirmation_attempts {
            self.confirmation_retry_delay.max(Duration::from_secs(10))
        } else {
            self.confirmation_retry_delay
        };

        if job.job.id != job_data.transaction_id {
            return Err(UserOpConfirmationError::InternalError {
                message: "Confirmation queue ID differs from admitted transaction ID".into(),
            })
            .map_err_nack(Some(retry_delay), RequeuePosition::Last);
        }
        validate_durable_confirmation(job_data)
            .await
            .map_err(|error| UserOpConfirmationError::InternalError {
                message: error.to_string(),
            })
            .map_err_nack(Some(retry_delay), RequeuePosition::Last)?;

        // 1. Get Chain
        let chain = self
            .chain_service
            .get_chain(job_data.chain_id)
            .map_err(|e| UserOpConfirmationError::ChainServiceError {
                chain_id: job_data.chain_id,
                message: format!("Failed to get chain instance: {e}"),
            })
            .map_err_nack(Some(retry_delay), RequeuePosition::Last)?;

        let chain = chain.with_new_default_headers(
            job.job
                .data
                .rpc_credentials
                .to_header_map()
                .map_err(|e| UserOpConfirmationError::InternalError {
                    message: format!("Bad RPC Credential values, unserialisable into headers: {e}"),
                })
                .map_err_nack(Some(retry_delay), RequeuePosition::Last)?,
        );

        crate::finality::check_continuity(&chain)
            .await
            .map_err(|e| UserOpConfirmationError::ReceiptQueryFailed {
                user_op_hash: job_data.user_op_hash.clone(),
                message: "Finality checkpoint continuity unavailable".into(),
                inner_error: Some(e),
            })
            .map_err_nack(Some(retry_delay), RequeuePosition::Last)?;

        // 2. Query for User Operation Receipt
        let receipt_option = chain
            .bundler_client()
            .get_user_op_receipt(job_data.user_op_hash.clone())
            .await
            .map_err(|e| UserOpConfirmationError::ReceiptQueryFailed {
                user_op_hash: job_data.user_op_hash.clone(),
                message: engine_core::error::rpc_error_diagnostic(&e),
                inner_error: Some(e.to_engine_bundler_error(&chain)),
            })
            .map_err_nack(Some(retry_delay), RequeuePosition::Last)?;

        let receipt = match receipt_option {
            Some(receipt) => receipt,
            None => {
                return Err(UserOpConfirmationError::ReceiptNotAvailable {
                    user_op_hash: job_data.user_op_hash.clone(),
                    attempt_number: job.job.attempts,
                })
                .map_err_nack(Some(retry_delay), RequeuePosition::Last);
                // NACK - triggers on_nack hook which keeps lock for retry
            }
        };

        if !userop_identity_matches(job_data, &receipt) {
            return Err(UserOpConfirmationError::FinalityPending {
                message: "Bundler receipt does not match persisted operation identity".into(),
            })
            .map_err_nack(Some(retry_delay), RequeuePosition::Last);
        }
        // Independently fetch the chain receipt: bundler success flags and logs are
        // not execution evidence. Bind the EntryPoint event to the stored operation.
        let canonical = chain
            .provider()
            .get_transaction_receipt(receipt.receipt.transaction_hash)
            .await
            .map_err(|e| UserOpConfirmationError::ReceiptQueryFailed {
                user_op_hash: job_data.user_op_hash.clone(),
                message: engine_core::error::rpc_error_diagnostic(&e),
                inner_error: Some(e.to_engine_error(&chain)),
            })
            .map_err_nack(Some(retry_delay), RequeuePosition::Last)?;
        let Some(canonical) = canonical else {
            return Err(UserOpConfirmationError::FinalityPending {
                message: "Chain receipt unavailable".into(),
            })
            .map_err_nack(Some(retry_delay), RequeuePosition::Last);
        };
        if canonical.block_hash != receipt.receipt.block_hash
            || canonical.block_number != receipt.receipt.block_number
            || !canonical.status()
            || !userop_event_matches(job_data, &receipt, &canonical)
        {
            return Err(UserOpConfirmationError::FinalityPending {
                message: "Bundler outcome is not corroborated by the chain receipt".into(),
            })
            .map_err_nack(Some(retry_delay), RequeuePosition::Last);
        }
        let finality =
            crate::finality::assess(&chain, receipt.receipt.transaction_hash, &canonical)
                .await
                .map_err(|e| UserOpConfirmationError::ReceiptQueryFailed {
                    user_op_hash: job_data.user_op_hash.clone(),
                    message: "Finality query failed".into(),
                    inner_error: Some(e),
                })
                .map_err_nack(Some(retry_delay), RequeuePosition::Last)?;
        let FinalityAssessment::Finalized(finality) = finality else {
            return Err(UserOpConfirmationError::FinalityPending {
                message: "Operation is not canonically settled under the configured policy".into(),
            })
            .map_err_nack(Some(retry_delay), RequeuePosition::Last);
        };
        if let Some(journal) = engine_core::recovery::global() {
            let mut proof = durable_userop_identity(job_data);
            proof["transactionHash"] = serde_json::json!(canonical.transaction_hash);
            proof["outcome"] = serde_json::json!(if receipt.success {
                "success"
            } else {
                "reverted"
            });
            proof["finality"] = serde_json::json!(finality);
            journal
                .record_terminal("erc4337", &job_data.transaction_id, proof)
                .await
                .map_err(|_| UserOpConfirmationError::InternalError {
                    message: "Cannot persist verified operation settlement".into(),
                })
                .map_err_nack(Some(retry_delay), RequeuePosition::Last)?;
        }
        if !receipt.success {
            return Err(UserOpConfirmationError::TransactionFailed {
                receipt: Box::new(receipt),
                finality,
            })
            .map_err_fail();
        }

        tracing::info!(
            transaction_id = job_data.transaction_id,
            user_op_hash = ?job_data.user_op_hash,
            transaction_hash = ?receipt.receipt.transaction_hash,
            success = ?receipt.success,
            "User operation confirmed on-chain"
        );

        // 4. Record metrics if original timestamp is available
        if let Some(original_timestamp) = job_data.original_queued_timestamp {
            let confirmed_timestamp = current_timestamp_ms();
            let queued_to_confirmed_duration =
                calculate_duration_seconds_from_twmq(original_timestamp, confirmed_timestamp);
            record_transaction_queued_to_confirmed(
                "erc4337-external",
                job_data.chain_id,
                queued_to_confirmed_duration,
            );
        }

        // 5. Success! Lock cleanup will happen atomically in on_success hook
        Ok(UserOpConfirmationResult {
            user_op_hash: job_data.user_op_hash.clone(),
            receipt,
            finality: Some(finality),
            deployment_lock_released: None, // Ownership is checked later inside the Redis hook.
        })
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        success_data: SuccessHookData<'_, Self::Output>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Remove transaction from registry since confirmation is complete
        self.transaction_registry
            .add_remove_command(tx.pipeline(), &job.job.data.transaction_id);

        // Fence both cleanup and cache writes against newer lock owners.
        if let Some(lock_id) = job.job.data.deployment_lock_id.as_deref() {
            self.deployment_lock
                .release_lock_and_update_cache_with_pipeline(
                    tx.pipeline(),
                    job.job.data.chain_id,
                    &job.job.data.account_address,
                    lock_id,
                    true, // is_deployed = true
                );

            tracing::info!(
                transaction_id = job.job.data.transaction_id,
                account_address = ?job.job.data.account_address,
                "Added atomic lock release and cache update to transaction pipeline"
            );
        }

        // Queue success webhook
        if let Err(e) = self.queue_success_webhook(job, success_data, tx) {
            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                error = ?e,
                "Failed to queue success webhook"
            );
        }
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Self::JobData>,
        nack_data: NackHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        // NEVER release lock on NACK - job will be retried with the same lock

        // Only queue webhook for actual errors, not for "waiting for receipt" states
        let should_queue_webhook = !matches!(
            nack_data.error,
            UserOpConfirmationError::ReceiptNotAvailable { .. }
                | UserOpConfirmationError::FinalityPending { .. }
        );

        if should_queue_webhook {
            if let Err(e) = self.queue_nack_webhook(job, nack_data, tx) {
                tracing::error!(
                    transaction_id = job.job.data.transaction_id,
                    error = ?e,
                    "Failed to queue nack webhook"
                );
            }
        } else {
            tracing::debug!(
                transaction_id = job.job.data.transaction_id,
                attempt = job.job.attempts,
                "Skipping webhook for receipt not available - transaction still mining"
            );
        }

        tracing::debug!(
            transaction_id = job.job.data.transaction_id,
            attempt = job.job.attempts,
            "Confirmation job NACKed, retaining lock for retry"
        );
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        fail_data: FailHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Cancellation/administrative failure does not prove execution outcome.
        if !matches!(
            fail_data.error,
            UserOpConfirmationError::TransactionFailed { .. }
        ) {
            return;
        }
        // Remove transaction from registry only after a verified final revert.
        self.transaction_registry
            .add_remove_command(tx.pipeline(), &job.job.data.transaction_id);

        // Release only this send attempt's lock on permanent failure.
        if let Some(lock_id) = job.job.data.deployment_lock_id.as_deref() {
            self.deployment_lock.release_lock_with_pipeline(
                tx.pipeline(),
                job.job.data.chain_id,
                &job.job.data.account_address,
                lock_id,
            );

            let failure_reason = match fail_data.error {
                UserOpConfirmationError::ReceiptNotAvailable { .. } => {
                    "Max confirmation attempts exceeded"
                }
                UserOpConfirmationError::StaleJob { .. } => {
                    "Confirmation job aged out after exceeding max retry lifetime"
                }
                _ => "Confirmation job failed permanently",
            };

            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                account_address = ?job.job.data.account_address,
                reason = failure_reason,
                "Added lock release to transaction pipeline due to permanent failure"
            );
        }

        // Queue failure webhook
        if let Err(e) = self.queue_fail_webhook(job, fail_data, tx) {
            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                error = ?e,
                "Failed to queue fail webhook"
            );
        }
    }
}

// --- Trait Implementations ---
impl<CS> ExecutorStage for UserOpConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn executor_name() -> &'static str {
        "erc4337"
    }

    fn stage_name() -> &'static str {
        "confirm"
    }
}

impl HasWebhookOptions for UserOpConfirmationJobData {
    fn webhook_options(&self) -> Vec<WebhookOptions> {
        self.webhook_options.clone()
    }
}

impl<CS> WebhookCapable for UserOpConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}
