use base64::Engine;
use engine_core::{
    credentials::SigningCredential,
    error::{EngineError, SolanaRpcErrorToEngineError},
    execution_options::{
        WebhookOptions,
        solana::{SolanaPriorityFee, SolanaTransactionOptions},
    },
    signer::SolanaSigner,
};
use engine_solana_core::{
    SolanaInstructionData,
    transaction::{
        InstructionDataEncoding, SolanaTransaction, SolanaTransactionInput,
        decode_transaction_wire, encode_transaction_wire,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::{
    config::{RpcSendTransactionConfig, RpcTransactionConfig},
    request::RpcRequest,
};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{EncodedTransactionWithStatusMeta, UiTransactionEncoding};
use spl_memo_interface::instruction::build_memo;
use std::{sync::Arc, time::Duration};
use tracing::{error, info, warn};
use twmq::{
    DurableExecution, FailHookData, NackHookData, Queue, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, JobError, JobResult, RequeuePosition, ToJobError},
};

use crate::{
    solana_executor::{
        rpc_cache::SolanaRpcCache,
        storage::{LockError, SolanaTransactionAttempt, SolanaTransactionStorage, TransactionLock},
    },
    transaction_registry::TransactionRegistry,
    webhook::{
        WebhookJobHandler,
        envelope::{ExecutorStage, HasTransactionMetadata, HasWebhookOptions, WebhookCapable},
    },
};

const CONFIRMATION_RETRY_DELAY: Duration = Duration::from_millis(200);
const NETWORK_ERROR_RETRY_DELAY: Duration = Duration::from_secs(2);
const MAX_SEND_ATTEMPTS_WITHOUT_TRANSACTION: u32 = 500;
const MAX_RECONCILIATION_CHECKS: u32 = 500;
const MAX_BROADCASTS_PER_ATTEMPT: u32 = 20;
const RECOVERY_PARK_DELAY: Duration = Duration::from_secs(3600);
const PROCESS_TIMEOUT: Duration = Duration::from_secs(90); // shorter than the 120s storage lock

// ========== JOB DATA ==========
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SolanaExecutorJobData {
    pub transaction_id: String,
    pub transaction: SolanaTransactionOptions,
    pub signing_credential: SigningCredential,
    #[serde(default)]
    pub webhook_options: Vec<WebhookOptions>,
}

impl HasWebhookOptions for SolanaExecutorJobData {
    fn webhook_options(&self) -> Vec<WebhookOptions> {
        self.webhook_options.clone()
    }
}

impl HasTransactionMetadata for SolanaExecutorJobData {
    fn transaction_id(&self) -> String {
        self.transaction_id.clone()
    }
}

// ========== SUCCESS RESULT ==========
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SolanaExecutorResult {
    pub transaction_id: String,
    pub signature: String,
    pub signer_address: String,
    pub chain_id: String,
    pub submission_attempt_number: u32,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub transaction: EncodedTransactionWithStatusMeta,
}

// ========== ERROR TYPES ==========
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum SolanaExecutorError {
    #[error("Failed to build transaction: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    TransactionBuildFailed { inner_error: String },

    #[error("Failed to sign transaction: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    SigningFailed { inner_error: EngineError },

    #[error("Failed to send transaction: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    SendFailed { inner_error: EngineError },

    #[error("Transaction failed on-chain: {reason}")]
    #[serde(rename_all = "camelCase")]
    TransactionFailed { reason: String },

    #[error("RPC error: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    RpcError { inner_error: EngineError },

    #[error("Failed to get priority fees: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    PriorityFeeError { inner_error: EngineError },

    #[error(
        "Blockhash expired, retrying with new blockhash (resubmission {submission_attempt_number})"
    )]
    #[serde(rename_all = "camelCase")]
    BlockhashExpired { submission_attempt_number: u32 },

    #[error("Max retries exceeded: {max_retries}")]
    #[serde(rename_all = "camelCase")]
    MaxRetriesExceeded { max_retries: u32 },

    #[error("Transaction sent successfully")]
    #[serde(rename_all = "camelCase")]
    TransactionSent {
        /// The signature of the sent transaction
        signature: String,
        /// Resubmission attempt number of this transaction
        submission_attempt_number: u32,
    },

    #[error("Transaction not yet confirmed")]
    #[serde(rename_all = "camelCase")]
    NotYetConfirmed {
        /// The signature of the transaction we're waiting for
        signature: String,
    },

    #[error("Failed to acquire lock: another worker is processing")]
    #[serde(rename_all = "camelCase")]
    LockHeldByAnotherWorker,

    #[error("Lost lock during execution")]
    LockLost,

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("Transaction outcome unresolved; recovery evidence retained: {reason}")]
    #[serde(rename_all = "camelCase")]
    RecoveryRequired { signature: String, reason: String },

    #[error("Transaction cancelled by user")]
    UserCancelled,
}

impl From<LockError> for SolanaExecutorError {
    fn from(e: LockError) -> Self {
        match e {
            LockError::AlreadyLocked => SolanaExecutorError::LockHeldByAnotherWorker,
            LockError::RedisError(msg) => SolanaExecutorError::InternalError { message: msg },
        }
    }
}

impl From<TwmqError> for SolanaExecutorError {
    fn from(error: TwmqError) -> Self {
        SolanaExecutorError::InternalError {
            message: format!("Queue error: {error}"),
        }
    }
}

impl UserCancellable for SolanaExecutorError {
    fn user_cancelled() -> Self {
        SolanaExecutorError::UserCancelled
    }
}

impl SolanaExecutorError {
    /// Check if this is actually a successful send (not an error)
    pub fn is_send_success(&self) -> bool {
        matches!(self, SolanaExecutorError::TransactionSent { .. })
    }

    /// Get the signature if this error contains one
    pub fn signature(&self) -> Option<&str> {
        match self {
            SolanaExecutorError::TransactionSent { signature, .. } => Some(signature.as_str()),
            SolanaExecutorError::NotYetConfirmed { signature } => Some(signature.as_str()),
            _ => None,
        }
    }
}

// ========== HANDLER ==========
pub struct SolanaExecutorJobHandler {
    pub solana_signer: Arc<SolanaSigner>,
    pub rpc_cache: Arc<SolanaRpcCache>,
    pub storage: Arc<SolanaTransactionStorage>,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub transaction_registry: Arc<TransactionRegistry>,
}

impl ExecutorStage for SolanaExecutorJobHandler {
    fn stage_name() -> &'static str {
        "solana_executor"
    }

    fn executor_name() -> &'static str {
        "solana_executor"
    }
}

impl WebhookCapable for SolanaExecutorJobHandler {
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}

impl DurableExecution for SolanaExecutorJobHandler {
    type Output = SolanaExecutorResult;
    type ErrorData = SolanaExecutorError;
    type JobData = SolanaExecutorJobData;

    #[tracing::instrument(
        skip(self, job),
        fields(
            transaction_id = job.job.id,
            stage = Self::stage_name()
        )
    )]
    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        // let queued_at_ms = job.job.created_at * 1000;
        let data = &job.job.data;
        let transaction_id = &data.transaction_id;

        info!("Starting to process Solana transaction");

        // Try to acquire lock - NACK if another worker has it
        let lock = self
            .storage
            .try_acquire_lock(transaction_id)
            .await
            .map_err(|e| match e {
                LockError::AlreadyLocked => {
                    info!(transaction_id = %transaction_id, "Another worker holds lock, nacking");
                    SolanaExecutorError::LockHeldByAnotherWorker
                        .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last)
                }
                LockError::RedisError(msg) => {
                    // An unavailable store says nothing about a prior broadcast's outcome.
                    // Requeue before creating an RPC client or touching the chain.
                    SolanaExecutorError::InternalError { message: msg }
                        .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last)
                }
            })?;

        info!(transaction_id = %transaction_id, "Acquired lock");

        let rpc_client = self
            .rpc_cache
            .get_or_create(data.transaction.execution_options.chain_id)
            .await;

        let result = tokio::time::timeout(
            PROCESS_TIMEOUT,
            self.execute_transaction(&rpc_client, data, &lock, job.job.attempts),
        )
        .await
        .unwrap_or_else(|_| {
            Err(SolanaExecutorError::InternalError {
                message:
                    "Solana processing deadline exceeded; any persisted attempt will be reconciled"
                        .into(),
            }
            .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last))
        });

        if let Err(e) = lock.release().await {
            warn!(transaction_id = %transaction_id, error = ?e, "Failed to release lock");
        }

        result
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        success_data: SuccessHookData<'_, Self::Output>,
        tx: &mut TransactionContext<'_>,
    ) {
        let transaction_id = &job.job.data.transaction_id;
        info!(
            transaction_id = %transaction_id,
            signature = %success_data.result.signature,
            slot = success_data.result.slot,
            "Transaction confirmed"
        );

        if let Ok(fingerprint) =
            crate::solana_executor::storage::solana_admission_fingerprint(&job.job.data)
        {
            self.storage.add_terminal_admission_command(
                tx.pipeline(),
                transaction_id,
                &fingerprint,
                "completed",
                false,
            );
        }
        self.storage
            .add_delete_attempt_command(tx.pipeline(), transaction_id);
        self.transaction_registry
            .add_remove_command(tx.pipeline(), transaction_id);

        if let Err(e) = self.queue_success_webhook(job, success_data, tx) {
            error!(
                transaction_id = %transaction_id,
                error = ?e,
                "Failed to queue success webhook"
            );
        }
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        fail_data: FailHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        let transaction_id = &job.job.data.transaction_id;
        error!(
            transaction_id = %transaction_id,
            error = ?fail_data.error,
            "Transaction permanently failed"
        );

        // Cancellation and infrastructure failures do not establish the on-chain outcome.
        // Keep their signed attempt available for explicit reconciliation.
        if let Ok(fingerprint) =
            crate::solana_executor::storage::solana_admission_fingerprint(&job.job.data)
        {
            match fail_data.error {
                SolanaExecutorError::TransactionFailed { .. } => {
                    self.storage.add_terminal_admission_command(
                        tx.pipeline(),
                        transaction_id,
                        &fingerprint,
                        "failed",
                        false,
                    )
                }
                SolanaExecutorError::TransactionBuildFailed { .. }
                | SolanaExecutorError::SigningFailed { .. }
                | SolanaExecutorError::MaxRetriesExceeded { .. } => {
                    self.storage.add_terminal_admission_command(
                        tx.pipeline(),
                        transaction_id,
                        &fingerprint,
                        "failed",
                        true,
                    )
                }
                _ => {}
            }
        }
        if matches!(
            fail_data.error,
            SolanaExecutorError::TransactionFailed { .. }
        ) {
            self.storage
                .add_delete_attempt_command(tx.pipeline(), transaction_id);
        }
        self.transaction_registry
            .add_remove_command(tx.pipeline(), transaction_id);

        if let Err(e) = self.queue_fail_webhook(job, fail_data, tx) {
            error!(
                transaction_id = %transaction_id,
                error = ?e,
                "Failed to queue fail webhook"
            );
        }
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Self::JobData>,
        nack_data: NackHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        let transaction_id = &job.job.data.transaction_id;

        match nack_data.error {
            // Special case: TransactionSent is actually a success, send success webhook with stage="send"
            SolanaExecutorError::TransactionSent {
                signature,
                submission_attempt_number,
            } => {
                info!(
                    transaction_id = %transaction_id,
                    signature = %signature,
                    submission_attempt_number = submission_attempt_number,
                    "Transaction sent to RPC"
                );

                #[derive(serde::Serialize, Clone)]
                #[serde(rename_all = "camelCase")]
                struct TransactionSentPayload {
                    signature: String,
                    submission_attempt_number: u32,
                }

                let payload = TransactionSentPayload {
                    signature: signature.clone(),
                    submission_attempt_number: *submission_attempt_number,
                };

                if let Err(e) = self.queue_webhook_with_custom_payload(
                    job,
                    payload,
                    crate::webhook::envelope::StageEvent::Success,
                    "send",
                    tx,
                ) {
                    error!(
                        transaction_id = %transaction_id,
                        error = ?e,
                        "Failed to queue send webhook"
                    );
                }
            }

            // Don't send webhook for NotYetConfirmed - silent retry
            SolanaExecutorError::NotYetConfirmed { .. } => {}
            SolanaExecutorError::RecoveryRequired { .. } => {
                warn!(transaction_id = %transaction_id, "Solana recovery paused; operator reconciliation required");
            }

            // For all other errors (network errors, RPC errors, etc.), send nack webhook
            _ => {
                warn!(
                    transaction_id = %transaction_id,
                    error = ?nack_data.error,
                    "Retrying after error"
                );
                if let Err(e) = self.queue_nack_webhook(job, nack_data, tx) {
                    error!(
                        transaction_id = %transaction_id,
                        error = ?e,
                        "Failed to queue nack webhook"
                    );
                }
            }
        }
    }
}

impl SolanaExecutorJobHandler {
    /// Helper to convert Solana RPC errors with context
    fn to_engine_solana_error(
        &self,
        e: &solana_rpc_client_api::client_error::Error,
        chain_id: &str,
    ) -> EngineError {
        e.to_engine_solana_error(chain_id)
    }

    /// Get priority fee at a given percentile
    /// ALWAYS NACK on error - this is a network operation that can always be retried
    async fn get_percentile_compute_unit_price(
        &self,
        rpc_client: &RpcClient,
        writable_accounts: &[Pubkey],
        percentile: u8,
        chain_id: &str,
    ) -> JobResult<u64, SolanaExecutorError> {
        let mut fee_history = rpc_client
            .get_recent_prioritization_fees(writable_accounts)
            .await
            .map_err(|e| {
                warn!(
                    chain_id = %chain_id,
                    "Failed to get priority fees"
                );
                let engine_error = self.to_engine_solana_error(&e, chain_id);
                SolanaExecutorError::PriorityFeeError {
                    inner_error: engine_error,
                }
                .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last)
            })?;

        fee_history.sort_by_key(|a| a.prioritization_fee);
        let percentile_index =
            ((percentile as f64 / 100.0) * fee_history.len() as f64).round() as usize;

        let result = if percentile_index < fee_history.len() {
            fee_history[percentile_index].prioritization_fee
        } else {
            // Fallback to max if index out of bounds
            let fallback = fee_history
                .last()
                .map(|f| f.prioritization_fee)
                .unwrap_or(0);
            warn!(
                chain_id = %chain_id,
                percentile_index = percentile_index,
                history_len = fee_history.len(),
                "Priority fee percentile out of bounds"
            );
            fallback
        };

        Ok(result)
    }

    fn get_writable_accounts(instructions: &[SolanaInstructionData]) -> Vec<Pubkey> {
        instructions
            .iter()
            .flat_map(|inst| {
                inst.accounts
                    .iter()
                    .filter(|a| a.is_writable)
                    .map(|a| a.pubkey)
            })
            .collect()
    }

    async fn get_compute_unit_price(
        &self,
        priority_fee: &SolanaPriorityFee,
        instructions: &[SolanaInstructionData],
        rpc_client: &RpcClient,
        chain_id: &str,
    ) -> JobResult<u64, SolanaExecutorError> {
        let writable_accounts = Self::get_writable_accounts(instructions);

        match priority_fee {
            SolanaPriorityFee::Auto => {
                self.get_percentile_compute_unit_price(rpc_client, &writable_accounts, 75, chain_id)
                    .await
            }
            SolanaPriorityFee::Manual {
                micro_lamports_per_unit,
            } => Ok(*micro_lamports_per_unit),
            SolanaPriorityFee::Percentile { percentile } => {
                self.get_percentile_compute_unit_price(
                    rpc_client,
                    &writable_accounts,
                    *percentile,
                    chain_id,
                )
                .await
            }
        }
    }

    /// Reconcile durable signed bytes before considering a new signature. A receipt
    /// cache miss or elapsed wall time never permits rebuilding a transaction.
    async fn execute_transaction(
        &self,
        rpc_client: &RpcClient,
        job_data: &SolanaExecutorJobData,
        lock: &TransactionLock,
        job_attempt_number: u32,
    ) -> JobResult<SolanaExecutorResult, SolanaExecutorError> {
        let transaction_id = &job_data.transaction_id;
        let chain_id_str = &job_data.transaction.execution_options.chain_id;
        let signer_address = job_data.transaction.execution_options.signer_address;
        let commitment_level = job_data
            .transaction
            .execution_options
            .commitment
            .to_commitment_level();
        let commitment = CommitmentConfig {
            commitment: commitment_level,
        };
        self.verify_lock(lock, transaction_id).await?;
        let stored_attempt = self
            .storage
            .get_attempt(transaction_id, lock)
            .await
            .map_err(|e| self.handle_redis_error(e, transaction_id))?;

        if let Some(mut attempt) = stored_attempt.clone() {
            if attempt.reconciliation_checks >= MAX_RECONCILIATION_CHECKS {
                return Err(Self::park(
                    &attempt,
                    "reconciliation budget exhausted; resume explicitly",
                ));
            }
            attempt.reconciliation_checks += 1;
            self.storage
                .update_attempt(transaction_id, &attempt, lock)
                .await
                .map_err(|e| self.handle_redis_error(e, transaction_id))?;

            // History lookup also finds landed transactions after the recent cache rotates.
            if let Some(result) = self
                .reconcile_status(rpc_client, job_data, &attempt, commitment)
                .await?
            {
                return result;
            }

            // Legacy records have no reproducible signed bytes and historically stored the
            // wrong hash for serialized inputs. They may be confirmed, never rebuilt.
            if attempt.signed_transaction.is_none() {
                return Err(Self::park(&attempt, "legacy attempt lacks signed bytes"));
            }
            let expired = if let Some(last_valid_height) = attempt.blockhash_last_valid_height {
                rpc_client
                    .get_block_height_with_commitment(CommitmentConfig::finalized())
                    .await
                    .map_err(|e| self.rpc_error(&e, chain_id_str.as_str()))?
                    > last_valid_height
            } else {
                // Serialized inputs have no trustworthy lastValidBlockHeight. A false
                // finalized validity result can also mean a newer/noncanonical hash.
                // Preserve evidence and stop rather than inventing an expiry bound.
                let valid = rpc_client
                    .is_blockhash_valid(&attempt.blockhash, commitment)
                    .await
                    .map_err(|e| self.rpc_error(&e, chain_id_str.as_str()))?;
                if !valid {
                    if let Some(result) = self
                        .reconcile_status(rpc_client, job_data, &attempt, commitment)
                        .await?
                    {
                        return result;
                    }
                    return Err(Self::park(
                        &attempt,
                        "serialized blockhash no longer valid; expiry height unknown",
                    ));
                }
                false
            };
            if !expired {
                return self
                    .broadcast_attempt(rpc_client, job_data, attempt, lock, commitment_level)
                    .await;
            }

            // Ordering matters: a transaction can land between the first status query
            // and the expiry check. Query history again AFTER finalized expiry.
            if let Some(result) = self
                .reconcile_status(rpc_client, job_data, &attempt, commitment)
                .await?
            {
                return result;
            }
            // A historical null is not proof of non-execution: a load-balanced RPC
            // can answer from a lagging or pruned history node. Even after finalized
            // expiry, refreshing a signature could execute an already-landed intent
            // twice. Preserve evidence for operator reconciliation instead.
            return Err(Self::park(
                &attempt,
                "blockhash expired but execution outcome is unknown; automatic re-signing disabled",
            ));
        } else if job_attempt_number > MAX_SEND_ATTEMPTS_WITHOUT_TRANSACTION {
            return Err(SolanaExecutorError::MaxRetriesExceeded {
                max_retries: MAX_SEND_ATTEMPTS_WITHOUT_TRANSACTION,
            }
            .fail());
        }

        let submission_attempt_number = 1;
        // Serialized transactions preserve their caller-provided hash. Fetching a new
        // blockhash here and recording it would associate unrelated validity metadata.
        let (recent_blockhash, last_valid_height) = match &job_data.transaction.input {
            SolanaTransactionInput::Instructions(_) => {
                let (hash, height) = rpc_client
                    .get_latest_blockhash_with_commitment(commitment)
                    .await
                    .map_err(|e| self.rpc_error(&e, chain_id_str.as_str()))?;
                (hash, Some(height))
            }
            SolanaTransactionInput::Serialized(_) => (solana_sdk::hash::Hash::default(), None),
        };

        // Build transaction - handle execution options differently for instructions vs serialized
        let versioned_tx = match &job_data.transaction.input {
            engine_solana_core::transaction::SolanaTransactionInput::Instructions(i) => {
                // For instruction-based transactions: calculate priority fees and apply execution options
                let compute_unit_price = if let Some(priority_fee) =
                    &job_data.transaction.execution_options.priority_fee
                {
                    Some(
                        self.get_compute_unit_price(
                            priority_fee,
                            &i.instructions,
                            rpc_client,
                            chain_id_str.as_str(),
                        )
                        .await?,
                    )
                } else {
                    None
                };

                // Add memo instruction with transaction_id for unique signatures
                // This ensures that even with the same blockhash, each resubmission has a unique signature
                let memo_data = format!("thirdweb-engine:{}", transaction_id);
                let memo_ix = build_memo(&spl_memo_interface::v3::id(), memo_data.as_bytes(), &[]);

                let mut instructions_with_memo = i.instructions.clone();
                let memo_data_base64 =
                    base64::engine::general_purpose::STANDARD.encode(memo_data.as_bytes());
                instructions_with_memo.push(SolanaInstructionData {
                    program_id: memo_ix.program_id,
                    accounts: vec![],
                    data: memo_data_base64,
                    encoding: InstructionDataEncoding::Base64,
                });

                let solana_tx = SolanaTransaction {
                    input: engine_solana_core::transaction::SolanaTransactionInput::new_with_instructions(instructions_with_memo),
                    compute_unit_limit: job_data.transaction.execution_options.compute_unit_limit,
                    compute_unit_price,
                };

                solana_tx
                    .to_versioned_transaction(signer_address, recent_blockhash)
                    .map_err(|e| {
                        error!(
                            transaction_id = %transaction_id,
                            error = %e,
                            "Failed to build transaction from instructions"
                        );
                        SolanaExecutorError::TransactionBuildFailed {
                            inner_error: e.to_string(),
                        }
                        .fail()
                    })?
            }
            engine_solana_core::transaction::SolanaTransactionInput::Serialized { .. } => {
                // For serialized transactions: ignore execution options to avoid invalidating signatures
                let solana_tx = SolanaTransaction {
                    input: job_data.transaction.input.clone(),
                    compute_unit_limit: None,
                    compute_unit_price: None,
                };

                solana_tx
                    .to_versioned_transaction(signer_address, recent_blockhash)
                    .map_err(|e| {
                        error!(
                            transaction_id = %transaction_id,
                            error = %e,
                            "Failed to deserialize compiled transaction"
                        );
                        SolanaExecutorError::TransactionBuildFailed {
                            inner_error: e.to_string(),
                        }
                        .fail()
                    })?
            }
        };

        let actual_blockhash = *versioned_tx.message.recent_blockhash();
        if matches!(
            job_data.transaction.input,
            SolanaTransactionInput::Serialized(_)
        ) {
            if versioned_tx.uses_durable_nonce() {
                return Err(SolanaExecutorError::TransactionBuildFailed {
                    inner_error: "Durable nonce transactions require a separate recovery policy"
                        .into(),
                }
                .fail());
            }
            if !rpc_client
                .is_blockhash_valid(&actual_blockhash, commitment)
                .await
                .map_err(|e| self.rpc_error(&e, chain_id_str.as_str()))?
            {
                return Err(SolanaExecutorError::TransactionBuildFailed {
                    inner_error:
                        "Serialized transaction blockhash is not valid at the requested commitment"
                            .into(),
                }
                .fail());
            }
        }

        let signed_tx = self
            .solana_signer
            .sign_transaction(versioned_tx, signer_address, &job_data.signing_credential)
            .await
            .map_err(|e| {
                error!(
                    transaction_id = %transaction_id,
                    error = ?e,
                    "Failed to sign transaction"
                );
                SolanaExecutorError::SigningFailed { inner_error: e }.fail()
            })?;

        let signature = *signed_tx.signatures.first().ok_or_else(|| {
            SolanaExecutorError::TransactionBuildFailed {
                inner_error: "Signed transaction has no signature".into(),
            }
            .fail()
        })?;
        let bytes = encode_transaction_wire(&signed_tx).map_err(|e| {
            SolanaExecutorError::TransactionBuildFailed {
                inner_error: e.to_string(),
            }
            .fail()
        })?;
        let attempt = SolanaTransactionAttempt::new(
            signature,
            actual_blockhash,
            last_valid_height,
            submission_attempt_number,
            base64::engine::general_purpose::STANDARD.encode(bytes),
        );
        if !self
            .storage
            .store_attempt_if_not_exists(transaction_id, &attempt, lock)
            .await
            .map_err(|e| self.handle_redis_error(e, transaction_id))?
        {
            return Err(SolanaExecutorError::LockLost
                .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last));
        }
        self.broadcast_attempt(rpc_client, job_data, attempt, lock, commitment_level)
            .await
    }

    fn park(attempt: &SolanaTransactionAttempt, reason: &str) -> JobError<SolanaExecutorError> {
        SolanaExecutorError::RecoveryRequired {
            signature: attempt.signature.to_string(),
            reason: reason.into(),
        }
        .nack(Some(RECOVERY_PARK_DELAY), RequeuePosition::Last)
    }

    fn rpc_error(
        &self,
        error: &solana_rpc_client_api::client_error::Error,
        chain: &str,
    ) -> JobError<SolanaExecutorError> {
        SolanaExecutorError::RpcError {
            inner_error: self.to_engine_solana_error(error, chain),
        }
        .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last)
    }

    /// Some means a status is known, including a not-yet-final status. Never rebuild
    /// while any execution is visible, even if it has not reached requested commitment.
    async fn reconcile_status(
        &self,
        rpc_client: &RpcClient,
        job_data: &SolanaExecutorJobData,
        attempt: &SolanaTransactionAttempt,
        commitment: CommitmentConfig,
    ) -> JobResult<Option<JobResult<SolanaExecutorResult, SolanaExecutorError>>, SolanaExecutorError>
    {
        let statuses = rpc_client
            .get_signature_statuses_with_history(&[attempt.signature])
            .await
            .map_err(|e| {
                self.rpc_error(&e, job_data.transaction.execution_options.chain_id.as_str())
            })?;
        if statuses.value.len() != 1 {
            return Err(SolanaExecutorError::InternalError {
                message: "RPC returned wrong status count".into(),
            }
            .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last));
        }
        let Some(status) = &statuses.value[0] else {
            return Ok(None);
        };
        if status.err != status.status.clone().err() {
            return Err(SolanaExecutorError::InternalError {
                message: "RPC returned inconsistent transaction status".into(),
            }
            .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last));
        }
        if !status.satisfies_commitment(commitment) {
            return Ok(Some(Err(SolanaExecutorError::NotYetConfirmed {
                signature: attempt.signature.to_string(),
            }
            .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last))));
        }
        if let Some(error) = &status.err {
            return Ok(Some(Err(SolanaExecutorError::TransactionFailed {
                reason: format!("{error:?}"),
            }
            .fail())));
        }
        let details = rpc_client
            .get_transaction_with_config(
                &attempt.signature,
                RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::Json),
                    commitment: Some(commitment),
                    max_supported_transaction_version: Some(0),
                },
            )
            .await
            .map_err(|e| {
                self.rpc_error(&e, job_data.transaction.execution_options.chain_id.as_str())
            })?;
        // A provider returning inconsistent receipt state must not turn a failed
        // execution into success, or bind a result from a different slot.
        let expected_signature = attempt.signature.to_string();
        let signature_matches = matches!(
            &details.transaction.transaction,
            solana_transaction_status::EncodedTransaction::Json(transaction)
                if transaction.signatures.first() == Some(&expected_signature)
        );
        if !signature_matches
            || details.slot != status.slot
            || details
                .transaction
                .meta
                .as_ref()
                .is_none_or(|meta| meta.err.is_some())
        {
            return Err(SolanaExecutorError::InternalError {
                message: "Inconsistent transaction receipt; retry reconciliation".into(),
            }
            .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last));
        }
        Ok(Some(Ok(SolanaExecutorResult {
            transaction_id: job_data.transaction_id.clone(),
            signature: attempt.signature.to_string(),
            signer_address: job_data
                .transaction
                .execution_options
                .signer_address
                .to_string(),
            chain_id: job_data
                .transaction
                .execution_options
                .chain_id
                .as_str()
                .into(),
            submission_attempt_number: attempt.submission_attempt_number,
            slot: details.slot,
            block_time: details.block_time,
            transaction: details.transaction,
        })))
    }

    async fn broadcast_attempt(
        &self,
        rpc_client: &RpcClient,
        job_data: &SolanaExecutorJobData,
        mut attempt: SolanaTransactionAttempt,
        lock: &TransactionLock,
        commitment: CommitmentLevel,
    ) -> JobResult<SolanaExecutorResult, SolanaExecutorError> {
        let now = crate::metrics::current_timestamp_ms();
        if attempt.broadcast_attempts >= MAX_BROADCASTS_PER_ATTEMPT
            || (attempt.last_broadcast_at != 0
                && now.saturating_sub(attempt.last_broadcast_at) < 2000)
        {
            return Err(SolanaExecutorError::NotYetConfirmed {
                signature: attempt.signature.to_string(),
            }
            .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last));
        }
        let wire = attempt
            .signed_transaction
            .as_ref()
            .ok_or_else(|| Self::park(&attempt, "missing signed bytes"))?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(wire)
            .map_err(|_| Self::park(&attempt, "invalid persisted wire encoding"))?;
        let decoded = decode_transaction_wire(&bytes)
            .map_err(|_| Self::park(&attempt, "invalid persisted transaction"))?;
        if decoded.signatures.first() != Some(&attempt.signature)
            || decoded.message.recent_blockhash() != &attempt.blockhash
            || decoded.verify_and_hash_message().is_err()
        {
            return Err(Self::park(
                &attempt,
                "persisted transaction identity does not match its attempt",
            ));
        }
        let wire = wire.clone();
        // Count before I/O: a crash can consume a send allowance, never make it unbounded.
        attempt.broadcast_attempts += 1;
        attempt.last_broadcast_at = now;
        self.storage
            .update_attempt(&job_data.transaction_id, &attempt, lock)
            .await
            .map_err(|e| self.handle_redis_error(e, &job_data.transaction_id))?;
        self.verify_lock(lock, &job_data.transaction_id).await?;
        let config = RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(commitment),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(0),
            ..Default::default()
        };
        let sent: Result<String, _> = rpc_client
            .send(RpcRequest::SendTransaction, json!([wire, config]))
            .await;
        match sent {
            Ok(signature) if signature == attempt.signature.to_string() => {
                Err(SolanaExecutorError::TransactionSent {
                    signature,
                    submission_attempt_number: attempt.submission_attempt_number,
                }
                .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last))
            }
            Ok(_) => Err(SolanaExecutorError::InternalError {
                message: "RPC returned a different signature; reconcile persisted transaction"
                    .into(),
            }
            .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last)),
            // Even AlreadyProcessed/preflight errors can follow an accepted send whose
            // response was lost. Only status and expiry establish a terminal outcome.
            Err(e) => Err(SolanaExecutorError::SendFailed {
                inner_error: self.to_engine_solana_error(
                    &e,
                    job_data.transaction.execution_options.chain_id.as_str(),
                ),
            }
            .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last)),
        }
    }

    async fn verify_lock(
        &self,
        lock: &TransactionLock,
        transaction_id: &str,
    ) -> JobResult<(), SolanaExecutorError> {
        match lock.still_held().await {
            Ok(true) => Ok(()),
            Ok(false) => {
                warn!(transaction_id = %transaction_id, "Lost lock");
                Err(SolanaExecutorError::LockLost
                    .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last))
            }
            Err(e) => Err(SolanaExecutorError::InternalError {
                message: format!("Failed to check lock: {e}"),
            }
            .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last)),
        }
    }

    fn handle_redis_error(
        &self,
        error: twmq::redis::RedisError,
        transaction_id: &str,
    ) -> JobError<SolanaExecutorError> {
        if error.to_string().contains("lock lost") {
            warn!(transaction_id = %transaction_id, "Lock lost during Redis operation");
            SolanaExecutorError::LockLost
                .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last)
        } else {
            SolanaExecutorError::InternalError {
                message: format!("Redis error: {error}"),
            }
            .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last)
        }
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod recovery_tests;
