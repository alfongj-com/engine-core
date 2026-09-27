use alloy::{
    primitives::B256,
    providers::{Provider, RootProvider},
    rpc::types::BlockNumberOrTag,
};
use engine_core::{
    chain::Chain,
    error::{AlloyRpcErrorToEngineError, EngineError},
    finality::{FinalityAssessment, FinalityPolicy, assess_receipt_finality},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::{
    FlashblocksTransactionCount,
    eoa::{
        EoaExecutorStore,
        store::{
            CleanupReport, ConfirmedTransaction, SubmittedTransactionDehydrated,
            TransactionStoreError,
        },
        worker::{
            EoaExecutorWorker,
            error::{EoaExecutorWorkerError, should_update_balance_threshold},
        },
    },
};

const FINALITY_POLL_INTERVAL_MS: u64 = 5_000;
// Four times the 50 tx/s target leaves capacity for provisional/absent reads
// and catch-up. These are read budgets, not a chain throughput guarantee.
const MAX_RECEIPTS_PER_POLL: usize = 1024;
const RECEIPT_RPC_CONCURRENCY: usize = 32;
const FINALITY_RPC_CONCURRENCY: usize = 8;

const NONCE_STALL_LIMIT_MS: u64 = 60_000; // 1 minute in milliseconds - after this time, attempt gas bump

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmedTransactionWithRichReceipt {
    pub nonce: u64,
    pub transaction_hash: String,
    pub transaction_id: String,
    pub receipt: alloy::rpc::types::TransactionReceipt,
}

/// Missing or failed receipt reads are unknown outcomes. The store may only
/// requeue a different intent when a known receipt proves same-nonce replacement.
async fn fetch_confirmed_transaction_receipts(
    provider: &RootProvider,
    submitted_txs: Vec<SubmittedTransactionDehydrated>,
) -> Vec<ConfirmedTransactionWithRichReceipt> {
    let receipt_futures = submitted_txs.into_iter().filter_map(|tx| {
        let hash = match tx.transaction_hash.parse::<B256>() {
            Ok(hash) => hash,
            Err(_) => {
                tracing::warn!(transaction_hash = tx.transaction_hash, "Invalid stored hash; keeping outcome unresolved");
                return None;
            }
        };
        Some(async move {
            match provider.get_transaction_receipt(hash).await {
                Ok(Some(receipt)) if receipt.transaction_hash == hash => Some(ConfirmedTransactionWithRichReceipt {
                    nonce: tx.nonce,
                    transaction_hash: tx.transaction_hash.clone(),
                    transaction_id: tx.transaction_id.clone(),
                    receipt,
                }),
                Ok(Some(_)) => {
                    tracing::warn!(transaction_hash = tx.transaction_hash, "RPC returned a receipt for a different hash; keeping outcome unresolved");
                    None
                }
                Ok(None) => None,
                Err(error) => {
                    tracing::warn!(transaction_hash = tx.transaction_hash, error = %engine_core::error::rpc_error_diagnostic(&error), "Receipt query failed; keeping outcome unresolved");
                    None
                }
            }
        })
    });
    futures::stream::iter(receipt_futures)
        .buffer_unordered(RECEIPT_RPC_CONCURRENCY)
        .filter_map(|receipt| async move { receipt })
        .collect()
        .await
}

/// A scheduling hint, never settlement evidence. Avoid querying every receipt
/// while the policy head still predates its sender nonce. A stale-low answer
/// delays work; a false-high answer still passes the independent receipt gate.
/// Unsupported finality tags fail closed instead of silently polling `latest`.
async fn receipt_candidate_count(
    chain: &impl Chain,
    sender: alloy::primitives::Address,
    latest_count: u64,
) -> Result<u64, EngineError> {
    let tag = match chain.finality_policy() {
        FinalityPolicy::Finalized => BlockNumberOrTag::Finalized,
        FinalityPolicy::Depth { confirmations: 0 } => return Ok(latest_count),
        FinalityPolicy::Depth { confirmations } => {
            let head = chain
                .provider()
                .get_block_number()
                .await
                .map_err(|error| error.to_engine_error(chain))?;
            let Some(height) = head.checked_sub(confirmations) else {
                return Ok(0);
            };
            BlockNumberOrTag::Number(height)
        }
    };
    chain
        .provider()
        .get_transaction_count(sender)
        .block_id(tag.into())
        .await
        .map_err(|error| error.to_engine_error(chain))
}

#[cfg(test)]
#[path = "receipt_tests.rs"]
mod receipt_tests;

#[path = "gap_replay.rs"]
mod gap_replay;

impl<C: Chain> EoaExecutorWorker<C> {
    // ========== CONFIRM FLOW ==========
    #[tracing::instrument(skip_all, fields(worker_id = self.store.worker_id))]
    pub async fn confirm_flow(&self) -> Result<CleanupReport, EoaExecutorWorkerError> {
        // Get fresh on-chain transaction counts (both latest and preconfirmed)
        let transaction_counts = self
            .chain
            .provider()
            .get_transaction_counts_with_flashblocks_support(
                self.eoa,
                self.chain.use_pending_for_preconfirmation(),
            )
            .await
            .map_err(|e| {
                let engine_error = e.to_engine_error(&self.chain);
                EoaExecutorWorkerError::RpcError {
                    message: format!("Failed to get transaction counts: {engine_error}"),
                    inner_error: engine_error,
                }
            })?;

        if self.store.is_manual_reset_scheduled().await? {
            tracing::info!("Manual reset scheduled, executing now");
            match self.store.reset_nonces(transaction_counts.latest).await {
                Err(TransactionStoreError::UnresolvedNonceReservations) => {
                    tracing::warn!("Deferring manual nonce reset until signed attempts settle");
                }
                result => result?,
            }
        }

        let cached_transaction_count = match self.store.get_cached_transaction_count().await {
            Err(e) => match e {
                TransactionStoreError::NonceSyncRequired { .. } => {
                    tracing::warn!(
                        cached_transaction_count = transaction_counts.latest,
                        "Nonce sync required, store was uninitialized, updating cached transaction count with current chain transaction count"
                    );
                    self.store
                        .update_cached_transaction_count(transaction_counts.latest)
                        .await?;
                    transaction_counts.latest
                }
                _ => return Err(e.into()),
            },
            Ok(cached_nonce) => cached_nonce,
        };

        let submitted_count = self.store.get_submitted_transactions_count().await?;

        // A rollback or a stalled submitted suffix can contain many nonce gaps.
        // Recover existing wires on a separate bounded cadence; neither an RPC
        // error nor an accepted rebroadcast is counted as new useful work.
        self.replay_submitted_gap(transaction_counts.latest, cached_transaction_count)
            .await?;

        // no nonce progress
        if transaction_counts.preconfirmed <= cached_transaction_count {
            let current_health = self.get_eoa_health().await?;
            let now = EoaExecutorStore::now();
            // No nonce progress - check if we should attempt gas bumping for stalled nonce
            let time_since_movement = now.saturating_sub(current_health.last_nonce_movement_at);

            // Check if EOA has sufficient funds before attempting gas bump
            let is_out_of_funds = current_health.balance <= current_health.balance_threshold;

            // if there are waiting transactions and EOA has sufficient funds, we can attempt a gas bump
            if time_since_movement > NONCE_STALL_LIMIT_MS && submitted_count > 0 {
                if is_out_of_funds {
                    tracing::warn!(
                        time_since_movement = time_since_movement,
                        stall_timeout = NONCE_STALL_LIMIT_MS,
                        balance = ?current_health.balance,
                        balance_threshold = ?current_health.balance_threshold,
                        "Nonce has been stalled, but EOA is out of funds - skipping gas bump"
                    );
                } else {
                    tracing::info!(
                        time_since_movement = time_since_movement,
                        stall_timeout = NONCE_STALL_LIMIT_MS,
                        current_chain_nonce = transaction_counts.preconfirmed,
                        cached_transaction_count = cached_transaction_count,
                        "Nonce has been stalled, attempting gas bump"
                    );

                    // Attempt gas bump for the next expected nonce
                    if let Err(e) = self
                        .attempt_gas_bump_for_stalled_nonce(transaction_counts.preconfirmed)
                        .await
                    {
                        tracing::warn!(
                            error = ?e,
                            bumped_nonce = transaction_counts.preconfirmed,
                            preconfirmed_nonce = transaction_counts.preconfirmed,
                            latest_nonce = transaction_counts.latest,
                            "Gas bump failed; preserving submitted intent for reconciliation"
                        );
                    }
                }
            }

            // Check if EOA is stuck and record metric using the clean EoaMetrics abstraction
            let time_since_movement_seconds = time_since_movement as f64 / 1000.0;
            if self.store.eoa_metrics.is_stuck(time_since_movement) {
                tracing::warn!(
                    time_since_movement = time_since_movement,
                    stuck_threshold = self.store.eoa_metrics.stuck_threshold_seconds,
                    eoa = ?self.eoa,
                    chain_id = self.chain_id,
                    out_of_funds = is_out_of_funds,
                    "EOA is stuck - nonce hasn't moved for too long"
                );

                // Record stuck EOA metric (low cardinality - only problematic EOAs) with out_of_funds status
                self.store.eoa_metrics.record_stuck_eoa(
                    self.eoa,
                    self.chain_id,
                    time_since_movement_seconds,
                    is_out_of_funds,
                );
            }

            tracing::debug!("No nonce progress, still going ahead with confirm flow");
            // return Ok(CleanupReport::default());
        }

        tracing::info!(
            current_chain_nonce_latest = transaction_counts.latest,
            current_chain_nonce_preconfirmed = transaction_counts.preconfirmed,
            cached_transaction_count = cached_transaction_count,
            "Processing confirmations"
        );

        // Keep the consumed-count high-water mark as an allocator floor. A
        // lower observation activates exact-wire recovery above; it never opens
        // historical nonce slots for different intents.
        if transaction_counts.latest < cached_transaction_count {
            tracing::warn!(
                latest = transaction_counts.latest,
                cached_transaction_count,
                "Observed nonce rollback; preserving allocator floor and replaying retained wires"
            );
        } else if transaction_counts.latest > cached_transaction_count {
            self.store
                .update_cached_transaction_count(transaction_counts.latest)
                .await?;
        }

        // Settlement polling has its own cadence; provisional nonce observation
        // above still frees the mempool window on every send cycle.
        let mut health = self.get_eoa_health().await?;
        let now = EoaExecutorStore::now();
        if now.saturating_sub(health.last_finality_poll_at) < FINALITY_POLL_INTERVAL_MS {
            return Ok(CleanupReport::default());
        }
        // Bound failed/unknown continuity reads by the same polling cadence.
        health.last_finality_poll_at = now;
        self.store.update_health_data(&health).await?;
        // Check prior settled history even if this wallet currently has no
        // receipt candidates (for example latest nonce fell after a deep reorg).
        crate::finality::check_continuity(&self.chain)
            .await
            .map_err(|inner_error| EoaExecutorWorkerError::RpcError {
                message: "Finality checkpoint continuity unavailable".into(),
                inner_error,
            })?;
        let candidate_count =
            receipt_candidate_count(&self.chain, self.eoa, transaction_counts.latest)
                .await
                .map_err(|inner_error| EoaExecutorWorkerError::RpcError {
                    message: "Finality candidate nonce unavailable".into(),
                    inner_error,
                })?;
        let (waiting_txs, next_offset) = self
            .store
            .get_submitted_transaction_page(
                candidate_count,
                health.finality_scan_offset,
                MAX_RECEIPTS_PER_POLL,
            )
            .await?;
        health.finality_scan_offset = next_offset;
        self.store.update_health_data(&health).await?;
        if waiting_txs.is_empty() {
            return Ok(CleanupReport::default());
        }

        // Fetch receipts and categorize transactions
        let confirmed_txs =
            fetch_confirmed_transaction_receipts(self.chain.provider(), waiting_txs).await;

        // Process confirmed transactions
        let mut successes = Vec::new();
        // Block evidence is shared only within this cycle. Distinct blocks are
        // checked concurrently with a separate bound; no stale/global cache can
        // suppress continuity checks or cross endpoint/policy boundaries.
        let mut blocks = std::collections::HashMap::new();
        for tx in &confirmed_txs {
            use alloy::consensus::TxReceipt;
            if tx
                .receipt
                .inner
                .status_or_post_state()
                .as_eip658()
                .is_some()
            {
                blocks
                    .entry((tx.receipt.block_number, tx.receipt.block_hash))
                    .or_insert_with(|| tx.receipt.clone());
            }
        }
        let block_assessments: std::collections::HashMap<_, _> =
            futures::stream::iter(blocks.into_iter().map(|(key, receipt)| async move {
                (
                    key,
                    crate::finality::assess(&self.chain, receipt.transaction_hash, &receipt).await,
                )
            }))
            .buffer_unordered(FINALITY_RPC_CONCURRENCY)
            .collect()
            .await;
        for tx in confirmed_txs {
            use alloy::consensus::TxReceipt;
            if tx
                .receipt
                .inner
                .status_or_post_state()
                .as_eip658()
                .is_none()
            {
                continue;
            }
            let expected_hash = tx.receipt.transaction_hash;
            let key = (tx.receipt.block_number, tx.receipt.block_hash);
            let Some(assessment) = block_assessments.get(&key) else {
                continue;
            };
            let finality = match assessment {
                Ok(FinalityAssessment::Finalized(evidence)) => evidence.clone(),
                Ok(_) => continue, // Inclusion/orphaning never authorizes cleanup or a new nonce.
                Err(error) => {
                    tracing::warn!(transaction_id = tx.transaction_id, error = %error,
                        "Finality unavailable; preserving submitted identity");
                    continue;
                }
            };
            let (kind, id) = if tx.transaction_id == crate::eoa::store::NO_OP_TRANSACTION_ID {
                (
                    "eoa_noop",
                    format!(
                        "__engine_noop:{}:{:#x}:{}",
                        self.chain_id, self.eoa, tx.nonce
                    ),
                )
            } else {
                ("eoa", tx.transaction_id.clone())
            };
            if let Err(error) = crate::finality::validate_eoa_confirmation(
                kind,
                &id,
                self.chain_id,
                self.eoa,
                tx.nonce,
                expected_hash,
            )
            .await
            {
                tracing::warn!(transaction_id = tx.transaction_id, error = %error,
                    "Receipt does not match the durable reservation; retaining submitted evidence");
                continue;
            }
            crate::finality::record_evm_terminal(
                kind,
                &id,
                self.chain_id,
                expected_hash,
                tx.receipt.status(),
                &finality,
            )
            .await
            .map_err(crate::recovery::eoa_error)?;
            let receipt_data = match serde_json::to_string(&tx.receipt) {
                Ok(receipt_json) => receipt_json,
                Err(e) => {
                    tracing::warn!(
                        transaction_id = ?tx.transaction_id,
                        hash = tx.transaction_hash,
                        error = ?e,
                        "Failed to serialize receipt as JSON, using debug format"
                    );
                    format!("{:?}", tx.receipt)
                }
            };

            tracing::info!(
                transaction_id = ?tx.transaction_id,
                nonce = tx.nonce,
                hash = tx.transaction_hash,
                "Transaction confirmed"
            );

            successes.push(ConfirmedTransaction {
                nonce: tx.nonce,
                transaction_hash: tx.transaction_hash,
                transaction_id: tx.transaction_id,
                receipt: tx.receipt,
                receipt_serialized: receipt_data,
                finality,
            });
        }

        if successes.is_empty() {
            return Ok(CleanupReport::default());
        }
        let report = self
            .store
            .clean_submitted_transactions(
                &successes,
                transaction_counts,
                self.webhook_queue.clone(),
            )
            .await?;

        // If we confirmed any transactions, update the health timestamp even if latest hasn't caught up yet
        // This handles the case where flashblocks preconfirmed is ahead of latest
        if !successes.is_empty() {
            // Update health timestamp to reflect nonce movement from confirmations
            if let Ok(mut health) = self.get_eoa_health().await {
                let now = EoaExecutorStore::now();
                health.last_nonce_movement_at = now;
                health.last_confirmation_at = now;
                if let Err(e) = self.store.update_health_data(&health).await {
                    tracing::warn!(
                        error = ?e,
                        "Failed to update health timestamp after confirming transactions"
                    );
                }
            }
        }

        Ok(report)
    }

    // ========== GAS BUMP METHODS ==========

    /// Attempt to gas bump a stalled transaction for the next expected nonce
    async fn attempt_gas_bump_for_stalled_nonce(
        &self,
        expected_nonce: u64,
    ) -> Result<bool, EoaExecutorWorkerError> {
        tracing::info!(
            nonce = expected_nonce,
            "Attempting gas bump for stalled nonce"
        );

        // Get all transaction IDs for this nonce
        let submitted_transactions = self
            .store
            .get_submitted_transactions_for_nonce(expected_nonce)
            .await?;

        // A consumed nonce may briefly disagree with a receipt backend. Never
        // replace a canonically included attempt just because latest-count is stale.
        for attempt in &submitted_transactions {
            let Ok(hash) = attempt.transaction_hash.parse::<B256>() else {
                return Ok(false);
            };
            match self.chain.provider().get_transaction_receipt(hash).await {
                Ok(Some(receipt)) => {
                    match assess_receipt_finality(&self.chain, hash, &receipt).await {
                        Ok(FinalityAssessment::Orphaned) => {}
                        Ok(FinalityAssessment::Pending { canonical: false }) => {}
                        _ => return Ok(false),
                    }
                }
                Ok(None) => {}
                Err(_) => return Ok(false),
            }
        }

        // Load transaction data for all IDs and find the newest one
        let newest_transaction = if submitted_transactions.len() == 1 {
            submitted_transactions.first()
        } else {
            submitted_transactions
                .iter()
                .max_by_key(|tx| tx.submitted_at)
        };

        let newest_transaction_data = match newest_transaction {
            Some(tx) => self.store.get_transaction_data(&tx.transaction_id).await?,
            None => None,
        };

        if let Some(newest_transaction_data) = newest_transaction_data {
            tracing::info!(
                transaction_id = ?newest_transaction_data.transaction_id,
                nonce = expected_nonce,
                "Found newest transaction for gas bump"
            );

            let time_since_queuing =
                EoaExecutorStore::now().saturating_sub(newest_transaction_data.created_at);

            if time_since_queuing < NONCE_STALL_LIMIT_MS {
                tracing::warn!(
                    transaction_id = ?newest_transaction_data.transaction_id,
                    nonce = expected_nonce,
                    time_since_queuing = time_since_queuing,
                    stall_timeout = NONCE_STALL_LIMIT_MS,
                    "Transaction has not been queued for long enough, skipping gas bump"
                );
                return Ok(false);
            }

            // Get the latest attempt to extract gas values from
            // Build typed transaction -> manually bump -> sign
            let typed_tx = match self
                .build_typed_transaction(&newest_transaction_data.user_request, expected_nonce)
                .await
            {
                Ok(tx) => tx,
                Err(e) => {
                    // Check if this is a balance threshold issue during simulation
                    if let EoaExecutorWorkerError::TransactionSimulationFailed {
                        inner_error, ..
                    } = &e
                    {
                        if should_update_balance_threshold(inner_error)
                            && let Err(e) = self.update_balance_threshold().await
                        {
                            tracing::error!("Failed to update balance threshold: {}", e);
                        }
                    } else if let EoaExecutorWorkerError::RpcError { inner_error, .. } = &e
                        && should_update_balance_threshold(inner_error)
                        && let Err(e) = self.update_balance_threshold().await
                    {
                        tracing::error!("Failed to update balance threshold: {}", e);
                    }
                    // Check if nonce has moved ahead since we started the gas bump
                    // This handles the race condition where the original transaction
                    // confirmed between our initial nonce check and the gas bump attempt
                    let fresh_transaction_counts = match self
                        .chain
                        .provider()
                        .get_transaction_counts_with_flashblocks_support(
                            self.eoa,
                            self.chain.use_pending_for_preconfirmation(),
                        )
                        .await
                    {
                        Ok(counts) => counts,
                        Err(rpc_error) => {
                            tracing::warn!(
                                transaction_id = ?newest_transaction_data.transaction_id,
                                nonce = expected_nonce,
                                error = ?e,
                                rpc_check_error = %engine_core::error::rpc_error_diagnostic(&rpc_error),
                                "Failed to build typed transaction for gas bump and also failed to check nonce"
                            );
                            return Err(e);
                        }
                    };

                    // If nonce has moved ahead, the transaction likely confirmed
                    // Break out of gas bump flow and let regular confirmation flow handle it
                    if fresh_transaction_counts.preconfirmed > expected_nonce {
                        tracing::info!(
                            transaction_id = ?newest_transaction_data.transaction_id,
                            nonce = expected_nonce,
                            current_preconfirmed_nonce = fresh_transaction_counts.preconfirmed,
                            current_latest_nonce = fresh_transaction_counts.latest,
                            "Gas bump simulation failed but nonce has moved ahead - transaction likely confirmed. Breaking out of gas bump flow."
                        );

                        if let Ok(mut health) = self.get_eoa_health().await {
                            let now = EoaExecutorStore::now();
                            health.last_nonce_movement_at = now;
                            health.last_confirmation_at = now;
                            if let Err(update_err) = self.store.update_health_data(&health).await {
                                tracing::warn!(
                                    error = ?update_err,
                                    nonce = expected_nonce,
                                    "Detected nonce movement but failed to refresh health data"
                                );
                            }
                        }
                        // Return success to break out of gas bump flow
                        // The regular confirmation flow will handle the confirmed transaction
                        return Ok(true);
                    }

                    tracing::warn!(
                        transaction_id = ?newest_transaction_data.transaction_id,
                        nonce = expected_nonce,
                        current_preconfirmed_nonce = fresh_transaction_counts.preconfirmed,
                        current_latest_nonce = fresh_transaction_counts.latest,
                        error = ?e,
                        "Failed to build typed transaction for gas bump and nonce has not moved ahead"
                    );
                    return Err(e);
                }
            };
            let Some(bumped_typed_tx) = self.apply_gas_bump_to_typed_transaction(
                typed_tx,
                120,
                &newest_transaction_data.user_request,
            ) else {
                tracing::info!(
                    transaction_id = ?newest_transaction_data.transaction_id,
                    nonce = expected_nonce,
                    "Caller fee ceiling leaves no permitted increase; preserving submitted intent"
                );
                return Ok(false);
            };
            let bumped_tx = match self
                .sign_transaction(
                    bumped_typed_tx,
                    &newest_transaction_data.user_request.signing_credential,
                )
                .await
            {
                Ok(tx) => tx,
                Err(e) => {
                    tracing::warn!(
                        transaction_id = ?newest_transaction_data.transaction_id,
                        nonce = expected_nonce,
                        error = ?e,
                        "Failed to sign transaction for gas bump"
                    );
                    return Err(e);
                }
            };

            // Record the gas bump attempt
            self.store
                .add_gas_bump_attempt(
                    &SubmittedTransactionDehydrated {
                        nonce: expected_nonce,
                        transaction_hash: bumped_tx.hash().to_string(),
                        transaction_id: newest_transaction_data.transaction_id.clone(),
                        submitted_at: EoaExecutorStore::now(),
                        queued_at: newest_transaction_data.created_at,
                    },
                    bumped_tx.clone(),
                )
                .await?;

            // Send the bumped transaction
            crate::recovery::before_eoa(&newest_transaction_data.user_request, &bumped_tx).await?;
            let tx_envelope = bumped_tx.into();
            match self.chain.provider().send_tx_envelope(tx_envelope).await {
                Ok(_) => {
                    tracing::info!(
                        transaction_id = ?newest_transaction_data.transaction_id,
                        nonce = expected_nonce,
                        "Successfully sent gas bumped transaction"
                    );
                    Ok(true)
                }
                Err(e) => {
                    tracing::warn!(
                        transaction_id = ?newest_transaction_data.transaction_id,
                        nonce = expected_nonce,
                        error = %engine_core::error::rpc_error_diagnostic(&e),
                        "Failed to send gas bumped transaction"
                    );
                    // Don't fail the worker, just log the error
                    Err(EoaExecutorWorkerError::RpcError {
                        message: String::from("Failed to send gas bumped transaction"),
                        inner_error: e.to_engine_error(&self.chain),
                    })
                }
            }
        } else {
            tracing::warn!(
                nonce = expected_nonce,
                "Stalled nonce has no original request; preserving recovery evidence for reconciliation"
            );
            Ok(false)
        }
    }
}
