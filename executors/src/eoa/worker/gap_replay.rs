//! Bounded replay of retained EOA wires after a nonce rollback or long stall.
//! Redis holds scheduling hints; the independent journal supplies the only wire.
use alloy::{
    consensus::{Transaction, TxEnvelope},
    eips::eip2718::{Decodable2718, Encodable2718},
    primitives::B256,
    providers::Provider,
};
use engine_core::{chain::Chain, recovery};
use serde::{Deserialize, Serialize};
use twmq::redis::AsyncCommands;

use super::{EoaExecutorStore, EoaExecutorWorker, EoaExecutorWorkerError, NONCE_STALL_LIMIT_MS};
use crate::eoa::{EoaTransactionRequest, store::SubmittedTransactionDehydrated};

const GAP_REPLAY_INTERVAL_MS: u64 = 5_000;
const GAP_REPLAY_NONCES: u64 = 32;

#[derive(Debug, Serialize, Deserialize)]
struct GapReplayState {
    /// Fixed at activation. New admissions never extend a recovery window.
    through_nonce: u64,
    latest_observed: u64,
    next_at: u64,
}

fn invalid(message: impl std::fmt::Display) -> EoaExecutorWorkerError {
    crate::recovery::eoa_error(message)
}

impl<C: Chain> EoaExecutorWorker<C> {
    fn gap_replay_key(&self) -> String {
        format!("{}:gap_replay", self.store.eoa_health_key_name())
    }

    pub(super) async fn replay_submitted_gap(
        &self,
        latest: u64,
        cached_high_water: u64,
    ) -> Result<(), EoaExecutorWorkerError> {
        let Some(journal) = recovery::global() else {
            return Ok(());
        };
        let key = self.gap_replay_key();
        let mut redis = self.store.redis.clone();
        let stored: Option<String> = redis.get(&key).await.map_err(invalid)?;
        let now = EoaExecutorStore::now();
        let mut state = match stored {
            Some(encoded) => serde_json::from_str::<GapReplayState>(&encoded).map_err(invalid)?,
            None => {
                let health = self.get_eoa_health().await?;
                let rollback = latest < cached_high_water;
                let stalled =
                    now.saturating_sub(health.last_nonce_movement_at) >= NONCE_STALL_LIMIT_MS;
                if !rollback && !stalled {
                    return Ok(());
                }
                let highest = self
                    .store
                    .get_highest_submitted_nonce_tranasactions()
                    .await?;
                let Some(highest) = highest.first() else {
                    return Ok(());
                };
                let through_nonce = highest
                    .nonce
                    .checked_add(1)
                    .ok_or_else(|| invalid("Gap recovery nonce overflow"))?;
                if latest >= through_nonce {
                    return Ok(());
                }
                GapReplayState {
                    through_nonce,
                    latest_observed: latest,
                    next_at: 0,
                }
            }
        };
        if latest >= state.through_nonce {
            let _: () = self
                .store
                .with_lock_check(|pipe| {
                    pipe.del(&key);
                })
                .await?;
            return Ok(());
        }
        // Actual observed nonce progress, never an ambiguous send response,
        // refreshes the gas-bump clock while the high-water floor is retained.
        if latest > state.latest_observed {
            let mut health = self.get_eoa_health().await?;
            health.last_nonce_movement_at = now;
            self.store.update_health_data(&health).await?;
            state.latest_observed = latest;
            let encoded = serde_json::to_string(&state).map_err(invalid)?;
            let _: () = self
                .store
                .with_lock_check(|pipe| {
                    pipe.set(&key, &encoded);
                })
                .await?;
        }
        if now < state.next_at {
            return Ok(());
        }
        // Reserve the cooldown before any RPC. A crash/error consumes this
        // round; a reclaimed worker observes the same persisted deadline.
        state.next_at = now.saturating_add(GAP_REPLAY_INTERVAL_MS);
        let encoded = serde_json::to_string(&state).map_err(invalid)?;
        let _: () = self
            .store
            .with_lock_check(|pipe| {
                pipe.set(&key, &encoded);
            })
            .await?;
        crate::finality::check_continuity(&self.chain)
            .await
            .map_err(invalid)?;
        let end = latest
            .saturating_add(GAP_REPLAY_NONCES)
            .min(state.through_nonce);
        for nonce in latest..end {
            // One member is enough to identify this nonce's intent. The durable
            // replay key below detects Redis ID/nonce substitution. LIMIT keeps
            // replacement histories from expanding this read without bound.
            let rows: Vec<(String, u64)> = twmq::redis::cmd("ZRANGEBYSCORE")
                .arg(self.store.submitted_transactions_zset_name())
                .arg(nonce)
                .arg(nonce)
                .arg("WITHSCORES")
                .arg("LIMIT")
                .arg(0)
                .arg(1)
                .query_async(&mut redis)
                .await
                .map_err(invalid)?;
            let members = SubmittedTransactionDehydrated::from_redis_strings(&rows);
            let Some(member) = members.first() else {
                break;
            };
            if member.nonce != nonce
                || member.transaction_id == crate::eoa::store::NO_OP_TRANSACTION_ID
            {
                break; // A missing/unsupported gap must never spray higher nonces.
            }
            let id = &member.transaction_id;
            let expected_key = format!("evm:{}:{:#x}:{nonce}", self.chain_id, self.eoa);
            let Some(attempt) = journal
                .latest_eoa_attempt(id, &expected_key)
                .await
                .map_err(invalid)?
            else {
                break;
            };
            let admission = journal
                .admission("eoa", id)
                .await
                .map_err(invalid)?
                .ok_or_else(|| invalid("Gap recovery admission disappeared"))?;
            let request: EoaTransactionRequest =
                serde_json::from_value(admission.payload).map_err(invalid)?;
            let wire = attempt
                .get("signedTransaction")
                .and_then(|x| x.as_str())
                .ok_or_else(|| invalid("Gap recovery wire is missing"))?;
            let wire = hex::decode(
                wire.strip_prefix("0x")
                    .ok_or_else(|| invalid("Invalid wire prefix"))?,
            )
            .map_err(invalid)?;
            let mut remaining = wire.as_slice();
            let envelope = TxEnvelope::decode_2718(&mut remaining).map_err(invalid)?;
            if !remaining.is_empty() || envelope.encoded_2718() != wire || envelope.nonce() != nonce
            {
                return Err(invalid("Gap recovery wire or nonce is inconsistent"));
            }
            let signed = envelope.into_signed();
            let saved_hash: B256 = serde_json::from_value(
                attempt
                    .get("transactionHash")
                    .cloned()
                    .ok_or_else(|| invalid("Gap recovery hash is missing"))?,
            )
            .map_err(invalid)?;
            if signed.hash() != &saved_hash || request.transaction_id != *id {
                return Err(invalid("Gap recovery identity is inconsistent"));
            }
            crate::finality::validate_eoa_confirmation(
                "eoa",
                id,
                self.chain_id,
                self.eoa,
                nonce,
                saved_hash,
            )
            .await
            .map_err(invalid)?;
            // Owner validation and journal authorization are deliberately fresh
            // for each send. Losing a lease can leave at most an already
            // authorized in-flight call; it can only carry this recorded wire.
            let _: () = self
                .store
                .with_lock_check(|pipe| {
                    pipe.set(&key, &encoded);
                })
                .await?;
            crate::recovery::before_eoa(&request, &signed).await?;
            match self.chain.provider().send_raw_transaction(&wire).await {
                Ok(pending) if pending.tx_hash() == &saved_hash => {}
                Ok(_) => {
                    tracing::warn!(
                        nonce,
                        "Gap replay returned a different hash; retaining unknown outcome"
                    );
                    break;
                }
                Err(error) => {
                    tracing::warn!(nonce, error = %engine_core::error::rpc_error_diagnostic(&error),
                        "Gap replay response unknown; preserving wire and cooldown");
                    break; // Includes already-known: retry later, never reinterpret as proof.
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "gap_replay_tests.rs"]
mod tests;
