#![recursion_limit = "256"]
pub mod eip7702_executor;
pub mod eoa;
pub mod external_bundler;
pub mod metrics;
pub mod solana_executor;
pub mod transaction_registry;
pub mod webhook;

use alloy::{
    providers::Provider,
    rpc::json_rpc::{RpcRecv, RpcSend},
};

/// Extension trait for RpcWithBlock to select the block tag from an explicit endpoint capability
pub trait FlashblocksSupport {
    fn with_flashblocks_support(self, use_pending_for_preconfirmation: bool) -> Self;
}

impl<Params, Resp, Output, Map> FlashblocksSupport
    for alloy::providers::RpcWithBlock<Params, Resp, Output, Map>
where
    Params: RpcSend,
    Resp: RpcRecv,
    Map: Fn(Resp) -> Output + Clone,
{
    fn with_flashblocks_support(self, use_pending_for_preconfirmation: bool) -> Self {
        if use_pending_for_preconfirmation {
            self.pending()
        } else {
            self
        }
    }
}

/// Result of fetching transaction counts with explicit endpoint preconfirmation support
#[derive(Debug, Clone)]
pub struct TransactionCounts {
    /// Latest confirmed transaction count (always from "latest" block)
    pub latest: u64,
    /// Preconfirmed count: "pending" only for an explicitly enabled endpoint; otherwise latest.
    pub preconfirmed: u64,
}

/// Extension trait for Provider to fetch transaction counts with explicit endpoint preconfirmation support
pub trait FlashblocksTransactionCount {
    fn get_transaction_counts_with_flashblocks_support(
        &self,
        address: alloy::primitives::Address,
        use_pending_for_preconfirmation: bool,
    ) -> impl Future<Output = Result<TransactionCounts, alloy::transports::TransportError>> + Send;
}

impl<T> FlashblocksTransactionCount for T
where
    T: Provider,
{
    async fn get_transaction_counts_with_flashblocks_support(
        &self,
        address: alloy::primitives::Address,
        use_pending_for_preconfirmation: bool,
    ) -> Result<TransactionCounts, alloy::transports::TransportError> {
        if use_pending_for_preconfirmation {
            // For explicitly configured preconfirmation endpoints, fetch both latest and pending in parallel
            let (latest_result, preconfirmed_result) = tokio::try_join!(
                self.get_transaction_count(address),
                self.get_transaction_count(address).pending()
            )?;
            Ok(TransactionCounts {
                latest: latest_result,
                preconfirmed: preconfirmed_result,
            })
        } else {
            // For standard endpoints, fetch once and use same value for both
            let count = self.get_transaction_count(address).await?;
            Ok(TransactionCounts {
                latest: count,
                preconfirmed: count,
            })
        }
    }
}
