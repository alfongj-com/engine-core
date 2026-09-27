use engine_core::{error::EngineError, execution_options::solana::SolanaChainId};
use moka::future::Cache;
use serde_json::Value;
use solana_rpc_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_client::RpcClientConfig,
    rpc_sender::{RpcSender, RpcTransportStats},
};
use solana_rpc_client_api::{
    client_error::{Error as ClientError, ErrorKind, Result as ClientResult},
    request::{RpcError, RpcRequest, RpcResponseErrorData},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::info;

use crate::metrics::{
    ExecutorFamily, ExecutorOperation, MetricChain, OperationTimer,
    record_status_request_signatures,
};

// Solana SDK ClusterType constants, independently checked against official public
// RPCs. See docs/design/confirmation-identity.md for provenance and limitations.
const DEVNET_GENESIS: &str = "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG";
const MAINNET_GENESIS: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";

const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// Single-attempt transport: every HTTP call consumes one worker retry. The SDK's
/// default sender retries 429s internally and logs credential-bearing responses.
/// Never retain provider bodies or reqwest errors in transport error messages.
pub(super) struct BoundedRpcSender {
    url: String,
    client: reqwest::Client,
    next_id: AtomicU64,
    stats: Mutex<RpcTransportStats>,
    metric_chain: MetricChain,
}

impl BoundedRpcSender {
    pub(super) fn new(url: String) -> Self {
        Self::new_with_timeout(url, Duration::from_secs(15))
    }

    pub(super) fn new_with_timeout(url: String, timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .no_proxy()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(timeout)
            .build()
            .expect("fixed Solana HTTP client configuration must be valid");
        Self {
            url,
            client,
            next_id: AtomicU64::new(1),
            stats: Mutex::new(RpcTransportStats::default()),
            metric_chain: MetricChain::Unknown,
        }
    }

    /// Cluster labels are fixed configuration, never the credential-bearing URL.
    fn with_chain_id(mut self, chain_id: SolanaChainId) -> Self {
        self.metric_chain = MetricChain::Solana(chain_id);
        self
    }

    fn metric_operation(request: &RpcRequest) -> ExecutorOperation {
        match request {
            RpcRequest::GetSignatureStatuses => ExecutorOperation::RpcGetSignatureStatuses,
            RpcRequest::GetTransaction => ExecutorOperation::RpcGetTransaction,
            RpcRequest::GetBlockHeight => ExecutorOperation::RpcGetBlockHeight,
            RpcRequest::GetLatestBlockhash => ExecutorOperation::RpcGetLatestBlockhash,
            RpcRequest::IsBlockhashValid => ExecutorOperation::RpcIsBlockhashValid,
            RpcRequest::GetRecentPrioritizationFees => {
                ExecutorOperation::RpcGetRecentPrioritizationFees
            }
            RpcRequest::SendTransaction => ExecutorOperation::RpcSendTransaction,
            RpcRequest::GetGenesisHash => ExecutorOperation::RpcGetGenesisHash,
            RpcRequest::SimulateTransaction => ExecutorOperation::RpcSimulateTransaction,
            _ => ExecutorOperation::RpcOther,
        }
    }

    fn error(message: impl Into<String>) -> ClientError {
        ErrorKind::Custom(message.into()).into()
    }

    fn transport_error(error: reqwest::Error) -> ClientError {
        Self::error(if error.is_timeout() {
            "Solana RPC request timed out"
        } else if error.is_connect() {
            "Solana RPC connection failed"
        } else {
            "Solana RPC transport failed"
        })
    }

    async fn send_once(&self, request: RpcRequest, params: Value) -> ClientResult<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let body = request.build_request_json(id, params);
        let mut response = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(Self::transport_error)?;
        if !response.status().is_success() {
            // No retries, body reads, response Debug output, redirects, or header echoes.
            return Err(Self::error(format!(
                "Solana RPC HTTP status {}",
                response.status().as_u16()
            )));
        }
        if response
            .content_length()
            .is_some_and(|len| len > MAX_RESPONSE_BYTES as u64)
        {
            return Err(Self::error("Solana RPC response exceeds 16 MiB"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(Self::transport_error)? {
            if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(bytes.len()) {
                return Err(Self::error("Solana RPC response exceeds 16 MiB"));
            }
            bytes.extend_from_slice(&chunk);
        }
        let mut value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| Self::error("Solana RPC returned invalid JSON"))?;
        if !value.is_object()
            || value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || value.get("id").and_then(Value::as_u64) != Some(id)
            || value.get("result").is_some() == value.get("error").is_some()
        {
            return Err(Self::error(
                "Solana RPC returned an invalid response envelope",
            ));
        }
        if let Some(error) = value.get("error") {
            let code = error
                .get("code")
                .and_then(Value::as_i64)
                .ok_or_else(|| Self::error("Solana RPC returned an invalid error envelope"))?;
            return Err(RpcError::RpcResponseError {
                code,
                message: "Solana RPC rejected request; provider details withheld".into(),
                data: RpcResponseErrorData::Empty,
            }
            .into());
        }
        Ok(value["result"].take())
    }
}

#[async_trait::async_trait]
impl RpcSender for BoundedRpcSender {
    async fn send(&self, request: RpcRequest, params: Value) -> ClientResult<Value> {
        let timer = OperationTimer::start(
            ExecutorFamily::Solana,
            self.metric_chain,
            Self::metric_operation(&request),
        );
        if matches!(request, RpcRequest::GetSignatureStatuses) {
            if let Some(signatures) = params.get(0).and_then(Value::as_array) {
                // Count only; signatures, URLs and response bodies are never labels.
                record_status_request_signatures(self.metric_chain, signatures.len());
            }
        }
        let started = Instant::now();
        let result = self.send_once(request, params).await;
        timer.finish_result(&result);
        let mut stats = self.stats.lock().unwrap_or_else(|p| p.into_inner());
        stats.request_count += 1;
        stats.elapsed_time += started.elapsed();
        result
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        self.stats.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn url(&self) -> String {
        // This getter is diagnostic. The private URL above is used for requests.
        "[redacted Solana RPC URL]".into()
    }
}

/// Cache key for RPC clients
#[derive(Clone, Hash, Eq, PartialEq)]
pub struct RpcCacheKey {
    pub chain_id: SolanaChainId,
    pub rpc_url: String,
}

impl std::fmt::Debug for RpcCacheKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcCacheKey")
            .field("chain_id", &self.chain_id)
            .field("rpc_url", &"[redacted]")
            .finish()
    }
}

/// Solana RPC client cache with connection pooling
///
/// This cache maintains RpcClient instances per Solana cluster,
/// with a bounded HTTP transport providing TCP connection reuse.
#[derive(Clone)]
pub struct SolanaRpcCache {
    cache: Cache<RpcCacheKey, Arc<RpcClient>>,
    verified_clusters: Cache<RpcCacheKey, ()>,
    urls: SolanaRpcUrls,
}

#[derive(Clone)]
pub struct SolanaRpcUrls {
    pub devnet: String,
    pub mainnet: String,
    pub local: String,
}

impl SolanaRpcCache {
    /// Cache one HTTP client per configured cluster.
    pub fn new(urls: SolanaRpcUrls) -> Self {
        let cache = Cache::new(3);

        Self {
            cache,
            urls,
            verified_clusters: Cache::builder()
                .max_capacity(3)
                .time_to_live(Duration::from_secs(300))
                .build(),
        }
    }

    /// Get or create an RPC client for the given cluster
    pub async fn get_or_create(&self, chain_id: SolanaChainId) -> Arc<RpcClient> {
        let rpc_url = match chain_id {
            SolanaChainId::SolanaDevnet => self.urls.devnet.clone(),
            SolanaChainId::SolanaMainnet => self.urls.mainnet.clone(),
            SolanaChainId::SolanaLocal => self.urls.local.clone(),
        };

        let key = RpcCacheKey {
            chain_id,
            rpc_url: rpc_url.clone(),
        };

        self.cache
            .get_with(key.clone(), async move {
                info!(
                    chain_id = ?chain_id,
                    "Creating new Solana RPC client with connection cache"
                );

                // Create RPC client
                Arc::new(RpcClient::new_sender(
                    BoundedRpcSender::new(rpc_url).with_chain_id(chain_id),
                    RpcClientConfig::default(),
                ))
            })
            .await
    }

    /// Verify cluster identity before signing or reconciling named public-cluster
    /// work. Only successful checks are cached, by endpoint and expected cluster.
    pub async fn get_verified(
        &self,
        chain_id: SolanaChainId,
    ) -> Result<Arc<RpcClient>, EngineError> {
        let client = self.get_or_create(chain_id).await;
        let (expected, rpc_url) = match chain_id {
            SolanaChainId::SolanaDevnet => (DEVNET_GENESIS, self.urls.devnet.clone()),
            SolanaChainId::SolanaMainnet => (MAINNET_GENESIS, self.urls.mainnet.clone()),
            // Local validators have deliberately arbitrary genesis. This profile
            // is operator-selected, not qualification of an unknown remote cluster.
            SolanaChainId::SolanaLocal => return Ok(client),
        };
        let key = RpcCacheKey { chain_id, rpc_url };
        self.verified_clusters.try_get_with(key, async {
            let actual = client.get_genesis_hash().await.map_err(|_| EngineError::ValidationError {
                message: "Solana cluster identity is unavailable; retry before signing or reconciliation".into(),
            })?;
            if actual.to_string() != expected {
                return Err(EngineError::ValidationError {
                    message: "Configured Solana endpoint belongs to a different cluster; correct the endpoint before signing or reconciliation".into(),
                });
            }
            Ok::<(),EngineError>(())
        }).await.map_err(|error| (*error).clone())?;
        Ok(client)
    }

    /// Get the number of cached clients
    pub fn len(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
#[path = "rpc_cache_tests.rs"]
mod tests;
