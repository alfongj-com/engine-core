use crate::config::{EvmRpcConfig, ThirdwebConfig};
use engine_core::{
    chain::{ChainService, ThirdwebChain, ThirdwebChainConfig},
    error::EngineError,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    time::Duration,
};

const FALLBACK_CACHE_CAPACITY: usize = 256;

#[derive(Default)]
struct ChainCache {
    chains: HashMap<u64, ThirdwebChain>,
    order: VecDeque<u64>,
}

/// One configured provider per chain, shared by all wallet jobs. Explicit
/// endpoints are validated at startup and remain cached. The legacy fallback
/// cache is bounded because callers may supply arbitrary chain IDs.
pub struct ThirdwebChainService {
    thirdweb: ThirdwebConfig,
    rpc: EvmRpcConfig,
    configured: HashMap<u64, ThirdwebChain>,
    fallback: Mutex<ChainCache>,
}

impl ThirdwebChainService {
    pub fn new(thirdweb: &ThirdwebConfig, rpc: &EvmRpcConfig) -> Result<Self, EngineError> {
        if rpc.request_timeout_ms == 0 || rpc.connect_timeout_ms == 0 {
            return Err(EngineError::RpcConfigError {
                message: "RPC timeouts must be positive".into(),
            });
        }
        let mut service = Self {
            thirdweb: thirdweb.clone(),
            rpc: rpc.clone(),
            configured: HashMap::new(),
            fallback: Mutex::default(),
        };
        for (id, endpoint) in &rpc.endpoints {
            let chain_id = id.parse::<u64>().ok().filter(|id| *id > 0).ok_or_else(|| {
                EngineError::RpcConfigError {
                    message: "Configured EVM chain IDs must be positive integers".into(),
                }
            })?;
            if service.configured.contains_key(&chain_id) {
                return Err(EngineError::RpcConfigError {
                    message: "Duplicate configured EVM chain ID".into(),
                });
            }
            let chain = service.chain_config(chain_id).to_chain_with_rpc(
                Some(endpoint),
                Duration::from_millis(rpc.request_timeout_ms),
                Duration::from_millis(rpc.connect_timeout_ms),
            )?;
            service.configured.insert(chain_id, chain);
        }
        Ok(service)
    }

    pub fn is_configured(&self, chain_id: u64) -> bool {
        self.configured.contains_key(&chain_id)
    }

    fn chain_config(&self, chain_id: u64) -> ThirdwebChainConfig<'_> {
        ThirdwebChainConfig {
            chain_id,
            rpc_base_url: &self.thirdweb.urls.rpc,
            bundler_base_url: &self.thirdweb.urls.bundler,
            paymaster_base_url: &self.thirdweb.urls.paymaster,
            client_id: &self.thirdweb.client_id,
            secret_key: &self.thirdweb.secret,
        }
    }
}

#[allow(refining_impl_trait)]
impl ChainService for ThirdwebChainService {
    fn get_chain(&self, chain_id: u64) -> Result<ThirdwebChain, EngineError> {
        if let Some(chain) = self.configured.get(&chain_id) {
            return Ok(chain.clone());
        }
        let mut cache = self
            .fallback
            .lock()
            .map_err(|_| EngineError::InternalError {
                message: "RPC provider cache unavailable".into(),
            })?;
        if let Some(chain) = cache.chains.get(&chain_id) {
            return Ok(chain.clone());
        }
        let chain = self.chain_config(chain_id).to_chain_with_rpc(
            None,
            Duration::from_millis(self.rpc.request_timeout_ms),
            Duration::from_millis(self.rpc.connect_timeout_ms),
        )?;
        if cache.chains.len() >= FALLBACK_CACHE_CAPACITY {
            if let Some(oldest) = cache.order.pop_front() {
                cache.chains.remove(&oldest);
            }
        }
        cache.order.push_back(chain_id);
        cache.chains.insert(chain_id, chain.clone());
        Ok(chain)
    }
}

#[cfg(test)]
#[path = "chains_tests.rs"]
mod tests;
