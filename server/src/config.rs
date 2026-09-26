use engine_core::chain::RpcEndpointConfig;
use std::{collections::BTreeMap, env};

use config::{Config, File};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    pub server: ServerConfig,
    pub thirdweb: ThirdwebConfig,
    #[serde(default)]
    pub evm_rpc: EvmRpcConfig,
    pub queue: QueueConfig,
    pub redis: RedisConfig,
    #[serde(default)]
    pub recovery: RecoveryConfig,
    pub solana: SolanaConfig,
}

/// Only EVM RPC endpoints are overridden; bundler/paymaster configuration is separate.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EvmRpcConfig {
    pub endpoints: BTreeMap<String, RpcEndpointConfig>,
    pub request_timeout_ms: u64,
    pub connect_timeout_ms: u64,
}

impl Default for EvmRpcConfig {
    fn default() -> Self {
        Self {
            endpoints: BTreeMap::new(),
            request_timeout_ms: 30_000,
            connect_timeout_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SolanaConfig {
    pub devnet: SolanRpcConfigData,
    pub mainnet: SolanRpcConfigData,
    #[serde(default = "default_local_rpc_config")]
    pub local: SolanRpcConfigData,
}

fn default_local_rpc_config() -> SolanRpcConfigData {
    SolanRpcConfigData {
        http_url: "http://127.0.0.1:8899".to_string(),
        ws_url: "ws://127.0.0.1:8900".to_string(),
    }
}

#[derive(Clone, Deserialize)]
pub struct SolanRpcConfigData {
    pub http_url: String,
    pub ws_url: String,
}

impl std::fmt::Debug for SolanRpcConfigData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SolanRpcConfigData")
            .field("http_url", &"[redacted]")
            .field("ws_url", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueConfig {
    pub webhook_workers: usize,

    pub external_bundler_send_workers: usize,
    pub userop_confirm_workers: usize,
    pub eoa_executor_workers: usize,
    /// Per signer/chain mempool window. Size for RPC account limits and inclusion latency.
    #[serde(
        default = "default_eoa_max_inflight",
        deserialize_with = "deserialize_eoa_max_inflight"
    )]
    pub eoa_max_inflight: u64,
    pub solana_executor_workers: usize,

    pub execution_namespace: Option<String>,

    pub local_concurrency: usize,
    pub polling_interval_ms: u64,
    pub lease_duration_seconds: u64,

    #[serde(default)]
    pub monitoring: MonitoringConfig,

    #[serde(default = "default_completed_transaction_ttl_seconds")]
    pub completed_transaction_ttl_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MonitoringConfig {
    pub eoa_send_degradation_threshold_seconds: u64,
    pub eoa_confirmation_degradation_threshold_seconds: u64,
    pub eoa_stuck_threshold_seconds: u64,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            eoa_send_degradation_threshold_seconds: 10, // 10 seconds
            eoa_confirmation_degradation_threshold_seconds: 120, // 2 minutes
            eoa_stuck_threshold_seconds: 600,           // 10 minutes
        }
    }
}

fn default_completed_transaction_ttl_seconds() -> u64 {
    86400 // 1 day in seconds
}

fn default_eoa_max_inflight() -> u64 {
    50
}

fn deserialize_eoa_max_inflight<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Number {
        Integer(u64),
        Text(String),
    }
    let value = match Number::deserialize(deserializer)? {
        Number::Integer(value) => value,
        Number::Text(value) => value.parse().map_err(serde::de::Error::custom)?,
    };
    if !(1..=4096).contains(&value) {
        return Err(serde::de::Error::custom(
            "eoa_max_inflight must be between 1 and 4096",
        ));
    }
    Ok(value)
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    pub url: String,
}

/// Independent durable state. Ordinary startup never creates a missing ledger.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RecoveryConfig {
    pub journal_path: std::path::PathBuf,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            journal_path: "data/recovery.sqlite".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub log_format: LogFormat,
    pub diagnostic_access_password: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThirdwebConfig {
    pub secret: String,
    pub client_id: String,
    pub urls: ThirdwebUrls,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThirdwebUrls {
    pub rpc: String,
    pub bundler: String,
    pub paymaster: String,
    pub abi_service: String,
    pub iaw_service: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    Pretty,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 3000,
            host: "0.0.0.0".into(),
            log_format: LogFormat::Pretty,
            diagnostic_access_password: None,
        }
    }
}

/// EngineConfig is cached, it only loads once
pub fn get_config() -> EngineConfig {
    let base_path = env::current_dir().expect("Failed to determine the current directory");
    let configuration_directory = base_path.join("configuration");

    // Detect the running environment
    let environment: Environment = env::var("APP_ENVIRONMENT")
        .unwrap_or_else(|_| "local".into())
        .try_into()
        .expect("Failed to parse APP_ENVIRONMENT");

    let environment_filename = format!("server_{}.yaml", environment.as_str());

    // Load configuration from files
    let config = Config::builder()
        .add_source(File::from(configuration_directory.join("server_base.yaml")))
        .add_source(File::from(
            configuration_directory.join(environment_filename),
        ))
        .add_source(config::Environment::with_prefix("app").separator("__"))
        .build()
        .unwrap_or_else(|e| {
            eprintln!("Configuration error: {e}");
            panic!("Failed to build configuration");
        });

    // Deserialize the configuration
    config.try_deserialize::<EngineConfig>()
        .unwrap_or_else(|e| {
            eprintln!("Configuration error: {e}");
            eprintln!("Make sure all required fields are set correctly in your configuration files or environment variables.");
            panic!("Failed to deserialize configuration");
        })
}

/// The possible runtime environment for our application.
pub enum Environment {
    Local,
    Development,
    Production,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Local => "local",
            Environment::Development => "development",
            Environment::Production => "production",
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "development" => Ok(Self::Development),
            "production" => Ok(Self::Production),
            other => Err(format!(
                "{other} is not a supported environment. Use either `local`, `development`, or `production`."
            )),
        }
    }
}

#[cfg(test)]
mod throughput_config_tests {
    use super::*;

    #[derive(Deserialize)]
    struct Window {
        #[serde(
            default = "default_eoa_max_inflight",
            deserialize_with = "deserialize_eoa_max_inflight"
        )]
        eoa_max_inflight: u64,
    }

    #[test]
    fn inflight_window_accepts_environment_values_but_rejects_unsafe_bounds() {
        assert_eq!(
            serde_json::from_str::<Window>("{}")
                .unwrap()
                .eoa_max_inflight,
            50
        );
        for value in [serde_json::json!(1024), serde_json::json!("1024")] {
            let config: Window =
                serde_json::from_value(serde_json::json!({"eoa_max_inflight":value})).unwrap();
            assert_eq!(config.eoa_max_inflight, 1024);
        }
        for value in [
            serde_json::json!(0),
            serde_json::json!(4097),
            serde_json::json!(-1),
            serde_json::json!(1.5),
            serde_json::json!(true),
            serde_json::json!("1e3"),
        ] {
            assert!(
                serde_json::from_value::<Window>(serde_json::json!({"eoa_max_inflight":value}))
                    .is_err()
            );
        }
    }
}
