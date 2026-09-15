use alloy::primitives::{Address, ChainId};
use alloy::signers::local::PrivateKeySigner;
use alloy_signer_aws::AwsSigner;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::provider::future::ProvideCredentials as ProvideCredentialsFuture;
use aws_sdk_kms::config::{Credentials, ProvideCredentials};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use thirdweb_core::auth::ThirdwebAuth;
use thirdweb_core::iaw::AuthToken;

use crate::error::EngineError;

/// Cache for AWS KMS clients to avoid recreating connections
pub type KmsClientCache = moka::future::Cache<u64, aws_sdk_kms::Client>;

impl SigningCredential {
    /// Create a random private key credential for testing
    pub fn random_local() -> Self {
        SigningCredential::PrivateKey(PrivateKeySigner::random())
    }

    /// Queue only the public address. Resolve the secret locally on the worker.
    pub fn environment() -> Result<Self, EngineError> {
        let signer = environment_signer()?;
        Ok(Self::Environment {
            address: signer.address(),
        })
    }

    pub fn local_signer(&self) -> Result<PrivateKeySigner, EngineError> {
        match self {
            Self::PrivateKey(signer) => Ok(signer.clone()),
            Self::Environment { address } => {
                let signer = environment_signer()?;
                validate_signer_address(signer.address(), *address)?;
                Ok(signer)
            }
            _ => Err(EngineError::ValidationError {
                message: "A local EVM signer is required".into(),
            }),
        }
    }

    /// Inject KMS cache into AWS KMS credentials (useful after deserialization)
    pub fn with_aws_kms_cache(self, kms_client_cache: &KmsClientCache) -> Self {
        match self {
            SigningCredential::AwsKms(creds) => {
                SigningCredential::AwsKms(creds.with_cache(kms_client_cache.clone()))
            }
            other => other,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum SigningCredential {
    /// Reference to ENGINE_PRIVATE_KEY, pinned to its public address across queue retries.
    Environment {
        address: Address,
    },
    Iaw {
        auth_token: AuthToken,
        thirdweb_auth: ThirdwebAuth,
    },
    AwsKms(AwsKmsCredential),
    /// Private key signer for testing and development
    /// Note: This should only be used in test environments
    #[serde(skip)]
    PrivateKey(PrivateKeySigner),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AwsKmsCredential {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub key_id: String,
    pub region: String,
    #[serde(skip)]
    pub kms_client_cache: Option<KmsClientCache>,
}

impl Hash for AwsKmsCredential {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.access_key_id.hash(state);
        self.secret_access_key.hash(state);
        self.key_id.hash(state);
        self.region.hash(state);
        // Don't hash the cache - it's not part of the credential identity
    }
}

impl ProvideCredentials for AwsKmsCredential {
    fn provide_credentials<'a>(&'a self) -> ProvideCredentialsFuture<'a>
    where
        Self: 'a,
    {
        let credentials = Credentials::new(
            self.access_key_id.clone(),
            self.secret_access_key.clone(),
            None,
            None,
            "engine-core",
        );
        ProvideCredentialsFuture::ready(Ok(credentials))
    }
}

impl AwsKmsCredential {
    /// Create a new AwsKmsCredential with cache
    pub fn new(
        access_key_id: String,
        secret_access_key: String,
        key_id: String,
        region: String,
        kms_client_cache: KmsClientCache,
    ) -> Self {
        Self {
            access_key_id,
            secret_access_key,
            key_id,
            region,
            kms_client_cache: Some(kms_client_cache),
        }
    }

    /// Inject cache into this credential (useful after deserialization)
    pub fn with_cache(mut self, kms_client_cache: KmsClientCache) -> Self {
        self.kms_client_cache = Some(kms_client_cache);
        self
    }

    /// Create a cache key from the credential
    fn cache_key(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    /// Create a new AWS KMS client (without caching)
    async fn create_kms_client(&self) -> Result<aws_sdk_kms::Client, EngineError> {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .credentials_provider(self.clone())
            .region(Region::new(self.region.clone()))
            .load()
            .await;
        Ok(aws_sdk_kms::Client::new(&config))
    }

    /// Get a cached AWS KMS client, creating one if it doesn't exist
    async fn get_cached_kms_client(&self) -> Result<aws_sdk_kms::Client, EngineError> {
        match &self.kms_client_cache {
            Some(cache) => {
                let cache_key = self.cache_key();

                cache
                    .try_get_with(cache_key, async {
                        tracing::debug!("Creating new KMS client for key: {}", cache_key);
                        self.create_kms_client().await
                    })
                    .await
                    .map_err(|e| e.deref().clone())
            }
            None => {
                // Fallback to creating a new client without caching
                tracing::debug!("No cache available, creating new KMS client");
                self.create_kms_client().await
            }
        }
    }

    /// Get signer (uses cache if available)
    pub async fn get_signer(&self, chain_id: Option<ChainId>) -> Result<AwsSigner, EngineError> {
        let client = self.get_cached_kms_client().await?;
        let signer = AwsSigner::new(client, self.key_id.clone(), chain_id).await?;
        Ok(signer)
    }
}

fn environment_signer() -> Result<PrivateKeySigner, EngineError> {
    let key = std::env::var("ENGINE_PRIVATE_KEY").map_err(|_| EngineError::ValidationError {
        message: "ENGINE_PRIVATE_KEY is not configured".into(),
    })?;
    key.parse().map_err(|_| EngineError::ValidationError {
        message: "ENGINE_PRIVATE_KEY must be a valid 32-byte secp256k1 key".into(),
    })
}

pub fn validate_signer_address(actual: Address, requested: Address) -> Result<(), EngineError> {
    if actual != requested {
        return Err(EngineError::ValidationError {
            message: "Signing key does not match the requested signer address".into(),
        });
    }
    Ok(())
}

impl std::fmt::Debug for SigningCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Environment { address } => f
                .debug_struct("Environment")
                .field("address", address)
                .finish(),
            Self::PrivateKey(signer) => f
                .debug_struct("PrivateKey")
                .field("address", &signer.address())
                .finish(),
            Self::AwsKms(creds) => f.debug_tuple("AwsKms").field(creds).finish(),
            Self::Iaw { .. } => f.write_str("Iaw([REDACTED])"),
        }
    }
}

impl std::fmt::Debug for AwsKmsCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsKmsCredential")
            .field("key_id", &self.key_id)
            .field("region", &self.region)
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}
