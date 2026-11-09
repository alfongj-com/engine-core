use alloy::primitives::{Address, ChainId};
use alloy::signers::{
    Signer,
    local::{PrivateKeySigner, Secp256k1Signer},
};
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

    pub fn local_signer(&self) -> Result<Secp256k1Signer, EngineError> {
        match self {
            Self::PrivateKey(signer) => Secp256k1Signer::from_bytes(&signer.to_bytes())
                .map(|native| native.with_chain_id(signer.chain_id()))
                .map_err(|_| EngineError::ValidationError {
                    message: "Invalid local EVM signing key".into(),
                }),
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

    /// Persist only the configured Solana public identity, never the key file or bytes.
    pub fn solana_environment() -> Result<Self, EngineError> {
        use solana_signer::Signer;
        Ok(Self::SolanaEnvironment {
            public_key: environment_solana_keypair()?.pubkey(),
        })
    }

    pub fn solana_keypair(&self) -> Result<solana_keypair::Keypair, EngineError> {
        use solana_signer::Signer;
        let Self::SolanaEnvironment { public_key } = self else {
            return Err(EngineError::ValidationError {
                message: "A configured Solana signer is required".into(),
            });
        };
        let keypair = environment_solana_keypair()?;
        if keypair.pubkey() != *public_key {
            return Err(EngineError::ValidationError { message: "Configured Solana signing key changed since admission; reconcile queued jobs before rotating keys".into() });
        }
        Ok(keypair)
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
    /// Public identity of ENGINE_SOLANA_KEYPAIR_FILE, resolved by each worker.
    SolanaEnvironment {
        public_key: solana_pubkey::Pubkey,
    },
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

fn environment_signer() -> Result<Secp256k1Signer, EngineError> {
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
            Self::SolanaEnvironment { public_key } => f
                .debug_struct("SolanaEnvironment")
                .field("public_key", public_key)
                .finish(),
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

/// Read a Solana CLI keypair file with bounded input and non-secret error messages.
fn environment_solana_keypair() -> Result<solana_keypair::Keypair, EngineError> {
    use std::io::Read;
    let invalid = || {
        EngineError::ValidationError { message: "ENGINE_SOLANA_KEYPAIR_FILE must refer to a readable Solana CLI JSON keypair containing 64 valid bytes".into() }
    };
    let path = std::env::var_os("ENGINE_SOLANA_KEYPAIR_FILE").ok_or_else(invalid)?;
    let file = std::fs::File::open(path).map_err(|_| invalid())?;
    let mut encoded = Vec::new();
    file.take(4097)
        .read_to_end(&mut encoded)
        .map_err(|_| invalid())?;
    if encoded.len() > 4096 {
        return Err(invalid());
    }
    let bytes: Vec<u8> = serde_json::from_slice(&encoded).map_err(|_| invalid())?;
    solana_keypair::Keypair::try_from(bytes.as_slice()).map_err(|_| invalid())
}

#[cfg(test)]
mod local_signer_tests {
    use super::*;
    use alloy::{
        consensus::{SignableTransaction, TxEnvelope, TxLegacy},
        eips::Encodable2718,
        network::TxSignerSync,
        primitives::{B256, Bytes, TxKind, U256, address, keccak256},
        signers::SignerSync,
    };

    #[test]
    fn native_signer_matches_published_eip155_wire_and_rejects_other_chain() {
        // Independent published transaction: https://eips.ethereum.org/EIPS/eip-155
        let legacy = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x46))
            .unwrap()
            .with_chain_id(Some(1));
        let native = SigningCredential::PrivateKey(legacy)
            .local_signer()
            .unwrap();
        assert_eq!(
            native.address(),
            address!("9d8A62f656a8d1615C1294fd71e9CfB3E4855A4F")
        );
        assert_eq!(native.chain_id(), Some(1));
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce: 9,
            gas_price: 20_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(address!("3535353535353535353535353535353535353535")),
            value: U256::from(1_000_000_000_000_000_000u64),
            input: Bytes::new(),
        };
        let signature = native.sign_transaction_sync(&mut tx).unwrap();
        let envelope: TxEnvelope = tx.clone().into_signed(signature).into();
        assert_eq!(
            alloy::hex::encode(envelope.encoded_2718()),
            "f86c098504a817c800825208943535353535353535353535353535353535353535880de0b6b3a76400008025a028ef61340bd939bc2195fe537567866003e1a15d3c71ff63e1590620aa636276a067cbe9d8997f761aecb703304b3800ccf555c9f3dc64214b297fb1966a3b6d83"
        );
        assert!(
            native
                .with_chain_id(Some(2))
                .sign_transaction_sync(&mut tx)
                .is_err()
        );
    }

    #[test]
    fn native_prehash_curve_order_boundaries_recover_the_same_identity() {
        let legacy = PrivateKeySigner::from_bytes(&B256::repeat_byte(1)).unwrap();
        let native = SigningCredential::PrivateKey(legacy.clone())
            .local_signer()
            .unwrap();
        let order = U256::from_str_radix(
            "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141",
            16,
        )
        .unwrap();
        // libsecp256k1 reduces a digest modulo n before RFC6979 nonce seeding.
        // k256 0.13 does not. At/above n, require valid signatures and the same
        // public identity; byte-for-byte nonce/signature equality is not promised.
        for value in [
            order - U256::from(1),
            order,
            order + U256::from(1),
            U256::MAX,
        ] {
            let hash = B256::from(value.to_be_bytes::<32>());
            for signature in [
                native.sign_hash_sync(&hash).unwrap(),
                legacy.sign_hash_sync(&hash).unwrap(),
            ] {
                assert_eq!(
                    signature.recover_address_from_prehash(&hash).unwrap(),
                    legacy.address()
                );
            }
        }
    }

    #[test]
    fn native_signer_preserves_hash_personal_signatures_and_configured_chain() {
        let legacy = PrivateKeySigner::from_bytes(&B256::repeat_byte(1))
            .unwrap()
            .with_chain_id(Some(31337));
        let native = SigningCredential::PrivateKey(legacy.clone())
            .local_signer()
            .unwrap();
        assert_eq!(native.address(), legacy.address());
        assert_eq!(native.chain_id(), Some(31337));
        for hash in [B256::ZERO, keccak256(b"engine signing compatibility")] {
            let actual = native.sign_hash_sync(&hash).unwrap();
            assert_eq!(actual, legacy.sign_hash_sync(&hash).unwrap());
            assert_eq!(
                actual.recover_address_from_prehash(&hash).unwrap(),
                native.address()
            );
        }
        for message in [b"".as_slice(), b"engine", "Unicode: 🔐".as_bytes()] {
            assert_eq!(
                native.sign_message_sync(message).unwrap(),
                legacy.sign_message_sync(message).unwrap()
            );
        }
    }
}
