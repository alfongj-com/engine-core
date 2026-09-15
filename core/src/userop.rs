use alloy::{
    hex::FromHex,
    primitives::{Address, B256, Bytes, ChainId, eip191_hash_message},
    signers::Signer,
};
use thirdweb_core::iaw::IAWClient;

use crate::{
    constants::{DEFAULT_FACTORY_ADDRESS_V0_6, DEFAULT_FACTORY_ADDRESS_V0_7},
    credentials::SigningCredential,
    error::{EngineError, SerialisableAwsSdkError, SerialisableAwsSignerError},
    execution_options::aa::EntrypointVersion,
};

// Re-export for convenience
pub use engine_aa_types::VersionedUserOp;

/// Reviewed AccountCore signature policies; this is not a caller-selected raw
/// signing switch. The HTTP builder also binds the account to its factory/admin/salt.
/// See docs/design/userop-signing.md for source provenance and qualification limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserOpSigningProfile {
    ThirdwebV06,
    ThirdwebV07,
}

impl UserOpSigningProfile {
    pub fn for_factory(
        factory_address: Address,
        version: EntrypointVersion,
    ) -> Result<Self, EngineError> {
        match (factory_address, version) {
            (DEFAULT_FACTORY_ADDRESS_V0_6, EntrypointVersion::V0_6) => Ok(Self::ThirdwebV06),
            (DEFAULT_FACTORY_ADDRESS_V0_7, EntrypointVersion::V0_7) => Ok(Self::ThirdwebV07),
            _ => Err(EngineError::ValidationError {
                message:
                    "Local/KMS UserOperation signing requires a reviewed factory/version profile"
                        .into(),
            }),
        }
    }
}

#[derive(Clone)]
pub struct UserOpSigner {
    pub iaw_client: IAWClient,
}

pub struct UserOpSignerParams {
    pub credentials: SigningCredential,
    pub entrypoint: Address,
    pub factory_address: Address,
    pub userop: VersionedUserOp,
    pub signer_address: Address,
    pub chain_id: ChainId,
}

impl UserOpSignerParams {
    fn local_signing_digest(&self) -> Result<B256, EngineError> {
        let version = match self.userop {
            VersionedUserOp::V0_6(_) => EntrypointVersion::V0_6,
            VersionedUserOp::V0_7(_) => EntrypointVersion::V0_7,
        };
        let profile = UserOpSigningProfile::for_factory(self.factory_address, version)?;
        let userop_hash = self
            .userop
            .hash_with_custom_entrypoint(self.chain_id, self.entrypoint)
            .map_err(|e| EngineError::ValidationError {
                message: format!("Failed to hash userop: {e}"),
            })?;
        match profile {
            // Both reviewed versions call toEthSignedMessageHash(bytes32)
            // before recover. Prefix the 32 raw bytes, not their hex text.
            UserOpSigningProfile::ThirdwebV06 | UserOpSigningProfile::ThirdwebV07 => {
                Ok(eip191_hash_message(userop_hash.as_slice()))
            }
        }
    }
}

impl UserOpSigner {
    pub async fn sign(&self, params: UserOpSignerParams) -> Result<Bytes, EngineError> {
        match &params.credentials {
            SigningCredential::Iaw {
                auth_token,
                thirdweb_auth,
            } => {
                let result = self
                    .iaw_client
                    .sign_userop(
                        auth_token.clone(),
                        thirdweb_auth.clone(),
                        params.userop,
                        params.entrypoint,
                        params.signer_address,
                        params.chain_id,
                    )
                    .await
                    .map_err(|e| EngineError::ValidationError {
                        message: format!("Failed to sign userop: {e}"),
                    })?;

                Ok(Bytes::from_hex(&result.signature).map_err(|_| {
                    EngineError::ValidationError {
                        message: "Bad signature received from IAW".to_string(),
                    }
                })?)
            }
            SigningCredential::AwsKms(creds) => {
                // Reject unknown policy before contacting KMS or requesting a signature.
                let digest = params.local_signing_digest()?;
                let signer = creds.get_signer(Some(params.chain_id)).await?;
                crate::credentials::validate_signer_address(
                    signer.address(),
                    params.signer_address,
                )?;
                let signature = signer.sign_hash(&digest).await.map_err(|e| {
                    EngineError::AwsKmsSignerError {
                        error: SerialisableAwsSignerError::Sign {
                            aws_sdk_error: SerialisableAwsSdkError::Other {
                                message: e.to_string(),
                            },
                        },
                    }
                })?;

                Ok(Bytes::copy_from_slice(&signature.as_bytes()))
            }
            SigningCredential::PrivateKey(_) | SigningCredential::Environment { .. } => {
                let digest = params.local_signing_digest()?;
                let signer = params.credentials.local_signer()?;
                crate::credentials::validate_signer_address(
                    signer.address(),
                    params.signer_address,
                )?;
                let signature =
                    signer
                        .sign_hash(&digest)
                        .await
                        .map_err(|e| EngineError::ValidationError {
                            message: format!("Failed to sign userop: {e}"),
                        })?;

                Ok(Bytes::copy_from_slice(&signature.as_bytes()))
            }
        }
    }
}
