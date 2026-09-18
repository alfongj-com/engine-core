use std::sync::Arc;

use alloy::{
    dyn_abi::TypedData,
    primitives::{Address, B256, hex, keccak256},
    sol,
    sol_types::{SolCall, SolValue, eip712_domain},
};
use engine_core::signer::MessageFormat;
use engine_core::{
    chain::Chain,
    credentials::SigningCredential,
    error::{ContractErrorToEngineError, EngineError},
    signer::{AccountSigner, EoaSigner, EoaSigningOptions, Erc4337SigningOptions},
};
use serde::Serialize;

use crate::{
    account_factory::{AccountFactory, get_account_factory},
    smart_account::{DeterminedSmartAccount, SmartAccount, SmartAccountFromSalt},
};

sol! {
    #[sol(rpc)]
    contract ERC1271Contract {
        function isValidSignature(bytes32 hash, bytes signature) external view returns (bytes4 magicValue);
    }
}

sol! {
    #[sol(rpc)]
    contract AccountImplementationContract {
        function getMessageHash(bytes32 _hash) external view returns (bytes32);
    }
}

sol! {
    #[derive(Serialize)]
    struct AccountMessage {
        bytes message;
    }
}

/// ERC-6492 magic suffix
const ERC6492_MAGIC_SUFFIX: [u8; 32] =
    hex!("6492649264926492649264926492649264926492649264926492649264926492");

/// Builder for creating SmartAccountSigner with computed address and factory pattern detection
pub struct SmartAccountSignerBuilder<C: Chain> {
    eoa_signer: Arc<EoaSigner>,
    credentials: SigningCredential,
    options: Erc4337SigningOptions,
    chain: C,
}

impl<C: Chain + Clone> SmartAccountSignerBuilder<C> {
    pub fn new(
        eoa_signer: Arc<EoaSigner>,
        credentials: SigningCredential,
        options: Erc4337SigningOptions,
        chain: C,
    ) -> Self {
        Self {
            eoa_signer,
            credentials,
            options,
            chain,
        }
    }

    /// Build the signer with computed address and factory pattern detection
    pub async fn build(self) -> Result<SmartAccountSigner<C>, EngineError> {
        // 1. Parse Account Salt using the helper method
        let salt_data = self.options.get_salt_data()?;

        // 2. Determine Smart Account
        let smart_account = match self.options.smart_account_address {
            Some(address) => DeterminedSmartAccount { address },
            None => SmartAccountFromSalt {
                admin_address: self.options.signer_address,
                chain: &self.chain,
                factory_address: self.options.entrypoint_details.factory_address,
                salt_data: &salt_data,
            }
            .to_determined_smart_account()
            .await
            .map_err(|e| EngineError::ValidationError {
                message: format!("Failed to determine smart account: {e}"),
            })?,
        };

        let factory = get_account_factory(
            &self.chain,
            self.options.entrypoint_details.factory_address,
            None,
        );
        let init_calldata = factory.init_calldata(self.options.signer_address, salt_data);
        let impl_address = factory.implementation_address().await?;

        // Check if factory supports 712 pattern
        let supports_712_factory = self
            .check_712_factory_support(impl_address)
            .await
            .unwrap_or(false);

        Ok(SmartAccountSigner {
            options: self.options,
            chain: self.chain,
            eoa_signer: self.eoa_signer,
            credentials: self.credentials,
            smart_account,
            supports_712_factory,
            init_calldata,
        })
    }

    async fn check_712_factory_support(&self, impl_address: Address) -> Result<bool, EngineError> {
        let impl_contract =
            AccountImplementationContract::new(impl_address, self.chain.provider().clone());

        // Test with a dummy hash
        let dummy_hash = B256::ZERO;

        match impl_contract.getMessageHash(dummy_hash).call().await {
            Ok(response) => Ok(response != B256::ZERO),
            Err(_) => Ok(false),
        }
    }
}

/// Smart Account signer with pre-computed address and factory pattern support
#[derive(Clone)]
pub struct SmartAccountSigner<C: Chain> {
    options: Erc4337SigningOptions,
    credentials: SigningCredential,
    chain: C,
    eoa_signer: Arc<EoaSigner>,
    smart_account: DeterminedSmartAccount,
    init_calldata: Vec<u8>,
    supports_712_factory: bool,
}

impl<C: Chain + Clone> SmartAccountSigner<C> {
    /// Sign message with 712 factory wrapping if supported
    async fn sign_message_with_factory_pattern(
        &self,
        message: &str,
        format: MessageFormat,
    ) -> Result<String, EngineError> {
        if self.supports_712_factory {
            // Wrap message in EIP-712 domain for 712 factory pattern
            let message_hash = self.hash_message(message, format);
            self.sign_712_wrapped_hash(message_hash).await
        } else {
            // Direct EOA signing
            self.eoa_signer
                .sign_message(
                    EoaSigningOptions {
                        chain_id: Some(self.chain.chain_id()),
                        from: self.options.signer_address,
                    },
                    message,
                    format,
                    &self.credentials,
                )
                .await
        }
    }

    /// Sign typed data with 712 factory wrapping if supported
    async fn sign_typed_data_with_factory_pattern(
        &self,
        typed_data: &TypedData,
    ) -> Result<String, EngineError> {
        // Check if self-verifying contract (e.g., session key operations)
        let is_self_verifying = typed_data
            .domain
            .verifying_contract
            .map(|addr| addr == self.smart_account.address)
            .unwrap_or(false);

        if is_self_verifying {
            // Direct EOA signing for self-verifying contracts
            return self
                .eoa_signer
                .sign_typed_data(
                    EoaSigningOptions {
                        chain_id: Some(self.chain.chain_id()),
                        from: self.options.signer_address,
                    },
                    typed_data,
                    &self.credentials,
                )
                .await;
        }

        if self.supports_712_factory {
            // Wrap typed data hash in EIP-712 domain for 712 factory pattern
            let typed_data_hash =
                typed_data
                    .eip712_signing_hash()
                    .map_err(|_e| EngineError::ValidationError {
                        message: "Failed to compute typed data hash".to_string(),
                    })?;
            self.sign_712_wrapped_hash(typed_data_hash).await
        } else {
            // Direct EOA signing
            self.eoa_signer
                .sign_typed_data(
                    EoaSigningOptions {
                        chain_id: Some(self.chain.chain_id()),
                        from: self.options.signer_address,
                    },
                    typed_data,
                    &self.credentials,
                )
                .await
        }
    }

    /// Sign hash wrapped in AccountMessage EIP-712 structure for 712 factory pattern
    async fn sign_712_wrapped_hash(&self, hash: B256) -> Result<String, EngineError> {
        let domain = eip712_domain! {
            name: "Account",
            version: "1",
            chain_id: self.options.chain_id,
            verifying_contract: self.smart_account.address,
        };

        let account_message = AccountMessage {
            message: hash.abi_encode().into(),
        };

        // Get the EIP712 signing hash using alloy's native functionality
        let typed_data = TypedData::from_struct(&account_message, Some(domain));

        // Sign the hash directly with EOA
        self.eoa_signer
            .sign_typed_data(
                EoaSigningOptions {
                    chain_id: Some(self.chain.chain_id()),
                    from: self.options.signer_address,
                },
                &typed_data,
                &self.credentials,
            )
            .await
    }

    /// Verify ERC-1271 signature
    pub async fn verify_erc1271(&self, hash: B256, signature: &str) -> Result<bool, EngineError> {
        let signature_bytes = hex::decode(signature.strip_prefix("0x").unwrap_or(signature))
            .map_err(|_| EngineError::ValidationError {
                message: "Invalid signature hex".to_string(),
            })?;

        let contract =
            ERC1271Contract::new(self.smart_account.address, self.chain.provider().clone());

        match contract
            .isValidSignature(hash, signature_bytes.into())
            .call()
            .await
        {
            Ok(response) => {
                let expected_magic = ERC1271Contract::isValidSignatureCall::SELECTOR;
                Ok(response.as_slice() == expected_magic)
            }
            Err(e) => {
                Err(e.to_engine_error(self.chain.chain_id(), Some(self.smart_account.address)))
            }
        }
    }

    /// Create ERC-6492 signature for undeployed accounts
    async fn create_erc6492_signature(&self, signature: &str) -> Result<String, EngineError> {
        let signature_bytes = hex::decode(signature.strip_prefix("0x").unwrap_or(signature))
            .map_err(|_| EngineError::ValidationError {
                message: "Invalid signature hex".to_string(),
            })?;

        Ok(format!(
            "0x{}",
            hex::encode(encode_erc6492_signature(
                self.options.entrypoint_details.factory_address,
                &self.init_calldata,
                &signature_bytes,
            ))
        ))
    }

    /// Hash message according to format
    fn hash_message(&self, message: &str, format: MessageFormat) -> B256 {
        match format {
            MessageFormat::Text => {
                let prefixed =
                    format!("\x19Ethereum Signed Message:\n{}{}", message.len(), message);
                keccak256(prefixed.as_bytes())
            }
            MessageFormat::Hex => {
                let bytes = hex::decode(message.strip_prefix("0x").unwrap_or(message))
                    .unwrap_or_else(|_| message.as_bytes().to_vec());
                keccak256(bytes)
            }
        }
    }

    pub async fn sign_message(
        &self,
        message: &str,
        format: MessageFormat,
    ) -> Result<String, EngineError> {
        let is_deployed = self.smart_account.is_deployed(&self.chain).await?;

        // Get signature with appropriate factory pattern handling
        let signature = self
            .sign_message_with_factory_pattern(message, format)
            .await?;

        if is_deployed {
            // Verify ERC-1271 signature for deployed accounts
            // let message_hash = self.hash_message(message, format);
            // let is_valid = self.verify_erc1271(message_hash, &signature).await?;

            // if is_valid {
            Ok(signature)
            // } else {
            //     Err(EngineError::ValidationError {
            //         message: "ERC-1271 signature validation failed".to_string(),
            //     })
            // }
        } else {
            // Create ERC-6492 signature for undeployed accounts
            self.create_erc6492_signature(&signature).await
        }
    }

    pub async fn sign_typed_data(&self, typed_data: &TypedData) -> Result<String, EngineError> {
        let is_deployed = self.smart_account.is_deployed(&self.chain).await?;

        // Get signature with appropriate factory pattern handling
        let signature = self
            .sign_typed_data_with_factory_pattern(typed_data)
            .await?;

        if is_deployed {
            // Verify ERC-1271 signature for deployed accounts
            // let typed_data_hash =
            //     typed_data
            //         .eip712_signing_hash()
            //         .map_err(|_e| EngineError::ValidationError {
            //             message: "Failed to compute typed data hash".to_string(),
            //         })?;
            // let is_valid = self.verify_erc1271(typed_data_hash, &signature).await?;

            // if is_valid {
            Ok(signature)
            // } else {
            //     Err(EngineError::ValidationError {
            //         message: "ERC-1271 signature validation failed".to_string(),
            //     })
            // }
        } else {
            // Create ERC-6492 signature for undeployed accounts
            self.create_erc6492_signature(&signature).await
        }
    }
}

/// ERC-6492 requires abi.encode(address, bytes, bytes), including dynamic offsets
/// and word padding, followed by the fixed detection suffix.
fn encode_erc6492_signature(factory: Address, calldata: &[u8], signature: &[u8]) -> Vec<u8> {
    let mut encoded = (
        factory,
        alloy::primitives::Bytes::copy_from_slice(calldata),
        alloy::primitives::Bytes::copy_from_slice(signature),
    )
        .abi_encode_params();
    encoded.extend_from_slice(&ERC6492_MAGIC_SUFFIX);
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Bytes, address};

    #[test]
    fn erc6492_matches_standard_abi_layout() {
        // Independent ABI fixture: head words are address, offset 0x60, offset
        // 0xa0. The two tails contain their length and right-padded payload.
        let expected = hex::decode(concat!(
            "0000000000000000000000001111111111111111111111111111111111111111",
            "0000000000000000000000000000000000000000000000000000000000000060",
            "00000000000000000000000000000000000000000000000000000000000000a0",
            "0000000000000000000000000000000000000000000000000000000000000003",
            "abcdef0000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000002",
            "1234000000000000000000000000000000000000000000000000000000000000",
            "6492649264926492649264926492649264926492649264926492649264926492"
        ))
        .unwrap();
        let actual = encode_erc6492_signature(
            address!("1111111111111111111111111111111111111111"),
            &[0xab, 0xcd, 0xef],
            &[0x12, 0x34],
        );
        assert_eq!(actual, expected);
    }

    #[test]
    fn erc6492_verifier_can_decode_empty_and_non_word_aligned_payloads() {
        for length in [0, 1, 31, 32, 33, 65] {
            let calldata = vec![0x42; length];
            let signature = vec![0x24; 65];
            let encoded = encode_erc6492_signature(Address::ZERO, &calldata, &signature);
            let (body, suffix) = encoded.split_at(encoded.len() - ERC6492_MAGIC_SUFFIX.len());
            assert_eq!(suffix, ERC6492_MAGIC_SUFFIX);
            let decoded = <(Address, Bytes, Bytes)>::abi_decode_params(body).unwrap();
            assert_eq!(decoded, (Address::ZERO, calldata.into(), signature.into()));
        }
    }
}
