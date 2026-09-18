use std::sync::Arc;

use alloy::{
    hex,
    primitives::{Address, Bytes, U256},
    rpc::types::{PackedUserOperation, UserOperation},
    sol_types::SolCall,
};
use engine_aa_types::VersionedUserOp;
use engine_core::{
    chain::Chain,
    credentials::SigningCredential,
    error::{AlloyRpcErrorToEngineError, EngineError},
    execution_options::aa::{EntrypointAndFactoryDetails, EntrypointVersion},
    userop::{UserOpSigner, UserOpSignerParams, UserOpSigningProfile},
};

use crate::account_factory::{DefaultAccountFactory, SyncAccountFactory, createAccountCall};

pub struct UserOpBuilderConfig<'a, C: Chain> {
    pub account_address: Address,
    pub signer_address: Address,
    pub entrypoint_and_factory: EntrypointAndFactoryDetails,
    pub call_gas_limit: Option<U256>,
    pub call_data: Bytes,
    pub init_call_data: Vec<u8>,
    pub is_deployed: bool,
    pub nonce: U256,
    pub credential: SigningCredential,
    pub chain: &'a C,
    pub signer: Arc<UserOpSigner>,
}

pub struct UserOpBuilder<'a, C: Chain> {
    config: UserOpBuilderConfig<'a, C>,
}

const DUMMY_SIGNATURE: [u8; 65] = hex!(
    "0xfffffffffffffffffffffffffffffff0000000000000000000000000000000007aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1c"
);

impl<'a, C: Chain> UserOpBuilder<'a, C> {
    pub fn new(config: UserOpBuilderConfig<'a, C>) -> Self {
        Self { config }
    }

    pub async fn build(self) -> Result<VersionedUserOp, EngineError> {
        if !matches!(self.config.credential, SigningCredential::Iaw { .. }) {
            validate_local_account_profile(
                &self.config.entrypoint_and_factory,
                self.config.account_address,
                self.config.signer_address,
                &self.config.init_call_data,
            )?;
        }
        let mut userop = match self.config.entrypoint_and_factory.version {
            EntrypointVersion::V0_6 => UserOpBuilderV0_6::new(&self.config).build().await?,
            EntrypointVersion::V0_7 => UserOpBuilderV0_7::new(&self.config).build().await?,
        };

        tracing::debug!("UserOp built, proceeding with signing");

        let signature = self
            .config
            .signer
            .sign(UserOpSignerParams {
                credentials: self.config.credential.clone(),
                entrypoint: self.config.entrypoint_and_factory.entrypoint_address,
                factory_address: self.config.entrypoint_and_factory.factory_address,
                userop: userop.clone(),
                signer_address: self.config.signer_address,
                chain_id: self.config.chain.chain_id(),
            })
            .await?;

        match &mut userop {
            VersionedUserOp::V0_6(userop) => {
                userop.signature = signature;
            }
            VersionedUserOp::V0_7(userop) => {
                userop.signature = signature;
            }
        }

        tracing::debug!("UserOp signed succcessfully");

        Ok(userop)
    }
}

/// A caller-supplied smartAccountAddress cannot inherit a default factory's
/// signature policy. This bounded mode supports accounts derived from the
/// original admin and salt; IAW retains its external account-policy handling.
fn validate_local_account_profile(
    details: &EntrypointAndFactoryDetails,
    account_address: Address,
    signer_address: Address,
    init_call_data: &[u8],
) -> Result<(), EngineError> {
    let profile = UserOpSigningProfile::for_factory(details.factory_address, details.version)?;
    let invalid = || {
        EngineError::ValidationError {
        message: "Local/KMS UserOperation account must match the reviewed factory, original admin and salt".into(),
    }
    };
    let creation = createAccountCall::abi_decode(init_call_data).map_err(|_| invalid())?;
    if creation.admin != signer_address || creation.abi_encode() != init_call_data {
        return Err(invalid());
    }
    let factory = match profile {
        UserOpSigningProfile::ThirdwebV06 => DefaultAccountFactory::v0_6(),
        UserOpSigningProfile::ThirdwebV07 => DefaultAccountFactory::v0_7(),
    };
    if factory.predict_address_sync(&creation.admin, &creation.salt) != account_address {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod signing_profile_tests {
    use super::*;
    use engine_core::constants::{DEFAULT_FACTORY_ADDRESS_V0_6, DEFAULT_FACTORY_ADDRESS_V0_7};

    #[test]
    fn reviewed_profiles_require_the_expected_account_admin_and_salt() {
        // CREATE2 addresses independently computed by the Python fixture generator.
        let admin: Address = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
            .parse()
            .unwrap();
        for (version, factory_address, account) in [
            (
                EntrypointVersion::V0_6,
                DEFAULT_FACTORY_ADDRESS_V0_6,
                "0x2e5cf4c1226c261f873307a32ed586872ed1d0c9",
            ),
            (
                EntrypointVersion::V0_7,
                DEFAULT_FACTORY_ADDRESS_V0_7,
                "0xeae62eec20c302719330db3506c01dacaa6768a6",
            ),
        ] {
            let details = EntrypointAndFactoryDetails {
                version,
                factory_address,
                entrypoint_address: Address::repeat_byte(0x11),
            };
            let account = account.parse().unwrap();
            let creation = createAccountCall {
                admin,
                salt: Bytes::new(),
            }
            .abi_encode();
            assert!(validate_local_account_profile(&details, account, admin, &creation).is_ok());
            assert!(
                validate_local_account_profile(
                    &details,
                    Address::repeat_byte(0x99),
                    admin,
                    &creation
                )
                .is_err()
            );
            assert!(
                validate_local_account_profile(
                    &details,
                    account,
                    Address::repeat_byte(0x99),
                    &creation
                )
                .is_err()
            );
            let other_salt = createAccountCall {
                admin,
                salt: Bytes::from(vec![1]),
            }
            .abi_encode();
            assert!(validate_local_account_profile(&details, account, admin, &other_salt).is_err());
            assert!(validate_local_account_profile(&details, account, admin, &[]).is_err());
            let mut noncanonical = creation.clone();
            noncanonical.push(0);
            assert!(
                validate_local_account_profile(&details, account, admin, &noncanonical).is_err()
            );
            let unknown = EntrypointAndFactoryDetails {
                factory_address: Address::repeat_byte(0x99),
                ..details
            };
            assert!(validate_local_account_profile(&unknown, account, admin, &creation).is_err());
        }
    }
}

struct UserOpBuilderV0_6<'a, C: Chain> {
    userop: UserOperation,
    entrypoint: Address,
    chain: &'a C,
}

impl<'a, C: Chain> UserOpBuilderV0_6<'a, C> {
    pub fn new(config: &UserOpBuilderConfig<'a, C>) -> Self {
        let initcode: Bytes = if config.is_deployed {
            Bytes::default()
        } else {
            let mut initcode: Vec<u8> = config
                .entrypoint_and_factory
                .factory_address
                .into_array()
                .to_vec();

            initcode.extend_from_slice(config.init_call_data.as_slice());
            Bytes::from(initcode)
        };
        Self {
            userop: UserOperation {
                sender: config.account_address,
                nonce: config.nonce,
                init_code: initcode,
                call_data: config.call_data.clone(),
                call_gas_limit: config.call_gas_limit.unwrap_or(U256::ZERO),
                verification_gas_limit: U256::ZERO,
                pre_verification_gas: U256::ZERO,
                max_fee_per_gas: U256::ZERO,
                max_priority_fee_per_gas: U256::ZERO,
                paymaster_and_data: Bytes::default(),
                signature: Bytes::from(DUMMY_SIGNATURE),
            },
            entrypoint: config.entrypoint_and_factory.entrypoint_address,
            chain: config.chain,
        }
    }

    async fn build(mut self) -> Result<VersionedUserOp, EngineError> {
        // let prices = self
        //     .chain
        //     .provider()
        //     .estimate_eip1559_fees()
        //     .await
        //     .map_err(|err| err.to_engine_error(self.chain))?;

        // TODO: modularize this so only used with thirdweb paymaster
        let prices = self
            .chain
            .paymaster_client()
            .get_user_op_gas_fees()
            .await
            .map_err(|e| e.to_engine_error(self.chain))?;

        tracing::debug!("Gas prices determined");

        self.userop.max_fee_per_gas = U256::from(prices.max_fee_per_gas);
        self.userop.max_priority_fee_per_gas = U256::from(prices.max_priority_fee_per_gas);

        let pm_response = self
            .chain
            .paymaster_client()
            .get_user_op_paymaster_and_data_v0_6(&self.userop, self.entrypoint)
            .await
            .map_err(|err| err.to_engine_paymaster_error(self.chain))?;

        tracing::debug!("v6 Userop paymaster and data determined");

        self.userop.paymaster_and_data = pm_response.paymaster_and_data;

        let (call_gas_limit, verification_gas_limit, pre_verification_gas) = match (
            pm_response.call_gas_limit,
            pm_response.verification_gas_limit,
            pm_response.pre_verification_gas,
        ) {
            (Some(call_gas_limit), Some(verification_gas_limit), Some(pre_verification_gas)) => {
                (call_gas_limit, verification_gas_limit, pre_verification_gas)
            }
            _ => {
                tracing::debug!("No paymaster provided gas limits, getting from bundler");

                let bundler_response = self
                    .chain
                    .bundler_client()
                    .estimate_user_op_gas(
                        &VersionedUserOp::V0_6(self.userop.clone()),
                        self.entrypoint,
                        None,
                    )
                    .await
                    .map_err(|err| err.to_engine_bundler_error(self.chain))?;

                (
                    bundler_response.call_gas_limit,
                    bundler_response.verification_gas_limit,
                    bundler_response.pre_verification_gas,
                )
            }
        };

        self.userop.call_gas_limit = call_gas_limit;
        self.userop.verification_gas_limit = verification_gas_limit;
        self.userop.pre_verification_gas = pre_verification_gas;

        Ok(VersionedUserOp::V0_6(self.userop))
    }
}

// New V0.7 Builder Implementation
struct UserOpBuilderV0_7<'a, C: Chain> {
    userop: PackedUserOperation,
    entrypoint: Address,
    chain: &'a C,
}

impl<'a, C: Chain> UserOpBuilderV0_7<'a, C> {
    pub fn new(config: &UserOpBuilderConfig<'a, C>) -> Self {
        let (factory, factory_data) = if !config.is_deployed {
            // If not deployed, set factory and factory data
            (
                Some(config.entrypoint_and_factory.factory_address),
                Some(Bytes::from(config.init_call_data.clone())),
            )
        } else {
            // If deployed, use None for factory and empty bytes for factory data
            (None, None)
        };

        Self {
            userop: PackedUserOperation {
                sender: config.account_address,
                nonce: config.nonce,
                factory,
                factory_data,
                call_data: config.call_data.clone(),
                call_gas_limit: config.call_gas_limit.unwrap_or(U256::ZERO),
                verification_gas_limit: U256::ZERO,
                pre_verification_gas: U256::ZERO,
                max_fee_per_gas: U256::ZERO,
                max_priority_fee_per_gas: U256::ZERO,
                paymaster: None,
                paymaster_data: None,
                paymaster_verification_gas_limit: None,
                paymaster_post_op_gas_limit: None,
                signature: Bytes::from(DUMMY_SIGNATURE),
            },
            entrypoint: config.entrypoint_and_factory.entrypoint_address,
            chain: config.chain,
        }
    }

    async fn build(mut self) -> Result<VersionedUserOp, EngineError> {
        // Get gas prices, same as v0.6
        // let prices = self
        //     .chain
        //     .provider()
        //     .estimate_eip1559_fees()
        //     .await
        //     .map_err(|err| err.to_engine_error(self.chain))?;

        // TODO: modularize this so only used with thirdweb paymaster
        let prices = self
            .chain
            .paymaster_client()
            .get_user_op_gas_fees()
            .await
            .map_err(|e| e.to_engine_error(self.chain))?;

        tracing::info!("Gas prices determined");

        self.userop.max_fee_per_gas = U256::from(prices.max_fee_per_gas);
        self.userop.max_priority_fee_per_gas = U256::from(prices.max_priority_fee_per_gas);

        // Get paymaster data
        let pm_response = self
            .chain
            .paymaster_client()
            .get_user_op_paymaster_and_data_v0_7(&self.userop, self.entrypoint)
            .await
            .map_err(|err| err.to_engine_paymaster_error(self.chain))?;

        tracing::debug!("v7 Userop paymaster and data determined");

        // Apply paymaster data
        self.userop.paymaster = Some(pm_response.paymaster);
        self.userop.paymaster_data = Some(pm_response.paymaster_data);

        // Determine gas limits - either from paymaster or from bundler
        let (
            call_gas_limit,
            verification_gas_limit,
            pre_verification_gas,
            paymaster_verification_gas_limit,
            paymaster_post_op_gas_limit,
        ) = match (
            pm_response.call_gas_limit,
            pm_response.verification_gas_limit,
            pm_response.pre_verification_gas,
            pm_response.paymaster_verification_gas_limit,
            pm_response.paymaster_post_op_gas_limit,
        ) {
            (Some(call), Some(verification), Some(pre), Some(pm_verification), Some(pm_post)) => {
                (call, verification, pre, pm_verification, pm_post)
            }
            _ => {
                // If paymaster didn't provide all gas limits, get them from the bundler
                tracing::debug!("No paymaster provided gas limits, getting from bundler");

                let bundler_response = self
                    .chain
                    .bundler_client()
                    .estimate_user_op_gas(
                        &VersionedUserOp::V0_7(self.userop.clone()),
                        self.entrypoint,
                        None,
                    )
                    .await
                    .map_err(|err| err.to_engine_bundler_error(self.chain))?;

                (
                    bundler_response.call_gas_limit,
                    bundler_response.verification_gas_limit,
                    bundler_response.pre_verification_gas,
                    bundler_response.paymaster_verification_gas_limit,
                    bundler_response
                        .paymaster_post_op_gas_limit
                        .unwrap_or_default(),
                )
            }
        };

        tracing::debug!("Gas limits determined");

        // Set gas limits
        self.userop.call_gas_limit = call_gas_limit;
        self.userop.verification_gas_limit = verification_gas_limit;
        self.userop.pre_verification_gas = pre_verification_gas;
        self.userop.paymaster_verification_gas_limit = Some(paymaster_verification_gas_limit);
        self.userop.paymaster_post_op_gas_limit = Some(paymaster_post_op_gas_limit);

        Ok(VersionedUserOp::V0_7(self.userop))
    }
}
