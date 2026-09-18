use std::sync::Arc;

use alloy::primitives::U256;
use engine_core::{
    chain::{ChainService, RpcCredentials},
    credentials::SigningCredential,
    error::EngineError,
    execution_options::{
        BaseExecutionOptions, QueuedTransaction, SendTransactionRequest, SpecificExecutionOptions,
        WebhookOptions, aa::Erc4337ExecutionOptions, eip7702::Eip7702ExecutionOptions,
        eoa::EoaExecutionOptions,
    },
    transaction::InnerTransaction,
};
use engine_eip7702_core::delegated_account::DelegatedAccount;
use engine_executors::{
    eip7702_executor::{
        confirm::Eip7702ConfirmationHandler,
        send::{Eip7702SendHandler, Eip7702SendJobData},
    },
    eoa::{
        EoaExecutorJobHandler, EoaExecutorStore, EoaExecutorWorkerJobData, EoaTransactionRequest,
        authorization_cache::EoaAuthorizationCache,
    },
    external_bundler::{
        confirm::UserOpConfirmationHandler,
        send::{ExternalBundlerSendHandler, ExternalBundlerSendJobData},
    },
    solana_executor::worker::{SolanaExecutorJobData, SolanaExecutorJobHandler},
    transaction_registry::TransactionRegistry,
    webhook::WebhookJobHandler,
};
use twmq::{Queue, error::TwmqError, redis::aio::ConnectionManager};

use crate::chains::ThirdwebChainService;

pub struct ExecutionRouter {
    pub redis: ConnectionManager,
    pub namespace: Option<String>,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub external_bundler_send_queue: Arc<Queue<ExternalBundlerSendHandler<ThirdwebChainService>>>,
    pub userop_confirm_queue: Arc<Queue<UserOpConfirmationHandler<ThirdwebChainService>>>,
    pub eoa_executor_queue: Arc<Queue<EoaExecutorJobHandler<ThirdwebChainService>>>,
    pub eip7702_send_queue: Arc<Queue<Eip7702SendHandler<ThirdwebChainService>>>,
    pub eip7702_confirm_queue: Arc<Queue<Eip7702ConfirmationHandler<ThirdwebChainService>>>,
    pub solana_executor_queue: Arc<Queue<SolanaExecutorJobHandler>>,
    pub transaction_registry: Arc<TransactionRegistry>,
    pub chains: Arc<ThirdwebChainService>,
    pub authorization_cache: EoaAuthorizationCache,
}

impl ExecutionRouter {
    pub async fn execute(
        &self,
        execution_request: SendTransactionRequest,
        rpc_credentials: RpcCredentials,
        signing_credential: engine_core::credentials::SigningCredential,
    ) -> Result<Vec<QueuedTransaction>, EngineError> {
        validate_execution_request(&execution_request)?;
        if self
            .chains
            .is_configured(execution_request.execution_options.base.chain_id)
            && !matches!(rpc_credentials, RpcCredentials::Configured)
        {
            return Err(EngineError::ValidationError { message: "Configured RPC access requires x-engine-signing-token without Thirdweb RPC headers".into() });
        }
        if matches!(rpc_credentials, RpcCredentials::Configured)
            && (!self
                .chains
                .is_configured(execution_request.execution_options.base.chain_id)
                || !matches!(
                    execution_request.execution_options.specific,
                    SpecificExecutionOptions::EOA(_)
                ))
        {
            return Err(EngineError::ValidationError { message: "Configured RPC credentials require an explicitly configured chain and EOA execution".into() });
        }

        match execution_request.execution_options.specific {
            SpecificExecutionOptions::ERC4337(ref erc4337_execution_options) => {

                self.execute_external_bundler(
                    &execution_request.execution_options.base,
                    erc4337_execution_options,
                    &execution_request.webhook_options,
                    &execution_request.params,
                    rpc_credentials,
                    signing_credential,
                    // Persist the ERC-4337 nonce in the admitted job. A retry
                    // after an ambiguous broadcast must not create a new intent.
                    Some(U256::from_limbs([0, rand::random(), rand::random(), rand::random()])),
                )
                .await?;

                let queued_transaction = QueuedTransaction {
                    id: execution_request
                        .execution_options
                        .base
                        .idempotency_key
                        .clone(),
                    batch_index: 0,
                    execution_params: execution_request.execution_options,
                    transaction_params: execution_request.params,
                };

                Ok(vec![queued_transaction])
            }

            SpecificExecutionOptions::EIP7702(ref eip7702_execution_options) => {
                self.execute_eip7702(
                    &execution_request.execution_options.base,
                    eip7702_execution_options,
                    execution_request.webhook_options,
                    &execution_request.params,
                    rpc_credentials,
                    signing_credential,
                )
                .await?;

                let queued_transaction = QueuedTransaction {
                    id: execution_request
                        .execution_options
                        .base
                        .idempotency_key
                        .clone(),
                    batch_index: 0,
                    execution_params: execution_request.execution_options,
                    transaction_params: execution_request.params,
                };

                Ok(vec![queued_transaction])
            }

            SpecificExecutionOptions::Auto(_auto_execution_options) => {
                Err(EngineError::ValidationError {
                    message: "Automatic execution is not implemented. Choose EOA, ERC4337, or EIP7702 explicitly.".to_string(),
                })
            }

            SpecificExecutionOptions::EOA(ref eoa_execution_options) => {
                self.execute_eoa(
                    &execution_request.execution_options.base,
                    eoa_execution_options,
                    execution_request.webhook_options,
                    &execution_request.params,
                    rpc_credentials,
                    signing_credential,
                )
                .await?;

                let queued_transaction = QueuedTransaction {
                    id: execution_request
                        .execution_options
                        .base
                        .idempotency_key
                        .clone(),
                    batch_index: 0,
                    execution_params: execution_request.execution_options,
                    transaction_params: execution_request.params,
                };

                Ok(vec![queued_transaction])
            }
        }
    }

    async fn execute_external_bundler(
        &self,
        base_execution_options: &BaseExecutionOptions,
        erc4337_execution_options: &Erc4337ExecutionOptions,
        webhook_options: &[WebhookOptions],
        transactions: &[InnerTransaction],
        rpc_credentials: RpcCredentials,
        signing_credential: SigningCredential,
        pregenerated_nonce: Option<U256>,
    ) -> Result<(), TwmqError> {
        let job_data = ExternalBundlerSendJobData {
            transaction_id: base_execution_options.idempotency_key.clone(),
            chain_id: base_execution_options.chain_id,
            transactions: transactions.to_vec(),
            execution_options: erc4337_execution_options.clone(),
            signing_credential,
            webhook_options: webhook_options.to_owned(),
            rpc_credentials,
            pregenerated_nonce,
        };

        // Register transaction in registry first
        self.transaction_registry
            .set_transaction_queue(
                &base_execution_options.idempotency_key,
                "external_bundler_send",
            )
            .await
            .map_err(|e| TwmqError::Runtime {
                message: format!("Failed to register transaction: {e}"),
            })?;

        // Create job with transaction ID as the job ID for idempotency
        self.external_bundler_send_queue
            .clone()
            .job(job_data)
            .with_id(&base_execution_options.idempotency_key)
            .push()
            .await?;

        tracing::debug!(
            transaction_id = base_execution_options.idempotency_key,
            queue = "external_bundler_send",
            "Job queued successfully"
        );

        Ok(())
    }

    async fn execute_eip7702(
        &self,
        base_execution_options: &BaseExecutionOptions,
        eip7702_execution_options: &Eip7702ExecutionOptions,
        webhook_options: Vec<WebhookOptions>,
        transactions: &[InnerTransaction],
        rpc_credentials: RpcCredentials,
        signing_credential: SigningCredential,
    ) -> Result<(), TwmqError> {
        let job_data = Eip7702SendJobData {
            transaction_id: base_execution_options.idempotency_key.clone(),
            chain_id: base_execution_options.chain_id,
            transactions: transactions.to_vec(),
            execution_options: eip7702_execution_options.clone(),
            signing_credential,
            webhook_options,
            rpc_credentials,
            // This field is the replay UID for wrapped calls, not an EOA nonce.
            nonce: Some(U256::from_limbs([
                rand::random(),
                rand::random(),
                rand::random(),
                rand::random(),
            ])),
        };

        // Register transaction in registry first
        self.transaction_registry
            .set_transaction_queue(&base_execution_options.idempotency_key, "eip7702_send")
            .await
            .map_err(|e| TwmqError::Runtime {
                message: format!("Failed to register transaction: {e}"),
            })?;

        // Create job with transaction ID as the job ID for idempotency
        self.eip7702_send_queue
            .clone()
            .job(job_data)
            .with_id(&base_execution_options.idempotency_key)
            .push()
            .await?;

        tracing::debug!(
            transaction_id = base_execution_options.idempotency_key,
            queue = "eip7702_send",
            "Job queued successfully"
        );

        Ok(())
    }

    async fn execute_eoa(
        &self,
        base_execution_options: &BaseExecutionOptions,
        eoa_execution_options: &EoaExecutionOptions,
        webhook_options: Vec<WebhookOptions>,
        transactions: &[InnerTransaction],
        rpc_credentials: RpcCredentials,
        signing_credential: SigningCredential,
    ) -> Result<(), EngineError> {
        let chain = self
            .chains
            .get_chain(base_execution_options.chain_id)
            .map_err(|e| EngineError::InternalError {
                message: format!("Failed to get chain: {e}"),
            })?;

        let transaction = if transactions.len() > 1 {
            let delegated_account = DelegatedAccount::new(eoa_execution_options.from, chain);
            let is_minimal_account = self
                .authorization_cache
                .is_minimal_account(&delegated_account, None)
                .await
                .map_err(|e| EngineError::InternalError {
                    message: format!("Failed to check 7702 delegation: {e:?}"),
                })?;

            if !is_minimal_account {
                return Err(EngineError::ValidationError {
                    message: "EOA is not a 7702 delegated account. Batching transactions requires 7702 delegation. Please send a 7702 transaction first to upgrade the EOA.".to_string(),
                });
            }

            let calldata = delegated_account
                .owner_transaction(transactions)
                .calldata_for_self_execution();

            InnerTransaction {
                to: Some(eoa_execution_options.from),
                data: calldata.into(),
                gas_limit: None,
                transaction_type_data: None,
                value: U256::ZERO,
            }
        } else {
            transactions[0].clone()
        };

        let eoa_transaction_request = EoaTransactionRequest {
            transaction_id: base_execution_options.idempotency_key.clone(),
            chain_id: base_execution_options.chain_id,
            from: eoa_execution_options.from,
            to: transaction.to,
            value: transaction.value,
            data: transaction.data.clone(),
            gas_limit: transaction.gas_limit,
            webhook_options: webhook_options.to_vec(),
            signing_credential: signing_credential.clone(),
            rpc_credentials,
            transaction_type_data: transaction.transaction_type_data.clone(),
        };

        let eoa_executor_store = EoaExecutorStore::new(
            self.redis.clone(),
            self.namespace.clone(),
            eoa_execution_options.from,
            base_execution_options.chain_id,
            self.eoa_executor_queue
                .handler
                .completed_transaction_ttl_seconds,
        );

        // Add transaction to the store
        eoa_executor_store
            .add_transaction(eoa_transaction_request)
            .await
            .map_err(|e| match e {
                engine_executors::eoa::store::TransactionStoreError::TransactionConflict { .. } => EngineError::ValidationError {
                    message: "The idempotency key already belongs to a different or incomplete transaction. Use the original request or reconcile its status.".to_string(),
                },
                other => EngineError::from(TwmqError::Runtime {
                    message: format!("Failed to add transaction to EOA store: {other}"),
                }),
            })?;

        // Register transaction in registry
        self.transaction_registry
            .set_transaction_queue(&base_execution_options.idempotency_key, "eoa_executor")
            .await
            .map_err(|e| TwmqError::Runtime {
                message: format!("Failed to register transaction: {e}"),
            })?;

        // Ensure an idempotent job exists for this EOA:chain combination
        let eoa_job_data = EoaExecutorWorkerJobData {
            eoa_address: eoa_execution_options.from,
            chain_id: base_execution_options.chain_id,
            noop_signing_credential: signing_credential,
        };

        // Create idempotent job for this EOA:chain - only one will exist
        let job_id = format!(
            "eoa_{}_{}",
            eoa_execution_options.from, base_execution_options.chain_id
        );

        self.eoa_executor_queue
            .clone()
            .job(eoa_job_data)
            .with_id(&job_id)
            .push()
            .await?;

        tracing::debug!(
            transaction_id = base_execution_options.idempotency_key,
            eoa = ?eoa_execution_options.from,
            chain_id = base_execution_options.chain_id,
            queue = "eoa_executor",
            "EOA transaction added to store and worker job ensured"
        );

        Ok(())
    }

    pub async fn execute_solana(
        &self,
        request: engine_core::execution_options::solana::SendSolanaTransactionRequest,
        signing_credential: SigningCredential,
    ) -> Result<engine_core::execution_options::solana::QueuedSolanaTransactionResponse, EngineError>
    {
        use engine_core::execution_options::solana::{
            QueuedSolanaTransactionResponse, SolanaTransactionOptions,
        };

        if request.execution_options.max_blockhash_retries != 0 {
            return Err(EngineError::ValidationError { message: "Automatic Solana resubmission with a new blockhash is unsupported: maxBlockhashRetries must be 0; ambiguous expired transactions require operator reconciliation".into() });
        }
        let transaction_id = request.idempotency_key.clone();
        let chain_id = request.execution_options.chain_id;
        let signer_address = request.execution_options.signer_address;
        match &signing_credential {
            SigningCredential::SolanaEnvironment { public_key }
                if *public_key == signer_address => {}
            _ => {
                return Err(EngineError::ValidationError {
                    message: "Configured Solana signer does not match the requested payer".into(),
                });
            }
        }

        if matches!(
            &request.input,
            engine_solana_core::transaction::SolanaTransactionInput::Serialized(_)
        ) {
            if request.execution_options.compute_unit_limit.is_some()
                || request.execution_options.priority_fee.is_some()
            {
                return Err(EngineError::ValidationError { message: "Serialized Solana transactions must contain their own compute budget; submission preserves the supplied message".into() });
            }
            // Reject malformed wire/payer before durable admission. Cryptographic
            // signature validation is performed by the signer before broadcast.
            engine_solana_core::transaction::SolanaTransaction {
                input: request.input.clone(),
                compute_unit_limit: None,
                compute_unit_price: None,
            }
            .to_versioned_transaction(signer_address, solana_sdk::hash::Hash::default())
            .map_err(|error| EngineError::ValidationError {
                message: error.to_string(),
            })?;
        }

        let transaction = SolanaTransactionOptions {
            input: request.input,
            execution_options: request.execution_options,
        };

        let job_data = SolanaExecutorJobData {
            transaction_id: transaction_id.clone(),
            transaction,
            signing_credential,
            webhook_options: request.webhook_options,
        };

        crate::solana_admission::admit(
            &self.redis,
            &self.solana_executor_queue,
            &self.transaction_registry,
            &self.solana_executor_queue.handler.storage,
            &job_data,
        )
        .await?;

        tracing::debug!(
            transaction_id = %transaction_id,
            chain_id = %chain_id.as_str(),
            signer = %signer_address,
            queue = "solana_executor",
            "Solana job queued successfully"
        );

        Ok(QueuedSolanaTransactionResponse {
            transaction_id,
            chain_id,
            signer_address: signer_address.to_string(),
        })
    }
}

/// Validate before any provider calls or durable queue writes.
fn validate_execution_request(request: &SendTransactionRequest) -> Result<(), EngineError> {
    if request.params.is_empty() {
        return Err(EngineError::ValidationError {
            message: "At least one transaction is required.".to_string(),
        });
    }
    if matches!(
        request.execution_options.specific,
        SpecificExecutionOptions::Auto(_)
    ) {
        return Err(EngineError::ValidationError {
            message: "Automatic execution is not implemented. Choose EOA, ERC4337, or EIP7702 explicitly.".to_string(),
        });
    }
    // CALL-based account execution does not implement contract creation. Replacing
    // an absent recipient with address(0) would silently change the user's intent.
    let supports_creation = matches!(
        request.execution_options.specific,
        SpecificExecutionOptions::EOA(_)
    ) && request.params.len() == 1;
    if !supports_creation && request.params.iter().any(|tx| tx.to.is_none()) {
        return Err(EngineError::ValidationError {
            message: "Contract creation requires a single EOA transaction with no recipient."
                .to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod request_validation_tests {
    use super::*;
    use serde_json::json;

    fn request(executor_type: Option<&str>, params: serde_json::Value) -> SendTransactionRequest {
        let mut options =
            json!({"chainId": 31337, "from": "0x1111111111111111111111111111111111111111"});
        if let Some(executor_type) = executor_type {
            options["type"] = json!(executor_type);
        }
        serde_json::from_value(json!({"executionOptions": options, "params": params})).unwrap()
    }

    #[test]
    fn empty_and_automatic_execution_are_rejected_before_execution() {
        assert!(validate_execution_request(&request(Some("EOA"), json!([]))).is_err());
        for executor_type in [None, Some("auto")] {
            assert!(
                validate_execution_request(&request(executor_type, json!([{"to": null}]))).is_err()
            );
        }
    }

    #[test]
    fn contract_creation_requires_single_eoa_transaction() {
        assert!(validate_execution_request(&request(Some("EOA"), json!([{"to": null}]))).is_ok());
        assert!(
            validate_execution_request(&request(Some("EOA"), json!([{"to": null}, {"to": null}])))
                .is_err()
        );
        assert!(
            validate_execution_request(&request(Some("EIP7702"), json!([{"to": null}]))).is_err()
        );
    }
}
