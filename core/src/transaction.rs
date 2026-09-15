use crate::defs::{AddressDef, BytesDef, SignedAuthorizationSchema, U256Def};
use alloy::{
    eips::eip7702::SignedAuthorization,
    primitives::{Address, Bytes, U256},
};
use serde::{Deserialize, Deserializer, Serialize, de::Error};

/// ### InnerTransaction
/// This is the actual encoded inner transaction data that will be sent to the blockchain.
#[derive(Deserialize, Serialize, Debug, Clone, utoipa::ToSchema)]
pub struct InnerTransaction {
    #[schema(value_type = Option<AddressDef>)]
    pub to: Option<Address>,

    #[schema(value_type = BytesDef)]
    #[serde(default)]
    pub data: Bytes,

    #[schema(value_type = U256Def)]
    #[serde(default)]
    pub value: U256,

    /// Gas limit for the transaction
    /// If not provided, engine will estimate the gas limit
    #[schema(value_type = Option<u64>)]
    #[serde(default, rename = "gasLimit", skip_serializing_if = "Option::is_none")]
    pub gas_limit: Option<u64>,

    /// Transaction type-specific data for different EIP standards
    ///
    /// This is the actual encoded inner transaction data that will be sent to the blockchain.
    ///
    /// Depending on the execution mode chosen, these might be ignored:
    ///
    /// - For ERC4337 execution, all gas fee related fields are ignored. Sending signed authorizations is also not supported.
    #[serde(
        flatten,
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_transaction_type_data"
    )]
    pub transaction_type_data: Option<TransactionTypeData>,
}

#[derive(Serialize, Debug, Clone, utoipa::ToSchema)]
#[serde(untagged)]
#[schema(title = "Transaction Type Specific Data")]
pub enum TransactionTypeData {
    /// EIP-7702 transaction with authorization list and EIP-1559 gas pricing
    Eip7702(Transaction7702Data),
    /// EIP-1559 transaction with priority fee and max fee per gas
    Eip1559(Transaction1559Data),
    /// Legacy transaction with simple gas price
    Legacy(TransactionLegacyData),
}

/// Deserialize flattened optional fee fields without serde's untagged Option
/// fallback, which would turn an invalid/conflicting fee request into None.
pub fn deserialize_transaction_type_data<'de, D>(
    deserializer: D,
) -> Result<Option<TransactionTypeData>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct FeeFields {
        #[serde(default, deserialize_with = "deserialize_fee")]
        gas_price: Option<u128>,
        #[serde(default, deserialize_with = "deserialize_fee")]
        max_fee_per_gas: Option<u128>,
        #[serde(default, deserialize_with = "deserialize_fee")]
        max_priority_fee_per_gas: Option<u128>,
        authorization_list: Option<Vec<SignedAuthorization>>,
    }
    // Serde's flattened ContentDeserializer does not implement deserialize_u128.
    // Parse JSON numbers through deserialize_any, preserving integer validation.
    fn deserialize_fee<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u128>, D::Error> {
        Option::<serde_json::Number>::deserialize(d)?
            .map(|number| {
                number
                    .as_u128()
                    .ok_or_else(|| D::Error::custom("gas fees must be nonnegative u128 integers"))
            })
            .transpose()
    }

    let fields = FeeFields::deserialize(deserializer)?;
    let dynamic_fees =
        fields.max_fee_per_gas.is_some() || fields.max_priority_fee_per_gas.is_some();
    if fields.gas_price.is_some() && (dynamic_fees || fields.authorization_list.is_some()) {
        return Err(D::Error::custom(
            "gasPrice cannot be combined with EIP-1559 fees or authorizationList",
        ));
    }
    if let (Some(max_fee), Some(priority_fee)) =
        (fields.max_fee_per_gas, fields.max_priority_fee_per_gas)
    {
        if priority_fee > max_fee {
            return Err(D::Error::custom(
                "maxPriorityFeePerGas must not exceed maxFeePerGas",
            ));
        }
    }
    if let Some(authorizations) = fields.authorization_list {
        if authorizations.is_empty() {
            return Err(D::Error::custom("authorizationList must not be empty"));
        }
        Ok(Some(TransactionTypeData::Eip7702(Transaction7702Data {
            authorization_list: Some(authorizations),
            max_fee_per_gas: fields.max_fee_per_gas,
            max_priority_fee_per_gas: fields.max_priority_fee_per_gas,
        })))
    } else if let Some(gas_price) = fields.gas_price {
        Ok(Some(TransactionTypeData::Legacy(TransactionLegacyData {
            gas_price: Some(gas_price),
        })))
    } else if dynamic_fees {
        Ok(Some(TransactionTypeData::Eip1559(Transaction1559Data {
            max_fee_per_gas: fields.max_fee_per_gas,
            max_priority_fee_per_gas: fields.max_priority_fee_per_gas,
        })))
    } else {
        Ok(None)
    }
}

impl<'de> Deserialize<'de> for TransactionTypeData {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_transaction_type_data(deserializer)?
            .ok_or_else(|| D::Error::custom("expected transaction fee or authorization fields"))
    }
}

/// EIP-7702 transaction configuration
/// Allows delegation of EOA to smart contract logic temporarily
#[derive(Serialize, Deserialize, Debug, Clone, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(title = "EIP-7702 Specific Transaction Data")]
pub struct Transaction7702Data {
    /// List of signed authorizations for contract delegation
    /// Each authorization allows the EOA to temporarily delegate to a smart contract
    #[schema(value_type = Option<Vec<SignedAuthorizationSchema>>)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization_list: Option<Vec<SignedAuthorization>>,

    /// Maximum fee per gas willing to pay (in wei)
    /// This is the total fee cap including base fee and priority fee
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_fee_per_gas: Option<u128>,

    /// Maximum priority fee per gas willing to pay (in wei)
    /// This is the tip paid to validators for transaction inclusion
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_priority_fee_per_gas: Option<u128>,
}

/// EIP-1559 transaction configuration
/// Uses base fee + priority fee model for more predictable gas pricing
#[derive(Serialize, Deserialize, Debug, Clone, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(title = "EIP-1559 Specific Transaction Data")]
pub struct Transaction1559Data {
    /// Maximum fee per gas willing to pay (in wei)
    /// This is the total fee cap including base fee and priority fee
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_fee_per_gas: Option<u128>,

    /// Maximum priority fee per gas willing to pay (in wei)
    /// This is the tip paid to validators for transaction inclusion
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_priority_fee_per_gas: Option<u128>,
}

/// Legacy transaction configuration
/// Uses simple gas price model (pre-EIP-1559)
#[derive(Serialize, Deserialize, Debug, Clone, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(title = "Legacy Specific Transaction Data")]
pub struct TransactionLegacyData {
    /// Gas price willing to pay (in wei)
    /// This is the total price per unit of gas for legacy transactions
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gas_price: Option<u128>,
}

#[cfg(test)]
mod fee_parsing_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn legacy_and_dynamic_fees_survive_request_and_storage_roundtrip() {
        for gas_price in [0, 1, 1_000_000_007u128] {
            let input = json!({"to": null, "gasPrice": gas_price});
            let tx: InnerTransaction = serde_json::from_value(input).unwrap();
            assert!(
                matches!(tx.transaction_type_data, Some(TransactionTypeData::Legacy(ref data)) if data.gas_price == Some(gas_price))
            );
            let stored = serde_json::to_value(&tx).unwrap();
            assert_eq!(stored["gasPrice"], json!(gas_price));
            assert!(matches!(
                serde_json::from_value::<InnerTransaction>(stored)
                    .unwrap()
                    .transaction_type_data,
                Some(TransactionTypeData::Legacy(_))
            ));
        }
        let tx: InnerTransaction =
            serde_json::from_value(json!({"to":null,"maxFeePerGas":10,"maxPriorityFeePerGas":2}))
                .unwrap();
        assert!(matches!(
            tx.transaction_type_data,
            Some(TransactionTypeData::Eip1559(_))
        ));
        let no_fees: InnerTransaction = serde_json::from_value(json!({"to":null})).unwrap();
        assert!(no_fees.transaction_type_data.is_none());
    }

    #[test]
    fn invalid_fee_fields_are_not_silently_discarded_by_flattened_option() {
        for invalid in [
            json!({"gasPrice":1,"maxFeePerGas":2}),
            json!({"gasPrice":1,"maxPriorityFeePerGas":2}),
            json!({"gasPrice":1,"authorizationList":[]}),
            json!({"gasPrice":"not-a-number"}),
            json!({"maxFeePerGas":1,"maxPriorityFeePerGas":2}),
            json!({"authorizationList":[]}),
            json!({"authorizationList":[{"invalid":true}]}),
        ] {
            assert!(
                serde_json::from_value::<InnerTransaction>(invalid.clone()).is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn signed_authorizations_select_eip7702_without_losing_fees() {
        use alloy::{eips::eip7702::Authorization, primitives::Signature};
        let authorization = Authorization {
            chain_id: U256::from(31337),
            address: Address::repeat_byte(0x11),
            nonce: 7,
        }
        .into_signed(Signature::new(U256::from(1), U256::from(2), false));
        let input = json!({"to":Address::ZERO,"authorizationList":[authorization],"maxFeePerGas":10,"maxPriorityFeePerGas":2});
        let tx: InnerTransaction = serde_json::from_value(input).unwrap();
        assert!(
            matches!(tx.transaction_type_data, Some(TransactionTypeData::Eip7702(ref data)) if data.authorization_list.as_ref().unwrap().len() == 1 && data.max_fee_per_gas == Some(10))
        );
        let stored = serde_json::to_string(&tx).unwrap();
        assert!(matches!(
            serde_json::from_str::<InnerTransaction>(&stored)
                .unwrap()
                .transaction_type_data,
            Some(TransactionTypeData::Eip7702(_))
        ));
    }
}
