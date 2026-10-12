use super::*;
use solana_sdk::{hash::Hash, message::Message, signature::Keypair, signer::Signer};

#[test]
fn wire_matches_sdk_bincode_for_legacy_and_v0_and_preserves_signed_messages() {
    let payer = Keypair::new();
    let blockhash = Hash::new_unique();
    let instruction = Instruction {
        program_id: Pubkey::new_unique(),
        accounts: vec![],
        data: vec![0xab; 253],
    };
    for message in [
        VersionedMessage::Legacy(Message::new_with_blockhash(
            &[instruction.clone()],
            Some(&payer.pubkey()),
            &blockhash,
        )),
        VersionedMessage::V0(
            v0::Message::try_compile(&payer.pubkey(), &[instruction], &[], blockhash).unwrap(),
        ),
    ] {
        let transaction = VersionedTransaction::try_new(message, &[&payer]).unwrap();
        // The pinned Solana RPC SDK serializes transactions with bincode 1.
        let sdk_wire = bincode_legacy::serialize(&transaction).unwrap();
        assert_eq!(encode_transaction_wire(&transaction).unwrap(), sdk_wire);
        let decoded = decode_transaction_wire(&sdk_wire).unwrap();
        assert_eq!(decoded.message.serialize(), transaction.message.serialize());
        assert_eq!(decoded.signatures, transaction.signatures);
        assert!(
            decoded.signatures[0].verify(payer.pubkey().as_ref(), &decoded.message.serialize())
        );
        let preserved = SolanaTransaction {
            input: SolanaTransactionInput::new_with_serialized(Base64Engine.encode(&sdk_wire)),
            compute_unit_limit: None,
            compute_unit_price: None,
        }
        .to_versioned_transaction(payer.pubkey(), Hash::new_unique())
        .unwrap();
        assert_eq!(
            encode_transaction_wire(&preserved).unwrap(),
            sdk_wire,
            "a supplied recent blockhash must not alter serialized signed input"
        );
        let mut trailing = sdk_wire.clone();
        trailing.push(0);
        assert!(decode_transaction_wire(&trailing).is_err());
        assert!(decode_transaction_wire(&sdk_wire[..sdk_wire.len() - 1]).is_err());
    }
}

#[test]
fn malformed_payer_and_signature_counts_are_rejected_without_indexing() {
    let transaction = VersionedTransaction {
        signatures: vec![],
        message: VersionedMessage::Legacy(Message::default()),
    };
    assert!(decode_transaction_wire(&bincode_legacy::serialize(&transaction).unwrap()).is_err());
    let payer = Keypair::new();
    let mut transaction = VersionedTransaction::try_new(
        VersionedMessage::Legacy(Message::new_with_blockhash(
            &[],
            Some(&payer.pubkey()),
            &Hash::new_unique(),
        )),
        &[&payer],
    )
    .unwrap();
    transaction.signatures.clear();
    assert!(decode_transaction_wire(&bincode_legacy::serialize(&transaction).unwrap()).is_err());
}
