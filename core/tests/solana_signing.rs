use engine_core::{credentials::SigningCredential, signer::SolanaSigner};
use engine_solana_core::transaction::{decode_transaction_wire, encode_transaction_wire};
use solana_sdk::{
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{Message, VersionedMessage, v0},
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::VersionedTransaction,
};
use std::{fs::OpenOptions, io::Write, process::Command};
use thirdweb_core::iaw::IAWClient;

// Environment credentials are tested in a subprocess so other tests cannot
// observe a transient test key or change this test's signer identity.
#[test]
fn solana_file_signer_preserves_wire_and_rejects_missing_signers_and_rotation() {
    const CHILD: &str = "ENGINE_SOLANA_SIGNER_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let path = std::env::temp_dir().join(format!(
            "engine-solana-signing-{}.json",
            uuid::Uuid::new_v4()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(&path)
            .unwrap()
            .write_all(&serde_json::to_vec(&Keypair::new().to_bytes().to_vec()).unwrap())
            .unwrap();
        let result = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "solana_file_signer_preserves_wire_and_rejects_missing_signers_and_rotation",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("ENGINE_SOLANA_KEYPAIR_FILE", &path)
            .env_remove("ENGINE_PRIVATE_KEY")
            .output()
            .unwrap();
        std::fs::remove_file(path).unwrap();
        assert!(
            result.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let credentials = SigningCredential::solana_environment().unwrap();
        let payer = credentials.solana_keypair().unwrap();
        let serialized_credentials = serde_json::to_string(&credentials).unwrap();
        assert!(!serialized_credentials.contains("private_key"));
        assert!(!serialized_credentials.contains("ENGINE_SOLANA_KEYPAIR_FILE"));
        assert!(
            !serialized_credentials.contains(&std::env::var("ENGINE_SOLANA_KEYPAIR_FILE").unwrap())
        );
        assert!(
            !serialized_credentials
                .contains(&serde_json::to_string(&payer.to_bytes().to_vec()).unwrap())
        );
        let credentials: SigningCredential = serde_json::from_str(&serialized_credentials).unwrap();
        let signer = SolanaSigner::new(IAWClient::new("http://127.0.0.1:1").unwrap());
        let cosigner = Keypair::new();
        let blockhash = Hash::new_unique();
        let instruction = Instruction {
            program_id: Pubkey::new_unique(),
            accounts: vec![AccountMeta::new_readonly(cosigner.pubkey(), true)],
            data: vec![1, 2, 3],
        };
        for message in [
            VersionedMessage::Legacy(Message::new_with_blockhash(
                &[instruction.clone()],
                Some(&payer.pubkey()),
                &blockhash,
            )),
            VersionedMessage::V0(
                v0::Message::try_compile(&payer.pubkey(), &[instruction.clone()], &[], blockhash)
                    .unwrap(),
            ),
        ] {
            let message_bytes = message.serialize();
            let partial = VersionedTransaction {
                signatures: vec![Signature::default(), cosigner.sign_message(&message_bytes)],
                message,
            };
            let signed = signer
                .sign_transaction(partial.clone(), payer.pubkey(), &credentials)
                .await
                .unwrap();
            assert_eq!(signed.message.serialize(), message_bytes);
            assert_eq!(signed.signatures[1], partial.signatures[1]);
            let wire = encode_transaction_wire(&signed).unwrap();
            let decoded = decode_transaction_wire(&wire).unwrap();
            for (signature, key) in decoded
                .signatures
                .iter()
                .zip(decoded.message.static_account_keys())
            {
                assert!(signature.verify(key.as_ref(), &message_bytes));
            }
            let presigned = signer
                .sign_transaction(decoded, payer.pubkey(), &credentials)
                .await
                .unwrap();
            assert_eq!(encode_transaction_wire(&presigned).unwrap(), wire);
            let mut missing = partial.clone();
            missing.signatures[1] = Signature::default();
            assert!(
                signer
                    .sign_transaction(missing, payer.pubkey(), &credentials)
                    .await
                    .is_err()
            );
            let mut invalid_other = partial.clone();
            invalid_other.signatures[1] = cosigner.sign_message(b"different message");
            assert!(
                signer
                    .sign_transaction(invalid_other, payer.pubkey(), &credentials)
                    .await
                    .is_err()
            );
            let mut invalid_own = partial.clone();
            invalid_own.signatures[0] = payer.sign_message(b"different message");
            assert!(
                signer
                    .sign_transaction(invalid_own, payer.pubkey(), &credentials)
                    .await
                    .is_err()
            );
            let mut malformed = partial.clone();
            malformed.signatures.pop();
            assert!(
                signer
                    .sign_transaction(malformed, payer.pubkey(), &credentials)
                    .await
                    .is_err()
            );
            assert!(
                signer
                    .sign_transaction(partial, Pubkey::new_unique(), &credentials)
                    .await
                    .is_err()
            );
        }
        let path = std::env::var("ENGINE_SOLANA_KEYPAIR_FILE").unwrap();
        std::fs::write(
            &path,
            serde_json::to_vec(&Keypair::new().to_bytes().to_vec()).unwrap(),
        )
        .unwrap();
        assert!(
            credentials
                .solana_keypair()
                .unwrap_err()
                .to_string()
                .contains("changed since admission")
        );
        std::fs::write(&path, b"secret-malformed-file-content").unwrap();
        let error = SigningCredential::solana_environment()
            .unwrap_err()
            .to_string();
        assert!(!error.contains("secret-malformed"));
        assert!(!error.contains(&path));
    });
}
