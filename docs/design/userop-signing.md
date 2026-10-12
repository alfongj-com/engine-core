# UserOperation signing profiles

Status: bounded implementation, 2026-09-15. This corrects the local/KMS digest for the two reviewed Thirdweb account families; deployed-account acceptance remains a qualification task.

## Verified contract policy

Both verified factory source bundles construct a non-upgradeable `Account`. Their `AccountCore._validateSignature` methods hash the **32 raw bytes** of `userOpHash` with the EIP-191 personal-message prefix before recovering the signer. Version 0.6 accepts `UserOperation`; version 0.7 accepts `PackedUserOperation`. Neither reviewed policy recovers directly from the raw operation hash.

| Version | Factory | Account implementation |
| --- | --- | --- |
| 0.6 | `0x85e23b94e7F5E9cC1fF78BCe78cfb15B81f0DF00` | `0xf22175c80c6e074C171811C59C6c0087e2a6a346` |
| 0.7 | `0x4bE0ddfebcA9A5A4a617dee4DeCe99E7c862dceb` | `0x94eC38a5d2EDA5A543Ab4c08D998338D4082beb2` |

Sources: verified [0.6 factory bundle](https://etherscan.io/address/0x85e23b94e7F5E9cC1fF78BCe78cfb15B81f0DF00#code), [0.6 implementation](https://etherscan.io/address/0xf22175c80c6e074C171811C59C6c0087e2a6a346#code), and [0.7 factory bundle](https://etherscan.io/address/0x4bE0ddfebcA9A5A4a617dee4DeCe99E7c862dceb#code), accessed 2026-09-15. The 0.7 implementation page was unverified; its policy was read from the verified factory's embedded Account/AccountCore sources. Each factory constructor deploys Account with its first CREATE; independent address derivation matches the implementation constants above. [Source-file hashes](../../integration-tests/fixtures/userop-signing/source-provenance.json) identify the reviewed bundles. This establishes the source relationship, not the runtime identity of those addresses on every chain.

## Decisions and compatibility

- Local private keys, environment references and AWS KMS require an exact reviewed factory/version pair. Unknown factories and cross-version combinations fail before obtaining a signing key or calling KMS. There is no request parameter enabling arbitrary raw-hash signing.
- Hash the chosen UserOperation version with the requested chain ID and EntryPoint, then apply the reviewed EIP-191 bytes32 policy exactly once. Hex-text signing and double-prefix signing produce different, invalid signatures.
- The builder also checks canonical `createAccount(admin, salt)` calldata and its deterministic account address using the reviewed implementation. A caller-provided `smartAccountAddress` must match. This bounded mode supports the original admin and salt; accounts accessed through additional/session signers require another reviewed profile.
- IAW retains its existing external signing request and account-policy behavior, including custom factories. Local fixture success does not validate that service's policy.
- These checks apply before new local/KMS UserOperations are signed, including retries of queued jobs. Unknown/custom account jobs now fail explicitly. Existing signed operations are not modified by this change.

## Evidence and remaining qualification

[Fixtures and independent generator](../../integration-tests/fixtures/userop-signing/fixtures.json) use manually assembled ABI words and PyCryptodome Keccak, without Engine/Alloy hashing helpers. Tests check both contract digests, raw/text/double-prefix rejection, changed chain/EntryPoint, sender mismatch, unsupported factory/version rejection for all local/KMS credential variants, and preserved IAW HTTP payloads. Builder tests reject mismatched account, admin, salt, malformed/noncanonical creation calldata and unknown factories.

Validation on 2026-09-15: all nine signing integration tests and the builder profile/address test passed in the non-queue workspace suite. The ignored environment helper is executed by its parent test.

Before enabling a production profile, verify factory/implementation code and actual EntryPoint behavior on the target chain; run `validateUserOp` through that EntryPoint against the selected account, including permission and validity-window checks. AWS KMS interoperability, deployed account validation, additional signers, arbitrary custom factories and chain-specific code identity were not established by these local tests.
