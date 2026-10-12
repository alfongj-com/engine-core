# Integration tests

These tests use the actual signing implementations with disposable EVM keys. They verify recovered identities, EIP-191 encoding, transaction nonce/chain/destination binding, reviewed ERC-4337 account digests with custom EntryPoint binding, unsupported-profile rejection, preserved IAW request envelopes, and queue credential serialization. Environment-backed signing runs in child processes to keep environment mutation out of concurrent Rust tests.

```sh
cargo test --locked -p engine-integration-tests
```

No AWS, Thirdweb account, funded wallet, or RPC endpoint is needed for signing tests. The old Vault-dependent Solana fixture has been removed along with that backend. Solana signing is currently unsupported. The UserOperation fixtures independently encode the reviewed Thirdweb 0.6/0.7 contract digest; this suite does not execute deployed account validation. See [signing profiles](../docs/design/userop-signing.md) for the supported account binding and remaining qualification.

Real Redis EOA state-transition regressions live in `executors/src/eoa/store/tests.rs`; HTTP access-control tests live in `server/tests/`. Run them with the prerequisites in [the test design](../docs/design/testing-and-benchmarks.md).

The legacy `eip7702-core` integration test is explicitly ignored because it downloads live contract bytecode and depends on a Thirdweb bundler. Passing local tests does not validate that remote path.
