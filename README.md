# Engine Core — production hardening fork

Rust transaction infrastructure forked from [thirdweb-dev/engine-core](https://github.com/thirdweb-dev/engine-core) at `b6b7a0b`. It combines a Redis job queue, EOA transaction state machine, ERC-4337/EIP-7702 executors, signing, HTTP APIs, and webhooks.

**Status:** active hardening; local verification is not a production readiness or all-chain compatibility certification. Read [the handoff](TO_ALFONSO.md) for measured results and remaining release blockers.

## Start here

- [Chain compatibility design](docs/design/chain-compatibility.md): Ethereum, Arbitrum, OP Stack/Base, finality, fees, sequencing and capabilities.
- [UserOperation signing profiles](docs/design/userop-signing.md): supported default accounts, rejection rules and remaining qualification.
- [Test and benchmark design](docs/design/testing-and-benchmarks.md): safety invariants, failure injection, local versus network evidence.
- [Queue and EOA audit](docs/audit-queue.md) and [security audit](docs/audit-security.md): initial findings and scope.
- [Baseline provenance](docs/baselines/upstream.md) and [queue measurements](docs/baselines/queue-results.md).
- [Existing EOA state-machine description](README_EOA.md): useful background; audit findings take precedence over its correctness claims.

## Build

Install Rust through rustup; `rust-toolchain.toml` selects the tested toolchain.

```sh
cargo build --locked --bin thirdweb-engine
cargo test --locked -p engine-integration-tests
```

The private Thirdweb Vault SDK and its CI SSH requirement are removed. AWS KMS and IAW remain supported code paths; external-service interoperability is not established by local tests. Solana signing needs a new Ed25519 backend and currently returns a clear unsupported error.

## Local EVM signer

Set `ENGINE_PRIVATE_KEY` to a dedicated test key and `ENGINE_SIGNING_TOKEN` to a random token of at least 32 bytes. Use a secret manager or environment injection; do not commit either value. Requests authenticate this signer with `x-engine-signing-token`. Keys are never accepted in request bodies or saved into queued environment credentials. Each queued credential contains a public address; workers reject a key rotation that changes that address.

Run Redis and an Anvil node locally. Chain ID `31337` routes to `http://127.0.0.1:8545`. From the `server` directory:

```sh
APP_ENVIRONMENT=production \
APP__REDIS__URL=redis://127.0.0.1:16379 \
APP__SERVER__HOST=127.0.0.1 \
APP__SERVER__DIAGNOSTIC_ACCESS_PASSWORD="$ENGINE_DIAGNOSTIC_PASSWORD" \
cargo run --locked --bin thirdweb-engine
```

The existing RPC extractor still requires a Thirdweb credential header on transaction requests. For local chain 31337 only, use `x-thirdweb-secret-key: local-test`; it is not forwarded as a real service credential. Remote RPC/bundler routing still uses the Thirdweb chain service. Removing Vault does not remove those separate integrations.

AWS KMS request headers remain `x-aws-kms-arn`, `x-aws-access-key-id`, and `x-aws-secret-access-key`. The inherited KMS flow serializes credentials into queue state; replacing it with workload identity and key references is a release blocker. Prefer the environment reference for local experiments.

Webhooks are disabled unless `ENGINE_WEBHOOK_ALLOWED_ORIGINS` lists exact HTTPS origins. See [webhook egress policy](docs/design/webhook-egress.md) for configuration, DNS checks and delivery limits.

## Verification scope

Queue jobs per second are not blockchain transactions per second. The benchmark measures Redis-committed queue completion on one local host, with Redis persistence disabled. Production qualification also needs RPC failure/reorg tests, sustained load with persistence, signer limits, and actual chain confirmation/finality observations. See the handoff for what was run and what still needs an operator decision.
