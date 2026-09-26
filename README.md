# Engine Core — production hardening fork

Rust transaction infrastructure forked from [thirdweb-dev/engine-core](https://github.com/thirdweb-dev/engine-core) at `b6b7a0b`. It combines a Redis job queue, EOA transaction state machine, ERC-4337/EIP-7702 executors, signing, HTTP APIs, and webhooks.

**Status:** active hardening; local verification is not a production readiness or all-chain compatibility certification. Read [the handoff](TO_ALFONSO.md) for measured results and remaining release blockers.

## Start here

- [Chain compatibility design](docs/design/chain-compatibility.md): Ethereum, Arbitrum, OP Stack/Base, finality, fees, sequencing and capabilities.
- [Configured RPCs and Solana recovery](docs/design/rpc-and-solana-recovery.md): setup, authentication, retry rules and operational limits.
- [Finality and reorg handling](docs/design/finality-and-recovery.md): completion policies and chain assumptions.
- [Redis disaster recovery](docs/design/redis-disaster-recovery.md): required independent journal, initialization, restart and quarantine procedures.
- [50 TPS review](docs/design/throughput-50tps.md): chain constraints, bounded queues, RPC costs and measurement scope.
- [Confirmation identity and authentication](docs/design/confirmation-identity.md): durable attempt binding, legacy access tokens and disabled bundled EIP-7702.
- [RPC test plan and prices](docs/design/rpc-test-plan.md): EVM testnets, Solana Devnet, request estimates, provider limits and first-round budget.
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

The private Thirdweb Vault SDK and its CI SSH requirement are removed. AWS KMS and IAW remain legacy code paths; configured-provider submission currently selects the local environment signer. Live KMS/IAW interoperability is unqualified. Solana signing uses a configured Ed25519 key file; see [setup and recovery rules](docs/design/rpc-and-solana-recovery.md).

## Local EVM signer

Set `ENGINE_PRIVATE_KEY` to a dedicated test key and `ENGINE_SIGNING_TOKEN` to a random token of at least 32 bytes. Use a secret manager or environment injection; do not commit either value. Requests authenticate this signer with `x-engine-signing-token`. Keys are never accepted in request bodies or saved into queued environment credentials. Each queued credential contains a public address; workers reject a key rotation that changes that address.

Run Redis and an Anvil node locally. Chain ID `31337` routes to `http://127.0.0.1:8545`. From the `server` directory:

```sh
export APP_ENVIRONMENT=production
export APP__REDIS__URL=redis://127.0.0.1:16379
export APP__SERVER__HOST=127.0.0.1
export APP__SERVER__DIAGNOSTIC_ACCESS_PASSWORD="$ENGINE_DIAGNOSTIC_PASSWORD"
export APP__EVM_RPC__ENDPOINTS__31337__URL=http://127.0.0.1:8545
export APP__EVM_RPC__ENDPOINTS__31337__FINALITY__MODE=depth
export APP__EVM_RPC__ENDPOINTS__31337__FINALITY__CONFIRMATIONS=0
export APP__QUEUE__EXECUTION_NAMESPACE=local-test
export APP__RECOVERY__JOURNAL_PATH=data/recovery.sqlite
# Once, for a new deployment with an empty namespace:
cargo run --locked --bin thirdweb-engine -- --initialize-recovery
cargo run --locked --bin thirdweb-engine
```

The journal is mandatory for the server and supports one active process on one host. Store it on a persistent local volume independent of Redis; normal startup refuses a missing journal. Never initialize a replacement journal over existing work. Use the [recovery runbook](docs/design/redis-disaster-recovery.md) for upgrades and Redis restarts. The zero-depth policy above is restricted to local chain 31337; public chains default to `finalized` with no automatic downgrade.

The configured endpoint lets local requests use the operator token alone. Each public EVM chain accepts its own endpoint and headers using the same setting. Provider clients reuse connections and refuse redirects. Unconfigured chains retain the legacy Thirdweb routing; bundlers and paymasters are separate integrations.

Legacy KMS/IAW callers must also authenticate with `x-engine-access-token`, matching a separately configured `ENGINE_ACCESS_TOKEN` of at least 32 bytes. KMS headers remain `x-aws-kms-arn`, `x-aws-access-key-id`, and `x-aws-secret-access-key`. Mixing local and legacy signer headers is rejected. Legacy credentials persist in queue state and the permanent journal; replacing them with workload identity and key references remains a production KMS blocker.

Bundled EIP-7702 submission is disabled until a canonical witness can prove execution of its requested calls. Directly signed EOA type-4 transactions remain available. Solana public-cluster endpoints are checked against their expected genesis before signing; the explicit local profile is exempt.

Webhooks are disabled unless `ENGINE_WEBHOOK_ALLOWED_ORIGINS` lists exact HTTPS origins. See [webhook egress policy](docs/design/webhook-egress.md) for configuration, DNS checks and delivery limits.

## Verification scope

Queue jobs per second are not blockchain transactions per second. [Latest measurements](docs/baselines/review-2026-09-26/README.md) exercise the real server, independent durable journal, Redis AOF, and local EVM/Solana nodes. They report admission, inclusion and finalization separately. Production qualification still needs the intended RPC, signer, transaction workload, public-chain finality window and concurrent-chain load. Finite models and tests do not certify whole-program correctness. See the handoff for results and remaining work.
