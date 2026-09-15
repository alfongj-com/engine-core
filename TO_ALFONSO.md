# To Alfonso

## What is ready

The fork is [alfongj-com/engine-core](https://github.com/alfongj-com/engine-core/tree/production-hardening), branch `production-hardening`. The original audit/design was committed before implementation. Upstream `main` is preserved.

- Removed the private Vault SDK. The workspace builds with a pinned Rust toolchain. A local EVM signer reads an environment key; queued jobs contain its public address only and reject identity-changing key rotation.
- Fixed reproduced queue lease/Redis transaction races, unsafe nonce transitions, duplicate EOA re-admission, ambiguous receipt recovery, deployment lock ownership, legacy fee parsing, exposed admin mutations, secret diagnostics, webhook SSRF, AA replay identity, and default-account UserOperation signing. See the [audits](docs/audit-security.md) and [migration instructions](docs/replay-migration.md).
- Added regression tests that expose real failures: six of seven initial queue tests fail against upstream; eleven of thirteen EOA mutation checks fail when the old bugs are restored. Signing tests recover identities and decode signed transactions; Redis tests inspect committed state and competing owners.
- Wrote concise [chain](docs/design/chain-compatibility.md), [testing](docs/design/testing-and-benchmarks.md), and [webhook](docs/design/webhook-egress.md) designs with primary-source references.

## Measured results

| Experiment | Result | Limit |
| --- | --- | --- |
| Queue, 40k offered jobs/s | **9,992 → 26,627 completed jobs/s**; p99 **8.92 → 1.50 s** | Three-second saturation screen on one M4; includes drain. Not sustainable production capacity. |
| Queue, 20k offered jobs/s | **9,966 → 19,973 completed jobs/s**; p99 **2.98 s → 13 ms** | Same source-comparison dependencies and workload. |
| Accounting across 42 runs | **1,305,000 unique completions**, no missing results, admission drops or duplicate committed effects | Redis persistence disabled; does not establish power-loss durability or blockchain TPS. |
| Local EOA crash/restart | 24 pending transfers recovered; 24 unique on-chain effects after duplicate admission | Disposable Anvil + Redis + actual HTTP server; no public-chain spending. |
| Dependency audit | **26 → 0 known-vulnerability findings** | One unsoundness advisory and four unmaintained-package warnings remain, with reachability notes. |

Details and reproducible evidence: [queue results](docs/baselines/queue-results.md), [local recovery](docs/baselines/local-eoa-recovery.json), [dependency review](docs/baselines/dependency-security.md), [verification](docs/verification.md).

## Four things I need from you

1. **First production scope:** exact chain IDs, which need EOA / ERC-4337 / EIP-7702, and the representative transaction mix. Suggested first gate: ordinary EOA transfers and contract calls on one testnet per Ethereum, Arbitrum and OP family.
2. **Qualification access and spending:** chosen RPC/bundler providers, testnet credentials/funds, and explicit canary limits. Supply secrets through your secret manager, not this document.
3. **Deployment and signer identity:** target runtime, AWS KMS key/role versus environment signing, and single-tenant versus multi-tenant use. Existing KMS/IAW paths still persist credentials; workload-identity/key-reference integration needs that decision and live validation.
4. **Acceptance contract:** required throughput/p99, whether completion means inclusion/safe/finalized, tolerated Redis data loss, and recovery/spend limits. These define the soak, failover and chain qualification gates.

## Before production

This is a substantially hardened fork, **not an all-chain production certification**. Remaining release work includes reorg/finality reconciliation, tenant-scoped intent identities across executors, bounded fee/retry policy, Redis persistence/failover testing, sustained load, and live KMS/RPC/bundler/account-contract qualification. Environment signing is one configured EVM identity; Solana needs an Ed25519 signer and its own recovery review. Remote routing still uses Thirdweb RPC/bundler services.

Webhooks now default to disabled until allowed HTTPS origins are configured. Migrate retained jobs using the linked instructions. Upstream has no detected license file or GitHub license metadata; resolve the intended usage/distribution rights before release.
