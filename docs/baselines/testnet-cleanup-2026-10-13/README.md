# Recovery cleanup and public-testnet qualification

**Completed October 13, 2026.** [PR #4](https://github.com/alfongj-com/engine-core/pull/4) contains the cleanup and reproducible public harnesses. Runtime `835c7c2` changes only code structure; merged baseline runtime is `04d15d8` (`main` merge `8fa866e`). Final code head `4e262c8` also patches the lockfile; its normal local executable is byte-identical to frozen `835c7c2`. Generated reports stay on this branch.

## Completed checks

- Full local Rust workflow, including 181 explicit Redis/API tests.
- Ten local process scenarios: crashes, Redis loss/rollback, reorgs and both Solana recovery variants.
- 100 bounded model cases with 92 source guards. These are not whole-program or refinement proofs.
- 39 offline public-harness checks.
- Six finalized effects each on Sepolia, Arbitrum Sepolia and Base Sepolia after Engine/Redis crashes. The earlier harness did not drain late proxy failures before its final outcome; recorded chain effects remain verified, while the final harness is used for the next cases.
- Solana Devnet: baseline and cleanup each finalized six effects. A third cleanup run also passed with the final guard-draining harness; duplicate replay produced no additional broadcasts.

Optimism initially failed its independent check because the provider returned a finalized head below the receipt height. Read-only reconciliation verified all six original outcomes and stored checkpoints against the canonical chain. Replaying the saved IDs with every broadcast denied left queue identities, attempts, balances and nonces unchanged. Original failure evidence is preserved; 45 read-only RPC calls were used.

The matched local comparison is complete: both binaries completed all 9,600 offered transfers under the configured confirmation policies and drained every queue. Admission and terminal timings are broadly similar; journal commit means are nearly unchanged. Solana HTTP calls per intent rose 3.0% in this one pair. See [measurements and limits](performance.md) and [numeric comparison](comparison.json). No sustainable capacity or statistical equivalence claim is made.

Two reverted calls each passed on Sepolia, Arbitrum Sepolia and Base Sepolia, with zero duplicate effects after response loss, Engine/Redis crashes and same-ID replay. Optimism's reverted-call run stopped on an upstream HTTP 502 from `eth_getTransactionCount`. Its separate same-run reconciliation passed: both original receipts reached provider finality, journal and Redis terminal evidence matched, and replay changed no queue/attempt/journal identity, balance or nonce. It used 159 read calls and zero broadcasts. The original run remains failed.

| Public testnet | Baseline successful transfers | Cleanup reverted executions |
| --- | ---: | ---: |
| Sepolia | 6 | 2 |
| Arbitrum Sepolia | 6 | 2 |
| Base Sepolia | 6 | 2 |
| Optimism Sepolia | 6, reconciled after finalized-tag lag | 2, reconciled after HTTP 502 |
| Solana Devnet | 6 | Not tested; two cleanup success runs completed 6 each |

Across Engine campaigns, 50 original transactions were verified: 42 successful and 8 reverted, with zero duplicate effects. Four separately signed EVM fixture creations are additional setup, not Engine coverage. The 32 EVM Engine transactions and four fixtures used 128 send RPC calls; retransmitting identical signed bytes is distinct from a duplicate on-chain effect. [EVM totals and cleanup checks](evm-summary.json) confirm that all EVM chain locks were released only after resolution, all 24 owned EVM service ports are closed, and public Engine processes are stopped.

## Dependency audit and CI

Five new QUIC denial-of-service advisories required `quinn-proto` 0.11.15 → 0.11.18. This is a disabled HTTP/3 dependency; it is absent from the enabled workspace graph and compiler artifacts. The complete local Rust/Redis suite passes again after the update. Independently checked normal builds from the final head and frozen cleanup have the same SHA-256. [Lockfile, audit and build evidence](dependency-update.json) records the exact relationship. The audit has zero vulnerability findings; five existing informational warnings remain (four unmaintained dependency entries and one `lru` panic-safety warning).

At prior head `9b3b851`, the Rust workflow passed its tests, nine process scenarios and two short local load gates, then failed the dependency audit. The formal workflow proved all five fee harnesses, rejected both mutations, and passed all 100 protocol cases, but GitHub cancelled the protocol job at its 15-minute limit after report upload. [Prior Rust CI](prior-ci-summary.json) and [prior formal CI](prior-formal-ci.json) retain those non-green conclusions.

The [timeout-only patch](formal-ci-timeout.patch) adds overhead allowance without changing any model, bound or check. Publishing workflow changes is blocked by the CLI credential's missing `workflow` permission; the browser is signed out. The patch is also saved on local branch `cleanup-ci-timeout-local`. No permissions were changed. The final head's formal workflow passed within the existing limit, so the timeout patch is optional maintenance, not a merge blocker. The final [formal artifact verification](formal-ci.json) confirms all 100 cases, 92 exact-head source hashes, five fee proofs and two mutations. The final [Rust workflow](ci-summary.json) also passed, including the dependency audit, nine process scenarios and two short local load gates. Each load gate drained all 1,000 transactions; these short checks are not sustainable-capacity measurements.

## RPC usage

This pass recorded **12,249 paid-gateway method calls**. Applying [dRPC's published pricing](https://drpc.org/docs/pricing/compute-units) of $0.30 per million compute units and 20 units per ordinary method gives **$0.073494 estimated usage**. The ledger conservatively reserved 13,000 calls, or **$0.078**. Some identity methods are free, so the flat-rate estimate is conservative. This is not an invoice or a reading of the account balance.

The pass ceiling was 100,000 calls ($0.60); the existing global ledger was preserved. Separate rent-exemption reads used the official free Solana Devnet RPC. The gateway was drained and stopped, its port is closed, and its owned lock was removed. [Budget and aggregate method counts](rpc-budget.json).

## Harness findings and scope

The first Solana run stopped before signing because its proxy omitted `getGenesisHash`. It resumed the same saved run after the proxy fix; its elapsed time includes this interruption and is not a performance comparison. A later review tightened both proxies to drain handlers and reject late failures. The final Solana rerun uses those checks.

The first public fixture preflight stopped before signing at its old gas ceiling. Sepolia returned an estimate of 218,384 gas for the fixed tiny contract. The reviewed fixture cap is now 300,000 gas, preserving the existing fee/debit caps. Fixture setup uses a separate local signer; it does not qualify Engine contract creation. Local testing found Engine admits an absent recipient but does not set the explicit creation kind during preparation. That existing defect is documented for a separate fix.

[Public aggregates](public-testnets.json) · [Rust checks](tests.json) · [Process checks](process.json) · [Bounded models](formal.json)

Provider-finalized and canonical checks are not independent consensus proofs. Small funded testnet runs do not measure maximum or sustainable throughput. Raw wallets, signed bytes, addresses, journals and operational paths remain private.
