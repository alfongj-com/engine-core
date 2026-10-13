# Recovery cleanup and public-testnet qualification

**In progress.** [PR #4](https://github.com/alfongj-com/engine-core/pull/4) contains the cleanup and reproducible public harnesses. Runtime `835c7c2` changes only code structure; merged baseline runtime is `04d15d8` (`main` merge `8fa866e`). Generated reports stay on this branch.

## Completed checks

- Full local Rust workflow, including 181 explicit Redis/API tests.
- Ten local process scenarios: crashes, Redis loss/rollback, reorgs and both Solana recovery variants.
- 100 bounded model cases with 92 source guards. These are not whole-program or refinement proofs.
- 39 offline public-harness checks.
- Six finalized effects each on Sepolia, Arbitrum Sepolia and Base Sepolia after Engine/Redis crashes. The earlier harness did not drain late proxy failures before its final outcome; recorded chain effects remain verified, while the final harness is used for the next cases.
- Solana Devnet: baseline and cleanup each finalized six effects. A third cleanup run also passed with the final guard-draining harness; duplicate replay produced no additional broadcasts.

Optimism initially failed its independent check because the provider returned a finalized head below the receipt height. Read-only reconciliation verified all six original outcomes and stored checkpoints against the canonical chain. Replaying the saved IDs with every broadcast denied left queue identities, attempts, balances and nonces unchanged. Original failure evidence is preserved; 45 read-only RPC calls were used.

The matched local comparison is complete: both binaries finalized all 9,600 offered transfers and drained every queue. Admission and terminal timings are broadly similar; journal commit means are nearly unchanged. Solana HTTP calls per intent rose 3.0% in this one pair. See [measurements and limits](performance.md) and [numeric comparison](comparison.json). No sustainable capacity or statistical equivalence claim is made.

Bounded EVM reverted-call checks and final-head CI are still running.

## Harness findings and scope

The first Solana run stopped before signing because its proxy omitted `getGenesisHash`. It resumed the same saved run after the proxy fix; its elapsed time includes this interruption and is not a performance comparison. A later review tightened both proxies to drain handlers and reject late failures. The final Solana rerun uses those checks.

The first public fixture preflight stopped before signing at its old gas ceiling. Sepolia returned an estimate of 218,384 gas for the fixed tiny contract. The reviewed fixture cap is now 300,000 gas, preserving the existing fee/debit caps. Fixture setup uses a separate local signer; it does not qualify Engine contract creation. Local testing found Engine admits an absent recipient but does not set the explicit creation kind during preparation. That existing defect is documented for a separate fix.

[Public aggregates](public-testnets.json) · [Rust checks](tests.json) · [Process checks](process.json) · [Bounded models](formal.json)

Provider-finalized and canonical checks are not independent consensus proofs. Small funded testnet runs do not measure maximum or sustainable throughput. Raw wallets, signed bytes, addresses, journals and operational paths remain private.
