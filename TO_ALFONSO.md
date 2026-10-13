# To Alfonso

Updated October 13, 2026.

## Saved and pushed

PRs #1–#3 are merged. [PR #4](https://github.com/alfongj-com/engine-core/pull/4)
contains the recovery/Solana cleanup, bounded public-testnet harnesses and a
patch for five newly reported dependency advisories. Code is on
`cleanup-recovery-paths` at `4e262c8`.

Local checkout: **`/Users/alfongj/Code/engine-core-review-stack`**.
Its README and `LOCAL_TESTING.md` contain setup, test commands and a prompt for
another agent. `/Users/alfongj/Code/engine-core` remains the earlier checkout.
Generated reports are pushed separately on this `load-tests` branch. Private
wallets, signed bytes and journals stay under
`/Users/alfongj/.local/share/engine-core/runs/testnet-cleanup-2026-10-13/`.

## What the checks establish

- Full local Rust workflow, including **181 explicit Redis/API tests**, passes.
  It also passes after the lockfile patch; the normal executable is byte-identical
  to the tested cleanup binary `835c7c2`.
- **10 process scenarios** pass: crashes, Redis loss and rollback, reorgs, and
  both Solana retained-attempt recovery variants.
- **100 bounded protocol cases, 92 source guards, five fee proofs and two
  deliberate mutation checks** pass. This is not a proof of the entire service.
- Matched five-minute runs at **32 TPS combined** each complete all **9,600
  transfers** and drain. No clear material regression appears in this pair.
  Solana RPC calls per intent rose 3%; one pair cannot establish equivalence.
- Public checks verified **50 Engine transactions** across Sepolia, Optimism
  Sepolia, Base Sepolia, Arbitrum Sepolia and Solana Devnet: **42 successes, eight
  expected reverts, zero duplicate effects**. Optimism needed same-identity
  reconciliation after provider errors. [Exact outcomes and limitations](docs/baselines/testnet-cleanup-2026-10-13/README.md)
  distinguish original failures, same-identity reconciliation and completed runs.
- This pass used **12,249 RPC calls**, about **$0.074 estimated** ($0.078
  conservatively reserved). The gateway and owned test services are stopped.
  The provider account balance was not queried.

## Optional CI maintenance

A protocol CI run hit its 15-minute job limit after every model passed and its
report uploaded. The timeout-only patch is saved locally on
`cleanup-ci-timeout-local`; the GitHub CLI credential lacks `workflow` permission
and the browser is signed out. No authentication permissions were changed.
Both final-head CI workflows passed, including the dependency audit. This
timeout patch is optional maintenance, not a blocker; no user action is needed
for the current checks.

## Next work

1. Fix the existing EOA contract-creation bug and add a regression through the
   actual API/signing path. An absent recipient is accepted but never becomes an
   explicit creation transaction. The test fixture uses a separate signer.
2. Qualify sustained throughput with the intended production hardware, workload,
   RPC and concurrent chains. No 50 TPS-per-chain or public maximum is established.
3. Define recovery for lost queue jobs and quarantined outcomes, then qualify KMS
   and deployment custody. The latest independent journal and one active process
   on one host are still required; journal loss has no automatic repair.

Schema 3 has no in-place downgrade. Follow the [upgrade/rollback runbook](https://github.com/alfongj-com/engine-core/blob/cleanup-recovery-paths/docs/design/redis-disaster-recovery.md#schema-upgrade-and-rollback)
before opening deployed state. Old incident journals and the retained Nitro
environment were not changed.
