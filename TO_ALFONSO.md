# To Alfonso

Updated October 12, 2026.

## Saved and pushed

PRs #1 and #2 are merged. [PR #3](https://github.com/alfongj-com/engine-core/pull/3)
contains both recovery changes on `recover-retained-transactions`:

1. Restore a missing Solana Redis attempt from its exact retained journal
   transaction, with a durable recovery-send allowance. Never sign a replacement.
2. Reattach after a Redis restart when the exact target of a pending checkpoint
   survived. Acknowledge it and adopt the process together, then recheck Redis.

Code and local instructions are in **`/Users/alfongj/Code/engine-core-review-stack`**.
Runtime revision: `04d15d8`; current PR revision: `146de9d` (documentation follow-ups).
Its README and `LOCAL_TESTING.md` include setup commands and a prompt for another
agent. `/Users/alfongj/Code/engine-core` remains the earlier PR #2 checkout.
Generated reports stay on this `load-tests` branch.

## Regression checks

- The full local Rust workflow, **181 explicit Redis/API tests**, an actual
  old/new-binary schema upgrade, and **10 process-recovery scenarios** pass.
- **100 bounded protocol cases**, 91 source guards, five Rust fee proofs and
  two deliberate mutation checks pass. These are scoped proofs, not proof of
  the entire application.
- Four five-minute shared-load runs compare two baseline and two candidate
  runs at **32 TPS combined**: 8 fast EVM + 8 slow EVM + 16 Solana. Each run
  completes all **9,600 transactions** with exact effects and drains fully.
- The first candidate run had higher fast-EVM tail latency. That increase did
  not repeat with the identical binary; all four results are retained. This
  small local sample cannot establish statistical equivalence or a sustainable
  maximum, and the strict capacity screen remains unconfirmed.
- A forced Engine crash under load completes all **2,880 transactions** exactly
  once. Five same-ID retries, including an ambiguous HTTP response, produce
  no extra sends. The dedicated Solana fault test restores all 12 removed
  attempts and retransmits their original bytes.

[Results, commands and exact source hashes](docs/baselines/retained-recovery-2026-10-12/README.md).
No paid RPC credits were used.

## Upgrade note

The journal upgrades to **schema 3**. Older binaries refuse it; there is no
in-place downgrade. Follow the [upgrade/rollback runbook](https://github.com/alfongj-com/engine-core/blob/recover-retained-transactions/docs/design/redis-disaster-recovery.md#schema-upgrade-and-rollback) before opening deployed
state. Recovery allows at most 20 additional durable send grants across repeated
projection losses. The original send counter remains Redis-only, so arbitrary
Redis rollback still prevents a universal lifetime send bound.

## Remaining before production

1. Improve durable journal throughput and qualify the intended hardware, RPC,
   workload and real chain stacks. There is no 50 TPS per-chain claim; the earlier
   longer 40 TPS shared run accumulated backlog.
2. Define operator recovery for missing queue jobs and quarantined or unknown
   effects. This PR requires the original admission and one valid signed record;
   it cannot recover a lost or rolled-back journal.
3. Complete KMS/account and deployment qualification, including archival-provider
   latency with batching enabled and disabled.

The old incident journals and retained Nitro environment were not changed.
Unrecorded schema-1 mismatches do not become repairable just by upgrading.
One host and one retained authority are still required.
