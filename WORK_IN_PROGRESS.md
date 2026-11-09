# Current review stack

Updated November 8, 2026. Code: `/Users/alfongj/Code/engine-core-review-stack`,
branch `reconcile-public-read-failures`. Record `git rev-parse HEAD` before testing;
the branch name alone does not identify a tested executable.

## Scope and order

Apply the remaining entries in this order, skipping PRs already merged:

**#21 → #18 → #19**.

| PR | Change |
| --- | --- |
| #21 | Bring the already merged #5–#17 stack into main, including post-merge cancellation/intake, schema, queue-reconstruction and recovery-oracle corrections. |
| #18 | Repeated Linux/local capacity and fault measurements, strict qualification and explicit search limits. |
| #19 | Read-only public-run reconciliation, provider guards and bounded retries for throttled unsigned work. |

Fixes to an open PR are committed to that PR and its dependents are rebased.
Temporary follow-up PRs #20/#22 are closed; their fixes are in #19 and #13/#14 respectively.
PRs #5–#17 were merged into stack parents rather than main; #21
retains that history as the base for the remaining PRs. This review creates PRs;
it does not merge them. Earlier PRs can have historical
failing checks that are fixed above them. Assess the exact final head and merge
order, including comments whose fix is later in the stack.

## Validation

[LOCAL_TESTING.md](LOCAL_TESTING.md) contains installed tool paths, isolated
checkouts, commands and a prompt for another local agent. The separate
[evidence branch](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/pending-work-2026-11-08/README.md)
records exact sources, executable hashes, checks, failures and measured limits.
Historical test passes do not qualify later code.

Private logs and fixtures are under
`/Users/alfongj/.local/share/engine-core/runs/pending-work-2026-11-08/`.
Preserve unresolved journals, Redis AOF, validator ledgers and the older Nitro
environment. Never repair a failed test by clearing a fence or changing its IDs.

## Production constraints

- The [dependency migration](docs/design/dependency-security.md) removes the
  reported vulnerability/unsound dependency versions; maintenance warnings remain.
  Keep the audit enabled and retain the pinned fork sources.
- One current independent journal and one active Engine process on one Unix host
  remain required. Queue reconstruction is offline and selective; it does not
  resolve quarantined unknown outcomes or create fresh Solana signatures.
- Intent/replay history is permanent. [Compaction](docs/design/journal-maintenance.md)
  reclaims free pages, not live history. Provision storage for lifetime growth.
- Local Anvil cadence profiles do not qualify Nitro or public sequencers. Public
  testnets have small correctness/recovery checks; public-provider maximum TPS,
  simultaneous-chain production capacity and physical power loss remain unqualified.
- [KMS protocol vectors](docs/design/kms-protocol-tests.md) do not qualify a real
  AWS deployment. Legacy credential persistence and workload identity remain open
  production KMS concerns.

Follow the [schema/rollback runbook](docs/design/redis-disaster-recovery.md#schema-upgrade-and-rollback).
Schema 3 has no in-place downgrade. Finite models, source guards and fee proofs
do not establish whole-service correctness or universal chain support.
