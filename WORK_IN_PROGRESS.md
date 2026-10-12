# Retained transaction recovery

Updated October 12, 2026. PRs [#1](https://github.com/alfongj-com/engine-core/pull/1)
and [#2](https://github.com/alfongj-com/engine-core/pull/2) are merged into `main`.
This follow-up is on `recover-retained-transactions`, based on `918d7d9`.

## Scope

- Restore missing Solana Redis attempts from the exact signed journal record.
  Keep the same admitted payload, signer, signature and blockhash; preserve storage
  ownership and finalized-receipt checks. Conflicting data remains blocked.
- Count recovered sends durably so repeated Redis loss cannot reset the allowance.
  Schema 3 rejects old binaries that lack this rule. Normal transaction execution
  gains no additional durable commit.
- Let offline reattach acknowledge a pending update only when Redis contains its
  exact recorded target on a writable primary. Update process identity and
  acknowledge atomically; a post-commit observation must also pass. Retain the
  namespace, epoch, replay bindings and all unrelated safety halts.

The [Solana design](docs/design/retained-solana-recovery.md) and
[restart/upgrade runbook](docs/design/redis-disaster-recovery.md) describe the
protocol and operator actions. Recovery budget exhaustion, expiry and absent
history never authorize a replacement transaction.

## Validation plan

Implementation and validation are in progress. Before review, require:

- Workspace, real Redis, HTTP admission and queue regression suites.
- First-send gap, repeated projection loss, budget exhaustion, stale-lock,
  cancellation, corruption, terminal and migration regressions.
- Offline reattach crash cuts and lost replies; reject predecessor, missing,
  foreign and replica observations, and prohibited halts.
- Actual Agave process restart with missing Redis attempts: same signed bytes,
  exact effects and no new sends after terminal deduplication.
- Full bounded protocol models, deliberate fault counterexamples and fee proofs.
- Matched shared-chain load and Engine-crash runs against the prior binary.

A fresh baseline run completed all 9,600 transactions at 8/8/16 TPS and drained
all queues. Candidate results are not yet available. Historical schema-2 results
remain in the [PR #2 evidence](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/pr2-review-2026-10-12/README.md);
these finite runs did not establish a sustainable maximum.

Generated evidence belongs on `load-tests`, outside this implementation PR.
Private runs for this follow-up are under
`/Users/alfongj/.local/share/engine-core/runs/pr3-retained-recovery-oct12/`.
All fixtures use disposable local Redis, Anvil and Agave; no paid RPC is needed.

## Remaining limits

One process on one Unix host must retain the latest authoritative journal.
Restoring a signed attempt does not reconstruct lost queue/admission records,
release quarantined unknown effects, or recover lost/rolled-back authority.
A pending predecessor after Redis restart remains ineligible for reattach.

Production storage, public RPC capacity, real rollup settlement, sustained
throughput and KMS integration still require qualification. No whole-service
proof or 50 TPS-per-chain guarantee is claimed.

See [local test commands and agent prompt](LOCAL_TESTING.md).
