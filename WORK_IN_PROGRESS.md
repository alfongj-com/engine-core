# Retained transaction recovery

Updated October 12, 2026. PRs [#1](https://github.com/alfongj-com/engine-core/pull/1)
and [#2](https://github.com/alfongj-com/engine-core/pull/2) are merged.
[PR #3](https://github.com/alfongj-com/engine-core/pull/3) contains both follow-ups
on `recover-retained-transactions`, based on `918d7d9`.

## Implemented

- Restore a missing Solana Redis attempt from its exact signed journal record.
  Validate the full admitted request, signature, blockhash and signer. Preserve
  storage ownership and finalized-receipt checks; never sign a replacement.
- Retain a separate allowance of at most 20 recovery grants across all repeated
  projection losses and restarts. Lost replies consume grants. Schema 3 prevents
  older binaries from bypassing this rule. Normal execution adds no durable commit.
- Reattach a pending checkpoint only when its exact target survives on the Redis
  primary. Acknowledge it and adopt the process identity in one FULL commit, then
  observe Redis again. Predecessor/missing/foreign markers and unrelated halts
  remain blocked; namespace, epoch and replay identities stay unchanged.

The [Solana design](docs/design/retained-solana-recovery.md) and
[restart/upgrade runbook](docs/design/redis-disaster-recovery.md) describe the
protocol and operator actions. Healthy schema-1/2 journals migrate to schema 3;
old binaries reject the upgraded state. No in-place downgrade is supported.

## Verified runtime: `04d15d8`

- All 10 local Rust workflow steps pass, including 181 explicit Redis/API tests.
- All 10 local process scenarios pass: Engine/Redis crashes, live outage,
  rollback, quarantine, reorgs and both Solana recovery variants.
- The new Solana process scenario restores 12 deleted projections, retransmits
  identical bytes, finalizes all 12 original effects and produces no duplicates.
- Actual old/new binary upgrade succeeds; the old binary then rejects schema 3
  without changing the journal.
- All 100 bounded protocol cases pass locally and in CI; 91 source guards match.
  Five fee proofs and both deliberate mutation checks pass in CI.
- Full Rust correctness CI passes, including both default Solana harness cases
  and dependency audit. These checks validate this runtime revision; later
  documentation changes do not change the executable.

For matched load measurements, crash-campaign results and exact provenance, see
[PR #3 evidence](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/retained-recovery-2026-10-12/README.md).
Generated reports stay on `load-tests`. Local private data is under
`/Users/alfongj/.local/share/engine-core/runs/pr3-retained-recovery-oct12/`.
The fixtures use disposable local Redis, Anvil and Agave, without paid RPCs.

## Remaining limits

One process on one Unix host must retain the latest authoritative journal.
Restoration does not reconstruct missing queue jobs, release quarantined unknown
outcomes, choose among ambiguous legacy attempts, or repair lost/rolled-back
journal authority. Expiry and absent history never authorize a replacement.

Only the recovery grant pool is independently durable. The original Redis-only
send counter can roll back; a combined 40-send bound assumes that original
counter survived until recovery mode. Reconstructing Redis also resets its local
reconciliation-read counter, so it is not a lifetime RPC-read quota.

Production storage, public RPC capacity, real rollup settlement, sustained
throughput and KMS integration still need qualification. No whole-service proof,
sustainable maximum or 50 TPS-per-chain guarantee is claimed.

See [local test commands and agent prompt](LOCAL_TESTING.md). The active checkout
is `/Users/alfongj/Code/engine-core-review-stack`.
