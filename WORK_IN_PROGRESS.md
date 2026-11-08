# Engine code review stack

Updated November 8, 2026. Current branch: `pin-testnet-resume-provenance` in
`/Users/alfongj/Code/engine-core-review-stack`. Runtime implementation: `4dadaf5` ([PR #10](https://github.com/alfongj-com/engine-core/pull/10)).
The current branch adds strict public-testnet resume provenance and wallet-lock cleanup.

## Changes

PRs #1–#3 are merged. These review PRs are stacked on the open
[cleanup PR #4](https://github.com/alfongj-com/engine-core/pull/4), commit `4e262c8`:

| PR | Change |
| --- | --- |
| [#5](https://github.com/alfongj-com/engine-core/pull/5) | Explicit CREATE/CALL envelopes, legacy estimation and checked gas bounds. |
| [#6](https://github.com/alfongj-com/engine-core/pull/6) | Journal cancellation competes atomically with broadcast reservation; reuse one payload snapshot. |
| [#7](https://github.com/alfongj-com/engine-core/pull/7) | At most 100 batch items and 16 concurrent signing/ABI operations per request. |
| [#8](https://github.com/alfongj-com/engine-core/pull/8) | Compatible TLS, runtime, compression and constant-time utility patches. |
| [#9](https://github.com/alfongj-com/engine-core/pull/9) | Native secp256k1 environment-key derivation/signing, signature boundary checks and binary-upgrade recovery test. |

PR #10 adds [Solana processing-lock recovery](docs/design/solana-processing-locks.md),
found by killing Engine during a three-chain load run. A new worker may reclaim
a lock only after its recorded queue owner loses its lease.

Merge order: **#4 → #5 → #6 → #7 → #8 → #9 → #10 → provenance fix**. No journal
schema or queue job identity change is introduced. Cancelled IDs remain consumed.
The processing-lock value gains an owner record; legacy locks retain their TTL.

## Tests and evidence

Use [LOCAL_TESTING.md](LOCAL_TESTING.md) for tools, isolated checkouts, commands
and another agent's prompt. It includes contract creation and an old/new binary
restart against the same journal and Redis state.

Generated results and the handoff are maintained on the separate
[load-tests branch](https://github.com/alfongj-com/engine-core/tree/load-tests/docs/baselines/code-review-2026-11-08).
Each report identifies its source, binary and harness. Failed runs are retained;
finite models, source guards and fee proofs do not establish a whole-service or
cryptographic proof. Historical public-testnet results qualify only their recorded binaries.

Private runs are under `/Users/alfongj/.local/share/engine-core/runs/code-review-2026-11-08/`.
Keep raw keys, signed wires and journals out of Git. Old incident journals and the
retained Nitro environment are unrelated to these disposable fixtures.

## Remaining constraints

The [dependency review](docs/design/dependency-security.md) records one unresolved
transitive crypto-bigint advisory, six warnings and the native signing mitigation.
The audit remains enabled and fails visibly. Signature bytes need not match the
old backend for prehashes at or above the curve order; recovery uses retained wires.

The latest independent journal and one active process on one Unix host are required.
Durable writes are serialized and intent history is retained indefinitely. Restoration
does not reconstruct missing queue jobs or resolve quarantined unknown outcomes.
Missing Solana history never authorizes a fresh signature. Follow the
[upgrade/rollback runbook](docs/design/redis-disaster-recovery.md#schema-upgrade-and-rollback);
schema 3 has no in-place downgrade.

Production hardware, public RPC capacity, actual rollup settlement, sustained
throughput and KMS integration still require qualification. No 50 TPS-per-chain
or production-readiness claim follows from local checks.
