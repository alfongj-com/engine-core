# Recovery cleanup and public-testnet checks

Updated October 13, 2026. PRs [#1](https://github.com/alfongj-com/engine-core/pull/1),
[#2](https://github.com/alfongj-com/engine-core/pull/2), and
[#3](https://github.com/alfongj-com/engine-core/pull/3) are merged. PR #3 merged at
`8fa866e`; its executable implementation is `04d15d8`.

[PR #4](https://github.com/alfongj-com/engine-core/pull/4) is on `cleanup-recovery-paths` in
`/Users/alfongj/Code/engine-core-review-stack`. Runtime `835c7c2` separates offline
recovery operations and Solana transaction lifecycle code. The public harnesses
select frozen binaries explicitly, so ongoing harness edits do not change the
runtime under test.

## Verified results

| Scope | Result |
| --- | --- |
| Local Rust, `835c7c2` | All 10 workflow steps passed, including 181 explicit Redis/API tests. |
| Local process scenarios, `835c7c2` | All 10 passed: crashes, live Redis outage, rollback/quarantine, reorgs and both Solana recovery variants. |
| Bounded protocol models, `835c7c2` | All 100 expected outcomes passed; 92 source guards match. These guards are not a Rust refinement proof. |
| Public harness guards | All 39 offline tests passed, including late proxy failures, finalized evidence, fixed fixture limits and private signer input. |

Public-testnet, matched-load and CI results are maintained in the
[qualification report](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/testnet-cleanup-2026-10-13/README.md) with separate
runtime, binary and harness provenance. Small funded correctness checks do not
establish sustainable throughput or RPC capacity. Provider-based finality checks
are not independent consensus proofs.

Local testing exposed an existing EOA contract-creation defect: an absent `to`
is admitted, but preparation omits the explicit creation kind and never signs.
Fixing that path needs a separate behavior change and a creation regression test.
The reverted-call harness uses a separately signed, fixed fixture; its deployment
does not qualify Engine contract creation.

## Evidence and reproduction

Use [LOCAL_TESTING.md](LOCAL_TESTING.md) for local checks and
[bounded public harness commands](LOCAL_TESTING.md#6-bounded-public-testnet-correctness-runs).
Private data for this campaign is under
`/Users/alfongj/.local/share/engine-core/runs/testnet-cleanup-2026-10-13/`.
Keep failed runs, original identities, journals and signed bytes intact.

Reviewed public-testnet aggregates and generated reports belong on the separate
[`load-tests` branch](https://github.com/alfongj-com/engine-core/tree/load-tests).
Raw custody files, addresses, signed wires and credentials stay outside Git.
[PR #3's prior evidence](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/retained-recovery-2026-10-12/README.md)
remains historical evidence for its recorded source, not a result for this cleanup.

## Recovery contract and remaining limits

Schema 3 restores only one exact retained Solana wire; it never signs a
replacement. Recovery grants have a separate durable allowance of 20, and lost
replies consume grants. A combined original-plus-recovery allowance of 40 assumes
the original Redis send counter survived until recovery mode. Redis restoration
also resets its local reconciliation-read counter, so it is not a lifetime
RPC-read quota.

Offline reattachment accepts only an intact exact checkpoint target, acknowledges
it and adopts the Redis process identity in one FULL commit, then observes Redis
again. It does not restore old send permission. See the
[Solana design](docs/design/retained-solana-recovery.md) and
[restart/upgrade runbook](docs/design/redis-disaster-recovery.md).

One process on one Unix host must retain the latest authoritative journal.
Restoration does not rebuild missing queue jobs, release quarantined unknown
outcomes, choose among ambiguous legacy attempts, or repair lost journal
authority. Expiry and absent history never authorize a replacement.

Production storage, public RPC capacity, real rollup settlement, sustained
throughput and KMS integration remain unqualified. No whole-service proof or
50 TPS-per-chain guarantee is claimed.
