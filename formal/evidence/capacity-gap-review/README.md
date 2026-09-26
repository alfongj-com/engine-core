# Original-wire gap recovery: reviewed source and formal rerun

Date: September 26, 2026. This record covers the gap-recovery working-tree
candidate based on `3a248ac5bd13e16ce682f3e0548a0096a7502fe0`; that base commit is
**not** the modified candidate. [The report](report.json) records the exact 69
mapped source hashes and all model/configuration hashes. [Summary metadata](summary.json)
records the source-map/manifest/report digests and the release binary digest.
TLC checks source correspondence fingerprints, not compiled machine code.

## Results

- **61/61 expected outcomes passed**, using pinned TLC 1.7.4 in 199.451s.
- Fourteen positive configurations exhaust **3,164,944 distinct states**, summed
  across separate state spaces, not one composed service state space.
- Thirty-one fault, eleven boundary and five reachability cases produce their
  required counterexamples. An unrelated invariant, timeout or syntax error
  cannot satisfy the runner.
- All **69 source hashes** match before and after the run. The models and
  configuration manifest are unchanged from the [preceding review](../capacity-review/README.md).
  That evidence directory retains its earlier source and release scope.

The [runner log](runner.log) summarizes every case. The 61 raw traces are stored
alongside it. The checker code is unchanged; its three classification tests were
last run in the preceding review. No new Kani run is claimed; production fee
arithmetic is unchanged.

## What this review covers

The new journal getter and replay helper reuse a previously recorded EOA wire.
They reject terminal/quarantined/wrong-key authority, validate decoded wire and
durable hash/nonce/sender/chain membership, and repeat ownership/broadcast fences
before dispatch. Confirmation preserves the allocator's cached high-water when
the observed latest nonce falls. Read [the recovery design](../../../docs/design/eoa-gap-recovery.md)
and [model correspondence](../../eoa.md#exact-wire-gap-recovery-correspondence).

Repeated dispatch of the same identity is a stutter in EoaRecovery and
DisasterRecovery. This review does **not** add a proof of mempool eviction
recovery, the fixed window, five-second cooldown, 32-call limit, scheduler fairness
or wall-clock progress. NonceAllocator assumes monotonic chain consumption; the
actual lower-RPC-count integration is a Rust test obligation. These unchanged
models must not be presented as a liveness proof of the new implementation.

## Implementation evidence

These suite selections overlap; their counts are not an aggregate coverage score.

| Log | Result |
|---|---|
| [Targeted replay regression](targeted-tests.log) | 1 parent test passes in 8.78s, executing 11 isolated scenarios. Included in the full executor Redis suite below. |
| [Executor Redis](executor-redis-tests.log) | 61 passed serially in 14.37s; 37 nonignored tests filtered. |
| [Executor unit](executor-unit-tests.log) | 37 passed; 61 Redis tests ignored. |
| [Core journal](core-journal-tests.log) | 20 passed serially; 20 unrelated tests filtered. |
| [Core unit](core-unit-tests.log) | 20 passed; 20 journal tests ignored. |

[Commands](suite-commands.json), [workspace compilation](workspace-no-run.log)
and [release compilation](release-build.log) are retained. The replay regression
uses actual Redis/HTTP/SQLite and the production initial-send path; it checks
ascending byte-identical replay, bounded rounds, cooldown, nonce progress,
cached-floor preservation, missing/substituted identities, unknown responses,
terminal/halted authority and lease loss before/during dispatch.

Process-level reorg recovery and later capacity measurements are separate
qualification records and were pending when this record was written. This is
not a maximum-TPS, mainnet, simultaneous-chain or whole-system correctness claim.
