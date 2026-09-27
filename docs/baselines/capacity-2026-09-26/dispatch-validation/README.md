# EOA authorization/dispatch implementation validation

September 27, 2026. These are real Redis, FULL SQLite journal and loopback HTTP
checks of the overlapped dispatch pipeline, not throughput measurements.
[index.json](index.json) records exact source and evidence hashes. The independent
[TLA+ correspondence](../../../../formal/eoa.md#overlapped-authorization-and-dispatch-review)
covers retained identity and durable authorization; futures ordering, cancellation
and concurrency remain implementation-test obligations.

## Passing scope

- The **six new dispatch regressions** pass with the final stronger suffix
  assertion (2.43s). The restored send-pattern gate passes **seven tests** (2.55s):
  the same six plus one existing send regression. These counts overlap.
- Earlier broad executor gates pass **37 normal tests** and **67 ignored Redis
  tests** (0.53s / 15.93s). They use the same production source but precede the
  final test-only assertion strengthening; they are not additional distinct
  pipeline tests. Root's later workspace/release gates are separate.
- Formatting was checked by the coordinator. No production edit was made to fix
  the fixture described below.

The receiver verifies each actual signed wire against both its independent
journal attempt and its borrowed Redis reservation before acknowledging it.
Controlled gates exercise overlap with unfinished authorization, reverse reply
order, ambiguous RPC errors, a failed authorization with an accepted prefix,
worker takeover, cancellation and exact-wire recovery. Both actual new and
recycled production entry points run. An immediately-ready authorization failure
must suppress network construction, and draining a started prefix must not allow
another suffix authorization.

## Separate build and actual-process gates

The [workspace compile-only and release gates](engine-dispatch-build-validation.json)
passed in 18.579s and 38.565s. They do not add executed test cases to the counts
above. Built Engine SHA-256:
`35e307e16cafa7cbb17caafcf5ed90a840d58dddd2454542566619266e7a4c07`.

Two [actual Engine/Anvil reorg scenarios](engine-dispatch-reorg-validation.json)
passed on that release: successful execution (19.985s) and reverted execution
(20.005s). Each withheld provisional completion, observed the orphan disappear,
then recovered the original nonce with the original signed wire through Engine,
without manual wire replay. These are two process scenarios, not additional
unit-test counts or a measured capacity result. Reports and logs are preserved
separately for [success](engine-dispatch-reorg-success.json) and
[revert](engine-dispatch-reorg-revert.json).

## Negative controls

All four mutations compiled, then failed the intended runtime test. Source was
restored after each mutation and the final seven-test gate passed.

| Mutation | Observed failure |
|---|---|
| Restore whole-batch authorization barrier | Held second authorization prevents the first HTTP call; controlled 15s test deadline expires |
| Remove producer stop | Authorization calls become `[0,1,2]` instead of `[0,1]`, including after the started prefix drains |
| Remove post-authorization owner read | A wire starts despite takeover while authorization completes; held response reaches the controlled 15s deadline |
| Remove first-poll failure check | Network work is constructed after a known authorization failure; explicit assertion fails |

These are implementation mutations, not new TLC configurations. Deadline failures
are expected only in these gated test controls; a timeout in the formal runner
never counts as a proof or an expected model counterexample.
[Commands and outcomes](engine-dispatch-mutations-validation.json) and the
[mutation script](engine-dispatch-mutants.py) preserve the exact experiment.

## Original fixture failure

The [initial run](engine-dispatch-targeted.log) failed one new test: it seeded
recycled nonces 2 and 3 above the highest submitted nonce 1. Production cleanup
correctly removed them, because they should be allocated as future fresh nonces.
The fixture now reserves, independently journals and actually dispatches nonce 4,
then verifies that holes 2/3 survive cleanup and the actual recycled entry point
sends exactly two transactions. Five distinct nonce/ID/wire pairs must be
submitted; no assertion was weakened to accept zero recycled sends.

## Limits

No test proves an instantaneous network freeze after ownership changes; an
already-authorized call can race a later loss. A cancelled caller may interrupt
draining, so durable evidence and the full borrowed batch remain authoritative.
These small controlled tests do not establish provider throughput, fairness,
retry-rate limits, cross-chain isolation, real power-loss durability or a
source-to-model refinement proof. Final release/process/load evidence must bind
its own binary digest and remain separate from these source-level tests.
