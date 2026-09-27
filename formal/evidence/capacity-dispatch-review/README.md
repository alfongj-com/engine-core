# Overlapped EOA dispatch: formal review

September 27, 2026. The [report](report.json), [summary](summary.json),
[reviewed source map](reviewed-source-map.json), and
[commands/environment](run-metadata.json) bind this result to the final restored
working-tree source. The comparison commit in the metadata is not the modified
runtime. The independently built release digest is
`35e307e16cafa7cbb17caafcf5ed90a840d58dddd2454542566619266e7a4c07`;
TLC does not execute or verify that binary.

## Result

- **61/61 expected outcomes passed; 76 reviewed source hashes matched before
  and after the run.**
- The unchanged 14 positive configurations exhaust **3,164,944 distinct states**,
  summed across separate state spaces. The 31 fault, 11 boundary and 5 witness
  configurations produce their required named counterexamples.
- **Three checker safeguards passed.** Summed model time was **197.732s**. The
  model/configuration hashes and expected outcomes match the preceding
  [diagnostic-retention run](../capacity-diagnostic-review/README.md); no new
  model, theorem or configuration count is claimed.

Every raw TLC trace, [runner output](runner.log) and
[checker test log](checker-tests.log) is retained. Earlier evidence remains
unchanged. Fee arithmetic is unchanged; this is not a new Kani execution.

## Correspondence

Both actual new/recycled paths reserve the prepared signed batch in borrowed
Redis state first. Each send then requires its own immutable request/wire check
and successful independent journal commit/mirror, with Redis worker ownership
read before and after that authorization. One authorized network wait can overlap
later authorization. The first authorization error stops production; buffered
unstarted work checks the shared failure flag before network construction, while
started calls drain. Failure/cancellation keeps the whole borrowed batch;
post-dispatch errors retain `Uncertain` semantics and the original replay key.

`EoaRecovery.Broadcast` already permits nondeterministic delivery of retained
identities. `DisasterRecovery.Send` can overlap a later `AttemptCommit`/`Mirror`
and execution may follow a halt or crash. The former whole-batch barrier is not a
premise of either safety abstraction. Suppressing a buffered authorized send
adds no delivery or identity. See the [EOA correspondence](../../eoa.md#overlapped-authorization-and-dispatch-review)
and [journal correspondence](../../disaster-recovery.md#per-attempt-dispatch-pipeline-correspondence).

The models do not implement Tokio, Arc/atomic memory ordering, queue-worker owner
reads, first-poll revocation, ordered future results, draining/cancellation,
recycled-nonce preparation, concurrency, wall-clock fairness or RPC rate limits.
They remain separate finite abstractions without a source-level or composition
proof. An already-authorized call can race later ownership loss; no instantaneous
network freeze or throughput improvement is proved.

## Separate implementation evidence

[Dispatch validation](../../../docs/baselines/capacity-2026-09-26/dispatch-validation/README.md)
preserves six new real Redis/SQLite/HTTP tests and the restored seven-send-test
gate (overlapping counts), four compiled runtime mutations that fail the intended
tests, and the corrected recycled-hole fixture failure. Broader 37 normal and
67 ignored executor tests preceded one stronger test-only suffix assertion;
final targeted tests reran that assertion. The archive also retains workspace
compile-only/release results and successful/reverted actual Engine/Anvil reorg
scenarios on the stated release. These are implementation and process checks,
not additional formal cases, machine power-loss tests, or capacity measurements.
