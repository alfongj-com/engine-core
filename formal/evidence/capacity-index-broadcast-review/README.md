# History indexes and broadcast scheduling: formal review

September 27, 2026. This is the reviewed working-tree candidate based on
`5d602581dd0e2270a520fdba88ac3e71def00d86`; the base commit is **not** the modified
candidate. [The report](report.json) records all source/model/configuration
hashes; [the summary](summary.json) records their digests and release binary
`bb34deeec74afce0b52d03edb9ec73a84497429db0cc2e460590b9127c0d77ce`.
TLC checks mapped source fingerprints, not compiled machine code.

## Result

- **61/61 expected outcomes passed** with pinned TLC 1.7.4, in 191.612s summed
  model time; the runner and its three passing safeguard tests took 191.814s.
- Fourteen positive configurations exhaust **3,164,944 distinct states**, summed
  across separate state spaces. This is not one composed service state space.
- Thirty-one fault, eleven boundary and five reachability cases produce the
  required counterexamples. Unrelated violations, timeout or syntax errors do
  not count as passing results.
- All **72 reviewed source hashes** match before and after the run. Models and
  the 61-case manifest are unchanged from the [preceding run](../capacity-gap-review/README.md).

[Runner output](runner.log), [checker tests](checker-tests.log),
[commands/timing](run-metadata.json) and all 61 raw TLC traces are retained here.
No new Kani run is claimed; production fee arithmetic is unchanged.

## Reviewed correspondence

The [physical indexes](../../../docs/design/recovery-history-indexes.md) preserve
logical rows, replay bindings, query ordering, checkpoints and Redis tokens.
Their addition stutters over the journal model. Owner/health gates, atomic DDL
and cancellation ownership are implementation checks; SQLite/filesystem and
Tokio behavior are not proved by TLC.

The [broadcast setting](../../eoa.md#broadcast-concurrency-correspondence)
changes scheduling of existing authorized identities: default 32, range 1–128.
Preparation stays 32; NOOP uses the lower of the setting and 32; borrowed receipt
reads retain a separate ceiling of 32. No admission, nonce, wire or terminal
transition changes. Models do not prove actual buffer limits, global RPC rates,
fairness, recovery time or throughput. Higher-setting process capacity/fault
qualification remains separate.

The [earlier CI failure](../capacity-oracle-review/README.md) is preserved: an
unreviewed oracle hash stopped TLC before execution. The reviewed oracle now
proves an additional accepted original-wire broadcast without requiring another
durable attempt. That test-only change adds no TLA+ case or cryptographic proof.

## Implementation evidence

[Exact commands and hashes](runtime-validation.json) and
[copied-file hashes](implementation-evidence-files.json) accompany these logs.
Suite selections overlap; do not sum their counts.

| Check | Result |
|---|---|
| [Workspace test compilation](workspace-no-run.log) / [release build](release-build.log) | Passed without source repairs |
| [History migration](migration-tests.log) | 5 passed, including blocked-DDL caller/async cancellation |
| [Core journal](core-journal-tests.log) | 25 passing runner entries, including those five and existing helper/opt-in benchmark entrypoints |
| [Core unit](core-unit-tests.log) | 20 passed, 25 ignored |
| [Server unit](server-unit-tests.log) | 24 passed, 6 ignored; includes actual layered configuration validation |
| [Executor Redis](executor-redis-tests.log) | 61 passed serially |
| [Executor unit](executor-unit-tests.log) | 37 passed, 61 ignored |
| [Actual success reorg](local-eoa-reorg-index-broadcast-success.json) / [reverted reorg](local-eoa-reorg-index-broadcast-revert.json) | Both passed: one original durable wire, one accepted post-restart replay, no duplicate terminal retry |

These tests do not simulate machine power loss. The one-intent process cases
do not qualify high-concurrency or simultaneous-chain operation. Previous
evidence retains its own source/binary scope; no whole-service proof follows.
