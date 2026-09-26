# Capacity review: checkpoint and bounded scheduling changes

Date: September 26, 2026. This record covers reviewed candidate working-tree
bytes based on `64c231c5586f922b146c33b9ea7e9def46e1eacf`; that base commit is
**not** the modified candidate. [The report](report.json) contains the exact 67
mapped runtime/test hashes and model/configuration hashes. [Summary metadata](summary.json)
also records the manifest/source-map digests and candidate release digest. TLC
checks source correspondence fingerprints, not compiled machine code.

## Results

- **61/61 expected TLC outcomes passed**, using pinned TLC 1.7.4 in 201.013s.
- Fourteen positive configurations exhausted **3,164,944 distinct states**, summed
  across separate state spaces. This is not one composed service state space.
- Thirty-one fault, eleven boundary and five reachability cases produced their
  required counterexamples. Syntax errors, unrelated invariant failures and
  timeouts cannot satisfy the runner.
- All 67 source hashes matched before and after checking. The source-map review
  notes were recorded before execution; this report supplies their validation
  result. The older evidence directories remain unchanged.
- [Checker classification tests](checker-tests.log): 3 passed.

The [runner log](runner.log) summarizes every case. Each raw trace is alongside
it, named after its configuration; no counterexample was suppressed or replaced
with a passing assertion.

## New depth regression

[DepthCheckpoint](../../depth-checkpoint.md) adds explicit tip and qualified
boundary heights, which the earlier Finality abstraction collapsed. Its positive
case exhausts 898 states. The four short traces are:

| Trace | What it establishes |
|---|---|
| [Old tip anchor](DepthCheckpoint_tip_anchor.log) | With `H=3`, depth 2 and unchanged qualified block 1, replacing the tip causes the faulty stored-tip implementation to halt. `NoSpuriousHalt` fails. |
| [Ignored conflict](DepthCheckpoint_missing_halt.log) | A performed continuity check sees the qualified block change, but the faulty implementation stays healthy. `PositiveConflictHalts` fails. This checks the required response to a deeper rollback, not prevention of rollback. |
| [Shallow survival](DepthCheckpoint_shallow_witness.log) | After persisting block 1, replacing blocks 2–3 can be followed by a healthy continuity check. The deliberately false reachability invariant fails. |
| [Rollback boundary](DepthCheckpoint_rollback_boundary.log) | A deeper change after the observation rechecks can invalidate accepted depth evidence. `AcceptedBoundaryRemainsCanonical` fails; probabilistic depth cannot provide immutable consensus finality. |

This is one observer, one stored checkpoint, four heights, two hash values and
two changes. Multiple advancing checkpoints, policy migration, monotonic journal
CAS and model composition remain separate obligations. Real tests cover the
precise RPC order and legacy depth-checkpoint behavior.

## Implementation evidence

These are distinct selected suites, not an aggregate coverage score:

| Log | Result |
|---|---|
| [Core finality](core-finality-tests.log) | 9 passed; 31 unrelated tests filtered. |
| [Server library](server-lib-tests.log) | 23 passed; 6 external-service tests ignored. Includes bounded health checks, client cancellation, deadlines and actual configuration deserialization. |
| [Executor Redis](executor-redis-tests.log) | 60 passed serially; 37 nonignored tests filtered. Includes real-journal depth continuity, conservative finalized/legacy behavior and unchanged Solana signed bytes under configured polling. |
| [Executor unit](executor-unit-tests.log) | 37 passed; the 60 Redis tests ignored. |

The candidate also raises the new-nonce scheduling batch from 128 to 256 while
retaining concurrency 32 and existing in-flight/retained-work limits. It adds
bounded health probes and whole-second Solana confirmation polling (default 1s;
2s is an explicit experiment). These changes retain identity/authorization
transitions but are not formally proved to improve throughput or fairness.

Process-level reorg reproduction, native-node reconciliation and matched
capacity/chaos measurements are separate records. They were not complete when
this formal record was written. Nothing here establishes mainnet maximum
throughput, sustainable simultaneous-chain capacity, or whole-system correctness.
