# Depth checkpoint: qualified history versus observed tip

## Coverage gap

[Finality.tla](tla/Finality.tla) collapses heights to a head that either covers a
receipt or does not. It explicitly excludes cross-cycle checkpoint persistence.
It therefore cannot represent a tip above the depth-qualified boundary or the
later false halt caused by persisting that tip. Earlier passing model results
did not cover this availability defect. Refreshing source fingerprints alone
would not close the gap.

## Finite model

[DepthCheckpoint.tla](tla/DepthCheckpoint.tla) retains one durable checkpoint and
a separate semantic witness for the history qualified by a positive-depth
policy. Independent ledger state maps heights 1–4 to canonical hashes; the head
can grow from 3 to 4. Depth is 2. Up to two reorgs can replace suffixes. Head and
boundary reads and rechecks are separate actions. A fixed receipt at height 1
is assumed to pass the separate receipt-identity gate. This isolates checkpoint
selection and later continuity; it is not an end-to-end composition proof.

`Persist` normally stores boundary `Q = observed H - depth`. The old-behavior
mutation stores tip `H`. The semantic witness always records `Q`, independently
of this storage choice. `CheckContinuity` decides from the actual stored anchor;
ghost facts record whether a conflict affected qualified history. Thus
`NoSpuriousHalt` is not made true by guarding away a mistaken tip conflict. The
mutation can halt after replacement above `Q` while the qualified block remains
unchanged.

| Configuration | Verified expected result |
|---|---|
| `DepthCheckpoint.cfg` | Pass `TypeOK`, `NoSpuriousHalt`, `PositiveConflictHalts`. |
| `DepthCheckpoint_tip_anchor.cfg` | Fail `NoSpuriousHalt`: reproduce the old persisted-tip behavior. |
| `DepthCheckpoint_missing_halt.cfg` | Fail `PositiveConflictHalts`: even when a deeper rollback is permitted, observing the resulting boundary contradiction must halt. |
| `DepthCheckpoint_shallow_witness.cfg` | Fail `ShallowReorgSurvivalNotReached`: reach shallow replacement followed by a healthy continuity check. This is an expected reachability witness. |
| `DepthCheckpoint_rollback_boundary.cfg` | Fail `AcceptedBoundaryRemainsCanonical`: probabilistic depth permits deeper rollback. |

The five cases are registered in [models.json](models.json). The existing
manifest-driven checker requires no code change. The formatted candidate fingerprints are recorded after source review;
the [full candidate record](evidence/capacity-review/README.md) passes all 61
expected outcomes and the targeted implementation suites. This family passed
all five cases (898 distinct positive states). Earlier 56-case evidence does
not validate this candidate. Parser failures, timeouts and wrong invariant names
must fail validation, never substitute for expected counterexamples.

## Reviewed source and test correspondence

| Source boundary | Relationship to this model |
|---|---|
| [Core receipt assessment](../core/src/finality.rs) | Observe `H`, select `Q`, verify exact boundary number/hash, and recheck receipt, boundary and observed tip. The model abstracts the order of its two rechecks; Rust fixtures cover the actual RPC sequence. |
| [Core continuity assessment](../core/src/finality.rs) | Compare the persisted anchor with canonical history. Missing/error responses remain unknown; those response classes are not enumerated here. |
| [Executor checkpoint adapter](../executors/src/finality.rs) and [journal](../core/src/recovery.rs) | Positive contradictions halt. Monotonic/CAS persistence, transaction fencing and multiple checkpoint updates remain obligations of separate tests/models. |
| [Core regressions](../core/src/finality_tests.rs) | Select `Q` for success and revert, including a boundary distinct from both receipt and tip; reject missing, malformed and changing evidence. |
| [Real-journal executor regressions](../executors/src/finality_tests.rs) | Shallow tip replacement remains healthy; the boundary advances; a boundary replacement halts. Finalized-tag and legacy depth evidence retain conservative handling. |

Independent review found no concrete blocker in this abstraction or its source
correspondence. On 2026-09-26 both the focused and full checker runs passed all five cases,
including exact named counterexamples. The complete run passed 61 cases and
verified the 67 mapped source hashes before and after checking. Core and
real-journal regressions also passed; neither model success nor these tests
constitute whole-service qualification.

## Assumptions and limits

One checkpoint, one observer, four heights, two hash values and two changes;
there is no fairness or throughput claim. The normal case forbids rollback into
already qualified history after persistence, but permits changes during
observation. The rollback boundary configuration removes that assumption and disproves
continued canonicality. The ignored-conflict fault also permits deeper rollback,
but tests the required response to an observed contradiction; it does not claim
to prevent that rollback. A rollback can
also occur after rechecks but before persistence: the model must not turn the
RPC observations into an immutable-finality guarantee.

Depth does not provide consensus finality. This model does not establish
availability under null, stale or dishonest RPCs, storage failure or process
crash. It does not model advancing multiple checkpoints, journal CAS or policy
migration. [Migration handling](../docs/replay-migration.md) retains old tip
anchors conservatively until the new qualified boundary catches up; it never
silently rewrites an authoritative checkpoint. A later old-tip conflict can
still halt such a legacy journal.
