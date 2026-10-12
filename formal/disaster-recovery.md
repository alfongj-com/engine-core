# Recovery authority and restartable Redis publication

**Scope: schema3 runtime and finite models. See the [PR handoff](../WORK_IN_PROGRESS.md) for validation results.**

## Claim and limits

Two finite models cover different parts of the protocol:

- [DisasterRecovery](tla/DisasterRecovery.tla) separates admitted identity, exact
  attempts, Redis projection, network delivery and independent chain effects.
- [DisasterRecoveryProjection](tla/DisasterRecoveryProjection.tla) expands the
  pending-checkpoint protocol: SQL commit, Redis CAS, independent observation,
  SQL acknowledgement, caller return, cancellation, restart and schema migration.

Neither is a Rust refinement proof, a composition theorem, a throughput result,
 nor a proof of SQLite, Redis, filesystem durability or consensus. The models
 assume one authoritative local ledger, an exclusive owner and atomic durable
 SQLite commits. A coherent rollback or clone of both authority and Redis remains
 unsupported. [Finality](finality.md) separately models evidence for terminality;
 the identity model assumes a terminal observation is conclusive and attributable
 to that admission's recorded attempt.

## Protocol boundary

A schema3 authority mutation writes its new checkpoint and **one durable pending
transition in the same FULL SQLite transaction**. The record binds the exact
previous token, target token, Redis key and Redis process ID. Its target must
match current authority. Its predecessor is either the immediately preceding
same-schema checkpoint or a recorded schema1/2→schema3 migration at the same
counter. Validated pending schema2 writes, including schema1→2 migration, retain
their original schema until acknowledged.
A numeric checkpoint gap alone is never evidence of an interrupted transition.

The health gate accepts only the recorded predecessor or target from the same
Redis process acting as primary. It applies an idempotent CAS, observes the
result, then compare-deletes the pending record in a second FULL SQLite commit.
**Only after that acknowledgement may the caller obtain permission.** A lost
CAS response remains blocked and retryable; it is not evidence that the CAS
failed. Positive token, process or role contradictions and existing durable
halts remain fenced.

An Engine restart under the same Redis process may finish publication from either
recorded token. It does not enqueue
work, retransmit a wire, or recreate a dead caller's permission. Existing jobs
or original-ID retries follow the ordinary admission and broadcast checks.
Cancellation has the same distinction: durable pending state survives; local
return state does not.

Once acknowledgement removes pending, restoring an older marker hard-halts.
Restoring the exact predecessor *during an unacknowledged transition* is
indistinguishable from a CAS that never ran, and may be repaired: no permission
from that transition has escaped. This protects authority and replay identity;
it does not detect every individual Redis-key loss or intra-transition
projection rollback. A fault can also race the verified post-observation and
SQL acknowledgement, or an already-authorized network call. Later health checks
fence that fault; they cannot retract bytes already delivered.

## Migration and explicit recovery

Schema1 and schema2 migrate only after an exact healthy observation under
exclusive ownership. An existing validated schema2 pending write must finish
before migration. The schema3 bump and its exact old-schema→schema3 transition
are atomic. Old binaries must reject schema3: they cannot enforce its durable
Solana recovery mode and grant count. An older schema mismatch or halt is never
retroactively reclassified as a pending transition.

Redis process restart requires explicit offline handling. Reattach accepts a
validated pending record **only when the current primary already holds its exact
target**. It never publishes the predecessor or accepts a missing, foreign or
wrong-epoch token. One FULL SQLite transaction acknowledges the exact pending
record, adopts the observed Redis process ID, clears only permitted continuity
halts, and records the action. A crash cannot leave adoption and acknowledgement
half-committed. A fresh observation follows the commit; startup also checks
continuity. No old caller, queue entry, signature or send permission is revived.

Explicit fresh-namespace recovery remains separate: it advances the epoch,
quarantines attempted nonterminal identities, retains bindings, wire evidence
and Solana recovery counts, and clears pending with that state change. It
publishes the new marker separately; interrupted recovery stays halted.

## Reviewed source correspondence

| Model boundary | Runtime obligation |
| --- | --- |
| `Admit`, `AttemptCommit`, `TerminalCommit`; projection `Commit` | `reserve_admission`, `before_broadcast`, `record_terminal` and checkpoint updates commit authority and pending together. |
| `Mirror`; projection `Publish`, `Observe` | Exact Redis CAS plus a fresh process/role/token observation; uncertainty releases no new permission. |
| `Acknowledge`, `ReturnPermission` | Compare-delete the exact pending record in FULL SQLite before returning success. |
| `Crash`, `Cancel`, `Start` | Pending survives; caller and unissued permits do not. Actual blocking tasks must retain the OS owner through cancellation. |
| `Migrate` | Healthy schema1/2 observation, completion of older pending state, atomic schema3 bump and recorded migration transition. |
| `Send`, `Execute` | An already-authorized call or chain effect may occur after a later halt or process crash. |
| `Enqueue` | Original-ID retry uses the persisted payload/generated identity; terminal/quarantined work is not recreated. |
| `RedisUnavailable` | Observation failure retains its first cause; reattachment still requires an exact current marker and exclusive ownership. |
| `ReattachObserve/Commit/Finish`, `RecoverBegin/Publish/Finish` | Exact-target offline acknowledgement plus process adoption, or explicit new-epoch quarantine; never silent online adoption or revived permission. |

The identity model keeps operation metadata beside pending as specification
bookkeeping to associate the living caller with its eventual return. Runtime
`projection_pending` contains only checkpoint-transition fields. The focused
model fixes deployment identity to one deployment and abstracts Redis keys to
original/recovered namespace atoms; runtime token/key comparisons additionally
include the full deployment and namespace strings. Malformed persisted records,
unknown schemas, SQLite commit failures and actual lock lifetime need executable
regressions; they are not modeled storage-corruption recovery.

## Finite checks

The identity model retains its bounds: two intent IDs, replay keys, payload atoms
and workers; four authority mutations, one recovery epoch and one Redis restart.
Its positive invariants cover immutable intent, unique replay bindings, durable
attempt membership, retained evidence, caller epoch, terminal exclusion,
original retry payload, at most one business effect per intent, attribution and
recovery quarantine. Existing fault/boundary/witness configurations remain.

The main projection configuration has one exclusive owner, two authority
mutations, three schemas, one epoch change, one explicit offline reattach
invocation, two Redis process IDs and primary/replica roles. Migration-focused
positives use a one-write bound; a pre-existing same-schema write consumes that
bound. The main schema3 case separately explores two new authority writes.
A separate one-write configuration permits two offline reattach invocations
and witnesses a retry after the first acknowledgement committed but its caller
crashed. There are no state-space filters or fairness assumptions. A permanently failed
provider or halted state may remain blocked forever.

| New configuration | Required outcome |
| --- | --- |
| `DisasterRecoveryProjection`, `_legacy`, `_schema2`, `_schema2_pending`, `_schema2_migration_pending` | Exhaustive pass for the declared finite bounds. |
| `_early_permission` | `PermissionRequiresAcknowledgement` catches permission returned before SQL acknowledgement. |
| `_lost_pending` | `PendingRetained` catches deleting durable pending on a process crash. |
| `_forgiven_rollback` | `NoUnwitnessedRepair` catches repairing completed history without a pending witness. |
| `_run_adoption` | `NoAutomaticRunAdoption` catches adopting a restarted Redis process during repair. |
| `_revived_caller` | `NoResurrectedCaller` catches returning a crashed/cancelled caller's permission. |
| `_clear_halt` | `NoArbitraryHaltClear` catches automatically clearing an existing durable halt. |
| `_old_marker_witness`, `_new_marker_witness` | Named reachability violations demonstrate completed restart repair from each exact token. |
| `_migration_witness`, `_uncertain_witness` | Named reachability violations demonstrate migration restart and recovery after uncertain transport. |
| `_reattach_predecessor`, `_reattach_missing`, `_reattach_foreign` | `ReattachRequiresExactTarget` rejects acknowledgement without the exact observed target. |
| `_reattach_partial` | `PendingMatchesAuthority` catches adopting a new process ID without atomically acknowledging its old pending record. |
| `_reattach_revived`, `_reattach_halt` | Catch reviving a dead caller or clearing an unrelated durable halt during offline reattach. |
| `_reattach_pending_witness`, `_reattach_restart_witness` | Show completed exact-target reattachment, including a changed Redis process ID. |
| `_reattach_retry`, `_reattach_retry_witness` | Two-invocation positive and reachability case: retry after committed acknowledgement and caller crash. |

Witness counterexamples show a path exists; they are **not universal liveness
proofs**. Named fault counterexamples must fail their specified invariant;
parser errors, timeouts and resource failures do not count as successful tests.

## Implementation evidence and boundaries

`core/src/recovery/projection_tests.rs` kills real child processes after the
production FULL commit, Redis CAS, before acknowledgement and after acknowledgement.
It checks schema1 migration at the same cuts, unchanged history, post-ack rollback,
wrong tokens/process/role, malformed pending records, failed acknowledgement,
read-only inventory, explicit quarantine and cancellation/owner/drain behavior.
`projection_transport_tests.rs` drops a successful Redis CAS reply through a real
TCP proxy and checks that no permission returns until a later repair succeeds.
A separate race rolls Redis back between observation and acknowledgement: the
post-ack check rejects permission and retains a durable halt.

The runtime and focused model require another continuity observation after
acknowledgement, before returning permission. This is not an atomic
Redis/SQLite/network boundary. A fault can still occur after the final check and
before network emission.

Per-chain lanes change scheduling before the existing serialization lock. They
add no authority transition; cancellation, bounded lane cardinality and peer
progress are executable test obligations. A global admission bound can still be
filled by one chain. Health repairs are owned operations with a separate bounded
admission semaphore. No Tokio fairness or elapsed-time theorem is claimed.

The review fixes leave the authority transitions unchanged. Unscoped health
checks use separate bounded capacity, and 4337/7702 workers carry their chain
scope. Idle scheduling lanes may be reused only after all references are gone;
live overflow work must not acquire a second lane for the same chain. Ownership
registration fails fast when shutdown has queued its exclusive drain, releasing
any lane/health permits rather than waiting on an inverted lock order. These
are runtime scheduling obligations covered by deterministic concurrency tests,
not properties of the checkpoint model.
The server's final drain also permanently closes registration, so a detached
probe that wakes after the barrier cannot begin new authority work. Rejected
registrations perform no modeled authority transition.

The [retained Solana model](retained-solana.md) additionally separates exact-wire
restoration, durable recovery mode, and cumulative recovered-send allowance.
It is not a proof of model composition.

`RepairMarker` remains an arbitrary marker write in the identity model. If
unavailability was the first halt cause, manually restoring just the marker can
satisfy reattachment even when other keys were lost. The marker is not a dataset
checksum: do not repair it by hand. A previously recorded mismatch or mirror
failure stays non-clearable. Storage durability, coherent authority rollback,
consensus and the composition of models remain outside these finite checks.

Timing-only edits in the queue, EOA store and send wrappers preserve their
existing operations and error paths; recording a metric is a model stutter.
Solana coalescing shares transport for independent `Observe` actions and preserves
each position, history policy and subsequent finalized receipt check. Typed
`PublicationPending` adapters change retry delay only: all original admission,
identity and before-send checks run again. Their tests cover classification,
retained signed identity and rejected permanent faults; no new liveness theorem
is inferred from a shorter delay.

Solana status entries are decoded and checked independently after validating the
shared context and exact array length. A malformed entry fails only its own
observation; malformed envelopes still fail the batch. Reducing batch size or
disabling coalescing changes transport grouping, never the signed identity,
history policy, retry budget or terminal-proof requirements. Real HTTP tests
cover these boundaries; no transport refinement or provider-latency proof is
claimed.

The chain-context wrapper boxes its inner future before installing task-local
state. This limits stack growth when wrapping the large execution-router future;
it does not change polling order, chain scope, ownership or any model action.
A bounded-size regression and actual EVM admission/process test cover this
runtime boundary; the finite models do not represent stack allocation.
