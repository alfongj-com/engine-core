# Recovery authority and restartable Redis publication

**Status: draft model revision. The matching Rust protocol is not wired or runtime-validated; see [saved work](../WORK_IN_PROGRESS.md).**

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

A schema2 authority mutation writes its new checkpoint and **one durable pending
transition in the same FULL SQLite transaction**. The record binds the exact
previous token, target token, Redis key and Redis process ID. Its target must
match current authority. Its predecessor is either the immediately preceding
schema2 checkpoint or the recorded schema1→schema2 migration at the same counter.
A numeric checkpoint gap alone is never evidence of an interrupted transition.

The health gate accepts only the recorded predecessor or target from the same
Redis process acting as primary. It applies an idempotent CAS, observes the
result, then compare-deletes the pending record in a second FULL SQLite commit.
**Only after that acknowledgement may the caller obtain permission.** A lost
CAS response remains blocked and retryable; it is not evidence that the CAS
failed. Positive token, process or role contradictions and existing durable
halts remain fenced.

Restart may finish publication from either recorded token. It does not enqueue
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

Schema1 migrates only after an exact healthy observation under exclusive
ownership. The schema bump and its schema1→schema2 pending transition are atomic.
Old binaries reject schema2, including the interval where Redis has the target
but SQL has not acknowledged it. A schema1 mismatch or existing halt is not
retroactively reclassified as a pending transition.

Redis process restart still needs explicit offline handling. Reattach rejects
**any pending record**, even when Redis holds its target. A completed exact
checkpoint can use the existing reattach policy. Explicit fresh-namespace
recovery advances the epoch, quarantines attempted nonterminal identities,
retains their bindings and evidence, and clears pending atomically with that
state change. Recovery publishes its new marker separately; an interrupted
recovery remains halted. It does not restore automatic execution of quarantined
work.

## Source correspondence

| Model boundary | Runtime obligation |
| --- | --- |
| `Admit`, `AttemptCommit`, `TerminalCommit`; projection `Commit` | `reserve_admission`, `before_broadcast`, `record_terminal` and checkpoint updates commit authority and pending together. |
| `Mirror`; projection `Publish`, `Observe` | Exact Redis CAS plus a fresh process/role/token observation; uncertainty releases no new permission. |
| `Acknowledge`, `ReturnPermission` | Compare-delete the exact pending record in FULL SQLite before returning success. |
| `Crash`, `Cancel`, `Start` | Pending survives; caller and unissued permits do not. Actual blocking tasks must retain the OS owner through cancellation. |
| `Migrate` | Healthy schema1 observation, atomic schema bump and recorded migration transition. |
| `Send`, `Execute` | An already-authorized call or chain effect may occur after a later halt or process crash. |
| `Enqueue` | Original-ID retry uses the persisted payload/generated identity; terminal/quarantined work is not recreated. |
| `Reattach`, `RecoverBegin/Publish/Finish` | Exact completed-checkpoint reattachment or explicit new-epoch quarantine; never silent process adoption. |

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

The focused projection model has one exclusive owner, two authority mutations,
two schemas, one epoch change, two Redis process IDs and primary/replica roles.
There are no state-space filters or fairness assumptions. A permanently failed
provider or halted state may remain blocked forever.

| New configuration | Required outcome |
| --- | --- |
| `DisasterRecoveryProjection`, `_legacy` | Exhaustive pass for the declared finite bounds. |
| `_early_permission` | `PermissionRequiresAcknowledgement` catches permission returned before SQL acknowledgement. |
| `_lost_pending` | `PendingRetained` catches deleting durable pending on a process crash. |
| `_forgiven_rollback` | `NoUnwitnessedRepair` catches repairing completed history without a pending witness. |
| `_run_adoption` | `NoAutomaticRunAdoption` catches adopting a restarted Redis process during repair. |
| `_revived_caller` | `NoResurrectedCaller` catches returning a crashed/cancelled caller's permission. |
| `_clear_halt` | `NoArbitraryHaltClear` catches automatically clearing an existing durable halt. |
| `_old_marker_witness`, `_new_marker_witness` | Named reachability violations demonstrate completed restart repair from each exact token. |
| `_migration_witness`, `_uncertain_witness` | Named reachability violations demonstrate migration restart and recovery after uncertain transport. |

Witness counterexamples show a path exists; they are **not universal liveness
proofs**. Named fault counterexamples must fail their specified invariant;
parser errors, timeouts and resource failures do not count as successful tests.

## Validation status

The previous schema1 evidence remains archived unchanged. This revision's
model-only checks are reported separately while runtime implementation and
source correspondence are under review. Do not reuse an old source-map digest
as evidence for schema2. The final runner must pass the refreshed source map and
all registered configurations before this version is described as qualified.

Implementation tests must cover real FULL commit/CAS/ack cuts; lost CAS replies;
post-ack rollback; pending old/new restart; changed Redis process/role;
malformed pending state; schema1 healthy/mismatched migration; and cancellation
while blocking work still holds the exclusive owner. Real crash/restart process
qualification remains separate from these finite models.
