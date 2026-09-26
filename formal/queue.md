# Queue leases and Redis ownership

## Claim and scope

[QueueLease.tla](tla/QueueLease.tla) checks **finite safety properties of a hand-written
abstraction**, not the Rust program, Redis, or external transaction delivery.
The checked actions follow the single-queue implementation; a single Multilane
lane uses the same lease and completion contract. Lane fairness and the lane
scheduler are outside the model.

The model has one reusable job ID, up to two admissions, two lease issuances,
two concurrent completion callers, two physical Redis sessions, and two
completion invocations. Callers can finish an old borrowed job or duplicate a
completion. `WATCH`, `EXISTS`, and `EXEC` are separate steps: expiry, competing
completion, or connection abandonment may interleave between them. Success,
permanent failure, immediate nack, delayed nack, cancellation, and pruning are
available transitions. A caller can abandon its connection at every completion
step.

There is **no lease-renewal operation in current twmq**. Expiry and reborrow
create a fresh lease key; this model makes no claim about a future renewal API.

## Source correspondence

| Model action | Implementation and atomic boundary |
| --- | --- |
| `Push` | [Queue::push](../twmq/src/lib.rs#L245), [push_to_lane](../twmq/src/multilane.rs#L150): Lua dedup check, data/meta writes, index insertion. A duplicate admission stutters. |
| `Poll` | [pop_batch_jobs](../twmq/src/lib.rs#L601): one Lua step for lease cleanup → cancellation → due delays → borrow. The model preserves this order without allowing an intervening action. [Multilane](../twmq/src/multilane.rs#L559) cleans the relevant lane before settling cancellation. |
| `Expire` | Redis TTL expiration removes the lease key and invalidates sessions watching it. No wall-clock duration is modeled. |
| `Cancel` | [cancel_job](../twmq/src/lib.rs#L407), [Multilane cancellation](../twmq/src/multilane.rs#L362): immediate pending/delayed removal or an ID-keyed deferred cancellation. |
| `Begin`, `Watch`, `Read` | [TransactionConnections](../twmq/src/transaction.rs#L22): exclusive checkout, then `WATCH lease_key`, then `EXISTS`. Missing lease triggers `UNWATCH` and clean return. |
| `ExecCommit`, `ExecAbort` | [commit_if_leased](../twmq/src/transaction.rs#L39): `Some` commits the hook+queue pipeline; nil executes no commands and retries. Ack/fail/nack writes come from [completion](../twmq/src/lib.rs#L1273). |
| `DropConnection` | The checked-out connection is dropped on error/cancelled future instead of returning abandoned `WATCH` state to the pool. |
| `Prune` | [success](../twmq/src/lib.rs#L980) and [failure](../twmq/src/lib.rs#L1179) Lua pruning: trim the selected terminal list, then delete records only if the ID is absent from live indexes and both retained terminal lists. Final record deletion also clears deferred cancellation. |

The model's `generation` and logical lease issuance numbers are **ghost state**
for checking histories. Redis stores no generation number for cancellation or
terminal-list entries. `Poll` uses the actual corrected guard: a success-list ID
wins over deferred cancellation only when no recovered pending or delayed work
exists. It does not assume that the list entry belongs to the current admission.

## Properties

- `FencedEffects`: every committed completion used the current, still-live lease.
  The check is a ghost event recorded at commit; **it is not a transition guard**.
- `AtMostOncePerLease`: a lease issuance commits at most one completion/hook
  pipeline. This is not exactly-once external job execution.
- `NoFalseCommit`: nil `EXEC` never reports a committed transaction.
- `SessionIsolation`, `SessionCleanOnReturn`: completion calls cannot share a
  checked-out physical connection, and idle sessions carry no `WATCH` state.
- `LiveRecordIntegrity`, `ExclusiveLiveIndex`: runnable/active work retains its
  data, metadata and dedup entry, and occupies at most one live index.
- `RetainedHistoryIntegrity`: shared data and metadata remain while either
  terminal list retains a reference to the ID. This does not promise a separate
  payload/result snapshot for each historical admission.
- `CancellationNoReborrow`: after deferred cancellation settles without a
  success of the current admission, recovered pending work cannot remain
  runnable. Delayed cancellation is checked by the corresponding real Redis
  regression as well as the model's live-index transitions.

`successes`/`failures` contain ghost admission generations, separate from live
indexes. Thus two physical terminal-list entries with the same job ID remain
distinct elements in the model. These are not mutually exclusive current job
statuses: ID reuse can leave old success entries while new work is live or failed.
Pruning removes the selected historical reference first, then checks the union
of the remaining success/failure references. Runtime checks `LPOS` in both
trimmed lists; it neither stores nor compares the ghost generations.

## Configurations and counterexamples

All constants are explicit in each config; no fairness premise or liveness
claim is made. `CHECK_DEADLOCK FALSE` permits exhausting the finite admission,
lease, and call budgets. This does not suppress invariant checking.

| Config (`formal/tla/`) | Expected result |
| --- | --- |
| `QueueLease.cfg` | All ten safety/type invariants pass; one admission, concurrent callers. |
| `QueueLease_reuse.cfg` | Pass; two admissions, one caller/session, cancellation and pruning. |
| `QueueLease_combined.cfg` | Pass; reuse, cancellation and pruning with concurrent callers/sessions. |
| `QueueLease_permanent.cfg` | Pass; dedup persists until pruning, then ID reuse is permitted. |
| `QueueLease_stale_owner_bug.cfg` | `FencedEffects` fails: omit WATCH → read live lease → expire → stale EXEC writes. |
| `QueueLease_shared_session_bug.cfg` | `FencedEffects` fails: another caller clears connection-scoped WATCH before stale EXEC. The config checks the resulting stale effect, not just the intentionally violated ownership rule. |
| `QueueLease_exec_abort_bug.cfg` | `NoFalseCommit` fails: expiry after EXISTS → nil EXEC treated as committed. |
| `QueueLease_id_reuse_bug.cfg` | `FencedEffects` fails: reuse one physical lease-key identity across logical issuances; a cached old job completes the new incarnation. |
| `QueueLease_prune_live_bug.cfg` | `LiveRecordIntegrity` fails: historical completion pruning deletes a reused live ID's records. |
| `QueueLease_delayed_prune_bug.cfg` | `LiveRecordIntegrity` fails: pruning ignores delayed membership. |
| `QueueLease_historical_cancel_bug.cfg` | `CancellationNoReborrow` fails: old success → reuse → cancel new active job → expiry → historical success defeats cancellation. |
| `QueueLease_retained_history_bug.cfg` | `RetainedHistoryIntegrity` fails: a pruned terminal reference deletes shared records still referenced by retained history. |
| `QueueLease_orphan_cancel_bug.cfg` | `RetainedHistoryIntegrity` fails: active cancellation → successful completion → final prune leaves cancellation behind → housekeeping recreates failed history without job data. |

Each mutant changes one implementation assumption. The last four were discovered
while constructing this model and reproduced against real Redis before fixing
the Rust-embedded Lua. The Multilane `false ~= nil` pruning leak is **test-only**:
this model checks retention safety, not eventual storage reclamation.

Run with the repository's pinned TLC runner, or directly from `formal/tla`:

```sh
java -XX:+UseParallelGC -Xmx2g -cp "$TLA2TOOLS_JAR" tlc2.TLC \
  -workers 1 -cleanup -metadir /tmp/queue-lease-tlc \
  -config QueueLease.cfg QueueLease.tla
```

A mutant is valid evidence only when TLC reports the named invariant violation;
a syntax error, timeout, or arbitrary nonzero exit is not an expected failure.

## Assumptions and reductions

1. **Redis execution and data types.** Each Lua call and successful EXEC is
   indivisible relative to other clients. All queued commands have valid key
   types and complete successfully. Redis does **not** roll back earlier commands
   on a Lua runtime error or an EXEC command error; that behavior is excluded
   from the atomic successful-transition model. The actual error path returns
   without replaying a partially executed pipeline. Its separate regression is
   [completion_error_does_not_replay_partially_executed_commands](../twmq/src/lease_tests.rs#L321).
2. **Storage continuity.** No eviction, flushed keys, restore to older data,
   failed persistence, replication rollback, or administrative dedup removal.
   Expiry of the lease itself is included. Safety under Redis data loss is not
   established by this model or the process-crash tests.
3. **Unique lease identities.** Normal borrowing creates a fresh key. Random-ID
   collision probability and the UUID/nanoid implementation are not verified;
   `FaultReuseLease` tests the necessity of that assumption.
4. **Projection.** Queue ordering, payload
   serialization, timestamps, batching throughput, and other job IDs are omitted.
   Pruning eligibility is nondeterministic: unrelated completions can fill the
   retention list. `Poll` may leave this ID unborrowed because another ID could
   fill the batch. Finite checking is not a parameterized proof for every bound.
   Historical generations distinguish duplicate physical ID entries but do not
   represent their list order. Batch pruning is projected into one reference
   removal at a time; allowing interleavings between removals overapproximates
   its atomicity for these safety checks. Numeric retention limits and eventual
   reclamation are covered by Redis tests, not a model liveness property.
5. **Cancellation scope.** Cancellation is keyed by job ID, not admission
   generation. A deferred request can overlap fresh admission under the same ID;
   no generation-specific cancellation guarantee is claimed. A live owner may
   finish successfully before cancellation settles. Cancellation hooks execute
   after the Lua transition and are not verified as atomic or exactly once.
6. **Effects and retries.** The committed hook pipeline is represented by one
   audit event. Arbitrary handler code, direct side effects inside hooks, outgoing
   HTTP/blockchain effects, lost replies, serializer behavior, and queue-error
   deserialization hooks are not verified. After a successful commit, repeating
   the same completion is modeled and fenced. Rust's ten-conflict retry bound
   affects termination, which is not claimed here.

## Implementation evidence

Twelve new deterministic Redis regressions failed before the fixes and passed
afterwards, covering success **and** failed terminal history, delayed reuse,
actual eligible record deletion, active ownership, expired-lease recovery,
delayed nacks, cancellation, and late acknowledgements:

- [Queue pruning](../twmq/src/lease_tests.rs#L70) and [Multilane pruning](../twmq/src/multilane_lease_tests.rs#L26).
- [Queue cancellation after reuse](../twmq/src/lease_tests.rs#L141) and [Multilane cancellation after reuse](../twmq/src/multilane_lease_tests.rs#L99).
- [Queue same-kind history](../twmq/src/lease_tests.rs#L633) and [cross-kind history](../twmq/src/lease_tests.rs#L640), plus [Multilane same-kind](../twmq/src/multilane_lease_tests.rs#L515) and [cross-kind](../twmq/src/multilane_lease_tests.rs#L522), preserve the newest shared record until the final terminal reference disappears.
- [Queue zero retention](../twmq/src/lease_tests.rs#L647) and [Multilane zero retention](../twmq/src/multilane_lease_tests.rs#L529) remove the index and records; `LTRIM 0 -1` would incorrectly keep the whole list.
- [Queue final-prune cancellation](../twmq/src/lease_tests.rs#L689) and [Multilane final-prune cancellation](../twmq/src/multilane_lease_tests.rs#L571) check that pruning cannot leave a cancellation that recreates an orphan record or persists forever.

Existing regressions check [stale completion](../twmq/src/lease_tests.rs#L230),
[competing acknowledgements](../twmq/src/lease_tests.rs#L273),
[cancellation after expiry](../twmq/src/lease_tests.rs#L204), and
[lease generation reuse](../twmq/src/multilane_lease_tests.rs#L189).

```sh
TEST_REDIS_URL=redis://127.0.0.1:6385/ \
  cargo test --locked -p twmq --lib -- --include-ignored
```

On 2026-09-26, all 26 queue library tests passed against disposable Redis 7.4.2.
The tests contact no public RPC and perform no blockchain transactions.

## Pruning cost

The retained-history guard adds up to two `LPOS` scans per pruned entry: worst-case
additional work is `O(pruned_entries × (retained_success + retained_failure))`
inside Redis's atomic script. Existing pending-list scans add their own cost.
Defaults retain 1,000 successes and 10,000 failures; large configured histories
can therefore delay other Redis commands. This change favors correct retention
and does not introduce a reference-counting schema. The queue baseline harness
retains the entire measured workload, so its previous throughput figures do not
qualify this pruning-heavy case. No new pruning throughput claim is made here.
