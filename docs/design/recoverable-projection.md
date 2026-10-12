# Recoverable journal checkpoints and measured scheduling

**Status: implemented in PR #2. Validation and measured results are recorded in the PR handoff.**

## Scope

Follow-up to PR #1. One Engine process owns a durable local SQLite journal;
Redis remains a queue projection. The change repairs an interrupted checkpoint
update and measures contention before changing scheduling or Solana polling.
It does not reconstruct a lost authoritative journal or introduce multiple writers.

## Failure to fix

The prior process-kill experiment left SQLite at checkpoint 13,436 and Redis at
13,435. The journal retained the transaction identities, but startup could not
distinguish its own interrupted update from stale Redis data and stopped.
Accepting an arbitrary one-checkpoint lag would weaken rollback detection.

## Protocol

1. Check the current authority, durable halt, Redis process identity, primary
   role and checkpoint under the journal's serialization lock.
2. Commit the authority mutation and one exact pending transition in the same
   FULL SQLite transaction. Record the previous and target tokens, Redis key and
   process identity. Do not return permission yet.
3. Compare-and-swap the recorded predecessor to the target. An ambiguous response
   leaves the pending record intact. Only the exact predecessor or target from
   that record is admissible; an absent or different token stops execution.
4. Verify the target, unchanged Redis process and primary role. Commit a
   compare-and-delete of the pending record in SQLite, then check continuity again before returning permission.

Startup or the next health check may finish a pending transition. It does not
recreate the crashed caller's send permission or enqueue a request. Normal
original-ID retry and retained executor state govern further execution.
After acknowledgement, the predecessor is invalid and a rollback still halts.
Existing manual, storage and contradictory-evidence halts remain authoritative.
Redis restart with an unresolved pending update requires explicit recovery.

Schema 2 prevents old binaries from bypassing acknowledgement. A schema-1 journal
migrates only after exact healthy continuity is observed; the schema/token change
itself has a durable pending transition. A pre-existing schema-1 mismatch remains
stopped. Blocking SQLite tasks retain the OS ownership lock through cancellation.
Migration is automatic and one-way; see the [maintenance and rollback procedure](redis-disaster-recovery.md#schema-upgrade-and-rollback)
before opening a production schema-1 journal.

The marker does not hash every Redis key. Retrying an unacknowledged transition
does not prove preservation of every queue mutation; that projection limitation
already exists with equal tokens. Exact intent, replay ownership, signed attempts,
finality evidence and all send gates remain required.

## Checkpoint repair versus transaction recovery

`PublicationPending` retries the same work after two seconds; all identity and
send gates run again. Repairing the marker does not reconstruct executor data:

- **First Solana attempt:** `before_broadcast` commits the signed wire and binding
  to SQLite before the worker stores its Redis attempt. A crash or lost publication
  response in that interval can leave the binding without a Redis attempt. Health
  repair can finish the checkpoint, but the job then parks instead of signing again.
  Original-ID resubmission does not rebuild that record. Operator reconciliation
  is required; this PR adds no automatic reconstruction or resolution command.
- **Terminal Solana update:** if the signed Redis attempt remains, a lost terminal
  publication response requeues without cleanup. After checkpoint repair, the worker
  can verify the same signature and finalized receipt again, then complete through
  the existing queue ownership fence. It creates no new signature.

EOA confirmation now nacks continuity failures instead of permanently failing the
job. The same request retries through the health and identity gates; a durable
halt still prevents progress until operator recovery. Only `PublicationPending`
uses the shorter two-second retry; other continuity failures retain the existing
ten-second recovery delay.

## Measurements and acceptance

Capture unchanged behavior first, using the same local fixtures, offered rates,
durability, lease and polling settings as the candidate. Separate journal serial
wait/hold, blocking-pool wait, SQLite mutex wait/execution/commit, Redis roundtrip,
executor phases and actual Solana RPC methods. Histogram sums across concurrent
calls are accumulated time, not a wall-clock critical path. Scrape overhead and
process restart boundaries remain visible.

Keep scheduling changes only with measured progress across chains and exact
transaction reconciliation. Evaluate Solana coalescing using actual HTTP calls
per intent and bounded batch wait, while preserving historical lookup, positional
mapping, existing retry budgets and independent finalized receipt checks.

The protocol requires an additional durable acknowledgement per new checkpoint.
Measure its cost explicitly; do not weaken SQLite synchronization to hide it.
Tests must cover every commit/CAS/ack crash cut, ambiguous responses, cancellation,
migration, post-ack rollback and unchanged fences. Bounded models supplement the
runtime tests; they do not prove the storage stack or the complete Rust service.

## Sources

SQLite documents [WAL concurrency and commit synchronization](https://www.sqlite.org/wal.html)
and [FULL durability settings](https://www.sqlite.org/pragma.html#pragma_synchronous).
Redis [Lua execution](https://redis.io/docs/latest/develop/programmability/eval-intro/)
makes the checkpoint comparison/update atomic within Redis; it cannot make a
SQLite write and Redis write one transaction.

## Shared-chain scheduling

The journal keeps its single writer. Before contending for that lock, an operation
acquires its chain's one-slot lane. HTTP execution and EOA, Solana, 4337 and 7702 workers supply
an explicit task-local chain context; detached journal operations retain it.
There are at most 32 EVM lanes, three Solana lanes and one shared unscoped/overflow
mutation lane. Idle EVM lanes can be reclaimed once no holder or waiter retains
them. While overflow work remains, a new chain stays in overflow to avoid assigning
two lanes to the same chain. Unscoped HTTP gates, public health checks and the
monitor use a separate two-slot health lane, so they do not wait behind unscoped
or overflow mutations before reaching the journal mutex.

This prevents a busy chain from filling the global lock's FIFO queue. It allocates
journal operations, not equal TPS. Admission still has one global 64-request bound;
a flood can fill it before another chain reaches its lane. Overflow chains and
unscoped mutations share a lane. Neither per-chain ingress quotas nor a wall-clock
fairness guarantee is claimed.

Health checks acquire their chain lane before a global health slot. Waiting polls
from one chain therefore cannot occupy all 32 slots. Both waits are cancellable;
after registration, the owned repair retains both permits through acknowledgement
and participates in shutdown drain. Registration fails immediately with `Busy`
if a drain writer is queued or active. Waiting there while holding a lane would
deadlock with an already registered mutation that needs the same lane to finish.
HTTP reports this as retryable busy state, not a durable recovery incident.

Interpret `lane_wait`, `serial_wait` and health-capacity wait together. Moving wait
ahead of the global lock is not itself an improvement; compare each chain's actual
attempt/terminal progress and backlog under the same offered load.
