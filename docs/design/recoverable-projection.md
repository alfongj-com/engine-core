# Recoverable journal checkpoints and measured scheduling

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
   compare-and-delete of the pending record in SQLite, then return permission.

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

The marker does not hash every Redis key. Retrying an unacknowledged transition
does not prove preservation of every queue mutation; that projection limitation
already exists with equal tokens. Exact intent, replay ownership, signed attempts,
finality evidence and all send gates remain required.

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
