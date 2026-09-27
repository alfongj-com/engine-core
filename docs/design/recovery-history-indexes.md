# Recovery history indexes

## Problem and change

The durable journal retains historical attempts and terminal evidence. The first terminal commit inserts directly; ordinary fresh-ledger throughput is not evidence of a history-scan bottleneck. Restart cleanup and duplicate terminal reconciliation, however, query the first proof by ID, and gap recovery queries the newest recorded attempt by ID. Without matching indexes, SQLite scans the entire terminal history or sorts one intent's attempt history while holding the connection shared by every chain.

Add two **nonunique** indexes:

- `terminal_evidence(id, sequence)`: locate the first committed proof without discarding later contradictory evidence.
- `attempts(id, sequence)`: locate the newest wire and traverse attempt membership in sequence order without a temporary sort.

No table, row, replay binding, proof, query ordering, journal schema version, checkpoint, or Redis token changes. This is an additive physical migration; older binaries can read the same schema. Indexes reduce lookup work, but do not bound retained disk size, the number of matching attempt rows inspected, or global journal contention. They add disk space and index maintenance to each new attempt/proof insert; selected fresh-ledger performance confirmations must be repeated before claiming a net benefit.

## Startup and failure behavior

Fresh explicit initialization creates both indexes in its existing schema transaction. Existing ledgers migrate only during writable `RecoveryJournal::open`, after the exclusive OS owner lock, schema/integrity/namespace checks, and live Redis continuity check. A single `BEGIN IMMEDIATE` transaction creates both indexes under the existing WAL/FULL configuration. RAII rollback covers any DDL error; startup returns an error and exposes no usable journal. A second health check after DDL detects a Redis change while building indexes.

An owned startup task retains the journal across caller cancellation. Its blocking SQLite closure also retains the owner, including async-runtime shutdown while blocking work completes. A cancelled startup cannot release the exclusive owner while its index DDL is still running. A process crash is handled by [SQLite's transaction contract](https://www.sqlite.org/lang_transaction.html) under the existing [WAL/FULL setting](https://www.sqlite.org/pragma.html#pragma_synchronous); no new power-loss guarantee is claimed.

Read-only status/export and offline quarantine/reattach/recover do not migrate implicitly. Physical maintenance does not advance the application checkpoint or mirror a new Redis token. Existing legacy index-free journals remain readable by status/export. Unknown schema and unhealthy Redis projections still fail closed before migration.

## Rollout

Stop the old Engine; preserve the journal, WAL and Redis projection normally. Allow enough free disk and startup time for one index build over retained history. Start the new binary against the same journal and namespace. It must complete migration and both health checks before serving work. Do not kill a slow migration merely because startup is not ready; if interrupted, restart and let SQLite recover its transaction. A storage/DDL error requires operator inspection; never initialize a replacement journal or discard retained attempts to bypass it.

No explicit schema-format conversion is needed. The additional indexes can remain if rolling back the binary. Large ledgers should be rehearsed on a private copy during a maintenance window; this patch does not claim a fixed startup deadline or add live migration while workers are active.

## Verification

Regression command (use an isolated test Redis):

```sh
TEST_REDIS_URL=redis://127.0.0.1:26479/ cargo test --locked -p engine-core --lib recovery::history_index_tests -- --ignored --test-threads=1
```

Five tests cover populated pre-index restart and exact export/Redis-token preservation; indexed query plans; original/latest attempt membership; first proof, equivalent retry and contradictory-proof halt; read-only access and owner-lock refusal without migration; unhealthy projection refusal before DDL; second-index DDL failure rolling back the first before a safe retry; and deterministic caller/async-startup cancellation while a separate WAL writer blocks DDL, retaining exclusive ownership until the blocking operation finishes. Existing journal, executor recovery, process and formal-source-map checks remain required before release.

All five regressions passed in the September 26 capacity validation. Existing
journal, configuration and executor suites also passed; the validation record is
in the [capacity evidence](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/capacity-2026-09-26/README.md).

Query-plan evidence motivating this change was read from a stopped test journal:
terminal lookup used `SCAN terminal_evidence`; both ordered attempt lookups used
`USE TEMP B-TREE FOR ORDER BY`. This is a performance-risk observation, not a
measured Engine throughput improvement.
