# Queue and EOA execution audit

**Baseline:** `b6b7a0bbdc737b3a2b09611305b71b1bf6aba6e8`. Findings below describe that revision; line numbers refer to it. Static review covers `twmq/src`, `twmq/tests`, `twmq/benches`, `executors/src/eoa`, and `executors/src/transaction_registry.rs`. This is a correctness review, not a production certification. Follow-up verification is recorded separately below.

## Architecture and required invariants

`twmq` keeps payloads/metadata in Redis hashes, pending jobs in lists, delayed jobs in sorted sets, and active jobs behind expiring lease keys. Lua scripts enqueue and borrow batches. Rust workers execute concurrently, then use `WATCH` and `MULTI/EXEC` to combine completion with hook-generated writes. Multilane adds lane discovery and round-robin batching. Jobs can execute again after a crash or lease expiry; application side effects must tolerate this.

The EOA worker is scoped to `(chain ID, address)`. It recovers previously signed **borrowed** transactions, checks **submitted** receipts, prepares **pending** requests using recycled or incremented nonces, persists signed transactions before broadcast, and records the send outcome. An EOA owner token is intended to fence state changes after takeover. Completed requests expire; the transaction registry maps IDs to executor queues.

Required invariants:

1. Broadcast only after signed bytes and nonce reservation commit durably.
2. A stale owner cannot mutate state or erase a newer reservation.
3. One transaction intent has at most one live nonce; distinct intents cannot share a reservation.
4. Unknown send/receipt outcomes preserve the original signed transaction until reconciled.
5. Acknowledgement and its Redis hook effects commit together, once per live lease.
6. A missing receipt does not prove replacement; an included receipt does not prove finality.
7. Retry, cancellation, retention, and shutdown do not silently discard unresolved work.

## Findings, ordered by impact

### Q1 — Aborted transactions are reported as committed · Critical

`atomic.rs:248,363`, `twmq/src/lib.rs:1277,1382`, and `twmq/src/multilane.rs:1286,1400` deserialize `EXEC` into `Vec<Value>` and treat every `Ok` as success. Redis returns nil when a watched key changes. In redis-rs 0.31.0 nil converts to an empty vector, not an error. Therefore a worker can return successful pending→borrowed allocation after Redis actually rejected it and then broadcast unrecorded transactions.

Evidence: pinned redis-rs [pipeline implementation](https://github.com/redis-rs/redis-rs/blob/redis-0.31.0/redis/src/pipeline.rs) and [value conversion](https://github.com/redis-rs/redis-rs/blob/redis-0.31.0/redis/src/types.rs). Test by changing the lock or watched state after validation but before `EXEC`; require retry or `LockLost`, and verify no success or broadcast without stored bytes.

### Q2 — Concurrent transactions share connection-scoped WATCH state · Critical

`atomic.rs:194,304` and queue completion methods clone `ConnectionManager`. Clones share a physical connection; `WATCH`, `UNWATCH`, and `EXEC` affect that connection, not the Rust clone. One task's `EXEC` or `UNWATCH` can clear another task's fence. Independent EOAs also share this manager. Merely checking `Option<Vec<Value>>` does not fix the interference.

Use an exclusive connection for the complete watch/read/execute lifetime, or a single Lua compare-and-mutate operation. An automatically reconnecting connection also must not resume a transaction after losing its watched state. Evidence: [ConnectionManager source](https://github.com/redis-rs/redis-rs/blob/redis-0.31.0/redis/src/aio/connection_manager.rs), [Redis transaction semantics](https://redis.io/docs/latest/develop/using-commands/transactions/). Test with barriers and an independent Redis connection that takes ownership while another shared task clears WATCH.

### Q3 — “Atomic” pending failures bypass the EOA fence · High

`atomic.rs:570–743` builds and executes failure pipelines directly. Both single and batch methods omit owner checks and pending membership checks. A paused worker can resume after takeover and label a now-borrowed/submitted transaction failed, schedule a false failure webhook, and expire request data needed by the current owner. `send.rs:364` calls the batch method after concurrent RPC/signing work, making this a realistic stale-result window. Fence the write and validate membership at commit; test single/batch stale owners and requests that already moved states.

### Q4 — Ambiguous RPC outcomes can duplicate business actions · Critical

`worker/error.rs:164–167` classifies every non-JSON-RPC send error as deterministic failure. A timeout after the node accepted the transaction is ambiguous. `store/borrowed.rs:188–206` then requeues the intent and recycles its nonce. The original transaction can still execute.

`worker/confirm.rs:317–333` similarly conflates absent receipts and RPC errors. `store/submitted.rs:422–510` requeues any unconfirmed intent below the observed chain nonce. If the intent actually mined but its receipt read failed, it is resent at another nonce and can execute twice. Preserve uncertainty, retry exact bytes, and use canonical receipt/replacement evidence. Tests need a node that accepts a send then drops its response, and an advanced nonce with a failing/lagging receipt endpoint.

### Q5 — Zero-nonce recycling skips nonce zero · High

`store/submitted.rs:594–652` uses `cached_count.saturating_sub(1)` as the highest submitted nonce when no submissions exist, then computes `highest + 1`. With cached count 0, no submissions, optimistic count 1, and recycled nonce 0, cleanup deletes recycled 0 and leaves optimistic 1. The next transaction uses nonce 1 while the chain still requires nonce 0. Represent the empty range explicitly; test counts 0 and 1, no submissions, and trailing failed allocations.

### Q6 — Pending allocation does not fully validate its input · Medium

`store/pending.rs:31,171` omits the pending set from watched keys despite validating membership. Incremented allocation sorts nonce values for validation but derives the next counter from the last *original* transaction (`:137`); input `[n+1,n]` passes validation and moves the counter backward. Recycled allocation accepts duplicate nonce or transaction IDs. Current worker ordering limits exposure, but the public store API does not enforce its documented invariant. Test permutation invariance, duplicate IDs/nonces, and concurrent pending removal.

### Q7 — Ingress and lookup retention have durability gaps · Medium

`store/mod.rs:846` calls a pipeline “atomic” without `.atomic()`. It also overwrites existing request data and resets pending status when the same transaction ID is added again. A retry can mutate the request behind a live reservation or retain a previous completion TTL. `transaction_registry.rs:50–76` permits unconditional mapping replacement and has no retention mechanism; completed EOA data expires independently. Review transaction ID ownership/idempotency at router ingress and expire/remove registry entries with request retention.

### Q8 — Lease/cancellation/time boundaries need explicit contracts · Medium

- `twmq/src/lib.rs:31–40` rounds subsecond delays up to one second but floors longer fractional delays; scheduling uses floored wall-clock seconds. A requested 200 ms EOA continuation (`worker/mod.rs:254`) is not a 200 ms scheduler interval.
- `twmq/src/multilane.rs:651–674` checks cancellation before lease cleanup. An expired active cancellation can be reborrowed in that same batch (`:704,711`), delaying cancellation again. Unlike the single queue, `pop_batch_jobs(0)` skips lane housekeeping entirely.
- `twmq/src/multilane.rs:639` omits a random lease component. Active-mode same-ID reuse resets attempts; same-second reuse can recreate an old token. Single-lane tokens include a random pop ID.
- `twmq/src/hooks.rs:39–66` blindly appends a custom-ID job without checking the dedupe set, unlike normal push. Duplicate hook scheduling can create duplicate pending entries.
- Worker shutdown waits indefinitely for permits, but leased jobs have no execution deadline or cancellation mechanism (`twmq/src/lib.rs:525,565`). Expiry recovers queue ownership, not a hung future.

### Q9 — Confirmation is inclusion, with no reorg recovery · High

A receipt is immediately stored as terminal `confirmed` and removed from submitted state (`store/submitted.rs:373–387`). The worker explicitly logs that confirmed transactions are not retried after a chain nonce rollback (`worker/confirm.rs:197–205`). This cannot provide Ethereum/rollup finality guarantees. Receipt execution status is retained but not distinguished in the lifecycle status. Define included/safe/finalized and reverted behavior before claiming production delivery semantics.

## Existing coverage and next tests

Baseline `twmq/tests` contains 16 Redis integration tests for happy-path processing, retry counts, delays/order, idempotency modes, pruning races, leases, cross-queue hooks, and multilane batches. Tests assume Redis at port 6379; several rely on sleeps, process-global flags, or global tracing initialization. EOA has three classifier unit tests; no store/worker integration tests were found. Its tested `EoaErrorMapper` is separate from the worker's `classify_send_error` implementation.

Prioritize observable invariants over implementation snapshots:

| Layer | Required evidence |
| --- | --- |
| Pure unit/property | Nonce boundaries/permutations; retry classification; fee bump arithmetic; malformed submitted records; chain-count range conversion. |
| Real Redis integration | Contended owner fencing; aborted EXEC; cancellation at lease expiry; same-ID reuse; deduped hooks; pending→borrowed durability; pending failure after takeover. |
| Controlled RPC integration | Accept-then-timeout; missing/error receipts; mixed batch success; exact-byte recovery after crash; original-vs-replacement receipt; nonce rollback and finality. |
| Process/system | Kill between persist/send/record; Redis reconnect/failover; two workers on one EOA; multiple EOAs; endpoint stalls; bounded shutdown. |

Use barriers/channels to place failures at named transition boundaries, independent observer connections, unique namespaces, bounded waits, and teardown of only owned keys. Assert stored bytes, ownership, state sets, request identity, nonce continuity, and external call counts. Avoid sleeps as the synchronization mechanism or tests that merely restate getters.

## Benchmark plan and limits

Run the unchanged queue baseline first on an isolated Redis with recorded CPU, Rust/Redis versions, persistence settings, payload size, worker count, polling interval, concurrency, and revision. Separate producer acceptance, handler invocations, committed completions, and chain inclusion. Report p50/p95/p99 enqueue→commit latency, completed jobs/s, retries, errors, backlog slope, and Redis CPU/memory.

The existing benchmark counts successful *handler invocations* before durable acknowledgement, uses random retry decisions, serially awaits each producer push, and cleans the wrong key prefix (`twmq/benches/throughput.rs:217`). Its rates are not proven committed throughput and cannot substantiate blockchain TPS. Keep baseline output, then add deterministic finite workloads with final Redis reconciliation. Sweep 1/10/100 EOAs and batch/concurrency levels; compare no retry, controlled retry, lease loss, and delayed RPC. Run AOF/fsync and network-latency profiles separately. Optimize only after correctness gates pass.

## Follow-up status

The fork now fixes Q1/Q2 in both queue variants and the EOA store: exclusive nonreconnecting transaction connections, explicit nil/commit handling, and no replay of server/transport errors. Queue completion shares one helper and a reusable exclusive-connection pool. Workers also wake when completed jobs release permits, refilling backlog without waiting for the periodic poll. The timer remains responsible for discovering external work and housekeeping; an empty poll does not wake itself. Q3/Q5/Q6 are fixed with pending membership fencing, nonce-zero recovery, unique allocation inputs, and order-independent optimistic counters. Q7 admission is atomic and immutable while the request is retained: identical retries are no-ops across pending, borrowed, submitted, confirmed, and failed states; changed requests conflict. Both queue variants now use full-length random lease generations; cancellation reborrowing from Q8 is fixed. Receipt/send uncertainty (Q4) is addressed by the companion worker review; see its tests.

Evidence:

- Six of seven exact-transition queue tests fail on the pristine upstream runtime and pass on the fork. Both concurrent acknowledgement tests commit **32 outcomes instead of one** upstream. [Reproduction output](baselines/queue-upstream-regressions.log).
- Eleven of thirteen EOA regressions fail when the audited buggy branches are deliberately reintroduced, then all pass after restoring the fixes. This is a sensitivity check, not a pristine-upstream EOA build. [Mutation output](baselines/eoa-mutation-regressions.log).
- Sixteen EOA store regressions cover fencing, aborted execution, nonce allocation, failure transitions, and immutable admission. [Fixed regression output](baselines/eoa-fixed-regressions.log). Use `TEST_REDIS_URL=redis://127.0.0.1:16379/ cargo test -p engine-executors eoa::store::atomic::tests --lib -- --ignored` against a disposable Redis.
- Fourteen queue regressions pass, including immediate refill with an hour-long polling interval, bounded concurrency, no idle spin, and prompt shutdown. [Fixed regression output](baselines/queue-fixed-regressions.log). Run with `TEST_REDIS_URL=redis://127.0.0.1:16379/ cargo test -p twmq --lib -- --ignored`. Existing integration tests also require `TEST_REDIS_URL`; all sixteen passed again after the refill optimization and dependency updates.
- A local node process test verifies 24 transfers across worker SIGKILL/restart, then repeated submission of all 24 IDs produces no further balance/nonce change. [Local recovery result](baselines/local-eoa-recovery.json).

API changes: `QueueBuilder::redis_connection_manager(manager, source_client)` now requires the originating client (same Redis server, database, and credentials). `acquire_eoa_lock_aggressively(worker_id, metrics, client)` requires that client too. URL/client queue builders are unchanged. Idempotency lasts as long as retained request data; this does not claim permanent deduplication after retention expires.

Remaining issues include registry retention, hook-enqueue custom-ID deduplication, delay precision, bounded job cancellation/shutdown, full reorg/finality handling, and chain-specific delivery evidence. The local queue throughput comparison is recorded in [queue benchmark results](baselines/queue-results.md). No live-chain throughput or production finality claim has been established.
