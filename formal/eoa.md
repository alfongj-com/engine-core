# EVM nonce and receipt model

## Contract

For one `(chain, sender)`, reserve a nonce and persist the signed attempt before
sending it. A lost reply retains that identity. A fee replacement keeps the same
intent and nonce. A missing receipt, including a read after the account nonce
advanced, never authorizes repeating the intent at a new nonce. A mined revert
consumes its nonce and is terminal failure **after the configured finality gate**.
This older nonce/receipt model collapses receipt observation and settlement;
the separate [Finality model](finality.md) adds provisional inclusion, orphaning,
and checkpoint reads. Their composition is not mechanically proved.

`EoaRecovery` checks two intents, two independent preparation contexts, two
nonces and two fee versions. The chain ledger, possibly accepted broadcasts,
Redis attempt records and RPC observations are distinct state variables. The
chain can execute a broadcast after a process crash; reads can lag or return
absence after an earlier positive read. Both success and revert are explored.

## Correspondence to code

| Model action | Implementation / real boundary regression |
|---|---|
| `Prepare`, `Reserve`, `Discard` | [`pending.rs`](../executors/src/eoa/store/pending.rs), `MovePendingToBorrowedWithIncrementedNonces`; store tests `duplicate_intent_cannot_reserve_two_incremented_nonces`, `removed_pending_request_cannot_be_borrowed_from_a_stale_read` |
| `Broadcast`, `Crash`, `Restart` | [`send.rs`](../executors/src/eoa/worker/send.rs), persist-before-send; [`error.rs`](../executors/src/eoa/worker/error.rs), all post-dispatch errors become `Uncertain` and preserve the borrowed signed attempt; actual crash harness [`local_eoa_recovery.py`](../scripts/local_eoa_recovery.py) |
| `Bump` | [`confirm.rs`](../executors/src/eoa/worker/confirm.rs), `attempt_gas_bump_for_stalled_nonce` → [`transaction.rs`](../executors/src/eoa/worker/transaction.rs), `apply_gas_bump_to_typed_transaction`; numeric caps are separately checked by [Kani](fees.md) |
| `ReadReceipt`, `MissingReceipt`, `ObserveNonce` | [`confirm.rs`](../executors/src/eoa/worker/confirm.rs), `fetch_confirmed_transaction_receipts`; [`receipt_tests.rs`](../executors/src/eoa/worker/receipt_tests.rs) exercises null, RPC error and wrong hash through HTTP |
| `Confirm` | [`submitted.rs`](../executors/src/eoa/store/submitted.rs), `CleanSubmittedTransactions`; store regressions `reverted_receipt_fails_once_without_retrying_or_recycling_nonce`, `successful_receipt_still_confirms_once_and_retains_history` |

Atomic reservation abstracts a successful WATCH/EXEC commit with validated key
types and retained Redis writes. Queue ownership and WATCH connection isolation
are explored separately in [QueueLease](queue.md). This is a manually reviewed
mapping, **not an automatically proved refinement of Rust or Redis**.

## Counterexamples required by the runner

- `stale_reservation`: skipping commit-time validation lets two intents own the
  same nonce (`OneIntentPerNonce`).
- `missing_receipt`: retrying after nonce progress with no receipt lets the same
  intent execute successfully at two nonces (`AtMostOneEffect`).
- `reverted_success`: treating status-zero as success violates
  `TerminalMatchesExecution`.
- `reorg_boundary`: removing the included block invalidates the terminal result
  in this inclusion-only abstraction. The new Finality model checks the actual
  pre-finality gate separately and retains a catastrophic finalized-rollback
  boundary. This older counterexample no longer describes immediate completion
  on any receipt in the current runtime.
- `unfair_boundary`: a scheduler/RPC that never makes progress defeats eventual
  completion. Safety does not imply availability.

## Progress and limits

The optional `LiveSpec` adds strong fairness for useful progress on each intent
and weak fairness for restart. `Progress` includes a prepare/commit macro step
and a successful receipt-read/terminal-commit macro step. It assumes repeated
opportunities eventually complete those sequences. The check proves eventual
terminal inclusion under this stronger environment, **not production liveness
under arbitrary outages**. No symmetry reduction or state constraint cuts off
the search. Finite domains bound the model, not a claimed production capacity.

Not modeled: pre-dispatch rejection/nonce recycling, imported conflicting
attempts, external writers using the same key, manual resets, nonce exhaustion,
pending/preconfirmation ahead of canonical state, contract execution, gas
estimation, multiple chains/accounts, webhooks, Redis command-level partial
failure, storage loss or malicious receipts. The fee-version abstraction does
not establish economic replacement rules. A matching receipt is assumed truthful
and remains included unless the explicit reorg boundary is enabled. Runtime
`Confirm` now consumes evidence from the separate finality layer. The independent
[recovery authority](disaster-recovery.md) additionally fences replay keys after
Redis loss; this model's retained-Redis assumption is still explicit.

## Throughput review correspondence (September 26)

The separate [NonceAllocator model](nonce-allocator.md) adds the consumed-count
floor missing from this model's allocator abstraction. Bounded rank pages,
cycle-local block reuse and receipt concurrency change scheduling, not the
`Confirm` premise: exact receipt identity, durable sender/nonce/attempt membership
and finality evidence are still required. The 20k page/churn and 10k isolated-cleanup
Redis tests and 32-slot real HTTP test check those implementation boundaries.
The current measured candidate consumes at most 256 new reservations per allocation cycle, with ordered
32-task preparation/send concurrency; up to ten preparation refill passes can
visit 2,560 rejected pending jobs. Borrowed and recycled recovery have separate
set-sized work. The inflight window is separately configured. These bounds do not
prove polling fairness, elapsed-time latency, or sustained terminal throughput.

## Post-dispatch uncertainty review

`Broadcast` records possible network delivery independently from observer state.
For initial/recovered ordinary EOA sends, every RPC error after dispatch now
preserves the original borrowed ID, nonce and signed wire. Error text (including nonce-high, insufficient funds, invalid
signature or oversized) cannot prove that a previous dispatch was never accepted.
An exact included receipt moves borrowed state into the submitted projection
without a send-success webhook; it still needs the independent finality gate.
Absent/error/wrong-hash receipt reads retry only the original bytes. A matching
send acknowledgment may emit the send-attempt webhook, not a terminal event.
Fee-bump errors retain their submitted attempt and original intent/nonce.

The real Redis + HTTP + SQLite regression
[`rpc_rejection_keeps_original_nonce_wire_and_unknown_webhook_state`](../executors/src/eoa/worker/send_tests.rs)
uses the normal preparation/reservation/send path and later borrowed recovery. It
checks eight rejection texts, two independently bound nonces, unchanged wire,
no uncertain webhook, included-receipt reconciliation, and rejected NOOP retention.
The model collapses repeated delivery of the same identity and does not parse
RPC messages or model webhooks. That collapse preserves the safety premise;
the implementation regression, rather than a new TLC theorem, verifies this mapping.

NOOP now records its submitted hash and removes the recycled nonce before RPC,
after the independent journal commits its wire. No reply/error can make that
nonce available to another intent. A rejected or indefinitely absent NOOP has
no automatic borrowed-wire retry slot: it requires explicit offline journal
reconciliation. The model's conditional liveness must not be read as proving
NOOP availability. Normal uncertain borrowed retries occur once per worker cycle,
with 32 RPC tasks in flight. Unknown-only/no-progress cycles retain the 200ms
requeue delay, which TWMQ rounds to one second. A successful nondelegated cycle
with acknowledged send or reconciled recovery progress and unsigned backlog
instead rejoins the queue tail immediately. Mixed cycles can therefore retry
unknown attempts sooner while other intents make progress. A recovery cycle still
visits every borrowed attempt (potentially the configured 4,096 inflight window); high RTT or an outage can therefore delay
receipt/send work and consume substantial RPC budget. The 256 new-reservation
cap does not bound this recovery work. Retry lifetime, provider quotas and
elapsed-time guarantees are not modeled.

## Progress-driven scheduling follow-up

[`into_job_result`](../executors/src/eoa/worker/mod.rs) removes the rounded delay
only for a successful cycle with a positive nondelegation read, remaining unsigned
work, and `sent_transactions > 0` or `recovered_transactions > 0`. Unknown send
results do not increment those counters. Delegated accounts retain two seconds;
unknown delegation, no progress, unknown-only and finality-only work retain the
rounded one-second delay. Workflow errors keep their existing error-specific
handling. A mixed progress/unknown cycle can run sooner; this is not an outage
rate limiter.

The real Redis [scheduling regression](../executors/src/eoa/worker/scheduling_tests.rs)
uses the production decision with actual TWMQ lease completion. It checks tail
placement behind another job, immediate availability despite a one-hour polling
timer, exact delayed scores for non-progress paths and release of the old lease.
This changes how soon existing transitions are scheduled, not their identity,
finality or durable authorization premises. The TLA+ model already permits those
steps without wall-clock delays; no new invariant or throughput theorem follows.
Its conditional fairness assumptions and the unmodeled RPC budget remain explicit.

## Exact-wire gap recovery correspondence

The [gap recovery helper](../executors/src/eoa/worker/gap_replay.rs) and its
[implementation regression](../executors/src/eoa/worker/gap_replay_tests.rs)
address a dropped submitted suffix after nonce rollback or a long stall. The
[journal getter](../core/src/recovery.rs) only reads an already authorized EOA
attempt. The caller decodes/re-encodes its wire, checks sender/chain/nonce/hash
membership and executes the existing broadcast fence again. It never creates a
new signature or changes a replay key. Terminal/quarantined/halted records and
lost queue ownership prevent subsequent dispatches.

A repeated broadcast of the same durable identity collapses to stuttering in
this model: `sent` records possible future delivery, not a count of HTTP sends
or an evictable node mempool. The model therefore **does not prove recovery after
mempool eviction**, the five-second schedule, the 32-call bound, a recovery-time
limit, or fairness between send/confirmation phases. The runtime's fixed window
and cooldown are implementation-test obligations. The existing conditional
liveness check must not be presented as qualification of this new path.

Retaining the cached consumed-count high-water supports the separate allocator
floor. NonceAllocator assumes monotonic chain consumption and does not model a
lower RPC count after reorg; the actual confirmation-flow regression checks that
integration. The independent journal continues to bind IDs/nonces even when
Redis observations lag. No composition or source-level refinement is implied.

The 256 scheduling setting remains a measured candidate. The 100/s local sample
did not demonstrate sustainable capacity or isolate an improvement over 128;
model safety results imply neither throughput nor latency improvement.
