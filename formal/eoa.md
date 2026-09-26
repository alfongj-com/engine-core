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
| `Broadcast`, `Crash`, `Restart` | [`send.rs`](../executors/src/eoa/worker/send.rs), persist-before-send; [`error.rs`](../executors/src/eoa/worker/error.rs), transport/unknown errors become `PossiblySent`; actual crash harness [`local_eoa_recovery.py`](../scripts/local_eoa_recovery.py) |
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

Not modeled: deterministic rejection/nonce recycling, imported conflicting
attempts, external writers using the same key, manual resets, nonce exhaustion,
pending/preconfirmation ahead of canonical state, contract execution, gas
estimation, multiple chains/accounts, webhooks, Redis command-level partial
failure, storage loss or malicious receipts. The fee-version abstraction does
not establish economic replacement rules. A matching receipt is assumed truthful
and remains included unless the explicit reorg boundary is enabled. Runtime
`Confirm` now consumes evidence from the separate finality layer. The independent
[recovery authority](disaster-recovery.md) additionally fences replay keys after
Redis loss; this model's retained-Redis assumption is still explicit.
