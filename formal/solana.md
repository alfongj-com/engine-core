# Solana recovery model

**Status:** finite-state safety checks of a manually written abstraction, not a proof of the Rust binary. Initial review used runtime `224b638`; the source map and correspondence below were refreshed after the finality/journal runtime freeze on September 26, 2026. Earlier evidence reports retain their original source scope. No chain transactions or paid RPC calls are needed for the model.

## Contract

For one admitted intent, persist its signed transaction before dispatch, retain it while the outcome is unknown, and retransmit only that identity. A missing history response—even after blockhash expiry—is insufficient evidence to sign again. A terminal queue commit needs the current queue lease and finalized execution evidence. The runtime now enforces that floor even for an existing request that selected confirmed.

[`SolanaRecovery.tla`](tla/SolanaRecovery.tla) checks that contract for the **finalized** commitment. Each identity represents the entire immutable signed wire message, signature, and associated blockhash. Two identities deliberately permit the model to discover two executions of the same business intent.

## State and implementation mapping

The ledger and Engine's knowledge are separate. `Execute` and `Finalize` change `chain` without consulting worker observations. `Observe` can return absent history or an error even after execution/finality, or a visible status below the requested commitment. This includes stale history after a previously visible transaction. `effects` records successfully executed identities, not HTTP acknowledgments or queue completions.

| Model actions | Current implementation |
| --- | --- |
| `Claim`, `ExpireQueueLease`, `CommitTerminal` | [TWMQ queue claims/completion](../twmq/src/lib.rs), [isolated WATCH/EXEC lease check](../twmq/src/transaction.rs) |
| `Lock`, `LoadAttempt`, `Persist`, `ExpireStorageLease` | [Storage token checks, SET NX, persistent attempts](../executors/src/solana_executor/storage.rs) |
| `PrepareSend`, `Send` | [`broadcast_attempt`](../executors/src/solana_executor/worker.rs): durable broadcast counter, signature/blockhash/wire verification, `maxRetries=0` |
| `Observe`, `Decide`, `Validity` | `execute_transaction` and `reconcile_status`: history, commitment, receipt matching, expiry, second history read |
| `ReleaseForCommit`, `CommitTerminal` | Worker `process`, `on_success`, `on_fail`: storage release before fenced queue completion; cleanup and terminal admission update in that commit |
| `Park`, `Cancel`, `Resume` | Retain unknown attempts and active admission protection; explicit lock-protected `resume_reconciliation` resets read allowance without changing wire identity or lifetime send allowance |

Queue and storage leases are distinct. A worker can lose its queue lease while still holding storage ownership; its terminal commit must fail. The model also permits lease loss **after** the last send check and **before** HTTP dispatch. Redis fencing cannot undo an external request already in flight; identical signed bytes provide the relevant chain-side protection.

Successful send acknowledgments and lost/error responses are collapsed into the same unknown execution state because all those paths require later reconciliation. The network may never execute a dispatched transaction, or may execute it later, including after Engine crashes. `Crash` removes volatile worker state while preserving Redis and chain state.

## Invariants

| Invariant | Property checked |
| --- | --- |
| `EverySendDurable` | Each dispatch had persisted that exact identity previously. Historical evidence is recorded at dispatch; later cleanup cannot hide an unsafe send. |
| `EverySendReserved` | Dispatch count never exceeds reservations recorded before I/O. |
| `LifetimeSendLimit` | Total reservations and dispatches stay within the intent's lifetime broadcast allowance, including crashes and explicit resume. |
| `ImmutableSignedIdentity` | One intent never dispatches multiple signed identities. |
| `AtMostOneEffect` | At most one identity executes the business effect, regardless of observer knowledge. |
| `TerminalCommitOwned` | Every terminal commit held the current queue token at its linearization point. |
| `UnresolvedEvidenceRetained` | Once dispatched, an active/unknown admission retains its signed attempt, including cancellation and parking. |
| `TerminalHasChainProof` | Terminal success/failure agrees with the separately modeled finalized ledger outcome. |
| `TerminalCleanupAtomic` | Redis terminal admission, queue result, and attempt removal move together. The separate authoritative terminal proof is committed before this Redis transition, and can survive an interrupted cleanup. |

`TypeOK` additionally checks bounded counters and state domains. These are safety properties; there is **no eventual-completion claim**. Permanent RPC failure, absent history, exhausted allowance, or cancellation can prevent progress indefinitely.

## Configurations and expected outcomes

The main configuration uses two workers, two possible signed identities, three lease generations, broadcast/history allowances of two, and four possible dispatches. The budget configuration uses four lease generations and one broadcast/check allowance so parking, explicit resume, and retained send limits fit within the finite search. A separate configuration checks the **actual 20-broadcast limit**, with one stable worker/identity, 22 lease generations, 21 history checks, and room for 21 dispatches. Its reachability witness must reach the twentieth dispatch; the safety run must still forbid a twenty-first reservation. That targeted limit check disables crashes, lease expiry, cancellation and operator resume (`ExploreInterruptions=FALSE`); the smaller models exhaustively explore those interruptions. The production read limit of 500 remains abstracted. These finite checks do not prove arbitrary worker counts or unbounded executions.

| Configuration | Expected result |
| --- | --- |
| [`SolanaRecovery.cfg`](tla/SolanaRecovery.cfg) | All invariants pass |
| [`SolanaRecoveryBudget.cfg`](tla/SolanaRecoveryBudget.cfg) | All invariants pass with smaller allowances and more lease generations |
| [`SolanaRecoveryLimit20.cfg`](tla/SolanaRecoveryLimit20.cfg) | All invariants pass at the production broadcast limit |
| [`SolanaRecoveryLimit20Witness.cfg`](tla/SolanaRecoveryLimit20Witness.cfg) | `BroadcastLimitWitnessNotReached` fails, proving the twentieth dispatch is reachable; this is a coverage witness, not a safety defect |
| [`SolanaRecoveryRefreshFault.cfg`](tla/SolanaRecoveryRefreshFault.cfg) | `AtMostOneEffect` fails: old identity executes, expires, history is absent, refreshed identity executes again |
| [`SolanaRecoveryPersistFault.cfg`](tla/SolanaRecoveryPersistFault.cfg) | `EverySendDurable` fails at the first unpersisted dispatch |
| [`SolanaRecoveryLeaseFault.cfg`](tla/SolanaRecoveryLeaseFault.cfg) | `TerminalCommitOwned` fails after lease expiry and stale completion |
| [`SolanaRecoveryProviderBoundary.cfg`](tla/SolanaRecoveryProviderBoundary.cfg) | `TerminalHasChainProof` fails when a provider invents terminal evidence |
| [`SolanaRecoveryRedisBoundary.cfg`](tla/SolanaRecoveryRedisBoundary.cfg) | `UnresolvedEvidenceRetained` fails when Redis loses a submitted attempt |
| [`SolanaRecoveryReorgBoundary.cfg`](tla/SolanaRecoveryReorgBoundary.cfg) | `TerminalHasChainProof` fails if assumed-final state can be reversed |

Local TLC runs completed the main state graph (686,724 distinct states, depth 37), the budget graph (1,000,540 states, depth 39), and the production send-limit graph (102,029 states, depth 197). These counts describe the explicit configurations, not lines of Rust verified. An exploratory 20-send search with all interruptions was stopped before completion and is not counted as a pass.

The three fault configurations intentionally select the named invariant: the refresh mutant is allowed to progress beyond identity change so TLC demonstrates a **second actual effect**, rather than stopping at the earlier signature change. Expected-negative boundary configurations document assumptions, not newly discovered implementation failures. A parser error, timeout, arbitrary invariant failure, or incomplete state search is not an expected pass.

Run from `formal/tla` using the repository's pinned TLC jar and Java runtime:

```sh
java -XX:+UseParallelGC -Xmx2g -cp "$TLA_JAR" tlc2.TLC \
  -workers 1 -cleanup -metadir /tmp/solana-model-states \
  -config SolanaRecovery.cfg SolanaRecovery.tla
```

Substitute each configuration above; negative runs must identify its exact expected invariant. `CHECK_DEADLOCK FALSE` is deliberate: finite resource exhaustion and terminal/parked states are allowed, and the specification includes stuttering. It does not disable invariant checking.

## Assumptions and exclusions

- One admitted intent within its retention window. Admission fingerprint correctness, multiple intents/wallets, tombstone expiry and resubmission after retention are not modeled. Redis tombstones alone are not permanent idempotency guarantees; the current server's independent journal retains identity beyond those TTLs.
- Redis linearizes the fenced storage operations and atomic terminal transaction, and acknowledged recovery data survives. Partial Redis command errors, failover rollback, disk durability and replication are outside the positive model. The data-loss configuration demonstrates why that matters.
- A canonical ledger executes a signed identity at most once and does not execute it for the first time after expiry. The model verifies Engine does not create a second identity; it does not prove validator consensus or cryptography. Transaction failures may charge fees but do not count as successful business effects here.
- Honest terminal evidence and stable finality. History absence and stale below-commitment observations are already allowed; fabricated success/failure and reversal of finalized history are separate negative boundaries. The current durable executor cannot opt down to processed/confirmed terminal completion.
- Blockhash validity is a Boolean abstraction of `height > lastValidBlockHeight`; numerical off-by-one behavior remains covered by Rust tests. Serialization, signatures, malformed RPC responses, matching receipt fields and legacy records are validated by implementation tests, not expanded byte-for-byte in this state space. Durable nonce transactions are outside this model.
- Timeouts, two-second send spacing, lease durations, transport limits and performance are abstracted. Lease generations are unique within the finite run. Operator resume includes acquiring/releasing an available storage lock as one atomic action; it does not claim an exposed recovery API exists.

## Tests that connect the model to Rust

The existing [Redis/RPC recovery tests](../executors/src/solana_executor/recovery_tests.rs) provide implementation evidence for the modeled transitions:

- `persisted_before_send_crash_recovers_identical_bytes_without_signing`, `lost_response_and_already_processed_reconcile_one_signature`, and `response_timeout_retains_the_exact_attempt_for_recovery`.
- `landing_between_first_status_and_finalized_expiry_never_rebuilds`, `expiry_boundary_rebroadcasts_then_parks_without_re_signing`, `previously_visible_then_stale_absence_never_re_signs_or_erases_evidence`, and `visible_unfinalized_status_and_history_errors_cannot_trigger_replacement`.
- `reconciliation_budget_parks_without_rpc_and_explicit_resume_preserves_identity`, `lost_storage_lock_cannot_replace_or_resume_attempt`, and `terminal_cleanup_waits_for_commit_and_cancellation_retains_unknown_attempt`.
- `confirmed_revert_then_reorg_cannot_terminate_or_resign_a_legacy_confirmed_job`, `finalized_error_requires_matching_receipt_and_nonstale_context`, and `terminal_journal_resumes_cleanup_and_missing_projection_never_signs` connect the stronger completion floor and authoritative journal to recovery. The [DisasterRecovery model](disaster-recovery.md) separately models that authority; composition with this Redis protocol is not mechanically proved.

[Admission tests](../server/src/solana_admission_tests.rs) cover cancellation/pruning, fingerprint fences and aborted terminal commits. [The local validator harness](../scripts/local_solana_recovery.py) and [public Devnet crash report](../docs/baselines/testnet-solana-crash.json) exercise actual signed-byte recovery. Those tests supplement the abstraction; they do not establish a machine-checked refinement from Rust to TLA+.

## Primary references

Accessed September 26, 2026:

- Solana [`sendTransaction`](https://solana.com/docs/rpc/http/sendtransaction): RPC acceptance does not establish execution; clients supply signed bytes.
- Solana [`getSignatureStatuses`](https://solana.com/docs/rpc/http/getsignaturestatuses): history lookup and per-signature confirmation status.
- Solana [confirmation and expiry](https://solana.com/developers/cookbook/transactions/confirmation), [`getLatestBlockhash`](https://solana.com/docs/rpc/http/getlatestblockhash): signed-message blockhash lifetime and returned validity height. Treating stale absence as insufficient replacement evidence is this implementation's conservative policy.
- TLA+ Toolbox [model checking and counterexample traces](https://tla.msr-inria.inria.fr/tlatoolbox/doc/model/executing-tlc.html).
