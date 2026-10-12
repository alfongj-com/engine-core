# Restoring a retained Solana attempt

## Contract

A missing Redis attempt can be restored only from the one exact signed attempt
retained in the independent journal. Restoration creates no signature, network
permission, or evidence that the transaction did not execute. It preserves the
recorded blockhash and validity height. Expired or absent history still cannot
justify signing again. Terminal journal entries may restore bytes for matching
finality checks and fenced Redis cleanup, never for another send.

The original Redis allowance remains 20 sends. Once a projection has been lost,
its old mutable counter cannot establish the remaining allowance. Schema3 adds
a **separate durable pool of at most 20 recovery grants per intent**, shared by
all subsequent restorations. If the original Redis counter is retained until recovery mode activates, the
combined bound is 20 original plus 20 recovered sends. This is a conditional
expansion of the previous contract. Only the recovered pool is independently
durable: an intact checkpoint marker does not prove every old Redis field survived.
It adds a FULL journal mutation only to recovered sends, not to each healthy
original send. A grant whose response is lost is spent.

Activating durable recovery mode permanently excludes ordinary grants for that
intent, even if its Redis recovery flag is lost. Recreating Redis, clearing a
local counter, retrying, cancellation, or a namespace recovery cannot replenish
the durable pool. Counter rollback before recovery mode remains a separate
boundary for the original Redis-only pool. Fresh-namespace recovery still quarantines attempted work.

## Model and implementation correspondence

[`SolanaRetainedAttempt.tla`](tla/SolanaRetainedAttempt.tla) starts with one
already-journaled signed identity and its Redis projection. Loss before any send
represents the journal-before-Redis crash cut. It separates durable mode/count,
Redis state, queue/storage ownership, grant commit/return, network dispatch and
independent chain execution.

| Model action | Runtime obligation |
| --- | --- |
| `ActivateRecovery` | `prepare_solana_restore` validates immutable admission and exact retained attempt before activating durable recovery mode. Terminal reads do not authorize sends. |
| `Restore` | `restore_retained_attempt` validates wire/signature/blockhash/signer and `restore_attempt_if_absent` uses storage-token ownership plus SET NX. A conflicting value is not overwritten. |
| `OriginalGrant` | Ordinary `before_broadcast` refuses a Solana intent already in durable recovery mode. Redis flags are not the authority. |
| `RecoveryGrant`, `GrantReturned`, `LoseGrantResponse` | `before_solana_recovery_broadcast` consumes one durable cumulative credit, publishes/acknowledges it, then returns. Lost return or cancellation does not refund it. |
| `ValidateSend`, `Dispatch` | Verify storage ownership again after a recovered grant. A later lease loss or halt cannot retract already-authorized bytes. |
| `ObserveAbsent`, `ObserveFinal`, `Terminal` | Preserve expiry/finality handling; queue ownership remains mandatory at terminal cleanup. |
| `Quarantine` | Offline recovery retains identity, recovery mode and credits; attempted work is not automatically made runnable. |

The model treats an exact wire as an atom. Cryptographic verification, canonical
serialization, matching job fields, malformed retained records, Redis Lua error
behavior and SQLite durability need implementation tests. Queue ownership is
required at terminal commit; storage ownership governs attempt restoration.
The model does not invent a stronger queue-lease check at each Redis write.

## Checked invariants

- With the original Redis counter retained until mode activation, original
  grants stay within their allowance; cumulative recovered
  grants stay within their separate allowance, including lost responses.
- Every dispatched wire equals the retained journal identity and has a fresh
  matching grant. Restoring an old row does not revive an old permission.
- Durable recovery mode excludes new ordinary grants regardless of Redis flags.
- Conflicting Redis values are not overwritten; restoration uses the current
  storage token, and terminal completion uses the current queue token.
- The authority retains the identity and count across Redis loss, worker crash,
  expiry, halt and quarantine. At most one signed identity has a chain effect.

The `_original_counter_boundary` case deliberately rolls back the original
Redis counter before durable mode activation and violates `OriginalAllowanceBound`.
It prevents presenting the conditional combined bound as a general lifetime cap.

The small positive configuration explores both pools with allowance two and
three lease generations, including interruptions and contradictory projections.
The production-limit configuration uses 20+20 with interruptions disabled and the
original counter retained so it can exhaust that conditional bound; it is not the fault-interleaving configuration. Its
witness must reach both full pools. Separate witnesses demonstrate pre-first-send
recovery, expired restoration, a lost grant followed by recovery, and repeated
restoration. Witnesses prove reachability, not eventual progress.

Named mutations reset the durable budget, ignore durable recovery mode, accept a
substituted wire, overwrite a conflicting projection, restore under a stale
owner, or turn a restored record into a send permission. Each must violate its
specified invariant. Syntax errors and timeouts are failures, not evidence.

## Limits and relation to the existing model

The older [Solana worker model](solana.md) deliberately retains its Redis-only
loss boundary. Its local retention invariant still fails when Redis loses that
row: the new journal makes safe restoration possible, not Redis immortal.
[The projection model](disaster-recovery.md) separately checks durable claim
publication and acknowledgement. These abstractions are not a machine-checked
composition or Rust refinement proof.

No model proves timing, RPC availability, scheduler fairness, or eventual
completion. Recovery may burn its entire allowance without a send; an expired
unknown outcome can remain parked forever. Restoration resets local history
poll allowance, so this model makes no lifetime bound on read requests across
repeated projection losses. Dishonest terminal evidence, invalid cryptographic
assumptions and loss or coherent rollback of the independent authority remain
outside the supported recovery contract.
