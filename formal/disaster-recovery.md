# Redis disaster recovery: durable authority and replay identity

## Claim

[DisasterRecovery.tla](tla/DisasterRecovery.tla) checks a finite abstraction of
the [recovery journal](../core/src/recovery.rs), separating the authoritative
local ledger from the Redis queue projection. A ledger transaction and its
Redis checkpoint update are separate actions: a crash can occur between them.
This is neither a Rust refinement proof nor a proof of SQLite, filesystem,
Redis, cryptographic signatures, or blockchain consensus.

The [operational design](../docs/design/redis-disaster-recovery.md) specifies the
supported single-host topology and offline commands. The [finality model](finality.md)
separately checks the evidence needed to call an execution terminal.

## Independent state

The ledger stores immutable admitted payloads, replay-key bindings, exact attempt
identities, terminal state and an epoch/checkpoint. Redis stores an independently
mutable checkpoint marker, process identity and queue projection. Network
messages and chain effects survive Engine crashes and Redis loss. Observer state
cannot erase these effects.

An attempt contains an intent ID, replay key and payload atom. Payload atoms
represent the complete stored request, including a generated AA nonce or 7702
UID. EOA user and NOOP intents share the same ID/replay-key domains: their actual
string prefixes are not modeled. A chain can execute a replay key once; changing
the key for the same intent permits a second business effect. `AtMostOneEffectPerId`
checks the resulting effect set, rather than guarding chain execution by the
desired invariant.

Ghost history records accepted payloads and the evidence that must remain in
the authority. Send actions record whether their exact attempt was already
durable; this fact is checked afterward, not used to prevent the fault action.

## Source correspondence

| Model action | Runtime boundary |
| --- | --- |
| `Start`, `Crash` | Exclusive process-lifetime file lock; losing the process discards local authorization but preserves committed ledger rows. |
| `Admit` | `reserve_admission`: SQLite commit binds kind/ID/fingerprint to the original stored payload and advances the checkpoint. |
| `AttemptCommit` | `before_broadcast`: immutable ID/replay-key binding, globally unique replay key, durable exact attempt, then checkpoint advance. |
| `Mirror` | Redis compare-and-set from the old checkpoint, followed by continuity checks; only success releases broadcast authorization. |
| `Send`, `Execute` | The already-authorized network call may race a halt; the chain may execute an already-sent message after a process crash. |
| `Enqueue` | A matching unsent retry uses the original payload returned by the authority, not a freshly generated candidate UID. Terminal and quarantined admissions cannot enter this action. |
| `TerminalCommit` | `record_terminal`: persist a reconciled terminal outcome before updating Redis. The attempt must belong to the same admitted ID. Finality and contradictory-proof validation are delegated to runtime tests and the separate finality model. |
| `RedisLoss`, `RedisRollback`, `RedisRestart` | Missing/old marker or a changed Redis process. `DetectMismatch`/`Mirror` durably latch a halt; `RepairMarker` does not clear it. |
| `Reattach` | Offline, same-namespace, exact-checkpoint reattachment; only an absent halt or the Redis-process-change cause can be cleared. |
| `RecoverBegin`, `RecoverPublish`, `RecoverFinish` | Offline fresh-namespace recovery: commit a new epoch and quarantine every attempted nonterminal admission; separately publish/verify Redis; clear the recovery halt only after success. |
| `RecoveryCrash` | An interrupted recovery retains its halt and cannot authorize writes. |

The model abstracts the new namespace to an empty projection and a new epoch;
the runtime additionally validates the namespace and requires it to be empty.
It does not reconstruct or automatically resend attempted admissions. Unsent
admissions can be retried with the original payload. Terminal records and all
replay bindings survive recovery.

## Finite checks

Every configuration uses two intent IDs, two replay keys, two payload atoms and
two possible process IDs. The normal protocol permits only one active owner.
There are at most four journal writes, one recovery epoch and one Redis process
restart. These are explicit protocol bounds, not state-space filters. They allow
two admissions with conflicting replay keys, repeated attempts, terminal commit,
and a second identity after unsafe recovery. No population generalization or
liveness result is claimed. `CHECK_DEADLOCK FALSE` permits exhausted/parked states.

| Configuration | Expected result |
| --- | --- |
| `DisasterRecovery.cfg` | Pass: immutable admission, globally unique replay keys, persisted sends, retained evidence, current-epoch authorization, terminal exclusion, original retry payload, at most one effect per intent, and recovery quarantine. |
| `DisasterRecovery_send_before_journal.cfg` | `EverySendJournaled`: releasing a send without durable attempt evidence is detected immediately. |
| `DisasterRecovery_id_binding.cfg` | `ImmutableAdmission`: allowing a different payload under an existing ID breaks the admitted intent. |
| `DisasterRecovery_key_binding.cfg` | `ReplayKeyUnique`: different intents cannot reserve the same replay key, including the user/NOOP case. |
| `DisasterRecovery_shared_owner.cfg` | `NoOldEpochAuthorization`: removing exclusive ownership permits recovery alongside a live old-epoch process, which can obtain new authorization. Previously authorized inflight calls are deliberately still allowed. |
| `DisasterRecovery_fresh_recovery.cfg` | `AtMostOneEffectPerId`: bypassing quarantine and dropping bindings lets a formerly attempted intent execute again under another key. |
| `DisasterRecovery_retry_uid.cfg` | `OriginalPayloadOnRetry`: returning the new candidate rather than the stored payload changes a retry's generated identity. |
| `DisasterRecovery_authority_rollback.cfg` | `AtMostOneEffectPerId`: a coherent old copy of the authoritative ledger and Redis can forget an execution and admit another identity. This is an explicit unsupported disaster boundary. |
| `DisasterRecovery_terminal_attribution.cfg` | `TerminalHasOwnEffect`: attributing another ID's actual execution/attempt to this admission cannot make it terminal. |
| `DisasterRecovery_late_execution_witness.cfg` | `LateExecutionWitnessNotReached`: an expected counterexample demonstrates actual chain execution after a durable halt. It uses an already-journaled identity and does not violate positive safety checks. |

On 2026-09-26 the complete pinned-TLC run passed all 52 repository configurations
against the refreshed frozen source map. This family's positive case exhausted
**3,664,561 generated states, 698,992 distinct states**, with zero pending states,
in 21.6 seconds on the local machine. All eight fault/boundary/witness cases
reached exactly their named invariant violation. The runner's final source check
passed all 58 mapped files. These are finite model results, not a performance or
whole-service correctness claim.

## Assumptions and gaps

- Successful SQLite commits are atomic and survive the modeled process crash.
  Redis checkpoint CAS is atomic. Real power-loss durability depends on the
  filesystem, storage device and SQLite configuration; the model is not evidence
  that a particular deployment honors `fsync`.
- The authority and its lock live on one durable local filesystem with one
  supported owner. Missing, corrupt, copied, rolled-back or cloned authoritative
  ledgers are not repaired from Redis. A cloned host/ledger can invalidate lock
  exclusivity; the negative boundary does not claim otherwise.
- The model splits journal commit from Redis publication, but collapses a
  successful CAS and its subsequent health check into authorization. A later
  Redis fault may still race an authorized send; that exact identity was already
  durable. It does not claim a halt can retract bytes already sent to a network.
- Redis marker continuity is a generation check, not a checksum of every queue
  key. Arbitrary individual-key corruption with an unchanged marker, credential
  compromise and malicious operators are outside this model.
- Payload equality is an atom comparison. Canonical JSON, request fingerprints,
  key derivation, signer correctness and call-site coverage require source review
  and executable tests. The chain-side once-per-replay-key assumption is explicit;
  admission alone does not manufacture an idempotent remote protocol.
- Cross-cycle chain checkpoint CAS, chain-specific halt causes, terminal proof
  conflict normalization, storage errors, and manual quarantine are covered by
  runtime code/tests, not this module's state space. `TerminalCommit` assumes
  conclusive evidence from the finality layer. A halted or quarantined record can
  remain unresolved indefinitely; no recovery availability guarantee is asserted.

## Frozen-source review

On 2026-09-26, after the coordinated runtime freeze and formatting pass,
[`source-map.json`](source-map.json) was refreshed for `core/src/recovery.rs`
and its tests, both recovery CLI entry points, server admission/startup/HTTP
gates, executor pre-broadcast and terminal adapters, and the process harnesses.
The new `local_redis_disaster.py` covers flushed and rolled-back Redis state;
the existing EOA/Solana crash scripts now explicitly initialize the authority,
and the EOA intact-Redis-restart path requires exact-checkpoint reattachment.

Implementation regressions cover original generated-UID reuse, shared user/NOOP
replay ownership, failed storage writes, interrupted commit/mirror, SIGKILL
before and after ambiguous network delivery, terminal contradictions and the
atomic chain-halt/terminal-commit race found during review. These tests supplement
the abstraction: it does not derive signed-wire validation from payload atoms or
prove that every runtime call site refines a transition. Existing EOA, Solana,
admission, finality and queue models remain separate. Their composition is not
mechanically proved; source hashes are review tripwires, not a refinement relation.

## Throughput/security revision correspondence

The September 26 review additionally maps exact attempt/checkpoint no-ops to
stuttering: the unchanged identity still passes owner, health, CAS and chain-halt
checks. Admission overload likewise makes no durable state change.
`TerminalHasOwnEffect` now checks that a terminal admission has its own actual
executed attempt; the fault configuration deliberately permits attribution of
another ID's real execution. The positive model's existing effect/attempt state
is independent of that observation. The prior recorded state counts above are
for the earlier model version. The [latest full run](evidence/throughput-review/report.json)
passes all ten recovery configurations; terminal misattribution reaches its named
counterexample after 11,865 distinct states.

Runtime tests cover EOA hash/wallet/nonce membership, Solana chain/signature
membership, and ERC-4337 operation hashes recomputed from the signed request and
custom EntryPoint. Bundled7702 is now disabled pending independent UID evidence.
The model treats bytes/hashes as atoms; it cannot establish the correctness of
these parsing and cryptographic checks. Solana now commits its independent
signature reservation before the Redis attempt projection, and repeats the exact
authorization before actual send. A missing projection after that cut parks
rather than permitting another signature. The Redis-only Solana model does not
represent this new SQL-to-Redis cut; the real worker regression
`sql_attempt_precedes_redis_and_unbound_substitution_never_broadcasts` and this
model's retained-authority assumption cover separate parts, not a composition proof.

Matching terminal retries may bypass a full Redis intake queue without creating
work. This is also a stuttering operation.

EOA `Uncertain` retains its borrowed projection; NOOP submitted reservation now
precedes dispatch. Both preserve the durable replay binding. The model does not
promise automated NOOP reconciliation. Solana signature reservation precedes
Redis insertion; the SQL-before-Redis failure cut parks rather than signs a fresh
attempt. Bundled EIP-7702 is disabled and legacy jobs park; it is not qualified by
this model. Streaming snapshot export, API authentication, provider genesis checks
and semaphore bounds are implementation checks outside the state-machine proof.
