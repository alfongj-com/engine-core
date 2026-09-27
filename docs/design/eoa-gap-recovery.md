# EOA recovery after rollback and mempool loss

## Context and decision

A rollback followed by loss of queued transactions exposed an availability gap:
Engine's existing fee-bump path repairs the lowest missing nonce after 60 seconds
without progress. Each newly mined nonce restarts that wait. A long dropped
suffix therefore drains one gap at a time.

Recover the **existing signed bytes** on a separate bounded schedule. Start only
after a lower observed nonce than the retained consumed-count high-water, or the
existing 60-second stall condition. Snapshot the highest currently submitted
nonce plus one as the window's end. Keep that window active across observed nonce
progress; later admissions do not extend it. Preserve cached and optimistic
allocator floors. The original fee-bump path remains available for underpriced
wires; replay itself never signs or changes fees.

## Protocol

1. Reserve the next round start five seconds ahead in Redis under the current
   EOA owner. A crash/error consumes the reservation.
2. Verify prior finality checkpoint continuity. Consider at most 32 consecutive
   nonce slots, starting at the currently observed `latest` nonce.
3. Resolve one submitted member per nonce. Missing membership or an unsupported
   NOOP gap stops the round; do not skip it and flood higher nonces.
4. Read one newest existing EOA attempt from the independent journal. Initial
   acknowledged wires are not retained in Redis's attempt list. Reject terminal,
   quarantined, wrong-kind/replay-key and halted-chain records.
5. Decode the journal's EIP-2718 bytes; require complete consumption, identical
   re-encoding and matching nonce/hash. Verify admitted sender/chain/nonce and
   durable attempt membership. Recheck ownership and existing `before_eoa`
   authorization immediately before sending those bytes.
6. On any RPC error or wrong returned hash, keep all evidence and stop this round.
   Already-known is still an unknown result. No replay response is a receipt,
   terminal outcome, fresh attempt identity or useful-progress scheduling signal.

Only actual observed nonce progress refreshes the stall clock. Reaching the
window's fixed end clears its scheduling hint. An already authorized in-flight
call can finish after lease loss; it carries only the original recorded wire.

## Bounds and failure behavior

Inactive operation adds replay-state/health reads, without replay RPC calls.
Health initialization can use the existing balance RPC. Each active round adds
checkpoint continuity and at most 32 sequential sends, with journal/owner checks
per attempted send. The five-second value bounds round-start frequency, not
elapsed execution time: slow successful requests can hold the signer for 32
request latencies plus storage work. No lifetime retry limit or global provider
quota is implied. Existing borrowed recovery has separate bounds.

The journal getter returns one row; database work may still scan/sort that ID's
replacement history. Missing/corrupt authority, missing projection, NOOP gaps and
permanent rejection can remain unresolved. The feature does not weaken FULL
journal durability, terminal finality, admission identity, or lease fencing.

## Verification and rollout

The targeted regression passed: one parent test runs 11 isolated scenarios in
8.78 seconds. The broader executor Redis suite passed 61 tests, executor unit
suite 37, core journal suite 20 and core unit suite 20; these selections overlap
and are not an aggregate coverage score. The frozen-source
[formal rerun](https://github.com/alfongj-com/engine-core/blob/load-tests/formal/evidence/capacity-gap-review/README.md) passed all 61
expected outcomes with 69 reviewed source hashes. Process-level qualification
is separate and was pending when this record was written. The new
[Redis/HTTP/SQLite regression](../../executors/src/eoa/worker/gap_replay_tests.rs)
seeds acknowledged transactions through the production send flow. It checks
ascending byte-identical replay, round bound/cooldown, continuation after nonce
progress, actual confirmation-flow high-water retention, stall activation,
missing and substituted IDs, unknown responses, terminal/chain-halted authority,
and ownership loss before/during replay. Wire order is derived independently
from signed nonces, not concurrent HTTP arrival order.

Run the targeted regression and existing EOA/journal suites, then the actual
rollback-plus-mempool-loss scenario before claiming faster recovery. Preserve
previous benchmark binaries and bind later measurements to their source/binary
hashes. [Formal correspondence](../../formal/eoa.md) treats repeated original
transmission as the same durable identity; it does not prove this cooldown,
mempool-eviction recovery, or a wall-clock liveness bound. The separate models
are not mechanically composed.
