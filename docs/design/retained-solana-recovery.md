# Recovering retained Solana transactions

## Objective

Recover a missing Redis attempt when the independent journal still contains the
original signed transaction. Preserve the admitted request, signature, blockhash
and finalized-outcome checks. A Redis restart may also require the explicit
[checkpoint reattach procedure](redis-disaster-recovery.md#redis-restart-with-intact-data).

## Boundaries

The journal must be intact, owned by one Engine process, and pass its ordinary
Redis continuity checks. This does not reconstruct a lost journal, recreate a
missing queue job, or release quarantined requests. Conflicting Redis data is an
error, not permission to overwrite it. Expiry and absent RPC history never permit
a new signature.

Restoration requires one supported canonical attempt record. Missing signed bytes,
bad digests or multiple retained attempt rows require operator investigation;
the worker does not guess which legacy record is authoritative.

## Restore protocol

1. Hold the existing transaction storage lock and validate the full queued request
   against its journal admission.
2. If Redis has an attempt, retain its identity checks. If it is missing and a
   durable binding exists, verify the retained admission, digest and binding,
   then durably activate recovery mode before returning the journal record.
3. Validate its chain, signature, signed bytes, blockhash and signer before any
   Redis insertion or send. Invalid bytes leave recovery mode active and fail
   closed. A terminal record can be reconstructed only for confirmation and
   cleanup; quarantined or conflicting records remain blocked.
4. Insert the attempt only if Redis still has no value and the storage lock is
   still owned. A lost reply is handled by reading and validating the stored value
   on retry.
5. Run ordinary history/receipt reconciliation. Any retransmission sends the
   original bytes and requires a fresh durable recovery-send allowance.

Neither restoring a record nor reattaching Redis returns send permission. The
worker must still pass the current ownership, journal and transaction gates.

## Bounded retransmission

The original path retains its existing limit of 20 broadcasts. After restoration,
the journal grants at most 20 additional recovery broadcasts for that identity,
shared across all repeated projection losses and process restarts. A lost grant
response consumes an allowance conservatively. Recovery mode prevents the normal
authorization path from bypassing this budget when Redis loses its recovery flag.

The recovery allowance is 20 per identity, not per restoration. A combined bound
of 40 assumes the original Redis broadcast counter survived until recovery mode
was activated. That original counter is Redis-only: an intact-marker restore can
roll back its value, an existing limitation this feature does not remove. Sends
by another application are outside either allowance. Engine's RPC transport still
uses one HTTP request per invocation with provider retries disabled.

Budget exhaustion stops retransmission; it does not erase signed evidence or
establish failure on chain. Finalized status and matching receipts can still
resolve the original identity. The 500-check reconciliation limit remains in
Redis and resets when that projection is reconstructed. It is not a lifetime RPC
read budget; repeated losses can cause additional history lookups. Expiry still
blocks retransmission without authorizing a replacement.

## Storage and upgrade

Schema 3 retains recovery mode and its cumulative budget independently of Redis.
Older binaries reject this version. Healthy schema-1 and schema-2 journals migrate
under exclusive ownership; a recorded schema-2 checkpoint update must finish
before migration. The upgrade itself uses the same durable pending-token protocol.
Read-only status/export and offline reattach do not perform migration.

Normal transactions do not gain a new durable commit for this feature. Restoration
and recovered sends pay the existing FULL-commit/publication/acknowledgement cost.
See the [upgrade and rollback rules](redis-disaster-recovery.md#schema-upgrade-and-rollback)
before opening existing production state.

## Verification

Require runtime tests for the first-signature/Redis gap, already-broadcast loss,
repeated restoration, lost replies, cancellation, stale locks, conflicting data,
expiry, terminal reconciliation, cumulative budget exhaustion and migration.
The local Agave process test removes signed projections only while Engine is
stopped and requires identical-byte replay, exact effects and terminal deduplication.
Bounded models cover protocol cuts and deliberate faults; they do not prove RPC
honesty, filesystem durability or the entire service.
