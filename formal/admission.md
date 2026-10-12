# Request identity and retention model

**Scope:** the Redis admission projection. Since the independent recovery journal
was added, the actual server also reserves IDs there before this Lua operation.
The journal retains terminal IDs beyond Redis TTLs. See [DisasterRecovery](disaster-recovery.md)
for that layer; the two model abstractions are not mechanically composed.

## Contract

A request ID identifies one immutable request while its identity is retained.
Identical resubmission is a no-op; changed intent is rejected. Admission commits
the identity and queue data together. Active, cancelled and uncertain attempts
retain identity without a TTL. Only a fenced terminal commit starts expiration.

`Admission` explores one request ID, two competing fingerprints and up to two
admissions separated by retention expiry. The generation number is proof
bookkeeping, not a field promised by the API. Network effects remain possible
after cancellation. Fingerprints abstract a collision-resistant digest of the
entire canonical request; the model does not prove hashing or serialization.

## Source mapping

The direct implementation is Solana's [`admit`](../server/src/solana_admission.rs)
Lua script and [`SolanaTransactionStorage`](../executors/src/solana_executor/storage.rs)
(`add_terminal_admission_command` and attempt persistence). The script checks
key types and orphaned identity/history before mutating. Terminal completion
is supplied by the separately modeled queue lease fence.

`Admit` abstracts the single Lua commit; `PersistAttempt`, `Execute` and
`Complete` join this admission protocol to the separately checked Solana recovery
protocol. This abstraction assumes one immutable execution identity per retained
generation, supplied by the recovery argument; the composition is not mechanically
proved. `Cancel` represents a queue job that will not automatically resume;
its still-persistent identity prevents resubmission from creating a replacement.
`PruneHistory` and `ExpireIdentity` are independent, so either ordering is checked.
Crash is stuttering because only persisted admission state is represented.

The four real Redis tests in `server/src/solana_admission_tests.rs` cover duplicate
admission through queue pruning, conflicting requests, orphan/migration checks
and schema validation. EOA admission has a related contract tested in
`executors/src/eoa/store/tests.rs`; this model does not prove the EOA implementation
equivalent to the Solana Lua script.

## Negative checks and limits

The runner requires counterexamples for overwriting an existing fingerprint,
expiring an active identity, and returning from a split identity/queue commit.
It also disproves `LifetimeAtMostOnce`: after **both** terminal identity and job
history expire, this Redis-only protocol can admit a later resubmission. This
remains a valid projection boundary, but is no longer the server's full API
contract: a retained terminal ID in the mandatory independent journal blocks
another admission. See [replay migration](../docs/replay-migration.md) for the
cutover scope. Loss or rollback of that authoritative ledger remains outside
the supported guarantee.

Not modeled: the one-time 20,000-record migration scan, hash collisions, TTL
clock behavior, retention configuration parsing, Redis OOM/wrong-type partial
execution, administrative deletion or power-loss rollback. The atomic Lua step
assumes command preconditions and retained writes. Those assumptions need
integration/operational checks; Lua atomicity alone does not provide rollback.
