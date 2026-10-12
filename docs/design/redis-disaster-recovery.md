# Redis disaster recovery

## Contract

For queued execution, an accepted request and a possibly broadcast transaction must remain identifiable
after Redis loss. Redis alone cannot establish that a missing transaction never
executed. The independent SQLite ledger records immutable intent, replay identity,
exact attempted wire/request and terminal evidence before the relevant action can
escape the process. Missing evidence means **recovery required**, not permission
to create another transaction.

This implementation supports **one active Engine process on one Unix host**, one
durable local ledger directory, and one Redis primary projection. All workers and
signing routes must use the installed recovery health gate. The server requires it; library
tests may omit the global journal. File locks exclude cooperating processes using
the same ledger. They do not fence a different host, copied ledger, other wallet
software, or an old Engine binary that ignores the journal.

Stop all legacy workers before initialization. Do not put the ledger on an
ephemeral container filesystem, network filesystem, Redis backup volume, or a
volume that will be rolled back with Redis. Treat a lost, copied, corrupted or
rolled-back authoritative ledger as a separate incident: this implementation
cannot prove continuity or safely reconstruct unknown intent from that copy.
There is no automatic initialization or force-clear command.

## Authority and projection

| Record | Independent ledger | Redis |
| --- | --- | --- |
| Request identity | Global ID, executor kind, fingerprint and original full payload | Scheduling and executor data |
| Replay identity | One immutable key per ID; one owner per key | EOA nonce indexes, AA nonce/UID, Solana attempt |
| Before send | Exact attempted signed wire or bundler request | Borrowed/submitted/attempt projection |
| Outcome | Retained terminal evidence and chain checkpoints | Status, retention TTL, webhook work |
| Deployment | UUID, epoch, checkpoint, namespace, Redis process identity, durable halt | Mirrored checkpoint token |

The ledger retains records without the Redis terminal TTL. Repeating admission
returns the stored payload, preserving the originally generated ERC-4337 nonce
or a legacy EIP-7702 UID. Bundled EIP-7702 admission and execution are disabled;
retention of legacy records does not enable that path. See
[execution attribution](confirmation-identity.md). A changed intent conflicts. A terminal ID skips queue creation;
a quarantined ID is rejected. EOA and no-op records share the canonical replay
key `evm:<chain>:<lowercase sender>:<nonce>`, so neither can reuse the other's
allocation. Fee replacements can retain the same replay identity; a new nonce
for the same ID is rejected.

EOA signing also preserves the full admitted authorization list. The old
recipient-code filter could silently remove an authorization belonging to a
different authority and has been removed. Networks with stricter authorization
admission rules require valid caller-supplied tuples and separate qualification;
Engine does not rewrite the intent to make a provider accept it.

Before signing, validate the complete current job payload against the ledger and
check an existing replay binding once known. Before network emission, durably
record the exact attempt. Existing Solana attempts must be reconciled or replayed
unchanged; missing Redis attempt data cannot authorize a fresh blockhash/signature.
Direct transaction-signing and administrative mutation routes also require the
recovery gate. An already authorized request may reach the chain after a halt;
its queued execution attempt must already exist in the ledger. Direct signing
returns bytes to the caller; it does not create a queued admission or record the
returned wire as an execution attempt. Any external broadcast of those bytes is
outside Engine's recovery and inclusion guarantee.

## Commit ordering and failure cuts

Critical ledger writes are serialized in-process:

1. Check the durable halt, Redis primary role, process identity and checkpoint.
2. Commit the authority mutation and its exact pending predecessor/target tokens
   together in a FULL SQLite transaction.
3. Compare-and-swap Redis from the recorded predecessor to target, then observe
   the target on the same primary process.
4. Acknowledge by deleting the exact pending record in a second FULL commit.
5. Check continuity again before returning permission.

A crash or lost CAS response leaves a durable witness of the unfinished update.
Startup or a health check may finish that exact transition. It cannot accept an
arbitrary checkpoint lag, recreate a dead caller's permission, or enqueue work.
Once acknowledged, an older marker is a rollback and halts execution. See
[recoverable checkpoint protocol](recoverable-projection.md) for migration,
failure boundaries and implementation checks.

SQLite uses WAL with `synchronous=FULL`; initialization and private output files
are synced, including containing directory entries. Owned tasks retain the
serialization lock and admission permit through publication and acknowledgement.
Cancelling a caller does not abandon its update; retry the original ID and intent.
Blocking database work retains the OS owner lock. Graceful shutdown stops HTTP
and workers, then drains registered work. Health repair has a separate 32-operation
bound, and callers cancelled while waiting for that capacity spawn no detached task.

Positive marker, process or role contradictions still halt execution, as do storage
failures and existing durable halts. Transport uncertainty is retryable only while
an exact pending record exists. Without that witness, unavailable continuity still
halts. A possibly broadcast attempt retains its identity even when a crash happens
before its first network write. Redis replication acknowledgements cannot replace
this independent authority. [Redis WAIT](https://redis.io/docs/latest/commands/wait/)

**Projection limit:** the token is not a hash of every Redis key. A restore that
preserves the current token can still lose Redis-only scheduling changes. Job
payload validation, durable replay ownership and retained intent prevent that
loss from authorizing a different effect; missing projection work may require an
explicit original-request retry or offline recovery. Arbitrary selective Redis
corruption and hostile modification of the journal are outside the continuity
claim. Ordinary Redis persistence remains necessary for availability.

SQLite WAL requires all database users on the same host. FULL synchronization is
needed for power-loss durability; NORMAL may lose committed transactions after an
OS/power failure. These guarantees depend on the filesystem and device honoring
sync. [SQLite WAL](https://www.sqlite.org/wal.html),
[SQLite synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous)

## Operator commands

For an existing deployment without a journal, first follow the
[legacy upgrade procedure](../upgrade-from-legacy.md). It drains and reconciles
old work before a cutover to a new empty namespace; initialization does not
migrate retained queue state or historical idempotency keys.

Build with `cargo build -p thirdweb-engine --bin engine-recovery`. The CLI uses
`REDIS_URL` for operations that contact Redis; status/export need only the ledger.
The examples use a private directory whose parent already exists. Init can create
the final directory as 0700; existing directories must already be private.

```sh
export REDIS_URL=redis://127.0.0.1:6379/
engine-recovery init --path data/recovery.sqlite --namespace engine_v1
engine-recovery status --path data/recovery.sqlite
engine-recovery export --path data/recovery.sqlite --out data/recovery-export.json
```

Initialization requires a fresh ledger and empty destination namespace. It does
not import legacy queued jobs. Normal startup opens only an existing initialized
ledger and requires its configured namespace to match. Status exposes counts and
deployment identity plus whether checkpoint publication is pending. Full exports include credentials and signed requests: they
are written only to a new 0600 file inside a private directory, never to stdout.
SQLite data/WAL/lock files belong in the same protected directory. Errors suppress
SQL values and Redis URLs. This is filesystem protection, not encryption at rest.

The container image includes both binaries. Mount a persistent local volume at
`/app/data`, owned by the image's `appuser` with mode 0700. Keep it independent
of the Redis data volume. With that same volume and configuration mounted, run
the image once with `--initialize-recovery` before ordinary startup. For inventory
or export, override its entrypoint to `/app/engine-recovery`; this uses `REDIS_URL`
for Redis operations. The main server's `--reattach-recovery` and `--recover-redis`
commands instead use `APP__REDIS__URL`, `APP__RECOVERY__JOURNAL_PATH` and
`APP__QUEUE__EXECUTION_NAMESPACE` from its configuration. For recovery, that
namespace must be the new empty destination. Never initialize automatically on
each container start or scale this image to multiple active replicas.

### Redis restart with intact data

Stop Engine. If the persisted Redis checkpoint exactly matches the independent
ledger, explicitly reattach:

```sh
engine-recovery reattach --path data/recovery.sqlite --namespace engine_v1
```

Reattach only updates Redis process identity. It requires the same deployment,
epoch, checkpoint, namespace and primary role. It may clear only Redis process
change, unavailable-continuity or primary-role-change halts. Reattach refuses every unacknowledged pending transition. A network outage
without a pending transition still latches execution; stop Engine before reattaching.
It cannot override a checkpoint mismatch, storage failure, manual quarantine
or terminal-evidence conflict. Unknown attempts remain bound to their original
identity and must still follow the normal confirmation/replay rules.

### Redis loss or stale snapshot

Stop Engine and all legacy workers, preserve the old Redis state and export the
ledger. Recover into a different empty namespace:

```sh
engine-recovery recover --path data/recovery.sqlite --new-namespace engine_v2
```

The command advances the epoch, retains all immutable records/replay bindings,
and quarantines every attempted nonterminal ID. It clears any pending transition
in the same epoch-change transaction. It creates only the new checkpoint
projection; **it does not send or enqueue any transaction**. Configure Engine with
the new namespace, then restart. Old configuration is rejected by ledger metadata.

- New IDs may execute, subject to retained replay-key ownership.
- Original unsent requests can be retried; the ledger returns their original
  payload and random replay UID. Recovery does not reconstruct missing credentials.
- Terminal IDs remain terminal and cannot enqueue another effect.
- Possibly broadcast IDs remain quarantined, even if the crash occurred before
  network I/O. Null receipts, nonce progress and expired Solana blockhashes do not
  resolve them. This version exports the evidence but provides no CLI override
  that fabricates a terminal outcome or releases an unknown nonce.

`engine-recovery quarantine --path ...` records an offline global stop. Mutating
commands require the same exclusive OS lock as Engine; they fail while it runs.
Read-only status/export use a consistent SQLite read transaction and remain
available while Engine runs. Export streams one row at a time to a private file,
syncs it, and publishes it atomically without overwriting an existing destination.
A failed write leaves no partial destination. Long snapshots can delay WAL
reclamation, so allow disk headroom for both the export and concurrent journal writes.
Chain-finality conflicts survive namespace recovery;
this command is not a way to reset those fences. Contradictory terminal witnesses
also block namespace recovery until a dedicated reconciliation procedure exists.

## Finality integration

The ledger compare-and-swaps each chain checkpoint against the exact earlier
evidence whose canonicality the caller checked. A concurrent advancement requires
a new check. Lower checkpoints cannot advance the fence. Same-height hash or
policy contradictions durably halt the affected chain. Terminal evidence is
idempotent for the same transaction, block and execution outcome at a later
covering checkpoint; contradictory terminal evidence stops global execution and
retains both observations. Recording EVM terminal evidence checks the chain halt
in the same SQLite transaction, so an earlier health check cannot race a newly
recorded finality conflict. See [finality design](finality-and-recovery.md).

## Validation and remaining topology work

`core/src/recovery/tests.rs` covers immutable intent/UIDs, replay ownership across
reopen, explicit/private initialization, live read-only export, exclusive mutation
locking, stale/deleted checkpoints, SQL-commit-before-mirror crash cuts, exact
reattach, recovery quarantine, terminal contradictions, checkpoint races and
storage-write failure. Cancellation tests stop every mutation after its SQLite
commit and before Redis publication, abort its caller, then require exact marker
continuity without recovery; a separate test cancels blocking database work and
checks ownership remains held. `scripts/local_eoa_recovery.py --live-redis-restart`
kills Redis while Engine remains running, checks admission is fenced, stops Engine
with SIGTERM, and requires intact reattach without quarantine or a new namespace.
A child process is SIGKILLed both before network emission
and after a loopback HTTP server accepts a request and drops its response. Exact
attempt bytes survive, and new-identity replay is rejected. The HTTP stub measures
delivery ambiguity, not blockchain consensus. Full Engine/local-chain disaster
tests exercise the integrated server and executor hooks separately.

Run the journal suite only against disposable Redis:

```sh
TEST_REDIS_URL=redis://127.0.0.1:6385/ \
  cargo test -p engine-core recovery::tests -- --include-ignored --test-threads=1
```

Multi-host active/active deployment requires a shared transactional authority with
a defined durable failover policy, and fencing enforced by the signer/egress
boundary. A per-host file or shared Redis lock does not supply that. Journal
backup/restore must also preserve the latest acknowledged evidence independently;
an older journal backup cannot safely stand in for the lost current authority.
