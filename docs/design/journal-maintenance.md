# Journal retention and physical maintenance

## Retention contract

Retain admitted intent, replay ownership, signed history, terminal evidence and
recovery budgets permanently. Redis TTL does not expire journal identity. An old
ID must never become a fresh transaction because maintenance forgot it.

This version does not delete terminal payloads or offer bounded lifetime storage.
At sustained 50 TPS, one chain adds 4.32 million intents/day. Measure bytes/intent
with the intended workload and size storage for its duration and duty cycle.
Compaction reclaims free pages and checkpoints WAL; it does not remove logical
history or credentials. Cold archival with compact tombstones would require its
own schema, recovery and compatibility design.

## Offline command

Stop Engine and select its original persistent journal. Supply a new backup
filename inside a private directory:

```sh
engine-recovery compact --path data/recovery.sqlite --out data/before-compact.sqlite
```

The command:

1. Takes the existing exclusive owner lock; requires an unhalted current-schema
   journal with no pending checkpoint. It does not contact Redis or establish
   Redis continuity.
2. Checks SQLite integrity/foreign keys and conservative working-space reserves.
3. Writes a consistent SQLite snapshot, verifies all logical rows with a streaming
   digest, syncs it and publishes it atomically without overwriting another file.
4. Runs SQLite VACUUM on the original database, repeats row/control verification,
   then requests WAL truncation. A verification failure durably halts the source.

The snapshot is 0600 in a private directory and includes credentials and signed
requests. No companion owner lock is created. It is an investigation/backup
artifact, not a new runnable deployment. Never replace a newer authority with
this older snapshot; checkpoint continuity cannot certify such a rollback.

Control UUID, epoch, checkpoint, process identity, replay keys, recovery counters
and every logical row remain unchanged. Existing readers can delay WAL truncation;
the result reports that condition. Inspect byte counts and `checkpoint_truncated`
before claiming space was reclaimed. No automatic schema migration or halt reset.

## Verification and limits

Real SQLite/Redis tests compare complete exports and logical digests before,
after and in the backup; reclaim actual free pages; reopen through normal health
checks; and preserve terminal deduplication and nonce bindings. Active owners,
halts, pending transitions, public output directories and existing files reject
maintenance. Existing atomic-export tests cover shared publication cleanup.

This relies on [SQLite VACUUM](https://www.sqlite.org/lang_vacuum.html), its transaction
semantics and the filesystem/device honoring sync. It is not a host power-loss
qualification. Physical compaction stutters over the existing recovery model;
source guards and tests are not a SQLite/refinement proof.
