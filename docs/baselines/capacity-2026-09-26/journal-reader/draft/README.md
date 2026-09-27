# Cut 7: stopped WAL journal reader correction

27 September 2026. Outside-repository draft; no Engine, node, replay or original-ledger mutation.

## Finding

The corrected Redis crash genuinely triggered at about 75.72s. The Engine persisted `Redis checkpoint mirror failed`, halted, and returned 503. The offline reattach command correctly refused to clear this halt. That is the intended fail-closed cut, not an unsafe recovery or lost authority.

The original stopped journal has 2,355,200 bytes and no WAL/SHM sidecars. All six recorded owned process IDs were absent before copying. Exact private cold copy: `/tmp/engine-chaos07-journal-investigation/cold-original/recovery.sqlite`; SHA-256 `f323505182a117379fcc62763e2a73fa8363c9c234667569c9bf9943d4e81282`. Separate immutable cold-copy inspection passes `integrity_check`;909 admissions remain,648 terminal and261 nonterminal. Original hash remains unchanged. This is durable inventory, not proof of 261 chain outcomes.

On this Python 3.9 / SQLite 3.51.0 build, `mode=ro` opens that WAL-mode file but querying it fails with `OperationalError: unable to open database file`. Separate copies opened as immutable (closed copy only) or read/write query cleanly. A fresh synthetic checkpointed WAL fixture reproduces the current actual observer and custody-backup failures. This isolates the harness reader problem from the expected production halt. No general claim is made that every SQLite/VFS build has this behavior.

SQLite documents WAL/SHM lifecycle and the conditions for read-only WAL access: https://www.sqlite.org/wal.html#read_only_databases . An immutable connection must never replace a reader of a live ledger: it can omit committed WAL data.

## Narrow fix

`journal-reader.patch` changes only the campaign and adds one test file:

- Open an **existing** database using `mode=rw`, never create it.
- Enable `query_only=ON` before exposing the connection, preserving logical SQL read-only access.
- Set connection-local FULL synchronization/fullfsync to match the ledger, including possible last-reader checkpoint bookkeeping.
- Explicitly close every source reader on success/failure. All ten campaign reader/backup-source sites use the helper.
- Explicitly close the private writable backup target after its transaction context and before hashing it.

This permits SQLite's WAL/SHM bookkeeping; it is **not physically read-only forensic access**. It was never applied to the incident originals. It does not change replay, reattach, recovery, outcomes, fences or oracle rules.

## Validation

`negative-control.log`: two targeted tests against the original actual campaign fail as intended: observer raises the same OperationalError; custody reports journal unavailable.

`final-tests.log`: eight actual SQLite tests pass (0.038s). They cover missing-file noncreation, blocked SQL mutations, committed live-WAL rows, invisible uncommitted/rolled-back rows, stable transactions, sidecar-free checkpointed WAL reading, explicit close on exception, actual observer/fence reading, and actual custody backup count/hash after target close. No nodes or incident files are used in fixtures.

Focused command after applying:

```
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=scripts python3 -m unittest capacity_journal_reader_test -v
```

`manifest.json` pins the base harness, patch, revised source/test and logs. Root owns integration and the broader regression suite; no retrospective passing verdict is assigned to the incident.
