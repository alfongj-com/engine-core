# Remaining cuts: bounded independent review

27 September 2026. Read-only review of `/tmp/engine-chaos-launcher/launch.py`, current campaign, and the latest saved cut 4/cut 8 previews. No tests, processes, state creation or original-artifact edits.

## Readiness

- The inspected predecessor previews pin campaign SHA `959d1aa…`; reader-fixed source is `c3af22b4…`. Root subsequently launched cut 4 with the corrected source. Cut 8 must likewise freeze the current source; retain old previews and never bypass `verify_frozen`.
- The one-shot launcher claims global custody before resource preflight, unpauses Nitro only after preflight, pauses it in `finally`, and retains active/review state on every outcome. It deliberately does not call nominal `safely_settled` or automatically continue.
- At review start the global custody pointer identified corrected cut 7. Its expected halt and retained inventory require an explicit operator review/custody handoff before another cut; root owns that handoff. Releasing a scheduling pointer is not declaring its 261 unresolved IDs complete or releasing their replay reservations.
- Read actual `queue_settings.lease_duration_seconds=600`; the 1800s drain allows a surviving lease to expire naturally. The 2820s whole-child bound includes 300s load, 1800s drain, 120s setup and 600s verification. A timeout is still incomplete. No force-clear of leases/journal fences.

## Cut4: actual Engine SIGKILL

Latest plan: `/tmp/chaos04-engine-kill-lease600-postfix-preview.json`.

| Profile | Rate | Planned original offers |
|---|---:|---:|
| EVM12 |12/s|3,600|
| OP execution |12/s|3,600|
| Native Nitro |10/s|3,000|
| Solana |13/s|3,900|
| Total |47/s|14,100|

Transfer-only; Redis AOF everysec; trigger after 1,000 distinct accepted Nitro wires. HTTP 128, EOA send 32, Solana poll 5s, lag 100ms, nonce allowance 4,096. Native depth 2 and the separate-wallet drain hook remain explicit.

1. Require `chaos_trigger` with Nitro accepted count >= 1,000, then an actual `engine_sigkill`; ensure Redis/node identities persist. `requested_fault_validated` alone is insufficient.
2. Automatic-recovery claim needs `chaos_recovered(kind=engine-crash)`, no durable halt, original journal/projection/IDs and independently verified original-wire/replay bindings. An Engine kill within SQL-commit/mirror separation can instead correctly fence restart; preserve that as failed automatic availability, not successful recovery or a safety violation without evidence.
3. Reconcile the union of original and retry admissions by ID. `--retry-unknown` can first-admit requests rejected during downtime. The 100 terminal duplicate probes are selected globally, not per chain, and may overlap retried IDs. Preserve original HTTP counts and report actual probe coverage. Only duplicate-already-terminal requests must have zero new work; first admissions can legitimately send.
4. Complete settlement requires exact per-ID canonical/finalized receipts, outcomes/effects/fees/nonces, no identity substitution/duplicate execution, all nine numeric drain counters zero, no unexplained runtime errors, and all owned children stopped. Confirm native pause separately. Never equate the 14,100 planned offers with accepted custody without checking drops/retries.
5. A failed restart, incomplete oracle or nonzero unresolved count keeps manual review and private journal/AOF/node evidence. Owned Anvil snapshots now retain mined history; their separate pool inventory does not make pending transactions restartable automatically.

## Cut8: explicit offline quarantine, last

Latest revised plan: `/tmp/chaos08-offline-redis-lease600-rate8-trigger300-preview.json`: EVM12=4/s and OP=4/s, 300s, nominal 1,200 offers each, mixed workload, AOF always, OP accepted-wire trigger 300. No generic retry phase; 100 recovery probes **per chain/state maximum**.

1. Require trigger, `owned_redis_flushed`, actual health/admission 503, persisted halt, unsafe reattach rejection, explicit CLI recovery into a fresh empty namespace, and `offline_projection_recovered`. The intentional intake abort means 2,400 planned slots are not expected admissions.
2. Audit the complete before/final ledger inventories: same deployment; epoch + 1; new namespace; checkpoint 0 at recovery; halt cleared only by that authorized operation. Preserve every admission fingerprint/payload hash/replay binding, attempt row, terminal proof and finality table. Terminal stays terminal; attempted nonterminal becomes quarantined; unsigned remains unsigned. Require at least one quarantined ID.
3. Verify terminal probe 202 and quarantine probe 503 with at least one actual quarantine probe. Require no additional attempt rows, sends, effects or changed identity. `immutable_inventory_after_probes` checks admission/attempt maps in code; independently compare final terminal/checkpoint hashes too, rather than reading that flag as a complete-table proof.
4. Expected endpoint: `recovered_with_quarantine`, safety oracle passes for observed evidence, `original_completion_claimed=false`, cleanup succeeds. `journal_unresolved` must equal quarantined+unsent counts; it is intentionally nonzero. Missing offered slots and incomplete quarantined transactions remain visible. Do not require a normal all-zero drain or call this complete workload recovery.
5. Retain per-ID unresolved custody and verified snapshot hashes. Root may separately accept the deliberate quarantine endpoint after review; the launcher must not turn it into settled custody or automatically start another job. It does not prove new same-signer execution availability.

## Separate SQLite version finding

`core/Cargo.toml` enables bundled rusqlite 0.40.2; `Cargo.lock` pins libsqlite3-sys 0.38.2. Its actual bundled header/c source is SQLite 3.53.2, and the untouched cold journal's last-writer header is 3053002. Engine is therefore newer than the WAL-reset fix.

Python uses SQLite 3.51.0, in the affected release range. Upstream describes a rare corruption race involving competing checkpoint/write connections, fixed in 3.51.3 (or listed backports). This is separate from the reproduced sidecar-free `CANTOPEN` observer failure; the cold journal passes integrity checks. [SQLite advisory](https://www.sqlite.org/wal.html#the_wal_reset_bug).

The scoped campaign reader is query-only, performs no explicit checkpoint or SQL write, and last-close checkpointing first takes an exclusive database lock in upstream 3.51.0. I do not identify the advisory's concurrent writer/checkpointer path in this usage; that is a source-based assessment, not a proof about all Apple VFS behavior. Keep Python as dependency debt and use a patched SQLite before introducing independent live checkpoint/write operations. Do not use `immutable=1` on a live journal. [Upstream close implementation](https://raw.githubusercontent.com/sqlite/sqlite/version-3.51.0/src/wal.c).
