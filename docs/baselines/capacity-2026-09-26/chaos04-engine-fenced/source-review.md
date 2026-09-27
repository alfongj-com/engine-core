# Cut 4 restart refusal: source and log review

Read-only, 27 September 2026. Original report `/tmp/chaos04-engine-kill-lease600.json`, SHA-256 `00c6f6577213af337ca406205ac0749101db0ab7f3a0d7b72df82220d4ce73c1`. Campaign `c3af22b4…`; no state/DB/AOF reads, nodes, tests or source edits in this review. Independent custody audit is separate.

## Finding

**An actual runtime availability limitation was exercised: the journal requires operator recovery after an interrupted SQLite-to-Redis checkpoint transition.** The restart correctly fenced itself. This was not queue-lease waiting and not the preceding SQLite observer exception.

- Nitro reached 1,002 distinct accepted wires; Engine was SIGKILLed at load 100.431s.
- Replacement Engine spawned at 100.442s. Its entire `engine-5.log` is `Error: Recovery required: Redis checkpoint mismatch`.
- Redis PID 64263 stayed live throughout. Its only shutdown is ordinary cleanup at 07:36:55 UTC; no Redis crash/restart occurs in this cut.
- The harness reported the durable mismatch at 160.446s. The 60s interval comes from `Campaign.start_engine()` waiting for health 200 until its readiness deadline, without early child-exit detection. It is not the 600s queue lease.
- The reader correction worked: the campaign read the durable halt and completed an oracle/custody report. No SQLite OperationalError is recorded.

## Why the protocol stops

`core/src/recovery.rs` commits a ledger mutation and incremented checkpoint before calling the separate Redis CAS (`reserve_admission`, `before_broadcast`, terminal/checkpoint methods). Startup calls `healthy_locked` before worker startup/index migration, compares the Redis token to SQLite, and durably halts on mismatch. An Engine-only kill can land after the FULL SQLite commit but before Redis receives its mirror.

That interruption is consistent with these logs; the exact checkpoint delta/affected mutation requires the separate retained-state audit. Redis remaining live rules out a Redis-process restart as the recorded cause. Do not infer an exact missing command from the failure string alone.

This behavior is intentional and tested by `core/src/recovery/tests.rs:327` (`lost_checkpoint_or_sql_commit_before_mirror_is_fail_closed`), and described in `docs/design/redis-disaster-recovery.md` under commit ordering. `reattach` only permits an exact checkpoint and the allowed Redis-process-change cause; it cannot clear this mismatch. Waiting for leases or rerunning startup does not repair it.

## Honest outcome

The report contains 4,719 durable observed intents and 3,978 terminal entries, with 741 journal-nonterminal. Its captured chain oracle says safety=true but liveness=false; all 14,100 planned offers were not admitted/settled. `requested_fault_validated=false` means the requested automatic recovery did not complete, although the actual kill clearly fired. These are distinct claims.

Owned processes stopped; unresolved owned-Anvil mined-history snapshots and pool inventories are preserved, and native state is retained. The report does not authorize clearing unknown replay reservations. Independent audit must establish which nonterminal intents already executed and retain those not proven complete.

## Safe follow-up

No immediate fence weakening or source repair is justified by these logs. Preserve the original failure and independent ledger. Supported availability recovery is explicit recovery into a fresh empty projection, quarantining attempted nonterminal IDs and retaining all bindings; this does not automatically resolve 741 old intents or guarantee same-signer nonce progress.

Improving automatic process-crash availability is a separate protocol change: durable, recoverable journal/projection handoff plus proof/tests for ambiguous sends and Redis rollback. Merely accepting a one-checkpoint mismatch, overwriting the Redis token or clearing the halt would erase the distinction the continuity fence protects.

A small future harness improvement could notice an exited child and inspect its durable fence immediately instead of generating another 60s of failed offers. That affects incident timing/diagnostics only; it cannot repair this runtime protocol limitation and is not needed to reinterpret the current result.
