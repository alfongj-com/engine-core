# Index and broadcast configuration validation

Release: `bb34deeec74afce0b52d03edb9ec73a84497429db0cc2e460590b9127c0d77ce`. Workspace test compilation and release build passed; no repair was needed after applying the independently reviewed patches. The prior `a3d360e…8515` binary was preserved locally.

## Rust checks

| Separate suite | Passed | Evidence |
|---|---:|---|
| New index migration/ownership regressions |5|[Log](engine-index-broadcast-migration-tests.log)|
| Core journal runner |25|[Log](engine-index-broadcast-core-journal-tests.log)|
| Core unit |20|[Log](engine-index-broadcast-core-unit-tests.log)|
| Server configuration/health/unit |24|[Log](engine-index-broadcast-server-unit-tests.log)|
| Executor Redis |61|[Log](engine-index-broadcast-executor-redis-tests.log)|
| Executor unit |37|[Log](engine-index-broadcast-executor-unit-tests.log)|

**Do not add these counts.** The suites overlap; the journal runner includes the five new tests and existing opt-in benchmark/subprocess helper entrypoints. Ignored counts, exact commands, source/binary/log hashes and durations are in [validation.json](validation.json). [Workspace compilation](engine-index-broadcast-workspace-no-run.log) and [release build](engine-index-broadcast-release-build.log) are separate checks. These small logs are copied byte-for-byte; large node logs and credential-bearing journals are not published.

The five new tests exercise populated older-journal migration with unchanged exports/Redis token, indexed lookup, original/latest wire membership, first-proof and contradiction behavior, read-only/locked/unhealthy startup boundaries, atomic rollback on second-index failure, and ownership through caller/async-task cancellation while SQLite is blocked. They do not simulate storage power loss.

## Actual Engine reorg cases

| Case | Result | Time | Evidence |
|---|---|---:|---|
| Success |Pass|18.626s|[Report](local-eoa-reorg-index-broadcast-success.json), [accepted-send audit](reorg-success-rpc-audit.jsonl)|
| Revert |Pass|18.583s|[Report](local-eoa-reorg-index-broadcast-revert.json), [accepted-send audit](reorg-revert-rpc-audit.jsonl)|

Each case retained one original signed attempt, proved one accepted post-restart resend of that exact wire, withheld provisional terminality, and then matched canonical depth and outcome. Same-ID terminal retry produced no extra send/effect. The reports' legacy `unique_chain_effects` field counts canonical executions, including a revert; the revert leaves recipient value unchanged. No manual signed rescue occurred. The binary hash was checked again afterward; all owned Engine, Anvil and Redis processes stopped. Paths inside original reports identify execution-time private artifacts; the linked audit copies and reports remain portable evidence.

## Limits

This qualifies functional wiring at the default broadcast setting. It supplies no measured throughput gain or independent high-concurrency send-bound assertion. Matched native process A/B runs and all-chain accepted-response-loss qualification remain necessary. The indexes add write maintenance and disk space while improving historical lookup plans. Formal correspondence/model evidence is maintained separately; TLC does not execute this release binary.
