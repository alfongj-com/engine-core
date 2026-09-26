# Exact-wire gap replay validation

Release SHA-256: `a3d360eba3dc470bd830ec9d3a32e0be8e11b6764f9bbf6efeb319f080eb8515`. Prior baseline `a0b38…8ccc` and depth candidate `c0014…ce4` remain unchanged. Exact commands, source/log hashes and preserved paths are in `gap-runtime-validation.json`.

## Results

| Gate | Result |
|---|---|
| Every workspace test/binary | Compiled |
| New replay regression | 1 parent passed, exercising 11 isolated scenarios |
| Executor Redis suite | 61 passed |
| Executor normal suite | 37 passed; 61 ignored |
| Core journal suite | 20 runner passes |
| Core normal suite | 20 passed; 20 ignored |
| Release build | Passed, 40.75 seconds |

Do not sum overlapping suites: the targeted replay parent also runs in the 61-test suite. Core journal runner counts include an opt-in throughput probe that returns without work when its output variable is unset and a subprocess helper; these are not throughput results. No code repairs were needed. Formatting and diff whitespace checks passed. Owned Redis 26479 was stopped after verifying its process ID.

The first targeted attempt waited for a disposable Redis process that had exited with its tool session. Its aborted log is retained; the final run used an independently retained Redis session and passed in 8.78 seconds. No application failure was observed.

## Scope

The regression checks actual signed-wire equality, ascending nonce replay, cooldown and 32-nonce bound, continuation after observed progress, the actual confirmation path retaining its cached allocator high-water, unknown response retention, missing/wrong identity, terminal/halted admission, and ownership loss before and during a round.

Replay creates no signature or fee change. Five seconds is a minimum round cadence, not a wall-clock deadline for 32 sequential RPC calls. Missing or unknown lowest nonce stops higher dispatch. The SQL getter returns one row but can examine that ID's attempt history.

Actual-process recovery, sustainable rates, shared-chain capacity and final Linux CI remain separate qualification steps. This record does not infer them from unit/integration tests or bounded formal models.
