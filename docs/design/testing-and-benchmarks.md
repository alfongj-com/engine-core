# Testing and benchmark design

Status: proposed validation contract, 2026-09-15. Upstream baseline: `b6b7a0bbdc737b3a2b09611305b71b1bf6aba6e8`. Measured results belong in `docs/baselines/`; scenarios below are not automatically implemented or passed.

## Context, goals and non-goals

The dangerous failures are lost intent, executing the same intent twice, unsafe nonce reuse, false success, unauthorized signing, and unbounded recovery spending. A suite should expose those failures and make refactoring safer. Test count and line coverage are diagnostic measures, not the objective.

Goals: reproducible local tests without Thirdweb Vault; explicit chain guarantees; comparable queue and transaction-path baselines; fault recovery with observable outcomes. Non-goals: proving every RPC provider, obtaining production TPS from an empty local chain, or replacing a security review with passing tests.

## Decisions: test the real boundary

| Level | Best use | Oracle and isolation |
| --- | --- | --- |
| Unit / property | Fee arithmetic, error classification, transaction intent preservation, nonce transition planning, signing/encoding | Hand-calculated boundaries or independent decoding; generated sequences shrink to a minimal failure. No network. |
| Redis integration | Lua, WATCH/MULTI/EXEC, leases, idempotency, index/data consistency, pruning | Real dedicated Redis, independent connections, unique namespace. Inspect durable data as well as counts. |
| Local end-to-end | HTTP → queue → local signer → local chain → receipt → webhook | Actual server and Redis, disposable chain/account, assert contract state and final API state. A successful HTTP response alone is insufficient. |
| Fault injection | Crash windows, ambiguous RPC success, lease takeover, Redis disconnect/restart, reorg | Scriptable RPC proxy and controlled process termination; compare durable history with chain effects after recovery. |
| Chain/provider qualification | Nitro future-nonce expiry, OP fees/finality, preconfirmation, AA variants | Explicit profile from [chain compatibility](chain-compatibility.md), bounded funded testnet runs; no public-network load test by default. |

For asynchronous timers use Tokio's paused clock where code uses Tokio time. It does not control `SystemTime`, Redis server time, or remote RPC time; those need injected clocks or a small real-time integration test. [Tokio testing](https://tokio.rs/tokio/topics/testing).

Use barriers/channels to cause the intended race, not arbitrary long sleeps. Property tests should model legal state transitions independently of production code and persist failing seeds. Loom is useful for small in-process synchronization components that use its instrumented primitives; it does not simulate Redis, network partitions, or ordinary system calls. [Proptest state machines](https://proptest-rs.github.io/proptest/proptest/state-machine.html), [Loom scope and limitations](https://docs.rs/loom/latest/loom/).

## Invariants and priority cases

| Priority | Invariant | Minimal revealing scenario |
| --- | --- | --- |
| P0 | Each acknowledged intent remains durable or reaches an explicit terminal result. | Kill worker after Redis preparation, after RPC acceptance, and before submission-state commit; restart and reconcile. |
| P0 | A stale lease owner cannot change another owner's state. | Owner A pauses beyond lease; B acquires; A resumes ACK/NACK/renew/send-state commit. |
| P0 | One sender/chain nonce has one active intent; retries preserve intent. | Concurrent admission, external nonce movement, ambiguous timeout, replacement and recycling interleaved. |
| P0 | Transport uncertainty cannot become successful execution. | Send accepted but response lost; receipt null/error; receipt status zero; old receipt orphaned. |
| P0 | Authorization applies to every signing path and payload. | Wrong identity, key, chain, transaction target/value, structured-message domain; assert rejection and no secret persistence. |
| P0 | Gas recovery stays within explicit limits. | Fee 0/1, near integer maximum, repeated bumps, rising base fee, L1/operator costs; decode all attempts. |
| P1 | All queue indexes describe the same job lifecycle. | Duplicate push in each idempotency mode; ACK/NACK/requeue/cancel concurrent with prune; audit sets and records. |
| P1 | Lifecycle notification is durably coupled to its transition. | Commit succeeds then process dies before delivery; webhook accepts but reply is lost; delivery failure remains retryable. |
| P1 | Draining and restart do not strand work. | Shut down under admission and submission load, restart from persisted state, finish within deadline. |
| P1 | Chains retain their documented guarantees. | Run the chain-specific matrix with delayed heads, fee changes, future nonces, and AA failures. |

For external effects the realistic contract is at-least-once attempts with idempotent reconciliation, not exactly-once network delivery. Webhook consumers need stable event identity and deduplication. A stronger guarantee must be demonstrated across the crash boundary that spans Redis and the external system.

Redis transactions serialize commands, but execution errors do not roll back successful commands. WATCH conflicts and errors inside EXEC need separate assertions. Persistence settings also change crash guarantees: AOF every-second fsync can lose recent writes. State the accepted recovery point and measure it; an in-memory test cannot certify durability. [Redis transactions](https://redis.io/docs/latest/develop/using-commands/transactions/), [Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/).

## Baseline protocol

### Separate four measurements

1. **Pure CPU:** signing, encoding, classification and fee planning. Criterion samples steady-state execution, with setup excluded only when production excludes it. Use independent input sizes and consume outputs. [Criterion timing loops](https://bheisler.github.io/criterion.rs/book/user_guide/timing_loops.html).
2. **Durable queue:** admission-to-terminal-state latency and unique completions. Includes Redis round trips and durable state updates; excludes blockchain/signing unless explicitly added.
3. **Local transaction path:** successful contract effects per second and lifecycle latency, across 1 and multiple senders. Separately record reverts and retries.
4. **Real chain/provider:** constrained qualification/canary throughput with selected finality semantics. Report chain congestion, RPC limits, fees and spend; do not extrapolate from local results.

### Required experiment record

- Exact git SHA/patch, lockfile, Rust/tool versions, release/debug profile, OS/CPU/RAM, process counts, Redis version/config/persistence and location, local chain/client/fork, and endpoint category. Exclude keys and credential-bearing URLs.
- Workload: unique intents, payload/calldata sizes and contract operation, offered rate, achieved admission rate, senders, concurrency, worker/batch/inflight settings, failure distribution and random seed.
- Warmup, measurement interval, and separately timed drain. Repeat at least three times on an otherwise idle machine; report each run and variation.
- Accepted, rejected, unique terminal success, terminal failure, unresolved at deadline, duplicate attempts, duplicate effects, and missing durable records. Reconcile counts before reporting success.
- Latency p50/p95/p99/max with sample count, queue depth over time, RPC/signing/Redis calls, retries, CPU and RSS. Time local intervals with a monotonic clock; keep wall-clock timestamps for correlation only.

For saturation, schedule arrivals independently of response completion and record missed arrivals. A fixed number of clients that waits for each reply lowers offered load as latency rises and can hide overload. Keep that useful closed-loop experiment labeled separately. [k6 open/closed workload models](https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/open-vs-closed/).

Define sustainable capacity as a rate with stable queue depth during measurement, bounded tail latency/error rate, and complete drain under the chosen SLO. Do not measure “throughput” as handler starts, retries, submitted hashes, or accepted HTTP requests. Completion throughput over the full run and capacity during steady state are different quantities.

### Existing harness limitations

Upstream `twmq/benches/throughput.rs` increments `jobs_processed` in the handler before durable completion and counts NACK retries. Its sequential awaited producer and delayed missed ticks can miss the target offered rate. The sustainability heuristic checks backlog after a drain period. Its cleanup passes a wildcard to `DEL`, which does not expand patterns. Treat output as exploratory queue load data, not certified unique completions or blockchain TPS.

`scripts/benchmarks/eoa.ts` measures HTTP and webhook observations; webhook latency includes delivery. Preserve those results but add independent receipt/state verification. `integration-tests/README.md` claims full coverage from an in-process server; running code in-process only makes coverage measurable and does not prove it.

## Implementation sequence and gates

1. Preserve upstream checkout and record build blockers. If signer replacement is required to build, label the resulting transaction baseline as modified; independently benchmark unchanged queue code.
2. Add deterministic P0 regression tests before fixes. Record an expected failure separately; never change its oracle merely to match broken behavior.
3. Run the affected unit/integration slice after a behavioral change, broader suites at phase boundaries. Avoid rerunning the whole workspace after every small refactor. Every candidate release must run all applicable gates once.
4. Refactor small modules with unchanged externally observable behavior. Compare complete histories/state, not private function calls. Benchmark again only when the hot path changes or results suggest regression.
5. Enable profiles one at a time after local P0 invariants and chain qualification pass. Preserve raw JSON, commands and failure logs; list skipped/blocked cases explicitly.

A 24-hour soak, Redis failover/recovery, live signer quota testing, production-provider qualification and security review remain release work until measured. Open choices: durability target, completion guarantee, representative workload, allowed spend, and throughput/latency SLO. All sources in this document were accessed 2026-09-15.
