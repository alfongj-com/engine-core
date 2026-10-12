# PR #2 validation — October 12, 2026

**The five-minute 8/8/16 TPS run and crash recovery pass their campaign checks. The stricter capacity screen remains unconfirmed, and 40 TPS fails on a longer run. All five final CI checks pass.** The [validation manifest](validation.json) separates completed checks, earlier failures, and qualification limits.

## Baseline

One host runs shared Engine, Redis, and SQLite with one signer per chain. EVM uses local Anvil at 250 ms blocks; OP uses Anvil execution at 2 s blocks; Solana uses a local Agave validator. These results do not qualify Nitro, OP sequencing/L1 settlement, or public-cluster throughput.

SQLite uses WAL with FULL synchronous commits and fullfsync. Redis uses `appendfsync=everysec`. EOA broadcast concurrency is 32, HTTP concurrency 64, Solana workers 100, status polling interval 1 s, and queue lease 600 s.

- [Moderate baseline](baseline-moderate.json): transfer-only, 10/10/20 TPS for 120 s, 30 s warmup, 60 s late window. All local capacity gates passed; repeated confirmation is still required.
- [Mixed stress baseline](baseline-shared.json): 25/25/50 TPS for 60 s, 20 s warmup, 20 s late window. Safety and eventual drain passed; capacity failed.

| Baseline / chain | Offered TPS | Late admitted / attempted / terminal TPS | Admission p99, ms | Late unsigned backlog |
| --- | ---: | ---: | ---: | ---: |
| Moderate EVM | 10 | 9.984 / 9.850 / 10.217 | 153.9 | 5 → 13 |
| Moderate OP | 10 | 10.001 / 10.017 / 10.134 | 176.3 | 4 → 3 |
| Moderate Solana | 20 | 20.001 / 20.001 / 19.768 | 169.8 | 1 → 1 |
| Stress EVM | 25 | 25.368 / 0 / 2.502 | 499.7 | 726 → 1,233 |
| Stress OP | 25 | 25.368 / 0 / 2.502 | 498.4 | 728 → 1,235 |
| Stress Solana | 50 | 50.736 / 47.433 / 49.685 | 496.5 | 28 → 94 |

Admission p99 measures scheduled arrival to response. Late rates exclude drain. The JSON summaries also retain terminal backlog and durable latency percentiles. Latencies cover observed responses or durable results; dropped offers have no latency observation. Durable times are sampled observer upper bounds. The mixed workload deliberately includes reverting calls, producing 500 `eth_estimateGas` errors per EVM chain. These counts alone do not establish transport failure; the stalled attempt rates independently fail capacity.

### Storage and polling

Mean observations during each post-warmup load window:

| Metric, ms | Moderate | Stress |
| --- | ---: | ---: |
| Admission serial wait / hold | 10.410 / 3.886 | 132.874 / 3.910 |
| Broadcast serial wait / hold | 5.383 / 2.576 | 163.585 / 1.948 |
| Health serial wait / hold | 36.402 / 0.247 | 124.211 / 0.230 |
| Terminal serial wait / hold | 33.541 / 3.725 | 179.758 / 3.658 |
| FULL SQL commit | 3.299 | 3.201 |
| Redis checkpoint mirror | 0.106 | 0.107 |
| Redis continuity observation | 0.128 | 0.134 |

Moderate performs about 3.022 SQL commits per new admission in the observation window. Candidate acknowledgement adds a FULL commit; the comparison must report this cost. `sql_ack` includes a commit already counted by `sql_commit`, so adding their sums double counts it.

Solana uses 20.698 HTTP requests per new admission during moderate load, including 17.607 status requests. Stress uses 12.041 and 9.132 respectively, with growing backlog and different workload. Baseline status requests contain one signature each. These ratios describe the observed window, not the lifetime cost of its newly admitted cohort.

## Earlier candidate: 12aa6d8

| Required comparison | Moderate candidate | Mixed stress candidate |
| --- | --- | --- |
| Same workload, duration, durability and concurrency | Matched | Matched |
| Late attempt/terminal rates and unsigned backlog | EVM capacity failed; Solana passed | All chains failed; backlog grew |
| Admission and durable latency | Admission p99: 229/233/339 ms | Admission p99: 746/742/1,245 ms |
| Lane wait + serial wait + health-capacity wait | Higher combined waits | Health wait: 124 → 713 ms |
| SQL commit/acknowledgement count and time | Commit time: 35.88 → 69.53 s | Commit time: 25.80 → 34.96 s |
| Solana logical polls, HTTP calls, batch size and coordinator wait | Status HTTP requests: 31,693 → 4,651 | Status requests: 18,263 → 627, with slower progress |
| Safety, drain and process shutdown | Passed | Safety passed; admitted work drained; 833 offers dropped |

Candidate `12aa6d8` is retained in the [moderate report](candidate-12aa6d8-moderate.json), [stress report](candidate-12aa6d8-shared.json), and [analysis](candidate-12aa6d8-analysis.md). Both EVM unsigned backlogs grow despite eventual drain. Stress drops 232 EVM, 93 OP, and 508 Solana offers at the client concurrency limit; every dispatched initial request receives 202. This is a failed whole-cohort liveness result, despite safe completion of admitted work. Solana batching reduces HTTP work, while the second durable SQL commit nearly fills the shared journal service window. The subsequent scheduling correction is measured separately below.

A lower serial wait can move waiting into another semaphore. Assess all waits with per-chain progress and latency. Timer sums may overlap; they are not wall-clock utilization. Compare moderate with moderate and stress with stress. Keep load-only and full-cohort-plus-drain RPC ratios separate.

## Health-gate candidate: 4320ea8

The [detailed analysis](candidate-4320ea8-analysis.md) covers all waits, durable service cost, and RPC savings. Moving the chain lane before health capacity gives EVM more opportunities to progress, while Solana latency rises under load.

### Short 40 TPS run: campaign gates pass

The [120 s run at 10/10/20 TPS](candidate-4320ea8-moderate.json) passes safety, full-cohort liveness, drain, and campaign rate gates. Its warmup is 30 s and late window 60 s.

| Chain | Late attempt / terminal TPS | Admission p99, ms | Late unsigned backlog |
| --- | ---: | ---: | ---: |
| EVM | 9.818 / 9.601 | 115.7 | 3 → 14 |
| OP | 10.084 / 9.868 | 147.5 | 7 → 2 |
| Solana | 20.035 / 19.719 | 911.5 | 35 → 40 |

Solana admission p99 rises from 170 ms in the baseline to 912 ms. Journal holds occupy 89.35 of 89.99 observed seconds; SQL commits consume 72.88 s. Status HTTP calls fall from 31,693 to about 4,052, but longer scheduling delays also reduce logical polling.

The gate allows a 5% rate deficit and admission p99 up to 1 s. Solana's unsigned trend is 0.096 TPS, close to the 0.1 TPS cutoff. This short pass does not establish sustained 40 TPS.

### Overload at 100 TPS: all rate gates fail

The [60 s mixed run at 25/25/50 TPS](candidate-4320ea8-shared.json) drops 1,164 offers at the client; all 4,836 admitted intents drain safely. EVM late attempt rates improve to 10.108/8.307 TPS, while Solana falls to 8.257 TPS and admission p99 reaches 2.373 s. More EVM progress comes with less Solana progress and more dropped offers; this is no aggregate throughput win.

### Longer 40 TPS run: persistent backlog growth

The [240 s run](candidate-4320ea8-extended.json) fails capacity. Solana late attempts fall to 18.427 TPS, terminals to 16.951 TPS, and unsigned backlog grows 61 → 250. All 9,600 offers eventually complete safely.

Its [stricter screen](candidate-4320ea8-extended-strict.json) finds persistent Solana backlog growth. The 60 s warmup and 120 s late window differ from the short baseline profile. The earlier short pass does not persist.

### Five minutes at 32 TPS: campaign pass, strict screen unconfirmed

The [300 s run at 8/8/16 TPS](candidate-4320ea8-steady.json) passes campaign gates, with no drops and safe completion of all 9,600 offers. Warmup is 60 s; the late window is 120 s.

| Chain | Late attempt / terminal TPS | Admission p99, ms | Late unsigned backlog |
| --- | ---: | ---: | ---: |
| EVM | 7.984 / 7.876 | 70.4 | 7 → 8 |
| OP | 8.017 / 8.001 | 62.9 | 6 → 4 |
| Solana | 16.001 / 16.068 | 638.4 | 1 → 1 |

The [stricter screen](candidate-4320ea8-steady-strict.json) still declines qualification because of terminal backlog trends and observed stage deficits. No persistent-growth stage is identified. This is a finite observed workload, not a proven sustainable maximum.

### Crash at 32 TPS: all original effects recover

The [90 s crash run](candidate-4320ea8-crash.json) uses a shorter 10 s queue lease. All 2,880 offered intents have exactly one finalized execution after Engine is killed and restarted.

One lost HTTP response and four terminal duplicates are retried with the same IDs; all five retries return 202 without additional sends. This is recovery evidence, not a capacity result. Timing counters are not differenced across restart.

## Correctness and proof checks

- Full local suite passed after the shutdown fix, including **160 explicit Redis/API tests**: 44 journal, 73 executor, 4 Solana admission, 3 HTTP API, and 36 queue tests. Also passed: workspace suite, formatting, builds, 7 timing-parser tests, and 3 formal-runner tests.
- Candidate `4320ea8` passed the targeted health rerun: 29 core unit tests, 45 Redis journal tests, exact terminal retry, formatting, and build. The earlier full workspace result remains attached to `12aa6d8`.
- **Nine local process scenarios have passing results.** Eight ran after the scope-boxing fix; the live Redis-outage case was rerun after the queue shutdown fix. That rerun fenced execution, exited zero on SIGTERM, reattached safely, and completed all 24 original effects without duplication.
- [Final-source formal CI](../../../formal/evidence/pr2-2026-10-12/README.md) passed on `0ad0453`: 73 expected TLC outcomes, all 88 mapped source hashes, five Kani fee harnesses, and two deliberate mutation counterexamples. All five final-head checks passed: protocol models, fee proofs, Rust correctness, queue tests, and coverage. Earlier source-stage results remain in the manifest.
- An older binary rejected the upgraded schema. The corrected fixture reached schema validation; an earlier missing-lock fixture did not count.

### Failures retained in the record

The first real EOA admission exposed a worker stack overflow. Boxing the inner scoped future fixed the issue; five focused tests and subsequent process scenarios passed without increasing thread stack size. A later live Redis-outage test exposed shutdown waiting on blocked queue I/O; the fix passed three queue regressions and the process rerun.

An AOF-always 100,000-lane stress fixture was interrupted with SIGTERM without an observed assertion failure. It is not a passing result. The completed general suite used disposable Redis with `appendfsync=everysec`.

## Evidence boundaries

This measurement directory archives only allowlisted aggregates. Raw reports, observations, logs, credentials, endpoints, transaction identities, signed bytes, and local process paths remain private. Missing or invalid observations are not replaced with zero. Finite models and source guards do not constitute a proof of the complete Rust/Redis service.
