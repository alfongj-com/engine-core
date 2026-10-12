# Candidate 4320ea8: health gate ordering

**The first matched moderate run passes all campaign rate gates and completes all 4,800 offers safely. EVM progress recovers; Solana admission latency rises.** This candidate retains the extra durable acknowledgement and moves the cancellable chain-lane wait before the global health-capacity slot. Workload and settings match the baseline: transfer-only 10/10/20 TPS, 120 s, 30 s warmup, 60 s late window, FULL SQLite commits and Redis AOF every second.

The [aggregate report](candidate-4320ea8-moderate.json) retains code and binary provenance. There are no dropped offers, RPC method errors, proxy failures, durable halts, or outstanding work after drain. One passing run does not establish a sustainable maximum. The earlier [12aa6d8 failures](candidate-12aa6d8-analysis.md) remain in the record.

## Progress and latency

| Chain | Late attempt TPS, baseline / 12aa6d8 / 4320ea8 | Late terminal TPS, 4320ea8 | Admission p99, ms, baseline / 12aa6d8 / 4320ea8 | 4320ea8 unsigned backlog |
| --- | ---: | ---: | ---: | ---: |
| EVM | 9.850 / 8.269 / 9.818 | 9.601 | 153.9 / 229.4 / 115.7 | 3 → 14 |
| OP | 10.017 / 8.019 / 10.084 | 9.868 | 176.3 / 233.1 / 147.5 | 7 → 2 |
| Solana | 20.001 / 20.039 / 20.035 | 19.719 | 169.8 / 338.9 / 911.5 | 35 → 40 |

EVM attempt p99 observer bounds improve from 17.097/18.275 s to 6.208/6.282 s, versus 5.414/5.376 s in the baseline. Terminal p99 improves from 30.909/33.767 s to 13.218/16.674 s. Measured EVM handler cycles recover from 5/7 to 60/63; their mean duration falls from 16.200/11.222 s to 0.964/0.875 s.

Solana attempt p99 rises from 5.674 s to 7.674 s and terminal p99 from 23.126 s to 26.863 s. Its terminal backlog grows from 365 to 389 in the late window. Solana admission p99 is about 5.4 times the baseline and is only 88.5 ms below the campaign's 1 s limit. This is a fairness tradeoff near the storage limit, not a general latency improvement.

## All waits, not just the global mutex

Mean journal observations in ms:

| Operation | Baseline serial wait | 12aa6d8 combined wait | 4320ea8 lane wait | 4320ea8 serial wait | 4320ea8 health-capacity wait | 4320ea8 combined wait |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Admission | 10.410 | 48.175 | 208.036 | 2.914 | — | 210.950 |
| Broadcast | 5.383 | 58.324 | 229.401 | 3.083 | — | 232.484 |
| Terminal | 33.541 | 57.939 | 193.760 | 2.305 | — | 196.066 |
| Health | 36.402 | 110.075 | 190.685 | 2.416 | 0.000272 | 193.102 |

The health-capacity wait falls from 65.009 ms to effectively zero. This agrees with the expected removal of cross-chain obstruction: a chain can no longer fill global health slots with its own lane waiters. EVM makes progress while the busier Solana lane waits longer. These metrics pool chains and their changing work mix; they do not directly attribute each wait to Solana or prove isolated causality. Different phase counts at window boundaries make summed means approximate.

## Storage cost and RPC work

Over approximately 90 post-warmup seconds, summed journal serial holds are 89.351 s, versus 89.262 s for the earlier candidate and 55.103 s for the baseline. SQL performs 21,464 commits totaling 72.878 s at a mean 3.395 ms each. Acknowledgement time is 37.801 s, which includes commits already counted in SQL time. It must not be added again. Redis mirror and continuity observations average 0.073 and 0.103 ms.

The journal remains nearly fully occupied. The ordering correction does not remove the second FULL commit or provide a large reserve for higher throughput.

Solana makes 16,054 logical status polls in the measurement window, carried by 4,052 measured HTTP requests, averaging 3.962 signatures per request. Coordinator wait averages 4.237 ms. The proxy's independently sampled load window counts 4,051 status requests, versus 31,693 for the baseline; the one-request difference is a sampling boundary. Total HTTP calls per new admission in that window fall from 20.698 to 5.227. Over the full campaign including drain, the candidate makes 12,896 calls for 2,400 accepted Solana intents, or 5.373 calls per intent.

Fewer logical polls can reflect longer scheduling delays as well as less redundant polling. Lower RPC traffic by itself does not establish a throughput improvement.

## Qualification limits and next checks

The campaign gate allows a 5% stage-rate deficit, an unsigned-backlog trend up to 0.1 TPS, and admission p99 up to 1 s. Solana's unsigned trend is 0.096 TPS. Passing this finite window does not prove stable backlog indefinitely. The repository's stricter posthoc screen requires at least 180 s after warmup and lower growth tolerances; this 120 s run cannot satisfy that duration.

The extended 40 TPS run and matched mixed-stress results are recorded below. The lower-rate 32 TPS run and crash validation are recorded below. Retain both passing and failing results. Public-chain qualification still requires real network tests: Anvil at an OP-like cadence does not model sequencing or L1 settlement.

## Matched mixed stress: fair opportunities, lower Solana progress

**All chains fail the stress capacity gate.** At 25/25/50 TPS for 60 s, the client drops 1,164 of 6,000 offers before dispatch: 311 EVM, 133 OP, and 720 Solana. Every dispatched initial request receives 202. Safety passes and all 4,836 admitted intents drain, with no durable halt or operator recovery. Full offered-cohort liveness fails.

| Chain | Late attempt TPS, baseline / 12aa6d8 / 4320ea8 | Late terminal TPS, 4320ea8 | Admission p99, ms | 4320ea8 unsigned backlog |
| --- | ---: | ---: | ---: | ---: |
| EVM | 0 / 1.501 / 10.108 | 12.410 | 452.8 | 525 → 673 |
| OP | 0 / 1.701 / 8.307 | 12.110 | 426.4 | 584 → 848 |
| Solana | 47.433 / 20.812 / 8.257 | 4.904 | 2,372.5 | 992 → 1,491 |

EVM gets more opportunities to advance, while Solana slows substantially. The correction allocates journal opportunities across chains; it does not reserve a transaction rate or make equal work cost equal across protocols. Aggregate client drops increase from 833 to 1,164 relative to the earlier candidate. No overall throughput improvement is claimed.

Mean health-capacity wait remains effectively zero. Health lane plus serial wait totals 337.728 ms, down from 712.611 ms in the earlier candidate. Admission and broadcast waits increase to 782.829 and 941.984 ms; terminal wait is 305.688 ms. These pooled observations include a changed chain/work mix and must be assessed alongside the per-chain rates and dropped offers.

Journal holds total 39.834 s during approximately 39.989 measured seconds. SQL commits consume 34.877 s across 10,189 observations, averaging 3.423 ms. The earlier candidate had 39.828 s of journal holds and 34.963 s of commit time. The shared journal remains effectively occupied throughout the load window.

Solana makes only 226 logical status polls in the window, in 185 HTTP batches averaging 1.222 signatures. This low RPC rate primarily accompanies stalled progress and late initial confirmations; it is not evidence of higher efficiency under equal progress. Including drain, 7,946 calls serve 2,280 accepted intents, or 3.485 calls per accepted intent. Unsigned backlog reaches 1,491 and terminal backlog 2,039 at the end of load.

The [stress aggregate](candidate-4320ea8-shared.json) is retained separately. This run demonstrates bounded operation opportunities and safe eventual drain under overload, not sustainable capacity or full offered-cohort liveness.

## Extended 40 TPS: short pass does not persist

The [240 s run](candidate-4320ea8-extended.json) offers the same 10/10/20 TPS transfer mix, but uses a 60 s warmup and 120 s late window. It is a longer follow-up, not a repeat of the baseline's 120/30/60 s timing. Safety and full-cohort liveness pass: all 9,600 offers are accepted and eventually drain, with no client drops, RPC errors, durable halt, or operator recovery. **The all-chain capacity gate fails.**

| Chain | Late attempt / terminal TPS | Admission p99, ms | Late unsigned backlog | Late terminal backlog |
| --- | ---: | ---: | ---: | ---: |
| EVM | 10.059 / 9.501 | 231.3 | 19 → 11 | 38 → 97 |
| OP | 10.009 / 9.676 | 206.9 | 2 → 1 | 82 → 121 |
| Solana | 18.427 / 16.951 | 1,209.9 | 61 → 250 | 426 → 792 |

Solana's unsigned-backlog trend is 2.156 TPS during the late window. Its admission p99 exceeds the 1 s limit, and terminal throughput falls well below the offered rate. The earlier 120 s run's pass therefore cannot support a sustained 40 TPS claim.

The [stricter posthoc screen](candidate-4320ea8-extended-strict.json) classifies Solana as persistent unsigned-backlog growth. Its six 30 s mean unsigned backlogs are 46.3, 51.4, 54.2, 95.1, 177.2, and 254.7. The aligned unsigned slope is 1.390 TPS and terminal slope 1.927 TPS across the whole post-warmup window. EVM and OP remain unconfirmed because of stage deficits and backlog growth or cadence ambiguity. The sampled counter window is 179.998 s, just 2 ms short of the analyzer's 180 s requirement; this reason is retained exactly. Independent rate and growth failures also reject qualification.

Over approximately 180 s, serial holds total 178.974 s and 42,081 SQL commits consume 150.552 s, averaging 3.578 ms. Mean combined waits are 302.925 ms for admission, 354.143 ms for broadcast, 247.048 ms for terminal work, and 227.726 ms for health. Health-capacity wait remains effectively zero; chain-lane delay and the cost of durable service remain.

Solana sends 17,512 logical status polls through 5,304 HTTP batches, averaging 3.302 signatures. The full cohort including drain makes 22,775 HTTP calls for 4,800 intents, or 4.745 calls per intent. RPC savings do not compensate for growing transaction backlog.

The 8/8/16 TPS follow-up uses 300 s with a 60 s warmup and 120 s late window. Its result and the crash scenario follow. These are separate profiles, not matched baseline comparisons.

## Five minutes at 32 TPS: campaign pass, strict confirmation pending

The [300 s run at 8/8/16 TPS](candidate-4320ea8-steady.json) passes all campaign gates. All 9,600 offers receive 202, complete safely, and drain. There are no client drops, RPC method errors, proxy failures, durable halts, or operator recovery. This is evidence for the observed five-minute workload, not a proven sustainable maximum.

| Chain | Late attempt / terminal TPS | Admission p99, ms | Late unsigned backlog | Late terminal backlog |
| --- | ---: | ---: | ---: | ---: |
| EVM | 7.984 / 7.876 | 70.4 | 7 → 8 | 24 → 38 |
| OP | 8.017 / 8.001 | 62.9 | 6 → 4 | 79 → 79 |
| Solana | 16.001 / 16.068 | 638.4 | 1 → 1 | 287 → 279 |

Attempt p99 observer bounds are 6.376/6.086/5.898 s and terminal p99 bounds 18.003/16.462/23.778 s. The unsigned queues do not show the persistent growth seen at 40 TPS. This lower-rate profile has no matched baseline run, so its latency improvement relative to 40 TPS cannot be attributed to an implementation change.

The [stricter assessment](candidate-4320ea8-steady-strict.json) uses 239.99 post-warmup seconds divided into eight nominal 30 s windows. It identifies no persistent-growth stage on any chain, but still declines qualification:

- EVM unsigned backlog starts and ends at 8; terminal backlog grows from 21 to 38, with an aligned mean trend of 0.132 TPS and terminal rate 7.925 TPS.
- OP unsigned backlog falls 6 → 4; terminal backlog grows by one. Its observed included rate is 7.992 TPS, below 8 TPS by more than the screen's one-transaction allowance of 0.00417 TPS.
- Solana unsigned backlog starts and ends at 1. Terminal backlog falls 288 → 279, while the aligned mean terminal trend is 0.0122 TPS, slightly above the 0.01 cutoff. Its observed included rate is 15.842 TPS, below 16 TPS. Terminal rate is 16.034 TPS.

These measurements retain endpoint and stage/cadence uncertainty; the thresholds are not relaxed after seeing the result. More repeated evidence would be needed for strict qualification.

Summed journal holds are 201.221 s across approximately 239.990 s of observation, about 84% of the window. SQL commits consume 157.631 s across 46,256 observations, averaging 3.408 ms. Combined mean waits are 85.501 ms for admission, 39.654 ms for broadcast, 132.441 ms for terminal work, and 115.355 ms for health. Health-capacity wait remains effectively zero. Redis mirror and continuity observations average 0.079 and 0.108 ms.

Solana's 66,568 logical polls use 12,066 HTTP status batches, averaging 5.517 signatures per request. Including drain, 29,369 total calls serve 4,800 Solana intents, or 6.119 calls per intent. This ratio is higher than the overloaded 40 TPS run, which spent longer waiting between polls; RPC count alone is not a capacity measure.

All five final-head CI checks pass at `0ad0453`: protocol models, fee proofs, Rust correctness, queue tests, and coverage. The crash result below remains separate from steady throughput qualification.

## Crash and exact-intent recovery at 32 TPS

The [90 s crash run](candidate-4320ea8-crash.json) offers 8/8/16 TPS transfers, with a 20 s warmup and 30 s nominal late window. Its queue lease is **10 s**, compared with **600 s** in the capacity campaigns. It tests recovery and is excluded from throughput qualification.

Engine receives SIGKILL after the Solana proxy accepts 200 unique signed transactions, then restarts successfully. All 2,880 offered intents reach a terminal result with exactly one canonical, finalized, successful execution each: 720 EVM, 720 OP, and 1,440 Solana. No intent has multiple canonical executions. EVM nonces increase by exactly 720 per chain; recipient effects and fees reconcile. Every drain counter returns to zero, with no durable halt, operator recovery, execution error, or safety/liveness failure.

One EVM admission loses its HTTP response during the fault; the other 2,879 initial requests receive 202. The harness retries that ambiguous request and four terminal duplicates using their original IDs. All five retries receive 202 and add zero forwarded sends on every chain. This is the intended distinction between a lost response and a new transaction intent.

The process exits 2 because the harness excludes chaos from capacity qualification even when safety and liveness pass. This is recorded rather than treated as a test failure or a throughput pass. Journal/executor timing counters restart with Engine, so this evidence includes no histogram subtraction across the restart.
