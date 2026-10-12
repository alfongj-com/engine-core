# Candidate 12aa6d8: matched runs

**Safety and full drain pass; the two EVM chains fail capacity.** This result remains part of the record if another candidate supersedes it. The workload, duration, warmup, durability and concurrency settings match the moderate baseline.

| Chain | Late attempt TPS, baseline → candidate | Late terminal TPS | Admission p99, ms | Candidate unsigned backlog |
| --- | ---: | ---: | ---: | ---: |
| EVM | 9.850 → 8.269 | 10.217 → 9.269 | 153.9 → 229.4 | 25 → 130 |
| OP | 10.017 → 8.019 | 10.134 → 9.119 | 176.3 → 233.1 | 26 → 146 |
| Solana | 20.001 → 20.039 | 19.768 → 19.972 | 169.8 → 338.9 | 11 → 10 |

No RPC method errors, proxy overloads, or proxy failures were recorded. EVM attempt p99 observer bounds increase from about 5.4 s to 17.1/18.3 s. Mean confirmation phases rise from 139 ms to 7.88/5.48 s; send phases rise from 384/374 ms to 10.35/7.27 s. Each EVM worker completes only 5/7 measured cycles, versus 85/87 in the baseline, so longer confirmation and send phases delay each other.

## Durable service cost

Across approximately 90 post-warmup seconds:

- Summed journal serial-hold observations rise from 55.10 s to 89.26 s. These operations use one mutex, so this indicates a nearly occupied journal; boundary samples keep it from being an exact utilization measurement.
- SQL commits rise from 10,876 to 20,670. Mean commit time changes little: 3.299→3.364 ms. Accumulated commit time rises 35.88→69.53 s, consistent with the added FULL acknowledgement commit.
- Mean admission hold rises 3.886→7.606 ms; broadcast hold 2.576→5.036 ms; terminal hold 3.725→7.494 ms.

The additional durable acknowledgement is the main observed service-cost increase. Batching RPC calls does not remove that cost. This single comparison changes several components; it does not isolate their causal contributions as an experiment would.

## Waiting moved into other gates

| Operation | Baseline wait, ms | Candidate lane + serial wait, ms | Candidate health-capacity wait, ms |
| --- | ---: | ---: | ---: |
| Admission | 10.410 | 48.175 | — |
| Broadcast | 5.383 | 58.324 | — |
| Terminal | 33.541 | 57.939 | — |
| Health | 36.402 | 45.066 | 65.009 |

Health waits total about 110.075 ms. Its phase observation counts differ by fewer than ten at the window edges, so summed means are approximate. Lower serial-wait numbers alone would hide this regression.

`ensure_healthy` acquires one of 32 global health slots before waiting for its chain lane. A busy chain can occupy these slots with callers waiting behind its own single lane. Another chain then waits for a global slot before it can enter its independent lane. This is bounded head-of-line delay; it does not weaken a fence.

The subsequent correction acquires the lane cancellably before the health slot, then transfers both permits into the owned task and uses the existing global mutex. The task must not acquire its lane twice. Cancellation before spawn releases the permits; cancellation after spawn leaves them owned until journal work finishes. Peer-progress and cancellation regressions accompany the correction. This report describes the earlier candidate; its measurements are unchanged. This could improve admission fairness, but cannot remove the acknowledgement commit cost or guarantee the offered rates fit the journal.

## Solana batching

Logical status polls remain comparable: 31,693 baseline status calls versus 30,914 candidate logical polls. Actual candidate status HTTP calls fall to 4,651, about 85% fewer, averaging 6.647 signatures/request. Mean coordinator wait is 3.875 ms; logical status-call time is 4.522 ms. Total HTTP requests per new admission fall 20.698→5.605 during the load window. Solana finality progress remains near the offered 20 TPS, but admission latency rises.

These are useful RPC savings, not an aggregate throughput win. A later experiment must retain both the original and updated results, all durable commits, and the same safety oracle.

## Matched mixed stress

The same 25/25/50 TPS offered load overloads both versions. The candidate completes all 5,167 admitted intents safely, but **833 of 6,000 offers never reach Engine** because the load client's bounded concurrency is full. The liveness oracle therefore fails. There is no durable halt or operator recovery, all admitted work drains, and every dispatched initial request receives 202.

| Chain | Client drops | Late attempt TPS, baseline → candidate | Late terminal TPS | Admission p99, ms | Candidate unsigned backlog |
| --- | ---: | ---: | ---: | ---: | ---: |
| EVM | 232 | 0 → 1.501 | 2.502 → 0 | 499.7 → 746.5 | 774 → 1,144 |
| OP | 93 | 0 → 1.701 | 2.502 → 0 | 498.4 → 742.2 | 859 → 1,289 |
| Solana | 508 | 47.433 → 20.812 | 49.685 → 17.811 | 496.5 → 1,245.1 | 744 → 1,097 |

The few EVM attempts in the candidate late window do not establish improved capacity: both versions have growing backlogs and fail the rate gate. Candidate health wait reaches about 712.611 ms: 536.065 ms for global capacity, 172.618 ms for the chain lane, and 3.928 ms for the global mutex. Mean combined waits for admission, broadcast, and terminal work are 185.969, 355.758, and 410.254 ms.

During about 40 post-warmup seconds, serial-hold observations total 39.828 s versus 37.527 s in the baseline. Commit time rises from 25.797 s to 34.963 s; mean commit cost rises from 3.201 to 3.546 ms. There is little unused journal capacity at this offered load.

Solana status HTTP calls fall from 18,263 to 627 in the load window, but logical polling and transaction progress also slow. Average batch size is only 1.407 signatures and coordinator wait is 5.665 ms. The full candidate campaign makes 10,122 HTTP calls for 2,492 accepted Solana intents, or 4.062 calls per accepted intent, including drain. Dividing by all 3,000 offers would hide the 508 dropped offers. This stressed run cannot isolate batching efficiency from scheduling delays.
