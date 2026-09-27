# Capacity and recovery results

**Measured working points: 50 EVM transactions/second for 15 minutes, and 58.75 transactions/second across four chains for 15 minutes.** Both accepted every offer and independently reconciled every outcome. Higher inputs exposed queue growth or admission rejection. The useful operating envelope is clearer; an indefinitely sustainable maximum is not established.

## Scope and settings

One Engine used one EVM signer per chain, one Solana payer, an authoritative SQLite journal (`FULL`/`fullfsync`) and Redis AOF. The final capacity runs used the **600-second production queue lease**, Redis `everysec`, EOA send concurrency 32, HTTP concurrency 128 and Solana polling every 5 seconds. These are explicit campaign settings, not a claim that every setting is a production default. Redis-crash tests used the stronger AOF `always` setting.

The tested Engine binary is SHA-256 `35e307e16cafa7cbb17caafcf5ed90a840d58dddd2454542566619266e7a4c07`. Fixtures were Anvil EVM with 12-second blocks, Anvil OP execution with 2-second blocks, native Nitro dev L2, and local Agave. OP sequencing/derivation, Nitro L1 settlement, public RPC quotas and production hardware remain unqualified. The capacity/chaos campaign used **no paid RPC calls ($0 provider spend)**.

“Attempted” counts durably recorded signed attempts. “Completed” counts terminal journal outcomes, including expected reverts in mixed tests. Completion above the offered rate can be catch-up from an earlier queue. Independent settlement checks cover identities, receipts, effects and fees; they do not turn a growing queue into sustainable capacity.

## Individual operating envelope

Six-minute screens used a two-minute warmup; the rates below cover their last three minutes.

| Fixture | Offered TPS | Late attempted / completed TPS | Exact outcomes | Interpretation |
|---|---:|---:|---:|---|
| EVM, 12s | 55 | 54.33 / 55.12 | 19,800 | Unsigned work accumulated; use the longer 50 TPS result below. |
| OP execution, 2s | 55 | 54.47 / 56.41 | 19,800 | Average unsigned queue was nearly flat; block-phase comparisons remain inconclusive. |
| Native Nitro | 55 | 55.02 / 54.47 | 19,800 | Repeat candidate; small backlog trends prevent a strict stability pass. |
| Native Nitro | 65 | 57.29 / 55.45 | 23,400 | Clear overload: unsigned work grew throughout the post-warmup window. |
| Solana | 60 | 60.00 / 60.01 | 21,600 | Nearly flat with one 34-intent unsigned spike; repeat candidate. |

All offers in these runs were accepted and settled. The unchanged strict assessments still require repetition or resolution of cadence uncertainty; none is a confirmed maximum. At Nitro 65, post-warmup unsigned backlog grew **205→2,020**, with every aligned group rising. Its 55/65 comparison uses the same Engine and 600-second lease. [Final brackets](final-brackets-7790eecfb337/README.md), [Nitro upper screen](native65-upper-f6b8bbf5a938/README.md).

**EVM at 50 TPS for 900 seconds:** all **45,000** offers settled. Over the 720 seconds after warmup, attempted/completed rates were **50.003/50.020 TPS**; unsigned backlog moved **18→15**, and admitted-but-nonterminal work **1,845→1,830**. Full drain finished 45.4 seconds after offers ended. A small positive aligned unsigned-mean trend keeps the predefined stability classification unconfirmed, but the run demonstrates a clean 15-minute working point. [Long confirmation](long-confirm-e2cc04cbd054/README.md).

Earlier same-Engine screens at EVM/OP **60 TPS** grew queues; Solana **65 TPS** settled all 23,400 but retained a small positive terminal-backlog comparison. Those runs used a **10-second lease**, so they are not matched production-lease upper bounds. Solana 70 accepted 25,155 and rejected 45; a later read-only recovery audit reconciled the accepted outcomes after a harness guard interruption. Its rejected offers remain disqualifying. [Earlier 60/65 screens](phase-fixed-screen-bd44836d17c9/README.md), [Solana 70 incident](solana70-guard-incident/README.md).

The ordered Nitro 50 comparison improved from 42.67 to 49.85 late attempted TPS after the dispatcher change, with all 18,000 outcomes exact in each run. Database history, timing and different RPC response latency remain confounders; this is not a randomized attribution of the improvement. [Comparison](native-dispatch-pair/README.md).

## Shared capacity

Vectors are **EVM / OP / Nitro / Solana**, sharing the same Engine and journal. “Late completed” consistently uses the last 180 seconds.

| Offered vector | Offer duration | Accepted and settled | Rejected | Late completed TPS |
|---|---:|---:|---:|---:|
| 60 / 60 / 50 / 65 = **235** | 6 min | 55,105 | 29,495 | 41.19 |
| 12 / 12 / 10 / 13 = **47** | 6 min | 16,920 | 0 | 47.11 |
| 15 / 15 / 12.5 / 16.25 = **58.75** | 6 min | 21,150 | 0 | 58.74 |
| 15 / 15 / 12.5 / 16.25 = **58.75** | 15 min | 52,875 | 0 | 58.90 |

At 235 TPS, unsigned backlog grew **20,672→38,564** in the late window. EVM-family progress fell far below its individual rates while Solana continued processing more work. Eventual settlement of accepted requests does not erase rejected offers or establish fair service under overload. [Corrected shared pair](shared-lease600-1c3eec03d6cb/README.md).

The **15-minute 58.75 TPS run** is a useful combined working point. All 52,875 offers settled; all nine drain counters reached zero 30.3 seconds after offers ended. There were no retransmissions, RPC errors or lease errors. Post-warmup aggregate attempted/completed rates were **58.755/58.838 TPS**. EVM and OP pass the strict steady-window screen but require repetition; Nitro retains small backlog-comparison uncertainty and Solana a lagged inclusion-observation deficit. Preserve those flags rather than promote this to an all-chain strict pass or maximum. [Long confirmation](long-confirm-e2cc04cbd054/README.md).

The shorter 58.75 run recorded **two recovered Solana RPC warnings**, despite zero proxy/provider error counters. The long run did not repeat them. The 47 TPS vector remains the reference for four-chain fault tests. Earlier 10-second-lease overload logged 15 lost-ownership errors and 781 same-wire retransmissions; the 600-second run eliminated lease errors but still overloaded. Those settings must remain distinct.

### Captured resource and RPC pressure

In the long EVM 50 run, Engine/Redis sampled peak RSS was **90.4/119.7 MiB**, and late Engine RPC traffic was **151.6 requests/second**. In the long shared run, sampled peak RSS was **57.7/99.5 MiB** and late Engine RPC traffic **289.4 requests/second**. Shared calls per completed intent were approximately **3.14 / 3.36 / 6.30 / 7.00**, respectively. These exclude separate observer/guard reads; Nitro used gas estimation while the other EVM transfer fixtures supplied explicit gas limits.

These are sampled process-memory peaks. Captured CPU-time deltas are not whole-host utilization; VM resources, continuous memory peaks, storage latency and IOPS were not measured comprehensively. Low CPU alone does not identify fsync as the bottleneck. Public-provider sizing must account for the measured method mix and polling pressure, not transaction rate alone. [Current RPC sizing](final-review/RPC-SIZING.md). That note uses the six-minute shared screen (287.7 RPC/s); the 289.4 RPC/s above is the separate 15-minute run.

## Fault and mixed-workload evidence

| Scenario | What was independently verified |
|---|---|
| Mixed control, 12/12/13 TPS | 11,100 exact outcomes, including 2,400 expected EVM reverts. |
| Lost accepted responses, 12/12/10/13 TPS | 14,100 exact after **20 actual response losses per chain**; original signed identities retained. |
| Pre-forward send errors, 12/12/13 TPS | 11,100 exact after **20 injected errors per chain**. |
| OP shallow reorg and pool loss, 12 TPS | 3,600 exact; **19 orphaned plus 234 pooled intents** recovered through 253 unchanged-wire replays. |

All four drained. Each run's 100 duplicate-ID probes covered one EVM chain, not every family, and added no sends. Reorg replay began 0.125 seconds after the recorded rollback and finished sending the affected wires 40.2 seconds later; this is one observed sequence, not a general recovery SLA. It required no Engine restart or manual replay. [Mixed control](chaos02-mixed-lease600/README.md), [lost responses](chaos05-lease600/README.md), [send errors](chaos06-send-errors-lease600/README.md), [reorg](chaos03-op-reorg-lease600/README.md).

### Redis crash: genuine halt, incomplete recovery qualification

The first Redis test offered 37 TPS with AOF `always`. It overloaded: **7,548 initially accepted, 3,552 rejected**, and the requested 1,000-OP-acceptance trigger never fired. Later retries produced 11,100 exact outcomes. This remains a **failed fault qualification**, not a Redis recovery pass. [Untriggered attempt](chaos07-untriggered-aof-always/README.md).

The corrected **4/4/4 TPS** test reached the actual Redis SIGKILL at **75.721 seconds**. Recorded health and mutation probes returned **503**, and exact reattachment was refused at **75.861 seconds** because the independent journal retained a checkpoint-mirror halt. A harness SQLite-reader error then interrupted reconciliation.

The subsequent cold-journal inventory verifies:

- **909 acknowledged IDs exactly match 909 durable admissions.** Three transport-unknown requests are absent from that admission set.
- **904 durable attempts** exist, versus **903 provider-accepted unique wires**. These counts are not interchangeable.
- **648 terminal records and 261 nonterminal requests** remain in the journal. The 261 are unresolved; they have not been terminalized by this report.

The original oracle remains **incomplete with `safety_pass: null`**. Saved Anvil history excludes pending transactions; retaining node files does not authorize automatic resume. The separate read-only clone audit has now rechecked **all 648 recorded terminal proofs** (423 EVM and 225 Solana). It also observed provisional or pending work and additional receipts without changing the journal. **All 261 requests remain nonterminal in the journal.** Some have observed receipts; none was promoted to a recovered journal outcome by this review. This establishes custody and the actual fail-closed branch, not automatic recovery, complete settlement, zero drain or a throughput qualification. [Original report and custody audit](chaos07-redis-fenced-rate12/README.md).

The harness reader fix passed **163 integrated tests**, including stopped-WAL regressions; two separate negative controls reproduced the old observer and custody-backup failures. It changed no Engine code and does not retrospectively change the incident's verdict. [Reader correction](journal-reader/README.md).

### Engine kill: restart halted; recovery qualification failed

Cut 4 reached the actual SIGKILL at **100.431 seconds** under the 47 TPS four-chain input. The replacement Engine exited immediately with **Redis checkpoint mismatch**. Captured SQLite checkpoint **13,436** was one ahead of Redis marker **13,435**, with matching authority. This is consistent with a crash between the durable journal commit and Redis mirror; exact instruction-level timing is not proven. The 60-second gap in the report was the harness readiness wait, not queue-lease recovery.

At interruption, **4,719 HTTP 202 responses** were recorded, **3,978 journal records were terminal**, and **741 remained nonterminal**. Another 2,824 dispatched requests had transport errors; 6,557 planned offers were never dispatched. HTTP-to-admission correspondence is aggregate per chain, not an exact retained per-response-ID mapping.

Independent review verified all 3,978 terminal proofs. The 741 retained requests comprise **711 qualified chain executions not yet terminal in the journal, six provisional Nitro executions, one unbroadcast Solana signature and 23 unsigned EOA admissions**. They remain unchanged. The original oracle has **safety=true, liveness=false** and `requested_fault_validated=false`.

**Measured availability limitation: an Engine-only crash required operator recovery; automatic restart did not restore service.** The safety fence retained authority, but this is not successful recovery or a complete workload result. [Engine-kill evidence](chaos04-engine-fenced/README.md).

### Offline projection recovery: identity retained, work quarantined

Cut 8 completed and independently verified its explicit recovery branch at **4/4 TPS** with Redis AOF `always`. After deleting the owned Redis projection, health/admission returned 503 and unsafe reattachment was refused. Offline recovery rebuilt a fresh namespace from the journal: **605 admissions = 413 terminal + 187 quarantined + 5 unsent**.

Independent review checked all immutable columns for **605 admissions and 600 signed attempts**, plus unchanged terminal, finality-checkpoint and halt-table hashes. The proxies had accepted **599 unique wires** before recovery; a signed attempt is not itself evidence of an accepted broadcast.

The **187 quarantined requests** comprise **64 qualified chain executions not yet reconciled into terminal journal outcomes, 109 provisional executions, 13 pending transactions and one unbroadcast attempt**. Another **five requests remain unsigned**. None was silently retried, assigned a new identity or promoted to completion by the recovery review.

The report records **331 identity probes**: 200 terminal IDs returned 202 and 131 quarantined IDs returned 503. The full immutable inventory remained unchanged and **zero new sends** occurred during recovery/probes. `requested_fault_validated=true` and `recovered_with_quarantine` qualify this explicit projection-recovery branch. The original oracle remains **safety=true, liveness=false, incomplete**, with **192 nonterminal journal requests and 13 pending node transactions**.

This is an accepted deliberate-quarantine endpoint, not completed workload settlement or new same-signer liveness. An unconsumed quarantined nonce can safely block later work. [Offline recovery evidence](chaos08-offline-quarantine/README.md).

All timed workloads and independent audits are complete. Retained nonterminal work remains an operator-reconciliation obligation; explicit offline recovery is distinct from automatic restart.

## Preserved limits and next work

An earlier interrupted EVM 55 run retained 3,274 signed intents, but **1,980 canonical outcomes remain unverified** after losing disposable-node history. Later successful runs and snapshot improvements do not repair that evidence gap. [Incident](evm55-projection-incident/README.md).

Prioritize operator reconciliation of retained work before more load. For performance work, measure journal-lock, commit and Redis wait times separately; investigate cross-chain scheduling/admission fairness; then evaluate bounded Solana status batching. Preserve existing durability, identity and finality checks. Synthetic journal probes and finite formal models do not establish an Engine capacity ceiling or verify the whole service.

The [five-area review](final-review/OUTCOME.md) summarizes performance, security, reliability, readability and test/proof coverage. The [verification record](../../verification.md) binds the named Linux CI results, 163 local harness tests, compiled negative controls and finite-model evidence to their actual sources. Additional capacity CI steps remain a patch, not an executed Linux gate. **These findings do not qualify the service for unattended production operation.**
