# PR #2 review follow-up validation

**Validation passes at final revision `319035e`: all five CI checks, both recovery reruns, the five-minute 32 TPS campaign, and the mixed-chain crash campaign. All 9,600 steady-run and 2,880 crash-run intents finish with exactly one successful finalized effect each.** The completed strict capacity assessment remains unconfirmed. The [manifest](validation.json) records source revisions, scope and counts.

## Changes under test

- Unscoped HTTP health checks have a separate two-slot lane. All AA send/confirm workers now carry their chain context.
- Idle EVM lanes can be reclaimed; live dedicated or overflow waiters prevent an unsafe lane split.
- Owned registration returns retryable `Busy` when a drain writer is waiting, avoiding a shutdown lock cycle.
- Journal, queue, and executor collectors initialize before startup work can create timers.
- Solana batches contain at most 32 signatures. Invalid individual statuses fail only their own positions; malformed shared envelopes still fail the batch. Batching can be disabled while retaining the existing transport safeguards.

The original journal fence, immutable transaction identities, and durable acknowledgement remain enforced. Runbook changes clarify recovery boundaries; they add no automatic reconstruction of missing signed attempts.

## Review disposition

| Topic | Change or decision | Evidence and remaining limit |
| --- | --- | --- |
| Unscoped health and AA chain identity | Separate two-slot health lane; scope all four AA handlers | Bounded-health and peer-progress tests; per-chain ingress quotas remain absent |
| Historical EVM lane exhaustion | Reclaim idle lanes; preserve live holders and overflow waiters | Reclamation and queued-waiter regressions; overflow stays shared while active |
| Shutdown registration race | Nonblocking registration plus permanent `close_and_drain` | Drain-inversion and late-probe tests; final live-outage and Solana recovery reruns pass |
| Startup metric loss | Initialize the shared exported collectors before journal/worker work | Startup ordering fixed; the final campaign exports journal and executor observations |
| Malformed Solana batch member | Fail only the invalid position; keep valid/null neighbors intact | HTTP fixtures cover duplicates, cancellation, invalid entries, and malformed envelopes |
| Slow archival lookup in a batch | Cap batches at 32 and provide a transport-preserving opt-out | 11 batcher fixtures pass; **production archival-provider qualification remains open** |
| Redis restart with an unacknowledged target | **Exact-target reattach remains refused** | Runbook requires fresh-namespace recovery/quarantine; intact idle reattach does not qualify pending-under-load recovery |
| Recovery and upgrade expectations | Document one-way schema migration, retained uncertainty, and 2 s/10 s EOA retry distinction | No automatic signed-attempt reconstruction, new replay identity, or in-place schema downgrade |

These are implementation and runbook dispositions. They do not turn the finite local benchmarks into provider qualification or a throughput guarantee.

## Completed checks

| Check | Result | Boundary |
| --- | --- | --- |
| Full local Rust workflow | All 10 steps pass | Formatting, workspace tests, explicit Redis/API suites, build, metric and formal-runner helpers |
| Explicit Redis/API tests | 163 pass | 47 journal, 73 executor, 4 Solana admission, 3 HTTP API, 36 queue |
| Solana batching HTTP fixtures | 11 pass | Included in workspace tests; no double count |
| Projection models | 12 expected outcomes match | 2 positive, 6 deliberate faults, 4 witnesses |
| Source guards | All 88 hashes match | Verified at both revisions; change-review guards, not a proof of Rust refinement |
| Final shutdown follow-up | All 5 checks pass | Formatting, 33 core unit tests, 49 journal tests, exact terminal retry, build at `319035e` |
| Final focused projection models | 12 expected outcomes match | Same subset rerun at `319035e`; kept separate from full CI |
| Final CI | All five checks and Devin Review pass | Exact final head; workflow URLs and completion times in the [manifest](validation.json) |
| [Final protocol CI](formal-319035e.json) | 73 expected outcomes match | 16 positive, 37 deliberate faults, 11 boundary, 9 witnesses; all 88 source hashes verified |
| [Final fee-proof CI](formal-319035e.json) | 5 harnesses proved; 2 mutations rejected | Three source hashes verified; exact artifact hashes retained |
| [Final recovery reruns](process-319035e.json) | Both pass | Live Redis restart/graceful shutdown and Solana lost-response recovery at `319035e` |
| [Recovery process scenarios](process-b6a5704.json) | Nine pass | Engine/Redis crash, live Redis outage, reverted execution, quarantine, reorg, Solana lost send response |

The fairness regressions exercise queued and cancelled permit owners, live overflow waiters, bounded unscoped health, and an actual admission/drain lock inversion. Solana fixtures cover isolated malformed positions, batch and concurrency limits, cancellation, shutdown, and disabled coalescing through the same bounded transport.

The process scenarios preserve exact effects without duplication. The live Redis-outage case fences execution, exits zero on SIGTERM, reattaches safely, and completes 24 original effects. Solana recovers 12 effects through 12 identical-wire retransmissions after lost send responses. The final formal CI independently passes all 73 protocol cases and the fee proofs. Queue, coverage, Rust correctness, and review-bot CI also pass at the exact final head. The local Redis suite uses AOF every second; it is not an AOF-always stress result.

## Additional shutdown boundary

Review found that a health caller waiting before registration could start a repair after a reusable drain barrier returned. Revision `319035e` permanently closes registration during server shutdown and still waits for already owned repairs. Tests pass for queued health callers released after close, unchanged SQL/Redis checkpoints, retained acknowledgement after caller cancellation, repeated barriers, and reopening a fresh journal. The final binary retains the same FULL durability settings, two unscoped health slots, and enabled 32-signature batches. Both final process reruns pass. The live Redis restart completes all 24 original effects without duplication and exits zero on SIGTERM; Solana completes 12 effects after 12 identical-wire retransmissions, with no retained terminal attempt records. The earlier full suite and nine scenarios remain attached to `b6a5704`.

## Matched 32 TPS run

[Final aggregates](candidate-319035e-steady.json) and the [strict assessment](candidate-319035e-steady-strict.json) retain `319035e` provenance. This was 300 seconds at 8/8/16 TPS, with 60 seconds of warmup, a 120-second late window and a 600-second queue lease. Every offer received HTTP 202: 2,400 per EVM fixture and 4,800 Solana transactions. Independent observations show **9,600 successful finalized canonical effects, one per intent**, no duplicates or missing effects, and all nine drain counters at zero. There were no client capacity drops, reported errors, journal halt or operator recovery.

Both this run and the [previous runtime](../pr2-2026-10-12/candidate-4320ea8-steady.json) pass the campaign's finite limits. They do not confirm sustainable capacity. The older runtime is `4320ea8` with review head `0ad0453`; it used a 256-signature batch cap. The final runtime uses 32, a separate unscoped health lane, and permanent shutdown closure. These are two individual matched runs, not an isolated statistical comparison of each change.

### Rates, backlog and latency

Each cell shows **previous → final**. Rates and backlog use the late load window; drain is excluded. Durable latency values are sampled upper bounds over the completed campaign, including drain.

| Fixture | Attempt / terminal TPS | Unsigned backlog | Terminal backlog | Admission p99 ms | Attempt / terminal p99 s |
| --- | --- | --- | --- | --- | --- |
| Anvil, 0.25 s blocks | 7.984 / 7.876 → 7.968 / 7.901 | 7→8 / 3→7 | 24→38 / 39→51 | 70.447 → 68.716 | 6.376 / 18.003 → 6.512 / 11.262 |
| Anvil, 2 s blocks | 8.017 / 8.001 → 8.043 / 7.926 | 6→4 / 7→2 | 79→79 / 78→87 | 62.940 → 64.096 | 6.086 / 16.462 → 6.214 / 16.458 |
| Local Solana | 16.001 / 16.068 → 16.002 / 16.002 | 1→1 / 1→1 | 287→279 / 289→289 | 638.433 → 709.530 | 5.898 / 23.778 → 6.027 / 23.720 |

Solana admission p99 increases by 71 ms in this comparison; the review fixes do not demonstrate an overall latency improvement. The two-second Anvil fixture only represents slower block cadence, not OP sequencing or settlement.

The unchanged strict screen uses the full 240-second post-warmup interval and eight complete 30-second groups. All three results remain `unconfirmed_growth_or_cadence_ambiguity`, with no persistent-growth stage detected:

- Fast EVM: unsigned backlog grows by six and terminal backlog by twelve across interval endpoints; some stage rates fall below offered rate beyond the one-transaction allowance.
- Slower EVM: terminal aligned-mean slope is 0.0133 TPS, above the 0.01 threshold, despite unchanged terminal endpoints.
- Solana: unsigned aligned-mean slope is 0.0180 TPS, terminal endpoints grow by three, and terminal rate is slightly below the allowance.

Actual per-sample block heads were not recorded for cadence alignment. The earlier longer 40 TPS run still failed; this review rerun does not erase that result or establish a maximum.

### Waiting and storage cost

Mean waits below are **lane + global journal mutex + health-capacity**, in milliseconds per observation. A missing capacity term means that phase does not apply. Totals sum phase means, not end-to-end request latency.

| Operation | Previous mean wait | Final mean wait |
| --- | --- | --- |
| Admission | 83.415 + 2.086 = **85.501** | 83.614 + 2.079 = **85.693** |
| Broadcast | 37.504 + 2.150 = **39.654** | 30.762 + 2.031 = **32.793** |
| Terminal | 130.018 + 2.423 = **132.441** | 130.352 + 2.783 = **133.135** |
| Health | 113.904 + 1.451 + 0.000271 = **115.355** | 114.569 + 1.536 + 0.000274 = **116.105** |

Both timing windows are about 240 seconds. The serialized journal is occupied for 201.221 seconds previously and 197.336 seconds finally: approximately 83.8% and 82.2%. This leaves limited local storage headroom. Lower broadcast wait does not establish an aggregate fairness or throughput improvement.

| Measured phase | Previous total / mean | Final total / mean |
| --- | --- | --- |
| SQL commit | 157.631 s / 3.408 ms | 153.034 s / 3.312 ms |
| Database execution | 173.795 s / 0.421 ms | 169.313 s / 0.413 ms |
| Database mutex wait | 37.378 s / 0.091 ms | 34.997 s / 0.085 ms |
| Blocking-pool wait | 4.914 s / 0.012 ms | 4.842 s / 0.012 ms |
| Redis mirror | 1.824 s / 0.079 ms | 1.920 s / 0.083 ms |
| Redis continuity observation | 22.866 s / 0.108 ms | 23.324 s / 0.111 ms |
| SQL acknowledgement | 81.565 s / 3.527 ms | 78.672 s / 3.405 ms |

The final run records 46,208 SQL commits. Acknowledgement includes commits already counted in SQL commit time; these totals overlap and must not be added as independent costs. Aggregate waiter time can exceed wall time because many callers wait concurrently.

### Solana polling

In the timing window, 65,810 logical status polls use 11,806 HTTP batches, averaging 5.574 signatures per batch (previously 66,568 / 12,066 = 5.517). Mean coordinator wait is 3.687 ms; mean logical status-poll duration is 4.561 ms. The separate proxy counter window records one fewer status request because its samples differ by scrape duration.

Whole-campaign Solana RPC calls fall from 29,369 to 29,104, or 6.119 to 6.063 HTTP requests per accepted intent. Status requests fall from 3.090 to 3.031 per accepted intent. These observations do not qualify production archival-provider latency or demonstrate that the smaller cap caused the reduction.

## Crash rerun

The [final crash campaign](candidate-319035e-crash.json) runs 90 seconds at 8/8/16 TPS, with a 10-second queue lease. Engine is killed after the Solana proxy accepts 200 unique signed transactions, then restarts. The requested fault is observed and validated. All 2,880 original requests return HTTP 202; all 2,880 intents reach exactly one successful finalized canonical execution. EVM nonce/effect counts are 720 per fixture; Solana completes 1,440 effects. All nine drain counters end at zero, without journal halt, operator recovery or reported errors.

Four terminal intents are retried with the same IDs; all return HTTP 202 and cause **zero additional sends** on every chain. Ambiguous-response retry is enabled but not exercised because no initial response is lost in this run. The separate final Solana process scenario covers accepted-response loss and identical-wire recovery.

The harness exits 2 because chaos is excluded from capacity qualification, while safety and liveness pass. No timing counters are subtracted across the restart. This crash profile's 10-second lease differs from the steady run's 600-second lease.

## Evidence limits

This directory contains allowlisted aggregates only. Raw observations, signed bytes, transaction identities, credentials, endpoints and local process paths remain private. Missing observations are not replaced with zero.

Anvil at a two-second cadence does not qualify OP sequencing or L1 settlement, and these fixtures do not qualify Nitro or public-cluster throughput. Finite protocol models and selected arithmetic proofs do not prove the entire service or a sustainable throughput maximum.
