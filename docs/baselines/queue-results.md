# Queue baseline and controlled comparisons — 2026-09-15

**The upstream queue completed about 10,000 unique jobs/second in this local configuration. Waking the worker when execution permits become available raised completion throughput to 26,627/s in the highest-load screen.** Lease/transaction hardening alone preserved the upstream throughput. Across all three versions, 1,305,000 admitted jobs reconciled correctly. These are Redis-committed queue measurements, not blockchain TPS or power-loss durability.

## Unchanged upstream results

Each main workload ran three times for five seconds, after an excluded two-second warmup. Ranges show the three measured runs. Saturation screens ran once for three seconds.

| Offered jobs/s | Workload | Unique completions/s, including drain | p50 latency | p99 latency | Largest sampled backlog |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,000 | Normal | 994.9–995.2 | 13.4–13.8 ms | 23.0–23.2 ms | 19 |
| 5,000 | Normal | 4,973.0–4,974.7 | 13.1–13.7 ms | 22.3–22.8 ms | 70 |
| 10,000 | Normal | 9,940.9–9,944.6 | 14.5–14.6 ms | 23.7–23.9 ms | 140 |
| 1,000 | Every tenth job retries once | 995.0–995.1 | 13.8–14.1 ms | 32.2–32.3 ms | 14 |
| 20,000 | Saturation screen | 9,966.1 | 1.51 s | 2.98 s | 29,932 |
| 40,000 | Saturation screen | 9,991.8 | 4.51 s | 8.92 s | 89,900 |

All **435,000 admitted jobs** reached unique success with matching stored result/checksum and exactly one committed Redis effect. There were no generator drops, admission errors, terminal failures, missing records, duplicate success entries, or unresolved jobs. The retry workload produced exactly 1,500 additional handler attempts. These checks cover the exercised workload; they do not establish correctness under crashes or failover.

At 20,000 offered jobs/s, admission of 60,000 jobs took 3.00 s and completion took 6.02 s. At 40,000/s, 120,000 jobs took 12.01 s to complete. **Fast admission hides overload unless queue depth and completion latency are measured.**

## Matched changes

Both comparisons used the **identical harness, dependency lock, Redis instance and workload settings**. Each version ran the same 14 measured workloads, with 435,000 unique successes, no missing/corrupt results or duplicate committed effects, no generator drops, and exactly the expected 1,500 retry attempts. Queue source patches and hashes are recorded below. The fork's later dependency upgrades are outside this controlled comparison.

1. **Hardening:** isolate Redis transactions from shared connections and fence lease ownership. Completion rates remained similar to upstream; at 10,000 offered jobs/s, p99 increased from 23.7–23.9 ms to 24.6 ms. This short experiment does not establish a statistically significant latency difference.
2. **Hardening plus refill:** notify the worker when completed executions release permits; retain the existing poll timer for idle/delayed work. Concurrency remains 100 and the timer remains 10 ms. This isolates the scheduling change from a concurrency increase.

| Offered jobs/s | Workload | Refill completions/s, including drain | Refill p50 | Refill p99 | Largest sampled backlog |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,000 | Normal | 995.3–995.7 | 8.5–8.7 ms | 17.6–17.9 ms | 9 |
| 5,000 | Normal | 4,974.6–4,997.8 | 8.3–8.4 ms | 17.6–18.1 ms | 55 |
| 10,000 | Normal | 9,990.3–9,993.0 | 7.2–7.9 ms | 17.2–18.1 ms | 100 |
| 1,000 | Every tenth job retries once | 995.4–995.6 | 8.5–8.7 ms | 17.2–17.3 ms | 9 |
| 20,000 | Saturation screen | 19,972.9 | 7.2 ms | 13.0 ms | 40 |
| 40,000 | Saturation screen | 26,627.4 | 1.00 s | 1.50 s | 47,506 |

At 20,000/s, refill drained 60,000 jobs in 3.00 s instead of 6.03 s after hardening alone. At 40,000/s it drained 120,000 jobs in 4.51 s instead of 12.02 s: **2.67× full-run throughput**, while still accumulating a substantial backlog. The high-load screens each ran once; 26,627/s is not a qualified sustainable capacity or a production target. The result supports the diagnosis that waiting for the next 10 ms poll constrained upstream's 100 available execution slots.

Process CPU time (user + system) for hardening versus refill was 4.16 versus 4.01 s at the 20,000/s screen and 8.15 versus 7.09 s at 40,000/s; 10,000/s repeated cases were 3.58–3.64 versus 3.31–3.66 s. Peak RSS stayed comparable. These resource measurements include the load generator and final JSON serialization, exclude Redis CPU, and come from a shared desktop; they support no full-system efficiency claim.

## Method and limits

- Source: upstream `b6b7a0bbdc737b3a2b09611305b71b1bf6aba6e8`. Every `twmq/src` file matches that revision byte for byte; [verification](queue-2026-09-15/source-verification.json). The isolated source archive narrowed workspace membership to `twmq` to avoid the unavailable Vault dependency. Only the harness was added.
- Apple M4, 10 logical CPUs, 16 GiB RAM, macOS 26.6.2, Rust 1.98.1, release build. Redis 7.4.2 standalone on loopback, persistence disabled, no eviction. A shared desktop Chrome process used approximately one CPU core; Rust builds were paused during measurement. [Environment and exact hashes](queue-2026-09-15/environment.json).
- One worker process, four Tokio threads, local concurrency 100, 10 ms polling, 30 s leases, permanent idempotency. Retention exceeded the run size, so pruning was not exercised. Independent scheduled arrivals, at most 128 concurrent producers. 256-byte deterministic payload, checksum result, two Redis hook writes per success.
- Completion means a hook record is visible after the transaction committing the success state. The harness reconciles that record against success IDs, stored outputs, and per-ID effect counts. It measures from the beginning of `push` to observed completion. The 10 ms observer poll and its scheduling/RPC latency add observation delay; these are not exact server commit timestamps.
- Full-run throughput uses the monotonic elapsed time through drain, excluding final audit, cleanup and JSON serialization. Maximum process RSS ranged approximately 18–331 MiB and includes the harness and its per-job samples; it is not queue-only memory usage. `/usr/bin/time -l` files retain process CPU/resource measurements.
- These short runs are an initial baseline. They exclude disk persistence, Redis failover, TLS/network distance, signing, actual RPC calls, transaction fees, on-chain execution/finality, multiple processes, and long-running pruning. No production chain was load tested.

## Reproduce

Use a dedicated disposable Redis instance. Build the harness against the chosen source tree:

```sh
cargo build --release -p twmq --example queue_baseline
BENCH_REDIS_URL=redis://127.0.0.1:16379/ BENCH_RATE=10000 BENCH_SECONDS=5 \
  target/release/examples/queue_baseline > result.json
```

Set `BENCH_RETRY_EVERY=10` for the retry case. The screens use `BENCH_SECONDS=3` and rates 20000/40000. Other optional settings: `BENCH_CONCURRENCY=100`, `BENCH_PRODUCER_CONCURRENCY=128`, `BENCH_DRAIN_SECONDS=30`. Repeat the four main workloads three times; discard a preliminary two-second 1000/s warmup. Stop competing build/load processes.

To reproduce the upstream comparison, export that revision, copy [the harness](../../twmq/examples/queue_baseline.rs), restrict workspace members to `twmq`, and use the [recorded lockfile](queue-2026-09-15/queue-Cargo.lock). For the second or third version, apply the corresponding patch below to that upstream export with `git apply` before building. The retained old lockfile exists only for experimental reproducibility; deployment should use the maintained fork lockfile. Never run the load test against a production Redis instance.

## Evidence and next experiments

| Version | Per-run results | Exact source changes / provenance |
| --- | --- | --- |
| Upstream | [Summary](queue-2026-09-15/summary.json), [environment](queue-2026-09-15/environment.json) | [Byte comparison with upstream](queue-2026-09-15/source-verification.json) |
| Hardening | [Summary](queue-hardened-2026-09-15/summary.json), [environment](queue-hardened-2026-09-15/environment.json) | [Patch](queue-hardened-2026-09-15/queue-hardening.patch), [hashes](queue-hardened-2026-09-15/source-verification.json) |
| Hardening + refill | [Summary](queue-refill-2026-09-15/summary.json), [environment](queue-refill-2026-09-15/environment.json) | [Patch against upstream](queue-refill-2026-09-15/queue-hardening-and-refill.patch), [hashes](queue-refill-2026-09-15/source-verification.json) |

Each readable run report names its adjacent `.raw.json.gz` with all per-job observations and the SHA-256 of its decompressed JSON. Each directory includes process resource logs. All results are retained, including the overloaded screens.

Next: qualify sustained capacity with longer repeated loads and stable backlog; measure Redis CPU and disk persistence; vary concurrency one factor at a time. Add multiple workers, uneven handler latency, crash/failover and pruning workloads before using queue durability claims. Local EVM transaction benchmarks need a separate report with actual contract effects and receipt semantics.
