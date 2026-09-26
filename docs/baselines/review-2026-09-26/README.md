# Iterative production review — 2026-09-26

Local machine: Apple M4, 16 GiB RAM, macOS arm64, Rust 1.98.1. Engine binaries
use release optimization except the separate local debug journal probe and Linux
CI's debug builds. All transactions use disposable local funds and loopback RPCs. No paid
RPC credits were used for this review.

[All four Linux CI workflows pass](ci/SUMMARY.md) at the latest measured runtime,
`f309177`. CI debug workloads establish correctness and complete drains; the
release measurements below establish separate local performance evidence.

## Reading the measurements

`during_load` records progress at the end of the offered load window. Admission
responses may finish later; `admission_completion_seconds` makes that visible.
`final` includes a separate drain period. A report's `pass` means every offered
request was accepted and its expected effect and terminal record reconciled; it
does **not** by itself mean 50 sustained terminal TPS. Inspect the time series.

The EVM harness mines a local fixed-cadence Anvil chain with a generous block gas
limit, then advances blocks explicitly to release delayed finality during drain.
This isolates Engine capacity; it does not reproduce public consensus, fee
competition, provider quotas, sequencer nonce rules, or realistic contract gas.
The Solana harness uses an actual Agave 4.2.2 validator with one fee payer and one
writable recipient, and requests finalized commitment.

SQLite remains WAL / synchronous FULL / fullfsync ON in every run. Redis AOF
`always` and `everysec` are reported separately. `everysec` can lose recent Redis
writes on a host failure; the independent journal and quarantine requirements
still apply. Faster Redis is not a substitute for disaster recovery.

## Latest release: productive EOA scheduling

Runtime `f309177186e9f5215a1e0acd036bfba1bb15fa6c` immediately rejoins the
queue tail after useful nondelegated send/recovery progress when unsigned work
remains. Unknown-only, no-progress and finality-only cycles retain their delay.
The measured release is `a0b38a6444682de64cc9264f27f826cb14a4913692b5ed8e9cc837d360588ccc`;
[its manifest](progress-scheduling-source-hashes.json) hashes all 174 tracked Rust
and Cargo files. The [scheduling regression and model results](../../../formal/evidence/progress-scheduling/README.md)
record the safety checks separately from the performance measurements.

The [matched six-minute EVM run](progress-scheduling-eoa.json) used the same
50 requests/s, 12-second blocks, depth 2, one signer, 1,024 inflight window and
durability settings as the preceding six-minute run. Local compilation, model
checking and other node workloads were stopped during both runs.

| Measurement | Before scheduling change | After |
|---|---:|---:|
| Last-minute attempted TPS | 45.085 | 50.075 |
| Last-minute included TPS | 45.577 | 50.583 |
| Last-minute terminal TPS | 47.747 | 50.515 |
| Accepted requests lacking a signed attempt at load end | 505 | 12 |
| All 18,000 effects and terminal records reconciled | 385.218s | 375.124s |
| Admission p95 / p99 | 19.615 / 24.062ms | 19.611 / 23.575ms |

At 359.997s the revised run had 18,000 admissions, 17,988 attempts, 17,929
included transactions and 16,117 terminal records. It accepted every request
and ended with exactly 18,000 recipient wei and consumed nonces. Rates slightly
above 50 reflect block batches and drain of earlier work within the sampled
minute; offered load remains 50/s. This matched pair supports keeping the
scheduling change. It is one pair, not a statistical capacity estimate or a
qualification of every chain, transaction type, finality window or journal size.

Completed runs each made exactly 18,000 send, fee and receipt calls. The revised
run made 325 nonce-count calls versus 269 before; faster scheduling has a small
additional polling cost. Mixed progress/unknown cycles can also retry uncertain
wires sooner. No shared RPC rate limiter or lifetime EOA retry budget is added.

The [Solana rerun on the same binary](progress-scheduling-solana.json) offered
3,000 transfers over 60s. At 59.985s, 2,993 were admitted, 2,991 attempted, 2,180
effects finalized and 2,160 terminal. All admissions completed by 60.105s and all
effects/terminal records by 80.136s, with exactly 3,000,000 recipient lamports.
Admission p95/p99 was 199.706/248.064ms. Its reported 39.284 terminal TPS includes
startup; between the first nonzero terminal sample (20.009s, 175) and the load-end
snapshot, it averaged **49.655 terminal TPS**. This short post-startup interval
does not establish sustained public-cluster capacity. The EOA-only scheduling
change did not target the Solana executor.

## Release before the scheduling follow-up

Runtime source is `21e56c4`; `025a3d1` adds the CI load checks. The release
binary is `3cfc678585f598d8dea481ca2830de2b044b682f4c94e03eaa6eaabd602b2a9d`.
[Source and binary hashes](final-source-hashes.json) identify the measured build.

The [delayed-finality EVM run](final-eoa-delayed.json) offered 12,000 transfers
at 50/s for 240s. All admissions completed at 240.011s; the offered-window
snapshot had 11,951 included and zero terminal, as required while the synthetic
checkpoint remained delayed. Last-minute inclusion was 49.675/s. Admission
p50/p95/p99 was 13.692/19.376/24.259ms. All 12,000 effects and terminal records
reconciled by 317.351s after releasing the checkpoint, with no extra nonce or
recipient balance change. These are separate load and drain measurements.

The policy-head filter made **zero receipt reads during this delayed
window**, compared with 43,489 in the earlier iteration. Other changes also
separate these binaries; this is direct method-count evidence, not an isolated
speedup attribution. During-load nonce/fee/send reads remain in the raw report.

The [12-second-block EVM run](final-eoa-blocks.json) offered 6,000 transfers over
120s with depth 2 and inflight window 1024. At the offered-window end, 5,645 were
included and 4,146 terminal; all 6,000 completed at 140.098s. Last-minute rates
were 49.997 admitted, 47.240 attempted, 46.155 included and 49.980 terminal per
second. Completion can drain an earlier backlog while dispatch falls behind;
this is **not a uniform sustained 50 TPS pass across all stages**. Admission
p95/p99 was 19.365/23.423ms.

The longer [six-minute EVM run](final-eoa-sustained.json) used the same
12-second blocks, depth 2 and 1,024 inflight window. It offered 18,000 transfers;
all were admitted by 360.030s, with 17,495 attempted, 17,396 included and 15,732
terminal at 360.004s. Last-minute rates were **45.085 attempted, 45.577 included
and 47.747 terminal per second**. All 18,000 completed by 385.218s. Admission
p95/p99 was 19.615/24.062ms. This longer run confirms that dispatch fell behind
and motivated the progress-only scheduling follow-up.

The [single-payer Solana run](final-solana-throughput.json) offered 3,000 transfers
over 60s. Admissions completed at 59.993s; at 59.985s, 2,999 had signed attempts,
2,192 effects were finalized and 2,140 terminal records persisted. All 3,000
completed at 80.029s, with exactly 3,000,000 recipient lamports added. The reported
38.923 terminal TPS includes startup before finality; from the first nonzero
terminal sample (20.008s, 141) to the offered-window end (59.985s, 2,140), the
observed terminal rate was about 50.0/s. That short interval does not establish
long-run or public-cluster capacity. Admission p95/p99 was 201.028/247.708ms.

## Earlier iterations

| Report | What it establishes |
|---|---|
| `baseline-proxy-fault.json` | Discovery run against `5baa70b`. The original harness proxy had too small a connection backlog, causing receipt errors. It exposed the retained-old-receipt nonce rewind and stopped at 313 effects. This is fault evidence, not a clean throughput baseline. |
| `baseline-eoa-screen.json` | Proxy backlog corrected. Baseline effectively hardcodes a 50-nonce send window, ignoring the new requested 1024 setting. 3,000 accepted, 650 included at 60s; drain stopped before completion. This screen overlapped other local work and is not a controlled speedup comparison. |
| `baseline-solana-screen.json` | All 1,500 transfers eventually reconciled. Client workers backed up: admissions took 70.1s despite a 30s offered window. Earlier `during_load` was sampled after admission completion and latency excluded client queue time; later harness reports correct both. This is evidence of backlog, not 50 TPS. |
| `iteration-1-eoa-always.json` | Isolated revised binary, Redis AOF always: 3,000 accepted in 60s, 1,024 included then; all terminal at 110.2s. Large send cycles delayed confirmation work. |
| `iteration-2-eoa-delayed.json` | Isolated bounded worker cycles, Redis AOF everysec: 12,000 accepted in 240s, 11,908 included by the offered-window end; all 12,000 terminal at 317.1s after checkpoint release. 43,489 premature receipt reads motivated policy-head candidate selection. |

Each report records its exact binary SHA-256 and private log directory. Earlier
binaries intentionally differ from later revisions. Do not attach a later
source's correctness claims to them, or compare throughput across durability
profiles as if only the code changed.

## Journal service time

The [isolated journal probe](JOURNAL.md) measures the durable admission, attempt
and terminal phases separately in a debug build. It measures journal service time,
including SQL, serialization and Redis synchronization; it does not isolate
filesystem sync cost or establish a production limit. It excludes Engine queue,
signing, RPC and finality work.

## Limits

- No production-chain, multi-chain, KMS, account-abstraction or third-party RPC
  throughput qualification is established by these local runs.
- A four-minute delayed-finality run exceeds the old 10k cap but does not cover a
  complete Ethereum/rollup finality window or long settlement outage.
- Journal rows persist indefinitely. Row count, payload size, disk sync latency,
  backup duration and disk exhaustion require production capacity planning.
- The formal suite checks finite abstractions and selected production fee
  arithmetic. It does not prove throughput or whole-program correctness.

Design and provider/chain sizing: [50 TPS analysis](../../design/throughput-50tps.md).
