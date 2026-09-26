# Iterative production review — 2026-09-26

Local machine: Apple M4, 16 GiB RAM, macOS arm64, Rust 1.98.1. Engine binaries
use release optimization except the separate local debug journal probe and Linux
CI's debug builds. All transactions use disposable local funds and loopback RPCs. No paid
RPC credits were used for this review.

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

## Earlier iterations

| Report | What it establishes |
|---|---|
| `baseline-proxy-fault.json` | Discovery run against `5baa70b`. The original harness proxy had too small a connection backlog, causing receipt errors. It exposed the retained-old-receipt nonce rewind and stopped at 313 effects. This is fault evidence, not a clean throughput baseline. |
| `baseline-eoa-screen.json` | Proxy backlog corrected. Baseline effectively hardcodes a 50-nonce send window, ignoring the new requested 1024 setting. 3,000 accepted, 650 included at 60s; drain stopped before completion. This screen overlapped other local work and is not a controlled speedup comparison. |
| `baseline-solana-screen.json` | All 1,500 transfers eventually reconciled. Client workers backed up: admissions took 70.1s despite a 30s offered window. Earlier `during_load` was sampled after admission completion and latency excluded client queue time; later harness reports correct both. This is evidence of backlog, not 50 TPS. |
| `iteration-1-eoa-always.json` | Isolated revised binary, Redis AOF always: 3,000 accepted in 60s, 1,024 included then; all terminal at 110.2s. Large send cycles delayed confirmation work. |
| `iteration-2-eoa-delayed.json` | Isolated bounded worker cycles, Redis AOF everysec: 12,000 accepted in 240s, 11,908 included by the offered-window end; all 12,000 terminal at 317.1s after checkpoint release. 43,489 premature receipt reads motivated policy-head candidate selection. |

Each report records its exact binary SHA-256 and private log directory. Earlier
binaries intentionally differ from the final revision. Do not attach the final
source's correctness claims to them, or compare throughput across durability
profiles as if only the code changed.

## Journal service time

The [isolated journal probe](JOURNAL.md) measures the durable admission, attempt
and terminal phases separately. It establishes a local storage bottleneck; it
does not include Engine queue, signing, RPC or finality work.

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
