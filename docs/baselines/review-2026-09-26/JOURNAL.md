# Durable journal service-time probe

## Result

The isolated journal completed the equivalent of **53–57 three-stage intents/s**
on this local machine. Increasing closed-loop concurrency from 1 to 8 mostly
increased lock waiting. This is an attribution measurement using an **unoptimized
Rust test binary**, not Engine transaction throughput or a production capacity
limit.

| Run order | Concurrency | Admission ops/s | Attempt ops/s | Terminal ops/s | Three-stage intents/s |
|---|---:|---:|---:|---:|---:|
| [1](journal-c1-run1.json) | 1 | 172.8 | 153.4 | 153.3 | 53.1 |
| [2](journal-c8-run2.json) | 8 | 184.3 | 165.0 | 164.6 | 56.9 |
| [3](journal-c8-run3.json) | 8 | 169.9 | 164.5 | 168.0 | 55.8 |
| [4](journal-c1-run4.json) | 1 | 163.3 | 161.5 | 163.9 | 54.3 |

At concurrency 1, phase p50 method completion latency was 5.6–6.5 ms; at
concurrency 8, it was 43–49 ms, including waiting for the shared journal lock.
The three-stage rate is `256 / (admission time + attempt time + terminal time)`.
Each phase runs separately; this does not simulate mixed Engine traffic.

## Method and reconciliation

The [probe source](../../../core/src/recovery/benchmark.rs) uses the actual
journal APIs and SQLite WAL / synchronous FULL / fullfsync ON. Each run creates
a fresh private ledger and isolated Redis namespace. Redis 7.4.2 uses loopback,
no RDB saves, and no AOF. No compiler, Engine load test, or validator test ran
concurrently. The order was reversed for the second pair to expose simple
ordering effects; there are only two observations per concurrency.

Each run submitted 256 admissions, durably recorded one precomputed signed EOA
attempt per admission, then recorded one synthetic terminal witness per attempt.
All four runs reconciled exactly **256 admissions, 256 attempts, 256 terminal
records, checkpoint 768, healthy**. Combined canonical admission/attempt/witness
JSON was 1,178 bytes per intent, excluding SQL/index/WAL overhead. Chain evidence
is synthetic; no broadcast, public RPC, or funds were involved.

[Metadata](journal-probe-metadata.json) records the binary hash, source hashes,
profile, platform, and measurement interval. The parent Git commit is recorded
with `workingTreeChanges: true`; the hashes identify the measured implementation.

## Interpretation and limits

- The global journal serializes wallets and chains. More callers cannot remove
  its durable write bottleneck. These results do not support a claim of 50 TPS
  independently across several chains sharing this ledger.
- Timing includes SQL work, FULL synchronization, Redis continuity checks and
  checkpoint mirroring. It does not isolate disk flush time alone.
- The unoptimized test profile, 256-row fresh databases, short runs and local
  storage limit generalization. This does not measure a large retained ledger,
  long-run WAL growth, production fsync latency, or release Engine capacity.
- Signature preparation, queue work, RPC, chain execution, finality assessment,
  repeated checks and receipt polling are excluded. Finality checkpoints can
  require additional durable mutations in the real Engine.
- FULL durability was not weakened. Multi-chain capacity must be
  measured on production storage. Faster durable storage, a separately designed
  bounded group-commit protocol, or authority partitioning are candidates; each
  still needs crash/rollback qualification. A higher intake cap alone is insufficient.

## Reproduce

Use a disposable local Redis and a new output filename for each invocation:

```sh
TEST_REDIS_URL=redis://127.0.0.1:6385/ \
RECOVERY_BENCH_OUTPUT=/tmp/journal-c1.json \
RECOVERY_BENCH_COUNT=256 RECOVERY_BENCH_CONCURRENCY=1 \
cargo test -p engine-core recovery::benchmark::journal_throughput_probe \
  -- --ignored --exact --nocapture
```

For the recorded runs, the already-built test binary was invoked directly to
exclude compilation. Set concurrency to 8 for the second case. The probe allows
16–5,000 intents and 1–64 callers, requires loopback Redis, and never overwrites an
existing report. Without `RECOVERY_BENCH_OUTPUT`, correctness suites skip timing.
