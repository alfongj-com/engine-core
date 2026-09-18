# Solana polling cost

**Status:** measured recommendation; no polling change implemented.  
**Scope:** Solana Devnet, file-backed signer, System transfers, finalized commitment. Measurements ran September 17, 2026 EDT (September 18 UTC).

## Decision

Keep recovery behavior unchanged. Measure RPC dispatch timing before choosing a polling optimization. A small shared finalized-height cache is suitable for unknown-send recovery, but the successful runs below provide little evidence of savings during ordinary submission. Do not equate the worker's 200 ms retry constant with five polls per second: TWMQ maps positive subsecond delays to one queue second, uses integer timestamps, and RPC execution adds time.

## Observed baseline

All three runs passed exact finalized balance/fee accounting, distinct signatures per intent, duplicate-ID replay with no additional broadcasts, and terminal attempt cleanup. The first transfer funded the fresh recipient with the official RPC's current rent minimum, **650,240 lamports**. Remaining transfers were one lamport each.

| Run | Transactions / worker concurrency | Requested / observed paced admissions per second | Signature-status calls | Finalized-height calls | All gateway calls | Worker-start interval p50 / p95 |
| --- | --- | --- | --- | --- | --- | --- |
| [Baseline](../baselines/testnet-solana-write.json) | 10 / 2 | 1 / 0.979 | 66 | 1 | 123 | 0.929 / 1.678 s |
| [Engine crash](../baselines/testnet-solana-crash.json) | 10 / 10 | 1 / 0.966 | 16 upstream | 44 | 152 | 2.073 / 2.745 s |
| [Small ramp](../baselines/testnet-solana-5tps.json) | 20 / 20 | 5 / 5.009 | 44 | 0 | 150 | 3.503 / 5.188 s |

The crash proxy also answered status requests locally with null; its 16 upstream calls exclude those injected responses. It dropped 24 accepted send responses. All nine post-bootstrap intents retransmitted identical bytes after Engine SIGKILL/restart; 46 total send calls produced exactly ten effects, including the already-finalized bootstrap. Redis remained running with synchronous AOF.

Observed admission rates use intervals between the post-bootstrap HTTP admissions. Worker intervals come from per-intent process-start logs within each Engine lifetime; they include queue delay and HTTP/work duration and are **not direct RPC dispatch intervals**. Bootstrap waits, final verification, and sequential duplicate-ID replays are excluded from the offered rate. Reports retain source commit, binary hash, harness hash, receipt signatures, slots, fees, and latency scope. Private run directories retain AOF, manifests, keys, script snapshots, and logs.

These are small correctness runs, not sustained capacity measurements. Concurrency, provider conditions, and harness overhead differ. The policy proxy synchronously persists evidence, quotes each new signature's fee, and verifies finalized receipts separately. Engine accounted for 97 / 126 / 104 upstream calls; harness checks added 26 / 26 / 46. Three additional official free RPC calls queried rent. The 425 gateway calls total approximately $0.00255 at the campaign's conservative $6/million-call model; provider billing remains authoritative.

## Why height caching has a narrow benefit

[`execute_transaction`](../../executors/src/solana_executor/worker.rs) requests finalized block height only after historical status is absent. A visible processed/confirmed transaction awaiting finality returns earlier. This explains the baseline's one height read and the ramp's zero; the fault run's 44 reads are the useful optimization target.

A future cache should:

- Share only successful finalized-height observations for at most 500 ms, keyed by chain and configured endpoint. Coalesce simultaneous refreshes into one RPC call.
- Propagate refresh failures; never silently extend stale data. A stale lower height may delay parking or permit one more identical-byte resend, but must never authorize a new signature.
- Preserve historical reconciliation before expiry and again after expiry. Absent history remains an unknown outcome with retained signed bytes and admission identity.
- Test concurrent requests, expiry refresh, failed refresh, endpoint isolation, and exact `height > lastValidBlockHeight` behavior using a controlled clock and RPC stub.

## Status polling and batching

First instrument per-method dispatch timestamps, latency, in-flight count, and queue delay without logging credential URLs. Compare the same workload, concurrency, provider, and commitment across repeated runs. A 500–1000 ms retry constant alone may produce no reduction because of queue timestamp precision.

A later batch-status coordinator could amortize per-signature requests, but needs stronger tests than a cache: bounded batch size and wait time; strict positional response mapping and count validation; separation by chain/endpoint/history policy; cancellation and backpressure; bounded retry after partial or malformed responses; and safe behavior across worker lease loss. Status visibility must not become a terminal result before the requested commitment and matching receipt are verified. Existing persistence-before-send, identical-byte recovery, and atomic terminal cleanup remain the acceptance gates.

## Next gate

Use local deterministic fault tests to establish cache/coordinator correctness, then repeat a bounded public workload only after setting a fresh transaction and RPC budget. No optimization or extra public run is justified solely by this three-run sample.
