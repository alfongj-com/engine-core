# Solana polling cost

**Status:** whole-second confirmation pacing is configurable; the default remains
one queue second. The two-second experiment is not yet qualified.
**Historical measurements:** Solana Devnet, file-backed signer, System transfers,
finalized commitment; September 17, 2026 EDT (September 18 UTC).

## Decision

Preserve recovery guarantees and measure pacing before changing the default.
`APP__QUEUE__SOLANA_CONFIRMATION_POLL_INTERVAL_SECONDS` accepts whole seconds
from **1 to 5**, default **1**. The previous 200 ms delay already became one queue
second in TWMQ; it did not cause five polls per second. Integer timestamps, RPC
work and queue pressure affect actual dispatch intervals.

The setting changes ordinary retries after an accepted send, a visible provisional
status, and a pending attempt inside its broadcast throttle. First send remains
immediate. Network errors retain two seconds, lock retries retain their prior
queue delay, and unresolved recovery remains parked for one hour. Finalized
status plus the matching receipt, exact signed identity, journal/lease fences
and SQL-before-Redis completion are unchanged. No cache or batching is introduced.

The limits remain 500 reconciliation checks and 20 broadcasts, with at least two
seconds between broadcasts. Slower polling extends the elapsed observation window
before count-budget parking and can delay retransmission until a blockhash expires.
Expiry is still based on finalized chain height/validity, followed by historical
reconciliation; it never authorizes a new signature. Completion/webhook latency
can increase. These are liveness tradeoffs, not relaxed safety or larger budgets.

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

The new Redis/RPC regression follows processed status, same-wire retransmission,
and matching finalized receipt, checking the configured delay and finite counters.
The actual configuration and Redis-backed recovery regressions passed locally.
Actual validator crash/lost-response checks and paired measurements remain
required before selecting a different default.

Compare one- and two-second settings with the same binary, worker count and
durability. Start with the existing 50/60 TPS controls and experimental 75/100 TPS;
compare offered/admitted/attempted/finalized rates, unsigned and confirmation
backlog trends, RPC counts, p99 and exact effects. A drained workload alone does
not establish sustainable capacity. Keep the one-second default unless repeated
measurements support a change. Any further public-chain run needs its own bounded
transaction/RPC budget; the historical small sample does not justify one.
