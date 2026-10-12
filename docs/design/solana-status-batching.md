# Solana polling: measure first, then coalesce reads

Updated: 2026-10-12. Bounded coalescing is implemented on `crash-recovery-and-chain-fairness`; matched workload validation is still pending. Instrumentation-only baseline: `01d5048`.

## Observed implementation and baseline

Each retained Solana attempt performs one `getSignatureStatuses` request containing one signature and `searchTransactionHistory=true`. Provisional status requeues at the configured interval. Only a finalized status proceeds to a separate finalized `getTransaction`; the worker checks the first signature, slot, error consistency and durable attempt ownership before recording terminal evidence. Null/expired history does not permit fresh signing.

The prior local 60 TPS transfer screen used exactly 7 Engine calls per intent (roughly 420 RPC/s). The October 12 shared baseline recorded 36,872 status requests for 3,000 Solana intents. Its approximately 0.5 ms mean status HTTP duration does not identify RPC latency as the throughput bottleneck; shared journal contention must be assessed separately. Compare the candidate with unchanged polling cadence, durability and workload, and collect method counts, cancellation, handler elapsed time and journal/Redis waits.

## Instrumentation now

- At the existing single-attempt HTTP sender: fixed method/cluster labels, elapsed time and success/error/cancelled outcome. A histogram counts requested signatures, never signature values.
- At the Solana handler: one elapsed timer covering journal validation, lock acquisition/release, status/receipt reads, signing and send work. Early errors are errors; dropping the future records cancellation.
- Never label URLs, request IDs, payer addresses, signatures, error text or provider data. Histogram sums across concurrent requests measure accumulated wait, not wall-clock critical-path time.
- SDK response decoding and worker consistency checks can fail after an HTTP-level success; handler outcomes expose that distinction. No retry, scheduling, fee, identity or lease behavior changes in this phase.

## Implemented coordination

[`status_batcher.rs`](../../executors/src/solana_executor/status_batcher.rs) wraps the existing single-attempt HTTP sender in each cached cluster+endpoint client. Only canonical singleton `getSignatureStatuses` calls with exactly `searchTransactionHistory=true` coalesce. Other methods, multi-signature requests, different history policy and unknown options pass through unchanged. The cache key isolates both endpoint and expected cluster, even when two cluster configurations use the same URL.

FIFO entries retain duplicates and map position-for-position to response values. There is no status cache. Before fanout, the entire response must decode as typed statuses with an unsigned context slot, exact response count, each status slot no greater than the context, and matching legacy/current error fields. Malformed responses fail all affected polls; no missing value becomes an invented null. Context API version text is capped at 128 bytes before copying to callers, preventing amplification of provider-controlled metadata. Numeric provider error codes survive, while provider text and parse diagnostics remain redacted.

Limits per cached sender:

| Resource | Bound |
| --- | --- |
| Total admitted logical polls, queued plus in flight | 256 |
| Signatures in one HTTP batch | 256 |
| Concurrent status HTTP batches | 4 |
| Collection window from first waiting entry | 5 ms; a full batch flushes earlier |

The collection deadline does not restart as new entries arrive. Waiting can exceed 5 ms when all HTTP slots are occupied or the runtime is not scheduled. Saturation returns a normal retryable error before any chain mutation. Cancelled entries are removed before dispatch; cancellation after dispatch drops only that consumer's result. In-flight requests retain their capacity permits through HTTP completion, so cancellations cannot bypass the bound. Sender shutdown aborts its actor and drops queued/in-flight reply channels, waking remaining consumers with retryable errors. No actor-owned sender cycle or unbounded per-poll task spawning is used.

`status_logical_poll` counts callers including errors/cancellations; `status_coordinator_wait` measures admission-to-dispatch wait. Existing `rpc_get_signature_statuses` timing and the signature-count histogram now describe actual HTTP batches. All metric dimensions are fixed operation/cluster/outcome labels; signatures and endpoint URLs never appear. HTTP success can precede batch-validation failure, which appears in logical-poll and handler outcomes.

The coordinator never releases or transfers a transaction lease. The existing process timeout bounds each worker attempt; journal/lease checks remain authoritative before send and terminal cleanup. Batch errors consume each participating caller's existing logical reconciliation attempt; there is no hidden transport retry or expanded lifetime budget. Processed admission/preflight and finalized terminal receipt checks are unchanged. Null/expired history remains insufficient to re-sign an intent.

## Required evidence before keeping it

1. Real HTTP fixtures in [`status_batcher_tests.rs`](../../executors/src/solana_executor/status_batcher_tests.rs): mixed null/provisional/finalized statuses map to original callers, including duplicate signatures and cancelled waiters.
2. Exact256 boundary, queue saturation, timeout/shutdown and malformed-length batch failures; no lost waiter or unbounded batch.
3. Different endpoints/clusters/history settings never combine. Secrets remain absent from errors/metrics.
4. Existing real-Redis finality/expiry/lease/cancellation/journal regressions pass unchanged; corrupt status cannot bypass finalized receipt/identity proof.
5. Matched local baseline/candidate measure HTTP statuses per intent, actual batch distribution, handler/journal waits, admission/terminal latency and exact effect reconciliation. Keep only if measured savings justify added coordination; do not assume RPC savings equal TPS gains.

## Primary sources and limits

Rechecked 2026-10-12: [Solana getSignatureStatuses](https://solana.com/docs/rpc/http/getsignaturestatuses) specifies a maximum 256 signatures and recent-cache lookup unless history is requested. [Agave implementation linked by that reference](https://github.com/anza-xyz/agave/blob/v3.1.8/rpc/src/rpc.rs#L1542) builds returned statuses in supplied-signature order and checks history availability. This reference version is not a claim about every provider's deployed binary or retention policy.

[Solana getTransaction](https://solana.com/docs/rpc/http/gettransaction) supports explicit commitment and can return null when a transaction is unavailable at that commitment. A history request is not an archival-availability or non-execution guarantee. The existing fail-closed expiry and finalized receipt checks therefore remain necessary.
