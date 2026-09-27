# Solana polling: measure first, then coalesce reads

Date: 2026-09-27. Proposal for `crash-recovery-and-chain-fairness`; no batching is implemented yet.

## Observed implementation and baseline

Each retained Solana attempt performs one `getSignatureStatuses` request containing one signature and `searchTransactionHistory=true`. Provisional status requeues at the configured interval. Only a finalized status proceeds to a separate finalized `getTransaction`; the worker checks the first signature, slot, error consistency and durable attempt ownership before recording terminal evidence. Null/expired history does not permit fresh signing.

The prior local60 TPS transfer screen used exactly7 Engine calls per intent (roughly420 RPC/s); this suggests measurable polling overhead, not proof that transport is the throughput bottleneck. The new baseline must retain the unchanged polling cadence, durability and workload while collecting method counts, durations, cancellation and handler elapsed time alongside root's journal/Redis wait measurements.

## Instrumentation now

- At the existing single-attempt HTTP sender: fixed method/cluster labels, elapsed time and success/error/cancelled outcome. A histogram counts requested signatures, never signature values.
- At the Solana handler: one elapsed timer covering journal validation, lock acquisition/release, status/receipt reads, signing and send work. Early errors are errors; dropping the future records cancellation.
- Never label URLs, request IDs, payer addresses, signatures, error text or provider data. Histogram sums across concurrent requests measure accumulated wait, not wall-clock critical-path time.
- SDK response decoding and worker consistency checks can fail after an HTTP-level success; handler outcomes expose that distinction. No retry, scheduling, fee, identity or lease behavior changes in this phase.

## Candidate after baseline: bounded request coalescing

Use one coordinator per cached cluster+endpoint, and only merge identical method/configuration (`getSignatureStatuses`, history=true). Preserve FIFO request entries and exact request-index to response-index mapping; do not cache statuses, deduplicate identities or share different history policies. A response with the wrong count, malformed envelope or invalid context fails the affected batch without inventing nulls. The existing worker still validates status consistency and independently fetches the finalized receipt.

Prototype bounds for review: maximum256 signatures/request, short5ms collection delay, bounded pending-request queue and bounded batches in flight. Full queues return a normal retryable error before any chain mutation. Cancellation before dispatch removes the waiting entry; cancellation after dispatch drops only that consumer's result. One cancelled waiter must not cancel peers, and actor shutdown must fail queued waiters instead of stranding them. Metrics distinguish logical polls, actual HTTP batches, batch size and coordinator wait.

Do not release or transfer a transaction lease through the coordinator. The existing process timeout still bounds each worker attempt; journal/lease checks remain authoritative before send and terminal cleanup. Batch errors consume each participating caller's existing logical reconciliation attempt; no hidden transport retry or expanded lifetime budget.

## Required evidence before keeping it

1. Real HTTP fixture: mixed null/provisional/finalized/error statuses map to original callers under out-of-order arrivals, including duplicate signatures and cancelled waiters.
2. Exact256 boundary, queue saturation, timeout/shutdown and malformed-length batch failures; no lost waiter or unbounded batch.
3. Different endpoints/clusters/history settings never combine. Secrets remain absent from errors/metrics.
4. Existing real-Redis finality/expiry/lease/cancellation/journal regressions pass unchanged; corrupt status cannot bypass finalized receipt/identity proof.
5. Matched local baseline/candidate measure HTTP statuses per intent, actual batch distribution, handler/journal waits, admission/terminal latency and exact effect reconciliation. Keep only if measured savings justify added coordination; do not assume RPC savings equal TPS gains.

## Primary sources and limits

Checked2026-09-27: [Solana getSignatureStatuses](https://solana.com/docs/rpc/http/getsignaturestatuses) specifies a maximum256 signatures and recent-cache lookup unless history is requested. [Agave implementation linked by that reference](https://github.com/anza-xyz/agave/blob/v3.1.8/rpc/src/rpc.rs#L1542) builds returned statuses in supplied-signature order and checks history availability. This reference version is not a claim about every provider's deployed binary or retention policy.

[Solana getTransaction](https://solana.com/docs/rpc/http/gettransaction) supports explicit commitment and can return null when a transaction is unavailable at that commitment. A history request is not an archival-availability or non-execution guarantee. The existing fail-closed expiry and finalized receipt checks therefore remain necessary.
