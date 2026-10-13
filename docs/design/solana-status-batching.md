# Solana polling: measure first, then coalesce reads

Updated: 2026-10-12. Bounded coalescing is implemented in PR #2. [Matched measurements and qualification limits](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/pr2-2026-10-12/README.md) compare it with instrumentation-only baseline `01d5048`.

## Observed implementation and baseline

Each reconciliation lookup requests one signature with `getSignatureStatuses` and `searchTransactionHistory=true`; the coordinator may combine concurrent lookups into one HTTP request. Provisional status requeues at the configured interval. Only a finalized status proceeds to a separate finalized `getTransaction`; the worker checks the first signature, slot, error consistency and durable attempt ownership before recording terminal evidence. Null/expired history does not permit fresh signing.

The matched local baseline recorded 42,213 status HTTP requests for 2,400 Solana
intents. Batching reduces actual HTTP calls, but shared-journal contention also
changes how often workers reach a poll. Report logical polls, batch size, actual
HTTP counts and terminal latency separately; a lower request count alone is not
a throughput or latency improvement. Keep polling cadence, durability and
workload fixed for the baseline comparison.

## Instrumentation now

- At the existing single-attempt HTTP sender: fixed method/cluster labels, elapsed time and success/error/cancelled outcome. A histogram counts requested signatures, never signature values.
- At the Solana handler: one elapsed timer covering journal validation, lock acquisition/release, status/receipt reads, signing and send work. Early errors are errors; dropping the future records cancellation.
- Never label URLs, request IDs, payer addresses, signatures, error text or provider data. Histogram sums across concurrent requests measure accumulated wait, not wall-clock critical-path time.
- SDK response decoding and worker consistency checks can fail after an HTTP-level success; handler outcomes expose that distinction. Instrumentation alone changes no retry, scheduling, fee, identity or lease behavior.

## Implemented coordination

[`status_batcher.rs`](../../executors/src/solana_executor/status_batcher.rs) wraps the existing single-attempt HTTP sender in each cached cluster+endpoint client. Only canonical singleton `getSignatureStatuses` calls with exactly `searchTransactionHistory=true` coalesce. Other methods, multi-signature requests, different history policy and unknown options pass through unchanged. The cache key isolates both endpoint and expected cluster, even when two cluster configurations use the same URL.

FIFO entries retain duplicates and map position-for-position to response values.
There is no status cache. The shared envelope must contain an unsigned context
slot and an array of exactly the requested length. A bad envelope, wrong length,
transport failure or request-level RPC error fails the whole batch. Context API
version text is capped at 128 bytes before copying to callers.

Each array entry then decodes and validates independently: its slot cannot exceed
the context and its legacy/current error fields must agree. An unknown transaction
error variant, invalid slot or malformed status fails only that caller's position;
healthy and null neighbors retain their original positions, including duplicate
signatures. No missing or invalid value becomes an invented null. Numeric RPC
error codes survive; provider text and parse diagnostics remain redacted.

Limits per cached sender with coalescing enabled:

| Resource | Bound |
| --- | --- |
| Total admitted logical polls, queued plus in flight | 256 |
| Signatures in one HTTP batch | 32 |
| Concurrent status HTTP batches | 4 |
| Collection window from first waiting entry | 5 ms; a full batch flushes earlier |

The collection deadline does not restart as new entries arrive. Waiting can exceed 5 ms when all HTTP slots are occupied or the runtime is not scheduled. Saturation returns a normal retryable error before any chain mutation. Cancelled entries are removed before dispatch; cancellation after dispatch drops only that consumer's result. In-flight requests retain their capacity permits through HTTP completion, so cancellations cannot bypass the bound. Sender shutdown aborts its actor and drops queued/in-flight reply channels, waking remaining consumers with retryable errors. No actor-owned sender cycle or unbounded per-poll task spawning is used.

The 32-signature cap is below the protocol maximum to limit how many callers
share a slow archival lookup. The transport still has a 15-second request timeout;
a slow member can delay its whole HTTP batch. This cap is not provider
qualification or a latency guarantee. The earlier local measurements used the
256-signature cap; they do not establish performance for this revised limit.

Set `APP__SOLANA__STATUS_BATCHING_ENABLED=false` and restart Engine to disable
coalescing for all configured Solana clusters. The YAML setting is
`solana.status_batching_enabled`; its default is `true`. Disabled mode sends each
worker's singleton lookup directly through the same bounded HTTP transport, with
unchanged history policy, timeouts, redaction and no hidden retries. Worker
concurrency and finality checks remain in force; the coordinator's 256-poll/four-HTTP
limits and wait metrics apply only when enabled. Disabling batching can increase
RPC request volume, so compare it within the provider's quota.

`status_logical_poll` counts callers including errors/cancellations; `status_coordinator_wait` measures admission-to-dispatch wait. Existing `rpc_get_signature_statuses` timing and the signature-count histogram now describe actual HTTP batches. All metric dimensions are fixed operation/cluster/outcome labels; signatures and endpoint URLs never appear. HTTP success can precede envelope or entry validation failure, which appears in the affected logical-poll and handler outcomes.

The coordinator never releases or transfers a transaction lease. The existing process timeout bounds each worker attempt; journal/lease checks remain authoritative before send and terminal cleanup. Batch errors consume each participating caller's existing logical reconciliation attempt; there is no hidden transport retry or expanded lifetime budget. Serialized-input processed admission/preflight, instruction-input finalized preflight, and finalized terminal receipt checks are unchanged. Null/expired history remains insufficient to re-sign an intent.

Checkpoint publication retries are separate from polling batches. A transient
publication error retries the same work after two seconds. Schema-3 recovery can
restore a missing Redis attempt from its exact validated journal record; batching
does not perform that restoration. Missing or ambiguous journal evidence remains
parked. See [retained transaction recovery](retained-solana-recovery.md) for the
separate durable recovery allowance and [the failure boundaries](recoverable-projection.md#checkpoint-repair-versus-transaction-recovery).

## Validation requirements

1. Real HTTP fixtures in [`status_batcher_tests.rs`](../../executors/src/solana_executor/status_batcher_tests.rs): mixed null/provisional/finalized statuses map to original callers, including duplicate signatures and cancelled waiters.
2. The 256-poll boundary splits into batches of at most 32; queue saturation, four-request concurrency, cancellation, timeout and shutdown retain the bound. Bad envelopes fail all callers; malformed individual statuses fail only their own positions.
3. Different endpoints/clusters/history settings never combine. Disabling coalescing produces actual singleton history requests with the same timeout and provisional statuses. Secrets remain absent from errors/metrics.
4. Existing real-Redis finality/expiry/lease/cancellation/journal regressions pass unchanged; corrupt status cannot bypass finalized receipt/identity proof.
5. Matched local baseline/candidate measure HTTP statuses per intent, actual batch distribution, handler/journal waits, admission/terminal latency and exact effect reconciliation. Keep only if measured savings justify added coordination; do not assume RPC savings equal TPS gains.

## Primary sources and limits

Rechecked 2026-10-12: [Solana getSignatureStatuses](https://solana.com/docs/rpc/http/getsignaturestatuses) specifies a maximum 256 signatures and recent-cache lookup unless history is requested. [Agave implementation linked by that reference](https://github.com/anza-xyz/agave/blob/v3.1.8/rpc/src/rpc.rs#L1542) builds returned statuses in supplied-signature order and checks history availability. This reference version is not a claim about every provider's deployed binary or retention policy.

[Solana getTransaction](https://solana.com/docs/rpc/http/gettransaction) supports explicit commitment and can return null when a transaction is unavailable at that commitment. A history request is not an archival-availability or non-execution guarantee. The existing fail-closed expiry and finalized receipt checks therefore remain necessary.
