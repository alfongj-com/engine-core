# Queue diagnostic retention: formal review

27 September 2026. Reviewed working-tree change based on `dd9282a48640a97024dc277434ef3e4ebe8571b9`. The base commit excludes this change. [Report](report.json), [summary](summary.json), and [commands/environment](run-metadata.json) identify exact source and tool hashes. The separately built release digest is `300db9782868ddca5b6d552e7ead5d2621322a82c8297ffde8f6ecb067858b23`; TLC does not execute or verify that machine code.

## Result

- **61/61 expected outcomes passed**, with **75 reviewed source hashes** matching before and after the run.
- The same 14 positive configurations exhaust **3,164,944 distinct states**, summed across separate spaces. The other 31 fault, 11 boundary and 5 witness cases produce their required counterexamples.
- The three checker safeguard tests passed. Summed model time was **210.900 seconds**; model/configuration files and the 61-case manifest are unchanged. No new theorem or model-count increase is claimed.

[Runner output](runner.log), [checker safeguards](checker-tests.log) and every raw TLC trace are retained. The [preceding index/broadcast evidence](../capacity-index-broadcast-review/README.md) remains unchanged. Fee arithmetic is unchanged; no new Kani execution is claimed.

## Source correspondence and limits

The [retention contract](../../../docs/design/queue-diagnostics.md) bounds each job to its 100 newest diagnostic records, with each new serialization limited to 16 KiB. All six single/multilane nack, failure and deserialization-failure sites append and trim in the existing lease-fenced completion transaction. Queue operations, hooks, attempts, retry/backoff, ownership and expiry retain their prior transitions. Source search confirmed no retry or protocol decision uses diagnostic-list length; the Solana orphan check observes key presence, retained by the omission envelope.

Diagnostic content and size are absent from `QueueLease` variables. These writes project to the same existing completion action; changes confined to inspection data stutter over protocol state. This is manually reviewed correspondence, not a Rust/Redis refinement proof. The [model assumptions](../../queue.md#assumptions-and-reductions) still require valid key types and successful queued commands: Redis can partially execute a transaction containing command errors. No new recovery guarantee covers that boundary.

Ordinary JSON is unchanged. Oversized diagnostics use a distinct valid omission envelope, which external typed inspectors must recognize. Serialization bounds, TTL and bytes are tested implementation properties, not TLC theorems. Legacy oversized records need natural turnover, the first trim of a large list can be linear, and this is no global Redis memory bound. Queue fairness and a separately documented existing multilane `Last`/`RPOP` ordering issue remain outside this patch.

## Implementation evidence

[Eight isolated tests passed](diagnostic-tests.log): two serializer checks plus six real Redis scenarios across both queue variants. They cover exact byte/escaping bounds, ordinary format, unrelated serialization failures, more than 100 mixed nacks, monotonic attempts, newest-first retention, unchanged expiry, all six oversized-record completion sites, and stale owners unable to append **or trim**, change a live lease, or run hooks.

The [removed-trim negative control](mutation-no-trim.log) fails both variants at **121 records versus 100**, then the source was restored. An [initial fixture failure](initial-fixture-failure.log) exposed the unchanged multilane pop order; only the expectation was corrected. [Original isolated metadata](isolated-validation.json) and [copied evidence hashes](implementation-evidence-files.json) preserve that sequence. Root's broader workspace/release gates are separate evidence and are not added to these eight counts.
