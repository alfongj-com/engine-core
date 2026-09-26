# Queue pruning cost

## Result

The terminal-history safety checks have a material cost when retained histories
are full. Keep the checks: the old code can delete data still referenced by a
reused ID. Its faster timing is **not equivalent correct behavior** for ID reuse.

This comparison exercises unique IDs, where both versions produce the same
correct result. It measures the actual `Queue` pruning Lua from the two commits,
with one overflow entry per call and populated success **and** failure histories.
Each added `LPOS` misses, scanning both retained lists completely.

Median of four runs' Redis-reported mean service times, in microseconds:

| Retained success / failure entries | Pruned list | Old | Current | Added | Ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| 100 / 100 | Success | 6.18 | 9.34 | 3.16 | 1.51× |
| 100 / 100 | Failure | 5.77 | 9.85 | 4.08 | 1.71× |
| 1,000 / 10,000 (defaults) | Success | 6.85 | 123.18 | 116.33 | 17.98× |
| 1,000 / 10,000 (defaults) | Failure | 6.26 | 127.47 | 121.21 | 20.37× |
| 10,000 / 100,000 | Success | 7.38 | 1,159.75 | 1,152.37 | 157.23× |
| 10,000 / 100,000 | Failure | 6.16 | 1,204.98 | 1,198.82 | 195.49× |

The four current-script means ranged from 122.35–124.63 µs for default success
pruning and 126.98–128.06 µs for default failure pruning. Corresponding median
client round-trip p95s were 208.63 µs and 213.15 µs. Those are separate measurements;
client p95 is not a server service-time percentile.

**These are pruning service times, not queue-worker or blockchain transaction
throughput.** Redis executes each script atomically; a long scan also delays
other clients. The added work is
`O(pruned_entries × (retained_success + retained_failure))`. Existing pending-list
scans add further cost when live backlog is present. Earlier queue throughput
benchmarks retained all completions and did not exercise overflow pruning.

## Method and reconciliation

- Old: `224b638af35aeff7e1e3aac82e0cb9577cdbd379`.
- Current: `1c0bb18acd0f92ecb3fe275202f03517aef68bc5`.
- Lua extracted directly from `Queue::post_success_completion` and
  `Queue::post_fail_completion` in `twmq/src/lib.rs`; the four extracted scripts
  and their SHA-1 digests are saved beside the report. No checkout switching.
- Same machine, Redis process and owned namespace; identical IDs, payloads and
  settings restored before every trial. Four rounds alternate old/new and
  new/old order for each size and outcome.
- 50 warmups plus 500 measured calls per trial: 48 passing trials, **24,000
  measured prunes**. Completion-record writes, fixture setup, reconciliation and
  cleanup are outside the measured `EVALSHA` interval.
- Server mean is the `INFO commandstats` cumulative `EVALSHA` microsecond delta
  divided by the call count. Each delta must contain exactly 500 calls. A
  synchronous loopback client separately records every pruning round trip.
- Full terminal lists remain populated throughout, using distinct 36-byte IDs,
  267-byte JSON payloads, metadata, permanent deduplication, success results and
  failure error records. There are no live pending, active or delayed jobs.
- After every trial: compare exact ordered history IDs; exact data, deduplication
  and result IDs; all payload/result values; and all metadata/error **key sets**.
  Sample retained metadata timestamps. Metadata/error values are not exhaustively
  checked. Verify live indexes are empty and every prune deletes exactly one
  record. Final namespace cleanup passed.

The complete raw measurements and settings are in [results.json](results.json).
Independent read-only review checked script extraction, timing boundaries and
reconciliation. The report field names reflect the limited metadata-value check.

## Reproduce

Use a disposable local Redis on a free port, with no other benchmark client.
The script needs Python 3.9+, Git and the two commits; it uses no Python packages.
It mutates only a random owned namespace and never uses `FLUSHDB` or changes Redis
configuration.

```sh
redis-server --port 6385 --bind 127.0.0.1 --save '' --appendonly no
# In another terminal, from the repository root:
python3 formal/evidence/pruning/benchmark.py \
  --redis-port 6385 --iterations 500 --warmup 50 --rounds 4 \
  --sizes small,default,large --output /tmp/queue-pruning/results.json
```

Measured environment: Apple arm64 host, 10 logical CPUs, Python 3.9.6, Redis 7.4.2
using one execution thread, persistence disabled, loopback TCP. Populated fixture
memory was approximately 0.2, 11.2 and 110 MB for the three sizes. The desktop was
not isolated from other activity; raw reports retain load averages. These figures
are a local regression measurement, not a production capacity limit.

## Failed attempt and limits

The first full attempt crashed the local Redis process with `SIGSEGV` in
`dictFindPositionForInsert` while executing an ordinary `HSET` during fixture
setup. It had completed 17 trials. No pruning script was executing at the crash.
The root cause is unresolved; the full run was discarded rather than combined
with the retry. A clean restart of the same binary completed all 48 trials and
reconciled successfully. [failed-attempt](failed-attempt/) preserves the Redis
crash log, client traceback and explicitly failed partial report. Successful
retry does not establish Redis reliability or explain the crash.
Trailing whitespace in the saved crash log is normalized. The Lua extracts are
byte-exact, including inherited whitespace, so their recorded digests reproduce.

This benchmark covers one queue variant, one overflow per call, empty live
indexes and full-history misses. It does not quantify retained-reference hits,
large prune batches, network latency, concurrent callers, persistence costs or
the `MultilaneQueue` implementation. The separate Redis regressions and TLA+
models cover the reused-ID correctness defect.

Operational recommendation: qualify the configured retention sizes under the
expected pruning rate and backlog. Preserve the safety guard. A future constant-
time terminal-reference index would need migration and reuse/pruning correctness
tests before it could replace these scans; no runtime redesign was made here.
