# Progress-driven EOA scheduling — September 26, 2026

Runtime and regression source: `f309177186e9f5215a1e0acd036bfba1bb15fa6c`.
This changes scheduling after useful send/recovery progress, following the earlier
`9537ac6` fixture-only qualification. It does not change model transitions or
identity, finality and durable authorization gates.

- [Real Redis scheduling regression](scheduling-test.log): one test covering nine
  scenarios, passed in 0.05s. It exercises the production result decision through
  TWMQ lease completion, tail ordering and persisted delayed scores.
- [EOA suite](eoa-suite.log): all 40 selected tests, including ignored Redis tests,
  passed serially in 3.10s. The scheduling regression is included in this count.
- [Executor all-target check](all-targets-check.log): passed without warnings.

[The local model rerun](report.json) passes all 56 expected outcomes against
66 reviewed source hashes in this commit, in 198.596 seconds. Model transitions
are unchanged; the 13 positive configurations exhaust 3,164,046 states across
separate state spaces. Scheduling time, fairness, per-wire RPC rate and throughput
are outside these safety models. Raw logs remain in the formal CI artifacts.
Matched sustained-load qualification is recorded separately. The prior 56-case report in `../throughput-review/` retains its
`9537ac6` source scope; it is not evidence for this newer runtime revision.

All four Linux CI workflows pass at this commit. The [CI record](../../../docs/baselines/review-2026-09-26/ci/SUMMARY.md)
includes runtime scenario results, source fingerprint verification, production
Kani proofs and the queue coverage scope. Raw workflow logs and TLC/Kani traces
remain in the linked run artifacts.
