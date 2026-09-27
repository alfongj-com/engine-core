# Diagnostic limits and live observer validation

Engine release: `300db9782868ddca5b6d552e7ead5d2621322a82c8297ffde8f6ecb067858b23`.
[Manifest](manifest.json) records source/log hashes, individual commands and
separate suite counts. This is local implementation evidence, not a capacity
result. Historical runtime and failed-screen evidence remain unchanged.

## Completed gates

- Workspace test compilation passed; release build passed.
- Normal workspace library tests, by crate: AA core **5**, core **20**, EIP-7702
  core **1**, executors **37**, Solana core **2**, thirdweb core **1**, server
  **24**, queue **2**. AA types and integration-test library contain zero unit
  tests in this command. Ignored tests are excluded from these counts.
- Real-Redis ignored suites: queue **32**, executors **61**, both passed serially.
- Isolated diagnostic draft: **8** tests passed. Disabling trimming produced the
  intended **2 failures**: retained length 121 instead of 100.
- Observer campaign module: **26** tests passed, including **6** new regression
  methods. Restoring the old observer produced the intended continuous-arrival
  fairness assertion failure, with no fixture error.
- Broader Python gate: **90 tests passed**, 17.859s, confirmed completed with
  exit 0 by the root task. This is the point-in-time gate before the subsequent
  strict-assessor observer-error disqualifier change.
- Final strict-assessor gate: **5 tests passed**, including **one new** observer-
  error rejection case (four overlap Python90). The original assessor fails the
  intended negative-control assertion. These two Python gates cover91 distinct
  cases; they are not a95-test suite.

These suites overlap; the counts must not be added into a unique-test total.
The isolated eight include serializer tests also exercised normally and queue
cases also exercised in the integrated Redis suite. The observer26 are a subset
of Python90. Nested subprocess scenarios are not extra Rust test entries.

## Scope and limits

Queue diagnostics now retain the newest 100 records per job; new serialized
records are bounded at16KiB with an explicit omission marker for oversized
content. Tests cover owner-fenced append/trim, order, attempt counters, existing
expiry, and terminal/nack behavior. Legacy oversized content is not rewritten:
natural turnover is required for the byte bound. This is not a global Redis
memory bound and does not limit authoritative replay evidence.

The Solana harness observer now queues pending signatures fairly under new
arrivals and finalized removals. Each sample selects at most2,048 signatures in
at most eight256-item calls. A batch rotates before I/O, so repeated errors
cannot pin other pending entries behind it. Malformed replies remain errors.
Historical signature identity and independent final reconciliation are retained.
This fixes measurement starvation; it changes no Engine Solana execution path.

No new TLA model was added for diagnostics or observer scheduling. The separately
recorded formal run checks existing model/source correspondence; it is not a
proof of measurement freshness, queue memory bounds, or sustainable throughput.
Final strict-assessor validation is preserved separately in
`assessor-targeted-tests.log` and `assessor-draft/`; incomplete telemetry cannot
be promoted even when post-drain reconciliation is exact.

The `queue-draft/README.md` and `observer-draft/README.md` are preserved historical
draft documents: their “not applied” statements describe that earlier stage.
The root subsequently applied both reviewed changes before these integrated
gates. Current applied observer files match the tested draft hashes.
