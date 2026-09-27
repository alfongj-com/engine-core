# Resource budget after reconciliation begins

Applied harness fix: after Engine successfully stops for final reconciliation, budget at most the existing 600-second verification allowance instead of the unused maximum drain period. The deadline is the earlier of that bound and the original job deadline. Sampling and this transition share a lock; a monitor cannot apply the obsolete budget between successful stop and transition.

Observed growth peaks, filesystem checks, all floors, factor 2, and the 60-second observation plus 120-second cleanup margins remain. No load-period reserve was relaxed. Existing stops remain latched, failed Engine stop retains the original budget, and a second transition is rejected. Verification expiry fences at the next bounded observation; this is not an instantaneous hard timeout.

## Evidence

- [Exact applied source/test patch](applied.patch) and [source hashes](source-map.json).
- [Applied 94 tests pass](applied-94-pass.log): 8 new cases plus 86 existing cases. The [earlier 40-case check](draft-40-pass.log) is a subset, not another 40 unique tests.
- [Independent review](review.md): no blocker.
- [Initial fixture failures](draft-fixture-failures.log) retained: floating-point exact equality and a decimalMB/MiB threshold were corrected in tests. The initial shell wrapper also attempted assignment to zsh's read-only `status` variable after the test command; the corrected wrapper used `check_rc`.

The triggering Solana 70 run stopped Engine at campaign 419.372808 seconds, then its guard at 420.098811 seconds still reserved 2459.940893 seconds. At an observed 11,304,502.898 bytes/second, the doubled projection required 68,276,373,546 bytes against 32,865,955,840 available. The regression reproduces that arithmetic and retains the same measured peak while budgeting the remaining verification phase.

That historical run remains an interrupted run. Its original report is unchanged and independent receipt-history reconciliation was still unresolved when this archive was written. This fix does not convert it to a recovery pass or a capacity qualification.

The pending documentation CI patch includes `capacity_resource_phase_test`; no real workflow was changed by this archive task. Host cache cleanup, completed by root, is separate from this policy correction.
