# Preserve Anvil history for unresolved outcomes

**The final integrated suite passed 155 tests with no skips.** This fixes local campaign evidence retention; it does not change Engine, retry behavior or recovery verdicts.

## Problem and change

Ordinary fail-closed, quarantine or incomplete results could finish reconciliation with unresolved journal entries and then stop an in-memory Anvil node without preserving its history. The existing snapshot path covered exceptions and infrastructure stops only. Redis and SQLite retain intent custody but cannot reconstruct mined blocks and receipts.

The [applied two-file diff](applied.patch) adds a post-reconciliation hook when `drain.journal_unresolved > 0`. It stops/rechecks Engine, then uses the existing bounded snapshot primitive before owned-node cleanup. Evidence goes in `unresolved_chain_custody`; the completed oracle, drain, recovery flags and verdict remain unchanged. Snapshot failures remain visible without preventing cleanup. Settled runs, already captured interrupted runs, external Nitro and Solana receive no new snapshot calls.

## Validation

| Check | Result |
|---|---|
| [Old-source negative control](negative-control.log) | Expected failure: ordinary unresolved cleanup made zero snapshot calls. |
| [First integrated run](integrated-tests.log) | 155 tests; one fixture error. The simulated second Engine-stop failure also fired during node cleanup because its counter remained at two. |
| [Final integrated run](integrated-tests-repaired.log) | 155 passed, zero skips, 12.744 seconds. |

The [one-line fixture repair](fixture-repair.patch) restricts the injected failure to `label == 'engine_stop'`. It did not change campaign implementation or weaken snapshot/cleanup assertions. The first log also retains an expected closed-socket server traceback from a passing response-cap test.

Eight new lifecycle tests exercise actual `Campaign.run()` ordering, unchanged serialized oracle/verdicts, settled and prior-custody skips, external-node exclusion, missing Engine, and stop/snapshot failures. Existing pinned Anvil tests also passed for Ethereum and Optimism: three complete receipts, block/genesis/head identity and historical balances survived a fresh-node restore; two pending/queued transactions did **not** survive and remained in the separate inventory.

## Limits and provenance

Each snapshot retains the existing 45-second cooperative deadline, size and disk bounds, private files, hashes and no-overwrite publication. It preserves mined history; pending pool inventory is separate. A saved run remains restore-unverified until independently checked. No automatic replay, restart or clearance was added, and this fix cannot recover the earlier [EVM55 lost-history evidence](../evm55-projection-incident/README.md).

[Validation metadata](validation.json) pins the base commit, final source hashes, module counts and all three logs. [Equivalent reproduction command](reproduce.txt) lists the tested modules. Security independently reviewed the source and eight new regressions before execution. The final files are covered by [manifest.json](manifest.json).
