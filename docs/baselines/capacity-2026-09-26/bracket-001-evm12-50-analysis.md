# First longer EVM12 50 TPS trial

**Backlog is bounded in this trial; clean offered-load qualification is incomplete.**
One scheduled arrival was dropped, so 17,999 of 18,000 intended requests reached
Engine. Every admitted transaction settled exactly, with no safety, HTTP or RPC
errors and zero final drain counters. The 360s offered phase drained by 400.30s.
A same-rate repeat is needed before calling this a clean repeated operating rate.

| Measurement | Unsigned backlog | Terminal backlog |
| --- | ---: | ---: |
| Post-warmup endpoints | 26 → 22 | 1,822 → 1,819 |
| Four aligned 60s means | 76.80, 65.08, 67.92, 69.63 | 1,737.99, 1,740.92, 1,726.53, 1,728.34 |
| Mean slope, transactions/s | −0.031 | −0.072 |
| Last 180s range | 5–225 | 1,422–1,973 |
| Nominal phase-paired growth min / median / max | −202 / −8.98 / +220.92 | −443.73 / +0.10 / +341 |

Post-warmup admission / attempt / inclusion / terminal rates were
49.993 / 50.010 / 50.010 / 50.006 TPS. The strict result remains `unconfirmed`:
intake was not fully clean, and its admission counter deficit exceeded the
one-transaction window allowance. It did **not** reject positive endpoint growth.

Group distributions overlap, mean slopes are negative, and phase-paired changes
straddle zero. This supports bounded cycle variation for the observed accepted
load. It does not repair the dropped arrival or establish indefinite stability.
An exploratory 60 TPS run can test the next rate; it cannot retroactively qualify
this one. The supplemental rules were proposed before inspecting these values.

Evidence:

- [Strict assessment](capacity-bracket-e09d9dba3b49-001-evm12-50-assessment.json)
- [Group quantiles and all phase pairs](capacity-bracket-e09d9dba3b49-001-evm12-50-backlog-description.json)
- [Original full report](capacity-bracket-e09d9dba3b49-001-evm12-50.full.json.gz)
- [Exact per-ID observations](capacity-bracket-e09d9dba3b49-001-evm12-50.observations.jsonl.gz)
- [Predefined supplemental interpretation](interpretation-v1.md)

All source hashes are preserved in the JSON files. Phase alignment is nominal;
these reports do not contain a block-head timestamp at each sample.
