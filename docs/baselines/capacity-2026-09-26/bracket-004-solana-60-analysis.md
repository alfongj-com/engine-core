# First longer Solana 60 TPS trial, default poll1

**Late degradation prevents capacity qualification.** Engine accepted 21,564 of
21,600 intended requests. Thirty slots were dropped because all 64 client slots
were occupied; six were dropped by the 25ms schedule-lag rule. Every admitted
transaction settled exactly, with no safety/RPC errors and zero final drain
counters. The 360s offered phase drained by 380.50s.

| Measurement | Unsigned backlog | Terminal backlog |
| --- | ---: | ---: |
| Post-warmup endpoints | 1 → 92 | 1,077 → 1,085 |
| Eight aligned 30s means | 5.17, 3.67, 2.33, 3.33, 19.01, 47.50, 63.34, 139.62 | 1,062.75, 1,059.00, 1,069.66, 1,059.83, 1,067.08, 1,084.01, 1,101.17, 1,126.85 |
| Mean slope, transactions/s | +0.552 | +0.282 |
| Last 180s range | 0–328 | 1,013–1,179 |
| Time-paired growth min / median / max | +40.06 / +112.01 / +327.96 | +2.02 / +62.04 / +124.95 |

Post-warmup admission / attempt / inclusion / terminal rates were
59.695 / 59.316 / 51.724 / 59.662 TPS. The bounded inclusion observer lags and
cannot by itself diagnose execution throughput. Independent durable backlogs
and full client slots provide the adverse evidence here.

The strict result is `unconfirmed_growth_or_cadence_ambiguity`: initial groups
were flat, so its full-window monotonic-growth test does not fire. That label
must not hide the late rise in unsigned means/quantiles, upward terminal bands,
and positive growth in every paired window. A larger scheduler tolerance alone
cannot fix the thirty client-capacity drops.

On the unchanged binary, 50 TPS is the next lower probe. If the journal lookup
index changes the binary, a separate 60 TPS comparison can test the late-state
cost hypothesis; improvement must be measured, not presumed from the query plan.

Evidence:

- [Strict assessment](capacity-bracket-e09d9dba3b49-004-solana-60-assessment.json)
- [Group quantiles and all paired windows](capacity-bracket-e09d9dba3b49-004-solana-60-backlog-description.json)
- [Original full report](capacity-bracket-e09d9dba3b49-004-solana-60.full.json.gz)
- [Exact per-ID observations](capacity-bracket-e09d9dba3b49-004-solana-60.observations.jsonl.gz)

This trial uses a local Agave validator and the default one-second confirmation
poll. It does not measure a public Solana cluster. All source hashes remain in
the JSON evidence.
