# First longer OP execution 50 TPS trial

**No clear overload; offered-load qualification remains incomplete.** Two
scheduled slots were dropped: 17,998 of 18,000 requests reached Engine. Every
admitted request settled exactly, with no safety or RPC errors and all final
drain counters zero. Work offered for 360s settled by 370.07s.

| Measurement | Unsigned backlog | Terminal backlog |
| --- | ---: | ---: |
| Post-warmup endpoints | 24 → 29 | 515 → 473 |
| Eight aligned 30s means | 49.75, 35.40, 33.34, 55.24, 26.18, 36.33, 27.00, 32.83 | 461.91, 528.54, 474.91, 518.76, 453.75, 527.10, 445.92, 535.85 |
| Mean slope, transactions/s | −0.0716 | +0.0778 |
| Last 180s range | 5–120 | 403–623 |
| Nominal phase-paired growth min / median / max | −86 / −4.00 / +37 | −98 / +75.49 / +150.97 |

Post-warmup admission / attempt / inclusion / terminal rates were
49.997 / 49.977 / 49.389 / 50.173 TPS. Inclusion observation at selected endpoints
can lag a later terminal observation; its deficit alone does not establish
execution overload.

The strict result is `unconfirmed_growth_or_cadence_ambiguity`, preserving both
small endpoint/trend failures and incomplete offered intake. Unsigned averages
fall overall; terminal averages alternate rather than climb consistently. This
is unlike the large monotonic growth at 100 TPS. Nevertheless, the supplemental
paired terminal growth median is positive, so this run does not satisfy the
predefined bounded-jitter hypothesis automatically. A clean repeat and longer
phase evidence are appropriate; a 60 TPS probe would be exploratory, not proof
that this run already qualified a repeated steady rate.

Evidence:

- [Strict assessment](capacity-bracket-e09d9dba3b49-002-evm2-50-assessment.json)
- [Group quantiles and all phase pairs](capacity-bracket-e09d9dba3b49-002-evm2-50-backlog-description.json)
- [Original full report](capacity-bracket-e09d9dba3b49-002-evm2-50.full.json.gz)
- [Exact per-ID observations](capacity-bracket-e09d9dba3b49-002-evm2-50.observations.jsonl.gz)
- [Predefined supplemental interpretation](interpretation-v1.md)

This is Anvil's OP execution with synthetic cadence/depth finality. It does not
measure an OP sequencer, derivation pipeline or L1 settlement. All source hashes
remain in the JSON evidence.
