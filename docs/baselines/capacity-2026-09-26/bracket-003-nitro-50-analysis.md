# First longer native Nitro 50 TPS trial

**Clear overload during arrivals; accepted work recovered during drain.**
17,999 of 18,000 intended requests reached Engine, with one scheduled slot
dropped. Every admitted transaction settled exactly, with no safety/RPC errors
and zero final drain counters. The 360s offered phase drained by 400.31s.

| Measurement | Unsigned backlog | Terminal backlog |
| --- | ---: | ---: |
| Post-warmup endpoints | 227 → 1,778 | 423 → 1,926 |
| Eight aligned 30s means | 319.67, 306.91, 393.31, 614.59, 907.26, 1,183.01, 1,413.83, 1,637.23 | 445.50, 561.92, 628.03, 852.19, 1,118.25, 1,399.75, 1,637.92, 1,860.06 |
| Mean slope, transactions/s | +6.91 | +7.09 |
| Last 180s range | 277–1,778 | 582–1,944 |
| Time-paired growth min / median / max | +1,283 / +1,298.5 / +1,343 | +1,282 / +1,436.5 / +1,513 |

Post-warmup admission / attempt / inclusion / terminal rates were
49.994 / 43.532 / 43.736 / 43.732 TPS. Every paired window grows substantially,
terminal group means and quantiles rise consistently, and unsigned backlog grows
rapidly after its initially flatter interval. This is not a close endpoint or
cadence decision; native Nitro has no fixed block cadence assumed here.

On the same binary, 40 TPS is the next lower probe. If a production optimization
changes the binary, 50 TPS should instead be a separately identified repeat for
comparison; this result cannot establish the new version's operating range.

Evidence:

- [Strict assessment](capacity-bracket-e09d9dba3b49-003-nitro-50-assessment.json)
- [Group quantiles and all paired windows](capacity-bracket-e09d9dba3b49-003-nitro-50-backlog-description.json)
- [Original full report](capacity-bracket-e09d9dba3b49-003-nitro-50.full.json.gz)
- [Exact per-ID observations](capacity-bracket-e09d9dba3b49-003-nitro-50.observations.jsonl.gz)

Scope is Nitro dev L2-only execution, not a public Arbitrum sequencer or L1
settlement. The final zero pending-nonce delta is a weak pool observation; exact
per-ID receipts/effects/terminal evidence establishes admitted-work completion.
