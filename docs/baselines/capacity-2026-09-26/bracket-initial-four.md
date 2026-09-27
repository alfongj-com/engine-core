# Initial 360-second rate brackets

Binary `a3d360eba3dc470bd830ec9d3a32e0be8e11b6764f9bbf6efeb319f080eb8515`;
SQLite FULL/fullfsync, Redis AOF everysec, 25ms generator lag tolerance,
EOA broadcast concurrency32, Solana confirmation poll1. Each trial is isolated,
with 120s warmup and 240s of post-warmup observations.

| Trial | Intended / admitted | Durable attempt / terminal TPS after warmup | Result during offered load | Next measurement |
| --- | ---: | ---: | --- | --- |
| [EVM12 50](bracket-001-evm12-50-analysis.md) | 18,000 / 17,999 | 50.010 / 50.006 | Bounded backlog; one scheduler drop | Clean repeat;60 exploratory |
| [OP execution 50](bracket-002-evm2-50-analysis.md) | 18,000 / 17,998 | 49.977 / 50.173 | No clear growth; alternating terminal bands; two scheduler drops | Clean repeat/longer phase evidence;60 exploratory |
| [Native Nitro 50](bracket-003-nitro-50-analysis.md) | 18,000 / 17,999 | 43.532 / 43.732 | Clear growing backlog |40 on same binary |
| [Solana 60](bracket-004-solana-60-analysis.md) | 21,600 / 21,564 | 59.316 / 59.662 | Late backlog growth;30 client-capacity plus6 scheduler drops |50 on same binary |

All admitted work reconciled exactly and safely, with no unexpected RPC errors.
No trial meets clean offered-load qualification; successful drain is not steady
capacity. Changes to journal indexing, broadcast concurrency or generator lag
allowance require new, explicitly labeled runs. Preserve these original failures.

The proposed 100ms generator profile permits bounded late-arrival microbursts;
it must not be described as a hard real-time schedule or retroactively applied
to the 25ms reports. Actual observed start/response lateness belongs in the new
reports. A changed production binary also requires repeating candidate rates.

[Supplemental interpretation](interpretation-v1.md) preserves the strict analyzer
and documents nominal phase uncertainty. Every linked analysis includes the
complete report, exact per-ID evidence, raw strict assessment and descriptive
quantiles/paired windows with hashes. No absolute or indefinite capacity maximum
has been established by these finite trials.
