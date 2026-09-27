# Phase-fixed screens — cohort bd44836d17c9

All **66,600 offered intents** were accepted and reconciled exactly, with zero recorded drain and owned children stopped. An independent offline audit compared every captured per-ID observation against its original private journal. **None of these three single screens is selected as a sustained capacity result.**

| Profile | Offered TPS | Exact intents | Late attempted / terminal TPS | Late unsigned backlog | Nominal paired terminal growth, min / median / max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Anvil EVM, 12s cadence |60|21,600|56.675 /59.176|8→606|10 /500 /1,058.75 |
| Anvil OP execution, 2s cadence |60|21,600|58.733 /59.478|180→408|230.13 /400.08 /632.83 |
| Local Agave |65|23,400|65.004 /64.526|1→1|3 /7.51 /27 |

EVM and OP unsigned/terminal backlogs grew. Solana's raw candidate is true under the original coarse screen, while the unchanged strict assessment is `unconfirmed_growth_or_cadence_ambiguity`: small positive nominal paired terminal differences and a final-sample jump remain. Keep that raw candidate visible, but do not promote it to an independently confirmed rate. Drains completed at approximately 405.34s, 375.15s and 380.51s from offer start, respectively; those completion times do not show steady-state throughput.

## Configuration and provenance

Each profile ran separately for 360s, with 120s warmup and a 180s late window, one Engine/Redis/SQLite journal per run. These are **sequential individual screens**, not all chains running together. Settings: Redis `appendfsync=everysec`, SQLite `FULL`, HTTP concurrency 128, EOA send concurrency 32, Solana polling 5s, maximum 4096 in-flight intents per wallet, bounded 100ms scheduling lateness, and 1000ms admission-p99 limit. No fault or mixed workload was requested.

Engine SHA256: `35e307e16cafa7cbb17caafcf5ed90a840d58dddd2454542566619266e7a4c07`. All three use the same harness hashes in [summary.json](summary.json), [frozen jobs](frozen-jobs.json), and [final supervisor state](supervisor-final-state.json). The supervisor [source](supervisor-source.py), [log](supervisor.log), and labeled pre-Solana-completion snapshot remain preserved. Commit strings differ across launches; the reports retain actual dirty-state listings and identical frozen binary/harness hashes instead of treating commit labels as tested-byte identity.

## Evidence

Each profile directory contains:

- `full-report.json.gz`: exact original report bytes, deterministically compressed.
- `observations.jsonl.gz`: original sanitized per-ID evidence, copied without recompression.
- `rpc-audit.jsonl.gz`: complete proxy audit with only sequence/time, method, wire digest, returned identity and fixed event fields; all rows checked against an allowlist before archival.
- `resources.jsonl`, `resource-preflight.jsonl`, campaign log, and independent audit summary.
- Unchanged strict assessment, compact analysis and backlog description.

Profile directories: [EVM](evm60/independent-audit.json), [OP](op60/independent-audit.json), [Solana](solana65/independent-audit.json). [Analyst summary](analyst-cohort-summary.json) retains the interpretation and suggested next rates as hypotheses, not results. Exact analysis sources are included. [Source integrity](source-integrity.json) records original report/audit digests; [manifest](manifest.json) covers every archived file. `summary-pending.json` is a labeled historical partial snapshot, superseded by `summary.json`.

## Limits

Independent audit means captured observations were cross-checked against private durable records; it does not mean a second live RPC sweep. Inclusion/terminal series are sampled observer knowledge. Phase alignment is nominal because earlier report formats lack per-sample block heads. Finite single runs do not establish absolute or indefinite maxima.

Anvil EVM cadence is simulated. Anvil OP uses its OP execution backend but does not qualify a sequencer, derivation, L1 settlement or genuine L1 fee state. Local Agave is not a public cluster. Native Nitro and shared multichain operation were not included in this cohort. Empty/full-drain results do not replace repeated rate confirmation, long finality-retention tests or chaos qualification.

Private SQLite/WAL/SHM, Redis AOF, keypairs, raw signed attempts, environment credentials and node logs are deliberately excluded. Original evidence remains intact outside the repository.
