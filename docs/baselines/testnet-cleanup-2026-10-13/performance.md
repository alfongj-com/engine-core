# Matched local regression check

**Result:** Both runs completed all 9,600 offered transfers, passed safety/liveness checks, and drained fully. No clear material regression appears in this pair. This is one baseline-then-candidate comparison, not an equivalence or capacity qualification.

## Workload and provenance

- Baseline `04d15d8`; candidate `835c7c2`, each identified by its frozen binary hash in the JSON.
- Same harness hashes, command/settings, and queue configuration: 300s load, 60s warmup, 120s late window; 8 TPS EVM at 0.25s cadence, 8 TPS OP execution at 2s cadence, 16 TPS local Solana.
- Transfer-only, no injected chaos. SQLite WAL FULL/fullfsync; Redis AOF everysec. EVM terminal policy is depth 2; Solana is finalized on local Agave.
- All 9,600 offers dispatched and returned 202; zero dropped offers, reported errors, RPC overloads or proxy failures. Four same-ID retries per run produced zero extra sends.
- Supervisor elapsed: 357.52s → 357.70s. Drain after offered load ended: 20.17s → 20.45s.

## Latency

| Chain | p99 scheduled admission, ms | p99 observed attempt, s | p99 observed terminal, s |
|---|---:|---:|---:|
| EVM 0.25s | 64.99 → 54.11 | 6.009 → 6.127 | 11.008 → 11.140 |
| OP execution 2s | 58.10 → 50.40 | 5.841 → 5.963 | 15.968 → 15.095 |
| Solana | 648.45 → 634.75 | 5.526 → 6.038 | 23.523 → 23.275 |

Attempt/terminal timings are sampled upper bounds with a 5s observation cadence and include drain. The Solana attempt p99 increase is 0.512s (9.3%); its terminal p99 is 0.247s lower.

## Storage and Solana RPC work

| Mean measured phase | Baseline, ms | Candidate, ms |
|---|---:|---:|
| Journal lane_wait | 105.4178 | 106.2857 |
| Journal serial_wait | 1.6343 | 1.5671 |
| Journal serial_hold | 1.4606 | 1.4583 |
| Journal sql_commit | 3.5003 | 3.4974 |
| Journal sql_ack | 3.6081 | 3.6030 |
| Journal redis_observation | 0.1118 | 0.1167 |
| Journal redis_mirror | 0.0857 | 0.0892 |
| Solana status_coordinator_wait | 3.6664 | 3.7459 |
| Solana status_logical_poll | 4.4612 | 4.5536 |
| Solana rpc_get_signature_statuses | 0.5154 | 0.5227 |
| Solana rpc_get_transaction | 2.9620 | 2.4080 |

- Solana HTTP requests per offered intent: **6.0656 → 6.2500** (+3.0%). Post-warmup status batch mean: **5.58 → 5.34** signatures/request. All 4,800 intents used one send each in both runs.
- Journal commit/ack and serial hold means are nearly unchanged. Concurrent phase waits overlap; do not sum these numbers as wall-time utilization.

## Interpretation and next check

The small Solana attempt and RPC-cost movements are watch items, not an attributed code regression. A candidate-first, baseline-second repeat would help separate a persistent change from observation/batch timing and run order. No source change is justified by this pair alone.

Both strict posthoc screens remain unconfirmed because finite-window stage rates/backlog endpoints vary; neither flags persistent growth. Full drain proves completion of this offered workload, not sustainable capacity.

The operator reports the same bounded background fixture polling during both runs and no concurrent builds or public Engine processes. This was not an independent host audit. No public-chain throughput, 50 TPS, statistical equivalence, or additional chaos coverage is claimed.
