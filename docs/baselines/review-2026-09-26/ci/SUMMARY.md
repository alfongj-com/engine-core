# Progress-scheduling CI qualification

Source: `f309177186e9f5215a1e0acd036bfba1bb15fa6c`. **All four workflows passed.**

| Workflow | Run | Result |
|---|---|---|
| Rust correctness gates | [36272585507](https://github.com/alfongj-com/engine-core/actions/runs/36272585507) | success |
| Formal verification | [36272585476](https://github.com/alfongj-com/engine-core/actions/runs/36272585476) | success |
| twmq Tests | [36272585479](https://github.com/alfongj-com/engine-core/actions/runs/36272585479) | success |
| twmq Coverage | [36272585610](https://github.com/alfongj-com/engine-core/actions/runs/36272585610) | success |

## Verified outcomes

- Workspace unit/hermetic suites passed; targeted suites: journal **20**, executor **58**, Solana retention **4**, HTTP integration **3**, queue lease **26** tests passed.
- Executor regression suite completed in **14.61s**. The corrected 100k retained-entry fixture completed.
- **10/10 actual local-chain scenarios passed**, including Engine/Redis crash recovery, terminal reverts, Redis flush/rollback quarantine, pre-finality reorgs, Solana accepted-send response loss, and bounded EVM/Solana loads.
- Linux debug load reports each reconciled 1,000 intents: EVM final 30.023s; Solana final 40.466s. These establish exact outcomes and complete drain, not sustained 50 terminal TPS.
- Formal: **56/56 expected TLC outcomes**; all **66** source fingerprints match the commit. Kani proved **five production harnesses / 116 checks** and rejected both deliberate mutations.
- Queue line coverage: **1,155/1,800 (64.17%)**, including regular and ignored tests; this is not workspace coverage.
- Dependency audit passed with **five non-denied informational warnings**: two bincode versions, derivative and paste unmaintained; lru 0.16.4 unsound (RUSTSEC-2026-0253). These are not suppressed allow-list exceptions, and the result is not warning-free.

## Evidence and limits

`summary.json` provides concise machine-readable results. `test-results.json` contains the per-step test ledger. The four `run-*.json` files preserve complete job/step outcomes. The stored fee summary, source verification and queue coverage XML provide compact supporting evidence. Full logs, TLC/Kani traces, queue JUnit and coverage HTML remain in the linked workflow artifacts. Ten local-chain reports are in `rust/local-chain-recovery-reports/`.

Finite models and selected production arithmetic proofs do not establish whole-program correctness, multi-chain capacity, or public-chain behavior. This qualification is separate from the earlier successful 9537ac6 run and the diagnostically cancelled 025a3d1 run.
