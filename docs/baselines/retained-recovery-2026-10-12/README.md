# PR #3: retained transaction recovery

**Local validation and final-head CI pass. Four matched steady runs preserve exact effects and drain fully; all 12 strict capacity assessments remain unconfirmed.** Review head: `146de9d`. Tested macOS runtime: `04d15d8`; later changes affect documentation only. [Provenance](validation.json)

## Changes and boundaries

- Restore missing Solana Redis attempts from one validated journal record, preserving the original intent, signature and signed bytes. No replacement is signed.
- Durably limit recovery broadcasts to 20 additional grants across restarts and repeated projection loss. Lost replies consume grants; exhaustion still permits finalized-outcome reconciliation.
- Offline reattach can atomically acknowledge a pending checkpoint and adopt the Redis process only when the exact target survives on the primary. Missing, predecessor or foreign checkpoints and unrelated halts remain blocked.

Missing queue jobs, ambiguous records, unknown outcomes, quarantine and journal loss remain separate recovery problems. The original broadcast counter is Redis-only; the combined 20 ordinary + 20 recovery bound assumes that counter survived until recovery mode.

Schema 3 retains recovery mode and its budget. Healthy schema-1/2 journals migrate; older binaries reject schema 3. No in-place downgrade is supported. Follow the [upgrade/rollback runbook](https://github.com/alfongj-com/engine-core/blob/recover-retained-transactions/docs/design/redis-disaster-recovery.md#schema-upgrade-and-rollback) before opening deployed state.

## Validation

| Check | Result |
| --- | --- |
| Local workflow | All 10 steps pass; **181 Redis/API tests**: 64 journal + 74 executor + 4 admission + 3 API + 36 queue |
| Actual binary upgrade | Schema 2→3 succeeds; old binary rejects it without changing authority |
| Local process recovery | 10 scenarios pass, including live Redis outage and both Solana variants |
| Final-head correctness CI | 9 recovery scenarios and 2 short load/drain checks pass |
| Final-head formal CI | 100 expected model outcomes, 91 source guards, 5 arithmetic proofs, 2 mutation rejections |

[Commands and chronology](tests.json) · [Local processes](process.json) · [Final CI processes](ci-process.json) · [Formal evidence](formal.json)

Local and final CI projection-loss tests each restore 12 removed attempts, finalize 12 original effects, preserve retransmitted bytes and produce zero duplicates. Final CI verifies the harness default runs both Solana variants.

## Performance and chaos

Each 300-second steady run accepts and completes 9,600 intents at 8/8/16 offered TPS. Order: baseline → candidate → candidate → baseline, with a separate crash campaign between the candidate runs.

Fast-EVM admission/terminal p99 measures **61.038 ms / 11.383 s → 87.820 ms / 18.006 s → 51.413 ms / 11.008 s → 60.604 ms / 14.134 s**. The initial increase did not repeat with identical candidate bytes. Other tails vary, and all strict screens remain unconfirmed. These runs establish neither statistical equivalence nor sustainable capacity. [Full comparison](performance.md)

The crash campaign finalizes all 2,880 intents exactly once. Five retries, including one ambiguous HTTP response, cause zero additional sends; all queues drain. No timing counters are compared across restart. [Crash evidence](candidate-crash.sanitized.json)

Only sanitized aggregates are published. Local single-host fixtures and bounded models do not qualify production RPC providers, OP/Arbitrum settlement, public-cluster throughput or the entire Rust program.
