# To Alfonso

Updated October 12, 2026.

## Saved and pushed

PR #1 is merged. [PR #2](https://github.com/alfongj-com/engine-core/pull/2) implements
recoverable checkpoints, measured journal scheduling and bounded Solana polling.
The main local folder is `/Users/alfongj/Code/engine-core`; its README and
`LOCAL_TESTING.md` contain the installed tool paths and a prompt for another agent.
Generated evidence stays on this `load-tests` branch.

## What changed

A crash or lost reply during an exactly recorded checkpoint update can now be
repaired without forgiving unrelated rollback. Two FULL SQLite commits protect
the permission boundary. Busy-chain health checks cannot fill other chains' health
slots. Solana batches eligible status reads; stalled Redis polls no longer prevent
graceful shutdown, while running work still drains.

## What the measurements say

The shared journal is the throughput limit on this Mac. The extra durable
acknowledgement is expensive. In the matched short run, Solana status HTTP calls
fell from 42,213 to 5,633, but logical polling also slowed and Solana admission
p99 rose from 170 ms to 912 ms. This is not a blanket latency improvement.

A short 10 + 10 + 20 TPS shared run passed; a four-minute repeat grew Solana
backlog and failed capacity. At 25 + 25 + 50 TPS the system overloaded, although
both EVM chains now progressed where the baseline stalled. No 50 TPS-per-chain
or sustainable-maximum claim is justified.

At **32 TPS combined** (8 EVM + 8 OP-like + 16 Solana), a five-minute run
passed the campaign checks and reconciled all 9,600 transactions. Unsigned queues
remained bounded. A stricter screen still flags confirmation timing, so this is
a tested working point, not a proven sustained maximum.

Killing and restarting Engine during a separate 90-second load at the same rates
also passed: all 2,880 transactions reconciled, with no duplicate effects or
unresolved work. One lost API reply was retried using the original request ID.
This fault test used a 10-second queue lease to exercise recovery
within the test window; ordinary measurements used the 600-second default.

All five CI checks passed on `0ad0453`, including correctness, dependency audit,
queue tests, coverage, 73 bounded model configurations and the fee proofs.

[Exact results, failures, methods and source hashes](docs/baselines/pr2-2026-10-12/README.md).
The local fixtures use Anvil execution and Agave, not actual rollup settlement.
No paid RPC credits were used.

## What remains before production

1. Improve the journal's durable write throughput and qualify the intended hardware,
   RPC, workload and real chain stack. Preserve crash/rollback guarantees.
2. Add a recovery procedure for missing signed Redis projections and quarantined
   unknown effects. Checkpoint repair alone cannot recreate those records.
3. Complete the previously documented KMS/account and deployment qualification.

The old incident journals and retained Nitro environment were not changed. An
unrecorded schema-1 checkpoint mismatch does not become repairable merely by
upgrading. The September capacity results remain [historical evidence](docs/baselines/capacity-2026-09-26/RESULTS.md),
not capacity claims for schema 2. Finite models do not prove the whole service.
