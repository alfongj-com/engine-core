# Finality and Redis recovery: verification evidence

Runtime source: `7078799652b4c07c552a397be2faf89f0d18577d`.
CI source: `6f965440d7587cd68bbb9a5a7588601dfaa4b75e` (adds workflow gates only).

## Local checks

The [manifest](local-manifest.json) records tool versions, results, binary/report
hashes and evidence-file hashes. All 58 mapped source/test/harness files were
compared with the published runtime commit byte for byte.

| Gate | Result |
| --- | --- |
| [Workspace](local/workspace.log) and [both server/CLI binaries](local/build.log) | Pass. |
| [Executor Redis regressions](local/executors-redis.log) | 49 pass, including isolated checkpoint-conflict and terminal-projection cases. |
| [Journal faults](local/journal-faults.log) | 10 ignored cases pass; its pure unit case runs in the workspace suite. Includes child SIGKILL before and after ambiguous emission. |
| [HTTP](local/http.log), [admission](local/admission.log), [queue](local/queue.log) | 2, 4 and 26 tests pass respectively. Counts overlap broader/child test runs; they are not a coverage percentage. |
| [Clippy](local/clippy.log) | Pass with warnings. |
| [52 protocol cases](../../../formal/evidence/finality-recovery/report.json) | Every positive, fault, boundary and witness case matches its specified result. 3,163,401 distinct positive states. |

## Actual process faults

All four reports below use the final built binary and isolated loopback Redis/Anvil
processes. The harness never submits its own replacement transaction in the reorg
cases: Engine's normal stall recovery must do that at the original nonce.

| Scenario | Evidence |
| --- | --- |
| Delete Redis after one chain effect while Engine is stopped; recover in a new namespace; lose Redis again while running | [Pass](local/final-disaster-flush.json): unsafe startup/reattach blocked, uncertain ID quarantined, one fresh intent executes, no duplicate effect, live writes close. |
| Restore a Redis snapshot made before admission | [Pass](local/final-disaster-rollback.json): same recovery assertions. |
| Orphan a successful receipt before depth 2, restart, recover and finalize | [Pass](local/final-eoa-reorg.json): one original nonce, two attempts, one canonical effect, no early terminal result. |
| Same reorg with a reverting contract | [Pass](local/final-eoa-reorg-reverted.json): failure stays provisional until depth is reached, then one terminal failure; no transferred value or duplicate execution. |

The local depth policy is explicit and probabilistic. RPC fixtures separately
exercise the default finalized tag, unavailable/stale heads, block-hash mismatches,
checkpoint conflicts and mixed reads. These tests do not certify a public endpoint,
real consensus, host power-loss durability or multi-host operation. The journal
is a required independent authority; a copied or rolled-back journal is outside
the recovery guarantee. No paid RPC calls were made.

## Linux CI

**All four workflows passed** at `6f965440d7587cd68bbb9a5a7588601dfaa4b75e`:
[formal verification](https://github.com/alfongj-com/engine-core/actions/runs/36265904644),
[Rust correctness and process recovery](https://github.com/alfongj-com/engine-core/actions/runs/36265904652),
[queue tests](https://github.com/alfongj-com/engine-core/actions/runs/36265904675), and
[queue coverage](https://github.com/alfongj-com/engine-core/actions/runs/36265904647).
[Workflow metadata](linux/workflows.json) records the exact source and every step.
The later documentation/evidence commit leaves runtime, tests and models unchanged.
The [test summaries](linux/test-summaries.json) preserve suite counts. The
[dependency audit](linux/dependency-audit.log) passes with the same five allowed
warnings, including the existing `lru` panic-safety warning. A passing audit is
not a claim that dependencies have no remaining risk.

The hosted run repeats the runtime gates and all eight actual-process scenarios
at the CI source above. Downloaded scenario JSON omits only ephemeral log-directory
paths; values, hashes, counts and outcomes are unchanged.

- [EOA process restart](linux/processes/local-eoa-recovery.json),
  [intact Redis AOF restart](linux/processes/local-eoa-redis-crash.json), and
  [reverted execution with restart](linux/processes/local-eoa-reverted-recovery.json).
- [Successful reorg recovery](linux/processes/local-eoa-reorg.json) and
  [reverted reorg recovery](linux/processes/local-eoa-reorg-reverted.json).
- [Redis loss](linux/processes/local-redis-loss.json) and
  [stale snapshot](linux/processes/local-redis-rollback.json).
- [Solana accepted-send response loss](linux/processes/local-solana-recovery.json):
  12 original signatures, 12 identical-wire retransmissions, 12 finalized effects,
  zero duplicates, and no new work after queue-history pruning.

The downloaded [protocol report](linux/protocol-report.json) reproduces all 52
expected cases. The [fee summary](linux/fee-summary.json) records five successful
production harnesses, all 116 checks, and the required counterexamples from both
source mutations. Their model, configuration and Rust source hashes match the
published commit. Raw solver logs remain in the workflow artifacts.
