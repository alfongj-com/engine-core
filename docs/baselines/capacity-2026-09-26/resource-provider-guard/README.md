# Resource/provider guards: applied validation

**Applied checks and both live integration smokes passed.** This archive qualifies the guard integration, not sustained throughput or a capacity ceiling.

| Check | Result |
|---|---:|
| Applied guard, campaign, fence and measurement tests | 86 passed |
| Additional historical regressions | 43 passed initially; 2 Cast cases subsequently passed |
| Archived supervisor v4 | 11 passed |
| Four-profile transfer smoke, six seconds | 48/48 exact outcomes |
| Three-profile mixed smoke, six seconds | 18/18 exact outcomes |

There are **131 distinct main-script cases plus 11 supervisor cases**. The corrected three-case Cast run covers two previously skipped cases and reruns one prior pass. Both smoke oracles pass safety/liveness, all nine numeric drain counters are zero, owned children stopped, and capacity-candidate flags remain false. Direct provider probes recorded 28 and 21 successful observations, respectively, with no failures.

The four profiles are Anvil EVM, Anvil OP execution, native Nitro `--dev`, and a local Solana validator. Mixed workloads cover EVM transfer/storage/revert and Solana transfer/multiple-transfer fixtures; native Nitro mixed execution remains excluded. These are local profiles, not public-network or rollup L1-settlement qualification.

## Evidence

- [Structured results](summary.json), [source hashes](source-map.json), [archive hashes](manifest.json).
- [Four-profile report](all4-transfer-smoke.json) and [per-ID evidence](all4-transfer-observations.jsonl.gz).
- [Mixed report](three-mixed-smoke.json) and [per-ID evidence](three-mixed-observations.jsonl.gz).
- [Applied tests](applied-tests-86-pass.log), [supervisor tests](supervisor-11-pass.log), [additional tests](other-tests-43-pass-2-skipped.log), [corrected Cast cases](cast-corrected-3-pass.log).
- [Guard design and limits](design.md), [exact patch](resource-integration.patch), [archived runner](../supervisor-v4/runner.py).

Both reports record source commit `cbdedae642c48d1c8d26911a874f13a8fc950486` and Engine binary SHA256 `35e307e16cafa7cbb17caafcf5ed90a840d58dddd2454542566619266e7a4c07`. Their three harness hashes match the source map. Reports and compressed per-ID evidence are copied byte-for-byte; temporary paths inside them are original provenance. Resource samples and smoke launcher/plan sources are included. The plan's original “draft” status predates execution; completed reports determine the outcomes.

## Preserved command mistakes

Three unsuccessful invocations were command-selection errors, not failed source assertions: nonexistent recovery-test module names, missing `PYTHONPATH` for the runner test, and an incorrect Cast test class name. Their original logs remain as `applied-tests-command-error.log`, `supervisor-command-error.log`, and `cast-command-error.log`; corrected checks appear separately. The first additional-regression run skipped two Cast cases because `CAST_BIN` was unset, and its log remains unchanged.
