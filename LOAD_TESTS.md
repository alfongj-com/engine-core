# Load tests and historical evidence

This branch preserves the reports and generated artifacts removed from PR #1.
The original archive is byte-for-byte the content at `b31f43d4b6b699e885cd9601f8129dda158b1662`.
It includes failed and incomplete runs as well as successful runs.

- [PR #2 checkpoint recovery, fairness and polling results](docs/baselines/pr2-2026-10-12/README.md)
- [Capacity and chaos results](docs/baselines/capacity-2026-09-26/RESULTS.md)
- [Concise handoff](TO_ALFONSO.md)
- [Historical verification notes](docs/verification.md)
- [Benchmark reports and raw evidence](docs/baselines)
- [Formal verification execution logs](formal/evidence)
- [New projection-model WIP evidence](formal/evidence/recoverable-projection-wip/README.md)

The projection-model WIP directory preserves the earlier unfinished draft.
The October 12 report above records the implemented follow-up and distinguishes
its candidates, failures and final source checks. Earlier capacity figures do not
automatically apply to schema 2's additional durable acknowledgement.

The code snapshot on this archive branch is historical. PR #1 is merged into
`main`; active follow-up code and tests are on `crash-recovery-and-chain-fairness`
([PR #2](https://github.com/alfongj-com/engine-core/pull/2)).
