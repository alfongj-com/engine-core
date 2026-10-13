# Load tests and historical evidence

This branch preserves the reports and generated artifacts removed from PR #1.
The original archive is byte-for-byte the content at `b31f43d4b6b699e885cd9601f8129dda158b1662`.
It includes failed and incomplete runs as well as successful runs.

- [PR #4 public-testnet qualification and cleanup comparison](docs/baselines/testnet-cleanup-2026-10-13/README.md)
- [PR #3 retained transaction recovery and regression comparison](docs/baselines/retained-recovery-2026-10-12/README.md)
- [PR #2 review fixes and final validation](docs/baselines/pr2-review-2026-10-12/README.md)
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

The code snapshot on this archive branch is historical. PRs #1, #2 and #3 are
merged into `main`; current cleanup and test harnesses are on
`cleanup-recovery-paths` ([PR #4](https://github.com/alfongj-com/engine-core/pull/4)).
Use that branch's README and `LOCAL_TESTING.md` to run the current implementation.
Each evidence report identifies its own runtime, binary and harness; earlier
capacity figures do not automatically apply to later durability changes.
