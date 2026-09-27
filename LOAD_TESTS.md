# Load tests and historical evidence

This branch preserves the reports and generated artifacts removed from PR #1.
The original archive is byte-for-byte the content at `b31f43d4b6b699e885cd9601f8129dda158b1662`.
It includes failed and incomplete runs as well as successful runs.

- [Capacity and chaos results](docs/baselines/capacity-2026-09-26/RESULTS.md)
- [Concise handoff](TO_ALFONSO.md)
- [Historical verification notes](docs/verification.md)
- [Benchmark reports and raw evidence](docs/baselines)
- [Formal verification execution logs](formal/evidence)
- [New projection-model WIP evidence](formal/evidence/recoverable-projection-wip/README.md)

The additional projection-model evidence concerns the unfinished follow-up branch;
it does not qualify the unwired Rust draft or replace the earlier campaign results.
The code snapshot on this archive branch is historical. Active implementation and
regression tests remain on `production-hardening` and its stacked follow-up branch,
`crash-recovery-and-chain-fairness`.
