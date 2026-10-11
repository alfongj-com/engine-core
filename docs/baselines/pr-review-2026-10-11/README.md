# PR review validation — October 11, 2026

PR #1 head: [`8741b881d0e1f5b7290492d8e65a6384ac632cb2`](https://github.com/alfongj-com/engine-core/commit/8741b881d0e1f5b7290492d8e65a6384ac632cb2).
**All four final-head GitHub Actions workflows passed.** This archive does not qualify the unfinished PR #2.

## Results and provenance

The workspace suite and Redis suites passed on the earlier review candidate
(`review-oct11-w5lsrm3l`). After the final shutdown/drain changes, the affected
journal, signal and HTTP suites were rerun, the server rebuilt, and all nine
process scenarios passed (`review-oct11-ik9fs2jx`). The full workspace suite was
not repeated after that last change. Local source reports remain under
`~/.local/share/engine-core/runs/`; runtime logs and credentials are not copied here.

| Check | Result |
| --- | --- |
| Workspace unit/hermetic suites | Passed on earlier candidate |
| Explicit Redis/API suites | 140 tests: 29 journal + 71 executor + 4 Solana admission + 3 HTTP API + 33 queue |
| Signal child-process regression | Passed for SIGTERM and SIGINT |
| Local process scenarios | 9 passed; [sanitized summary](process-results.json) |
| RPC budget/load-generator tests | 19 passed |
| Offline public-testnet harness tests | 10 passed |
| Formal runner tests | 3 passed |
| Pinned TLC model checks | 61 expected outcomes: 14 positive cases and 47 intended fault, boundary or witness counterexamples |
| Formatting | Passed |

The journal count uses the final 29-test rerun, including the new shutdown-drain
regression; the earlier journal run had 28 tests. Other unchanged Redis suite
counts come from the earlier candidate, and the three HTTP tests also passed in
the final rerun. The 140 count excludes ordinary workspace tests and process scenarios.

The TLC result comes from `review-formal-oct11/report.json`, using TLC 1.7.4. All
61 cases matched their declared outcomes; deliberate invariant/liveness failures
count only when the expected counterexample was reached. That report's frozen
runtime hashes precede the final shutdown/drain changes. It must not be presented
as a final-head Rust source certificate. Final runtime changes were checked by
the affected Rust suites and process scenarios separately. The subsequent
[final-head CI formal run](https://github.com/alfongj-com/engine-core/actions/runs/38172595008)
passed all 61 cases with the final source map; its
[protocol report](ci-protocol-report.json) is archived here. The same run proved
all five fee-arithmetic harnesses and rejected both deliberate source mutations
([Kani summary](ci-fee-proof-summary.json)).

## Suite commands

Run against PR #1 with the disposable Redis and local tool setup in its
[local testing guide](https://github.com/alfongj-com/engine-core/blob/8741b881d0e1f5b7290492d8e65a6384ac632cb2/LOCAL_TESTING.md).

```sh
cargo fmt --all --check
cargo test --locked --workspace -- --test-threads=1
cargo test --locked -p engine-core --lib recovery -- --ignored --test-threads=1
cargo test --locked -p engine-executors --lib -- --ignored --test-threads=1
cargo test --locked -p thirdweb-engine --lib solana_admission::tests -- --ignored --test-threads=1
cargo test --locked -p thirdweb-engine --test api_safety -- --ignored --test-threads=1
cargo test --locked -p twmq --lib -- --ignored --test-threads=1
cargo test --locked -p thirdweb-engine --bin thirdweb-engine shutdown::tests
cargo build --locked --bin thirdweb-engine
node --test scripts/rpc/budget-gateway.test.mjs scripts/rpc/load-probe.test.mjs
python3 -m unittest discover -s scripts -p 'test_testnet_*.py' -v
python3 -m unittest discover -s formal -p 'test_*.py' -v
```

The last-change rerun covered formatting, `shutdown::tests`, the 29 journal
tests, the three HTTP tests and the server build. The remaining commands above
describe the broader review checks, not an additional final-head rerun.

## Process scenarios

All scripts ran with disposable local nodes/state and exited zero. Add
`--report <new-report-path>` to preserve each scenario's detailed output.

| Scenario | Command after `python3` |
| --- | --- |
| Engine crash and original-ID retry | `scripts/local_eoa_recovery.py` |
| Redis failure while Engine remains alive | `scripts/local_eoa_recovery.py --live-redis-restart` |
| Engine/Redis crash with AOF replay | `scripts/local_eoa_recovery.py --redis-crash` |
| Reverted EVM execution with Redis crash | `scripts/local_eoa_recovery.py --transactions 8 --redis-crash --revert` |
| Redis data loss | `scripts/local_redis_disaster.py --loss flush` |
| Redis rollback | `scripts/local_redis_disaster.py --loss rollback` |
| EVM reorg | `scripts/local_eoa_reorg.py` |
| Reorg with reverted execution | `scripts/local_eoa_reorg.py --revert` |
| Solana accepted-send response loss/recovery | `scripts/local_solana_recovery.py --solana-bin-dir "$SOLANA_BIN_DIR" --rust-log debug` |

The live-Redis case checked that the owned Engine process stayed alive, persisted
a continuity halt, rejected admission with HTTP 503 and exited zero on SIGTERM.
Exact-checkpoint reattachment retained the namespace and avoided quarantine;
all 24 original transactions completed, and retries added no chain effects.

## Final-head GitHub Actions

All four workflows completed successfully on `8741b88`; see [run metadata](ci-runs.json).

- [Rust correctness gates](https://github.com/alfongj-com/engine-core/actions/runs/38172595043): workspace and Redis/API suites, local recovery/reorg/load scenarios and dependency audit.
- [Formal verification](https://github.com/alfongj-com/engine-core/actions/runs/38172595008): protocol cases/source guards and fee proofs/mutation checks.
- [Queue tests](https://github.com/alfongj-com/engine-core/actions/runs/38172595001).
- [Queue coverage](https://github.com/alfongj-com/engine-core/actions/runs/38172595033).

## Final-head CI local-chain reports

The existing Rust workflow passed all eight recovery/reorg scenarios and both
local load-and-drain gates. Its [ten JSON reports](ci-local-chain-reports/) are
archived here. Both load gates offered 50 requests/s for 20 seconds and accepted
1,000 requests; all 1,000 reached terminal state after drain (about 30 seconds
total for EVM and 45.5 seconds for Solana). These short debug-build runs do not
show sustained 50 finalized TPS: the measured during-load attempt rates were
44.3/s and 38.0/s respectively. They are correctness smoke tests with backlog,
not maximum-capacity or long-duration steady-state measurements.

## CI patch and external review

[ci-live-redis-restart.patch](ci-live-redis-restart.patch) adds the new live-outage
scenario to the existing workflow. **It is not installed in PR #1:** GitHub
refused the workflow-file update because the available token lacks workflow-edit
permission. The scenario passed locally. All existing workflows passed on the final head.

Devin's UI reported that the diff exceeds its size limit. It produced no actual
review findings; this is not a completed independent review.

## Scope

No public RPCs were used, and no TPS ceiling was measured or requalified. Local
Redis/Anvil/Solana tests do not establish public-provider, real L2 settlement,
host power-loss or production recovery guarantees. Finite model checks and
source hashes are not a complete proof of Rust or of the composed service.
