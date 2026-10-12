# Local testing on Alfonso's Mac

Last updated: **October 12, 2026**. These instructions run disposable local
fixtures. They do not qualify public RPC capacity, rollup settlement or production
readiness.

## What is saved

| Item | Location |
| --- | --- |
| Main checkout | `/Users/alfongj/Code/engine-core` |
| PR #1 baseline | Merged into `main` at `154b0ac`; preserved branch `production-hardening` at `8741b88` |
| PR #2 baseline | Merged into `main` at `918d7d9`; preserved branch `crash-recovery-and-chain-fairness` |
| Current recovery follow-up | Branch `recover-retained-transactions`; implementation checkout `/Users/alfongj/Code/engine-core-review-stack` |
| Full historical evidence in Git | Local and remote branch `load-tests`; use its current head for new evidence |
| Browsable local evidence | `/Users/alfongj/.local/share/engine-core/evidence/load-tests-603c228/LOAD_TESTS.md` |
| Installed test tools and checksum manifest | `/Users/alfongj/.local/share/engine-core/tools/` |
| Private retained incident data; leave untouched | `/Users/alfongj/.local/share/engine-core-custody/` |

PRs #1 and #2 are merged. The current follow-up adds schema-3 retained Solana
recovery and offline reattachment of pending targets. Read [WORK_IN_PROGRESS.md](WORK_IN_PROGRESS.md)
for exact validation and remaining limits. Historical reports apply to their
recorded source hashes, not automatically to the current checkout.

The tools formerly under `/tmp/engine-*` were removed by temporary-file cleanup.
Use the persistent paths below. The old release binary is preserved as
`tools/engine-pr1-b31f43d` for provenance; build the checkout being tested instead
of treating that binary as PR #2.

## Installed tools

```sh
source /Users/alfongj/.local/share/engine-core/tools/env.sh
cargo --version
"$PYTHON_BIN" --version
"$ANVIL_BIN" --version
"$REDIS_SERVER_BIN" --version
"$SOLANA_BIN_DIR/solana-test-validator" --version
"$JAVA" -version
```

The environment selects Rust **1.98.1**, Homebrew Python **3.14**, Node **22**,
Foundry **1.8.1** Anvil/Cast, Redis **7.4.2**, Agave **4.2.2**, Java **21** and
TLC **1.7.4**. CMake and pkg-config are installed through Homebrew. Downloads,
SHA-256 values and version checks are retained in `tools/manifest.json` and
`tools/tool-versions.json`. The environment exports `ANVIL_BIN`, `CAST_BIN`,
`REDIS_SERVER_BIN`, `REDIS_CLI_BIN`, `SOLANA_BIN_DIR`, `JAVA`, `TLA_JAR` and
`PYTHON_BIN`; it does not start services or supply secrets.

Rust builds use two build jobs, no incremental compilation and no debug symbols
to limit memory/disk use. Some first-time Cargo dependencies may still require
downloads; the transaction tests themselves use loopback RPCs. The system
`/usr/bin/python3` is older; use the sourced environment or `"$PYTHON_BIN"`.

## 1. Use separate checkouts

Inspect any existing target directory before reusing it; do not reset or replace
another agent's work. These commands create detached test checkouts when absent:

```sh
cd /Users/alfongj/Code/engine-core
git status --short --branch
git worktree add --detach /Users/alfongj/Code/engine-core-test-baseline 918d7d9
git worktree add --detach /Users/alfongj/Code/engine-core-test-wip recover-retained-transactions
cd /Users/alfongj/Code/engine-core-test-baseline
source /Users/alfongj/.local/share/engine-core/tools/env.sh
unset CARGO_TARGET_DIR
```

Test the baseline before the WIP checkout. Several process harnesses explicitly
run **this checkout's `target/debug/thirdweb-engine`**. A shared or alternate
`CARGO_TARGET_DIR` can make them run the wrong binary or fail to find it.

Create a new private run directory and keep temporary fixture data there too:

```sh
mkdir -p /Users/alfongj/.local/share/engine-core/runs
umask 077
export ENGINE_TEST_RUN="$(mktemp -d /Users/alfongj/.local/share/engine-core/runs/local-test.XXXXXX)"
mkdir "$ENGINE_TEST_RUN/tmp" "$ENGINE_TEST_RUN/reports" "$ENGINE_TEST_RUN/redis"
export TMPDIR="$ENGINE_TEST_RUN/tmp/"
export ENGINE_TEST_REPORTS="$ENGINE_TEST_RUN/reports"
export PYTHONPATH="$PWD/scripts"
git rev-parse HEAD > "$ENGINE_TEST_REPORTS/source-commit.txt"
```

Keep failed reports and fixture directories. They may contain local test keys and
signed requests; do not commit them or print their contents into a public report.
Capture command exit codes as well as logs; a pipeline ending in `tee` can hide a
failure unless the shell enables `pipefail`.

## 2. Fast harness checks

```sh
node --test scripts/rpc/budget-gateway.test.mjs scripts/rpc/load-probe.test.mjs
"$PYTHON_BIN" -m unittest discover -s scripts -p 'test_testnet_*.py' -v
"$PYTHON_BIN" -m unittest discover -s formal -p 'test_*.py' -v
"$PYTHON_BIN" -m unittest \
  test_capacity_faults capacity_campaign_test capacity_scenarios_test \
  capacity_pool_test capacity_assess_test capacity_fence_test \
  capacity_resource_guard_test capacity_resource_phase_test \
  capacity_resource_projection_test capacity_anvil_custody_test \
  capacity_infrastructure_test capacity_journal_reader_test capacity_metrics_test test_local_eoa_reorg -v
```

The final command exercises the capacity and fault oracles; require zero skipped tests
with Anvil and Cast available. Some cases start disposable Anvil nodes. The
archived supervisor needed by one regression is retained in
`scripts/fixtures/capacity_supervisor.py`; do not run that historical supervisor
against the retained Nitro VM.

## 3. Build and run Rust tests with fresh Redis

Choose a free loopback port, start a new Redis child, and verify the responding
server is that child. Do not point `TEST_REDIS_URL` at an existing service: tests
can delete data.

```sh
ENGINE_TEST_REDIS_PORT="$("$PYTHON_BIN" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
"$REDIS_SERVER_BIN" --bind 127.0.0.1 --port "$ENGINE_TEST_REDIS_PORT" \
  --dir "$ENGINE_TEST_RUN/redis" --appendonly yes --appendfsync always \
  --save '' --daemonize no > "$ENGINE_TEST_RUN/redis.log" 2>&1 &
ENGINE_TEST_REDIS_PID=$!
sleep 1
kill -0 "$ENGINE_TEST_REDIS_PID"
"$REDIS_CLI_BIN" -p "$ENGINE_TEST_REDIS_PORT" INFO server \
  | tr -d '\r' | grep -qx "process_id:$ENGINE_TEST_REDIS_PID"
export TEST_REDIS_URL="redis://127.0.0.1:$ENGINE_TEST_REDIS_PORT/"
```

Proceed only if both identity checks succeed; a port race means starting a new
fixture, not connecting to the unrelated server. Keep this shell's PID for cleanup.

```sh
cargo fmt --all --check
cargo test --locked --workspace --no-run
cargo test --locked --workspace -- --test-threads=1
cargo test --locked -p engine-core --lib recovery -- --ignored --test-threads=1
cargo test --locked -p engine-executors --lib -- --ignored --test-threads=1
cargo test --locked -p thirdweb-engine --lib solana_admission::tests -- --ignored --test-threads=1
cargo test --locked -p thirdweb-engine --test api_safety -- --ignored --test-threads=1
cargo test --locked -p twmq --lib -- --ignored --test-threads=1
cargo build --locked --bin thirdweb-engine
```

Record failures before making any change. Review model/source correspondence
before updating proof hashes. Include the ignored recovery and executor suites:
a normal `cargo test` alone does not run real Redis/SIGKILL regressions.

After the Rust suites, stop only the child started above:

```sh
kill -TERM "$ENGINE_TEST_REDIS_PID"
wait "$ENGINE_TEST_REDIS_PID"
unset TEST_REDIS_URL
```

The following process harnesses each create their own Redis and chain nodes.

## 4. Actual local crash, reorg and recovery scenarios

Run sequentially after the build succeeds:

```sh
"$PYTHON_BIN" scripts/local_eoa_recovery.py --report "$ENGINE_TEST_REPORTS/eoa.json"
"$PYTHON_BIN" scripts/local_eoa_recovery.py --redis-crash --report "$ENGINE_TEST_REPORTS/eoa-redis.json"
"$PYTHON_BIN" scripts/local_eoa_recovery.py --live-redis-restart --report "$ENGINE_TEST_REPORTS/eoa-live-redis.json"
"$PYTHON_BIN" scripts/local_eoa_recovery.py --transactions 8 --redis-crash --revert --report "$ENGINE_TEST_REPORTS/eoa-revert.json"
"$PYTHON_BIN" scripts/local_redis_disaster.py --loss flush --report "$ENGINE_TEST_REPORTS/redis-flush.json"
"$PYTHON_BIN" scripts/local_redis_disaster.py --loss rollback --report "$ENGINE_TEST_REPORTS/redis-rollback.json"
"$PYTHON_BIN" scripts/local_eoa_reorg.py --report "$ENGINE_TEST_REPORTS/reorg.json"
"$PYTHON_BIN" scripts/local_eoa_reorg.py --revert --report "$ENGINE_TEST_REPORTS/reorg-revert.json"
"$PYTHON_BIN" scripts/local_solana_recovery.py --solana-bin-dir "$SOLANA_BIN_DIR" \
  --rust-log debug --report "$ENGINE_TEST_REPORTS/solana.json"
"$PYTHON_BIN" scripts/local_solana_recovery.py --solana-bin-dir "$SOLANA_BIN_DIR" \
  --drop-attempts-on-restart --rust-log debug --report "$ENGINE_TEST_REPORTS/solana-projection-loss.json"
```

These check actual signed identities, duplicate admission, reverted execution,
provisional reorgs, ambiguous responses and quarantine. A refusal to restart or
an unresolved transaction is a result to investigate, not permission to clear a
journal fence or replace a signed attempt. The bounded recovery fixtures use
short leases; their results do not establish high-load recovery.

Optional, after those gates pass: run short, low-rate throughput smoke checks.
Do not build or run other tests simultaneously with a timing measurement.

```sh
"$PYTHON_BIN" scripts/local_eoa_throughput.py --engine-bin "$PWD/target/debug/thirdweb-engine" \
  --rate 5 --seconds 20 --redis-fsync everysec --drain-seconds 120 \
  --report "$ENGINE_TEST_REPORTS/eoa-load.json"
"$PYTHON_BIN" scripts/local_solana_throughput.py --engine-bin "$PWD/target/debug/thirdweb-engine" \
  --solana-bin-dir "$SOLANA_BIN_DIR" --rate 5 --seconds 20 --redis-fsync everysec \
  --drain-seconds 120 --report "$ENGINE_TEST_REPORTS/solana-load.json"
```

These are correctness/load smoke tests, not new sustainable-capacity claims.
Historical maximum-rate experiments remain in the separate evidence archive.

## 5. Formal checks and report

Run `"$PYTHON_BIN" formal/check.py` with the sourced Java/TLC paths.
The runner verifies source hashes and requires the named counterexamples from
fault models. Hashes are a review guard, not a Rust refinement proof. Kani fee proofs use a separate verifier setup described in
[formal/fees.md](formal/fees.md); this local setup does not install Kani.

Write a concise `SUMMARY.md` inside `$ENGINE_TEST_REPORTS`: source commit and
binary hash, tool versions, exact commands, passed/failed/skipped counts, failure
reproductions, report locations and remaining gaps. Keep baseline and WIP results
separate. Confirm owned processes stopped; retain all unresolved state. Do not
change or resume `/Users/alfongj/.local/share/engine-core-custody/` or the old
`engine-nitro` VM/container.

## Prompt for another local agent

```text
Test Engine Core locally on this Mac.

Repository: /Users/alfongj/Code/engine-core
Read README.md and LOCAL_TESTING.md first. Read the saved WIP notes with:
  git show recover-retained-transactions:WORK_IN_PROGRESS.md
Load tools with:
  source /Users/alfongj/.local/share/engine-core/tools/env.sh

Use isolated test checkouts as documented; do not switch or reset the main
checkout. Compare baseline 918d7d9 (merged PR #2) and recover-retained-transactions
in separate checkouts. Inspect existing test directories
before reusing them. Build each checkout's own target/debug/thirdweb-engine.

Run the fast harness checks, compile, Rust/Redis regressions and local EVM/Solana
process scenarios in LOCAL_TESTING.md. Use fresh disposable Redis and chain state,
loopback endpoints and generated test keys. No paid/public RPCs, existing wallet
keys, faucets, old incident journals or the retained Nitro VM. Preserve failed
reports and runtime data. Do not clear safety fences or suppress failed tests.

The follow-up restores exact retained Solana attempts and reattaches pending targets.
Read WORK_IN_PROGRESS.md for the latest qualification results and remaining limits.
Verify the exact commit you build; historical test passes do not qualify later code.
Do not implement features or refresh proof hashes merely to make testing pass.
Report precise failures and keep independent baseline checks running when possible.
Run optional low-rate smoke tests only after correctness gates pass; do not start
a long capacity campaign.

Save reports under /Users/alfongj/.local/share/engine-core/runs/ with a concise
SUMMARY.md covering commits, commands, counts, failures, reproduction steps and
remaining gaps. Stop only processes you started. Keep code changes and any fixes
separate for review; do not merge or push them without further instruction.
```
