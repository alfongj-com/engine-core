# Verification record

Updated: **2026-09-17**. Upstream baseline: `b6b7a0bbdc737b3a2b09611305b71b1bf6aba6e8`. The current results below cover the local testnet-configuration, Solana recovery and RPC transport changes with the committed `Cargo.lock`. Earlier queue benchmarks and hosted results retain their original scope.

## Environment and scope

macOS arm64, Apple M4 (10 cores), 16 GiB RAM; Rust 1.98.1; Redis 7.4.2 built from official source; Anvil 1.8.1 from the official release with verified SHA-256; Agave local validator 4.2.2. Service tests use disposable loopback instances. Recovery scripts generate temporary local keys outside the repository.

Public dRPC endpoints were used for **reads and simulation** on Ethereum Sepolia, Arbitrum Sepolia, OP Sepolia, Base Sepolia and Solana Devnet. At the time of those gates, the wallets were unfunded and no Engine transaction was broadcast publicly. Subsequent [faucet funding](testnet-funding.md) succeeded on all five networks; that round verifies balances and faucet receipts only. Successful transfers below ran only on Anvil or the isolated Solana validator. AWS KMS and IAW were not exercised live.

## Current local gates — 2026-09-17

Every recorded gate completed successfully. [Commands, exit codes and timings](baselines/testnet-round-gates.json) accompany the logs. Saved console logs normalize trailing whitespace only. Counts refer to individual suites; they are not summed, because filtered runs, nested signing subprocesses and scenario assertions overlap.

| Gate | Result and evidence |
| --- | --- |
| Format, build and standard suites | `cargo fmt --all --check`, the full workspace test command, and the Engine binary build pass. Includes legacy queue integration tests, account-profile signing checks, Ed25519/wire fixtures, and the bounded Solana HTTP transport. [Workspace log](baselines/testnet-round-workspace.log), [build log](baselines/testnet-round-engine-build.log). |
| Clippy | `cargo clippy --locked --workspace --all-targets` passes with warnings; warnings are retained in the [log](baselines/testnet-round-clippy.log). |
| Queue lease regressions | 14/14 pass against real Redis: ACK ownership, takeover, cancellation, same-ID reuse, refill, concurrency limits and shutdown. [Log](baselines/testnet-round-queue-redis.log). |
| Executor Redis regressions | 34/34 pass, covering EOA ownership/replay/nonce state plus Solana persisted wire recovery, lost responses, bounded retries, stale history, expiry, lock errors, and atomic terminal cleanup. [Log](baselines/testnet-round-executor-redis.log). |
| Solana admission | 4/4 pass: concurrent admission, actual queue-history pruning, changed-intent conflicts, orphan/cancelled recovery evidence, conditional terminal retention and aborted commits. The manual backlog benchmark is excluded from this correctness gate. [Log](baselines/testnet-round-solana-admission.log). |
| HTTP integration | 2/2 pass through actual Axum servers and isolated Redis: EVM/Solana signer authentication, public-only queued identity, concurrent idempotent admission and changed-intent rejection. [Log](baselines/testnet-round-http.log). |
| RPC gateway/probe tests | Node test runner passes its budget, redaction, concurrency, timeout and load-generator tests. These use local fixtures. [Log](baselines/testnet-round-rpc-js.log). |
| Local EOA process recovery | HTTP → Redis → local signer → configured Anvil: 24 accepted pending transfers, Engine SIGKILL/restart, mining and duplicate admission. 24 unique effects, zero duplicates; missing/wrong tokens rejected. [Report](baselines/local-eoa-recovery-testnet-round.json), [log](baselines/testnet-round-eoa-recovery.log). |
| Local EOA Redis recovery | Same 24-transfer scenario with Redis SIGKILL and AOF replay (`appendfsync=always`): 24 unique effects, zero duplicates. This is not a host power-loss or failover test. [Report](baselines/local-eoa-redis-crash.json), [log](baselines/testnet-round-eoa-redis-crash.log). |
| Local Solana recovery | Actual Engine + Agave: 12 accepted send responses discarded, Engine SIGKILL/restart, 12 identical-wire retransmissions, 12 finalized transfer effects and zero duplicates. Recipient/payer balances include 60,000 lamports of fees. Terminal attempts are removed, 12 admission tombstones remain, and duplicate requests after queue-history pruning produce no new jobs or sends. RPC credential sentinel is absent under `RUST_LOG=debug`. [Report](baselines/local-solana-recovery.json), [log](baselines/testnet-round-local-solana-recovery.log). |
| Dependency audit | Zero vulnerability findings; five informational package findings remain, including the `lru` panic-safety advisory. [Current audit JSON](baselines/testnet-round-dependency-audit.json), [dependency review](baselines/dependency-security.md). |

The real Engine [public read/sign-only smoke](baselines/testnet-read-smoke.json) verified four EVM chain IDs and local Solana signing. Solana simulation returned `AccountNotFound` for the unfunded account; it does **not** prove public transaction execution. Public RPC rate measurements have separate [scope and evidence](baselines/rpc-results.md); read throughput is not transaction throughput.

Final review also added encoded/decoded URL credential redaction (path, userinfo and query) and malformed RPC-error-envelope rejection. Their focused regressions pass: [four transport regressions](baselines/testnet-round-rpc-url-variants.log), [17 Node tests](baselines/testnet-round-rpc-js.log).

## Solana admission backlog screen

The actual initialized admission path measured about 4,866 admissions/s with an empty backlog and 4,864/s with 100,000 pending jobs at concurrency 32. All 22,000 new identities matched persisted fingerprints; 400 duplicate retries created no jobs. This screens for a backlog-size regression, not sustainable transaction throughput: one run per case, debug build, local Redis 7.4.2 with persistence disabled, no HTTP, worker, signing or RPC. [Raw results](baselines/solana-admission-backlog.json).

## Earlier qualification and benchmark history — 2026-09-15

These records remain useful regression and performance evidence; they do not substitute for testing newer source.

| Evidence | Original result |
| --- | --- |
| Unchanged upstream queue replay | Six of seven new regressions failed, including 32 successful competing acknowledgements/hooks where one was required. [Log](baselines/queue-upstream-regressions.log). |
| EOA mutation checks | Deliberately restoring defective branches made eleven of thirteen regressions fail. [Log](baselines/eoa-mutation-regressions.log). |
| Initial fixed suites | Queue regressions 14/14, legacy queue tests 16/16, EOA store regressions 16/16. [Queue regressions](baselines/queue-fixed-regressions.log), [legacy tests](baselines/queue-legacy-tests-final.log), [EOA store](baselines/eoa-fixed-regressions.log). |
| Matched queue measurements | 42 runs; 1,305,000 results independently reconciled, using unchanged upstream and isolated patched variants with matching dependencies. [Report and raw evidence](baselines/queue-results.md). |
| Initial workspace and recovery | Earlier [workspace log](baselines/workspace-tests-final.log), [Redis regressions](baselines/redis-regressions-final.log), [HTTP log](baselines/http-regressions-final.log), and [local EOA report](baselines/local-eoa-recovery.json) are retained. |

## Hosted verification

Current source commit **`3aad56d21b479f5b62bd18bd60030d3f22ffcd9c` passed all three Linux workflows**: [Rust correctness](https://github.com/alfongj-com/engine-core/actions/runs/35189854266), [queue tests](https://github.com/alfongj-com/engine-core/actions/runs/35189854267), and [queue coverage](https://github.com/alfongj-com/engine-core/actions/runs/35189854352). The full workflow includes real Redis/HTTP faults, EOA process and Redis AOF recovery, the actual Solana validator scenario, and the dependency audit. [Final run metadata](baselines/testnet-round-ci-results.json) records every step and the exact source. Subsequent commits contain documentation/evidence only.

The **earlier** source commit `648ef088cb95168e12d5e7643ae825c8791ed7b5` passed three Linux workflows: [Rust correctness](https://github.com/alfongj-com/engine-core/actions/runs/34943655662), [queue tests](https://github.com/alfongj-com/engine-core/actions/runs/34943655977), and [queue coverage](https://github.com/alfongj-com/engine-core/actions/runs/34943655828). [Run metadata](baselines/ci-results.json) retains the exact commit and outcomes. These historical results retain their earlier scope; the current source is qualified by the new runs above.

## Reproduce the current local gates

Use disposable Redis: the legacy suite performs aggressive pruning. Install Rust, Node.js, Redis 7.4.2, Anvil 1.8.1 and Solana/Agave 4.2.2. Set `TEST_REDIS_URL`, `REDIS_SERVER_BIN`, `REDIS_CLI_BIN`, `ANVIL_BIN` and `SOLANA_BIN_DIR` to your isolated services/tools. The recovery scripts start their own local chains and Redis instances.

```sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets
cargo test --locked --workspace -- --test-threads=1
cargo test --locked -p twmq --lib -- --ignored
cargo test --locked -p engine-executors --lib -- --ignored --test-threads=1
cargo test --locked -p thirdweb-engine --lib solana_admission::tests -- --ignored
cargo test --locked -p thirdweb-engine --test api_safety -- --ignored
node --test scripts/rpc/budget-gateway.test.mjs scripts/rpc/load-probe.test.mjs
cargo build --locked -p thirdweb-engine --bin thirdweb-engine
python3 scripts/local_eoa_recovery.py --report /tmp/local-eoa-recovery.json
python3 scripts/local_eoa_recovery.py --redis-crash --report /tmp/local-eoa-redis-crash.json
python3 scripts/local_solana_recovery.py --rust-log debug --report /tmp/local-solana-recovery.json
cargo audit --json
```

Use the explicit `solana_admission::tests` filter: an unfiltered server `--ignored` run also selects the manual backlog benchmark, which has separate inputs. The ignored environment-key helper runs in fresh subprocesses from its parent signing test. The inherited live EIP-7702 test remains skipped because it requires a Thirdweb bundler and deployed Base Sepolia contracts; its skip is not coverage.

## Evidence boundaries

- Queue benchmarks use Redis with persistence disabled. They establish neither Redis restart/failover durability nor blockchain TPS. Final dependency changes are separate from the identical-lockfile queue comparisons.
- EOA Redis SIGKILL/AOF recovery covers one local crash window, not host power loss, replication failover, every state transition, reorgs or external nonce use. Solana's validator scenario restarts Engine with Redis kept alive; it does not test Solana recovery across Redis loss.
- Expired Solana signatures with absent or stale historical status remain **outcome unknown**. Their signed bytes/admission identity are retained; automatic re-signing is disabled. Cancellation/orphan recovery still needs operator reconciliation, and completed admission retention is finite/configurable.
- Local signature checks cannot prove deployed wallet/EntryPoint compatibility. ERC-4337 and EIP-7702 require qualification against pinned account implementations, EntryPoint, bundler and chain. [Account-specific scope](design/userop-signing.md).
- No Engine public submission test, live KMS/IAW, public transaction throughput, 24-hour soak, production webhook delivery or Docker build is claimed. Public faucet funding is recorded separately. Solana signing here uses a local Ed25519 key file.
- Compiler/Clippy warnings remain visible in the logs. A successful gate is not a warning-free build or a blanket production-readiness claim.
