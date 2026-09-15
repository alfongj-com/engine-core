# Verification record

Date: 2026-09-15. Upstream baseline: `b6b7a0bbdc737b3a2b09611305b71b1bf6aba6e8`. Applies to the hardening commit containing this file and its committed `Cargo.lock`.

## Local environment

macOS arm64, Apple M4 (10 cores), 16 GiB RAM; Rust 1.98.1; Redis 7.4.2 built from official source; Anvil 1.8.1 downloaded from the official release with verified SHA-256. Tests use an explicit disposable Redis URL on loopback; the server recovery script starts its own Redis and Anvil. No real funds or production credentials were used. The desktop was shared with other applications; benchmark limitations are reported separately.

## Gates

| Gate | Result and evidence |
| --- | --- |
| Standard suites | 45/45 pass across non-queue workspace crates; queue tests run separately below. [Log](baselines/workspace-tests-final.log). |
| Full workspace compilation | `cargo test --locked --workspace --no-run`; private Vault dependencies removed. |
| Queue regressions | 14/14 pass against real Redis: competing ACKs, lease takeover, cancellation, same-ID reuse, refill, concurrency limits, idle behavior and shutdown. [Log](baselines/queue-fixed-regressions.log). |
| Existing queue integration tests | 16/16 pass, including 100,000 lanes and pruning races. [Log](baselines/queue-legacy-tests-final.log). |
| EOA store regressions | 16/16 pass; real Redis ownership/EXEC aborts, immutable admission, replay retention, nonce reservation/recycling. [Log](baselines/eoa-fixed-regressions.log). |
| Other Redis regressions | Receipt outage/replacement and legacy signed-wire tests, plus deployment takeover: all pass. Together with the store suite, 19/19 pass. [Log](baselines/redis-regressions-final.log). |
| HTTP integration | Actual Axum HTTP server + isolated Redis: unauthorized mutation denied, authorized mutation works, diagnostics redact secrets, admission persists replay identity and preserves duplicates. 1/1 pass. [Log](baselines/http-regressions-final.log). |
| Signing | Independent recovery/decoding of message and transaction signatures, mismatched identity rejection, environment rotation/missing/invalid key, secret-free queue serialization and Debug output. 9/9 signing scenarios pass; default account-profile binding also passes. [Account-specific scope](design/userop-signing.md). |
| Webhook tests | 8/8 pass: origin policy, DNS address classification, redirect refusal, HTTPS enforcement, response bounds and UTF-8-safe errors. Real external HTTPS/DNS rebinding attack infrastructure was not exercised. |
| Local EOA process recovery | Actual HTTP → Redis → signer → Anvil: SIGKILL with 24 accepted pending transfers, restart, mine, confirm and resubmit same IDs. 24 unique effects, 0 additional effects; missing/wrong signer tokens rejected. [Report](baselines/local-eoa-recovery.json). |
| Dependency audit | Zero vulnerability findings; five informational findings remain. [Review](baselines/dependency-security.md). |
| Matched queue measurements | 42 runs, 1,305,000 results independently reconciled. [Report and raw evidence](baselines/queue-results.md). |

The queue regression suite was first replayed against unchanged upstream: six of seven tests failed, including 32 competing successful acknowledgements/hooks where one was required. Separately, deliberately restoring defective EOA branches made eleven of thirteen regressions fail. [Upstream queue evidence](baselines/queue-upstream-regressions.log), [EOA mutation evidence](baselines/eoa-mutation-regressions.log).

## Reproduce the final gates

Use a disposable Redis instance; the legacy suite also performs aggressive pruning. Install Rust via rustup and make Redis 7.4.2 available. Set `TEST_REDIS_URL`, `REDIS_SERVER_BIN` and `ANVIL_BIN` to your isolated tools/services.

```sh
cargo fmt --all --check
cargo test --locked --workspace --no-run
cargo test --locked --workspace -- --test-threads=1
cargo test --locked -p twmq --lib -- --ignored
cargo test --locked -p engine-executors --lib -- --ignored --test-threads=1
cargo test --locked -p thirdweb-engine --test api_safety -- --ignored
cargo build --locked --bin thirdweb-engine
python3 scripts/local_eoa_recovery.py --report /tmp/local-eoa-recovery.json
cargo audit
```

The local run partitioned standard tests into `--workspace --exclude twmq` and the queue suite; the combined command above is the equivalent CI gate. Across these standard suites and explicitly enabled Redis/HTTP tests, 95 test cases passed. Tests marked ignored require explicit services; the final gates above run them. The environment-key helper is also marked ignored, but the parent signing test runs it in four fresh subprocesses. The inherited live EIP-7702 test remains skipped because it requires a Thirdweb bundler and deployed Base Sepolia contracts; its skip is not counted as coverage.

## Evidence boundaries

- Queue results measure committed Redis state with persistence disabled. They do not establish Redis restart/failover durability, exactly-once external delivery, or blockchain TPS. Final dependency upgrades are separate from the queue's identical-lockfile comparisons.
- Local EOA recovery proves one process-crash window with a non-mining mempool. Redis power loss, crash before every state transition, chain reorgs, external nonce consumption, and multi-provider disagreement still need fault tests.
- Local signature recovery alone cannot prove deployed wallet/EntryPoint compatibility. ERC-4337 and EIP-7702 require qualification against pinned account implementations, EntryPoint, bundler and chain.
- No live KMS/IAW, public-chain load test, 24-hour soak, production webhook delivery, Docker build or hosted CI pass is claimed.
- Existing compiler warnings remain in legacy queue fixtures and unused executor tuning constants. They are not hidden by the gates.
