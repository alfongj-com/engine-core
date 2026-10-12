# PR #2 status

Updated October 12, 2026. PR #1 merged into `main` at `154b0ac`.
[PR #2](https://github.com/alfongj-com/engine-core/pull/2) now targets `main`.
Implementation is complete; final qualification and matched measurements are running.

## Changes

- Schema2 records the exact journal-to-Redis transition in the authority commit.
  Startup or health repair can finish it after a crash or lost reply. A second
  FULL SQLite commit acknowledges publication before permission returns.
- Arbitrary rollback, wrong Redis process/role, corrupt records and durable halts
  still block execution. Healthy schema1 journals migrate under exclusive ownership;
  old binaries reject schema2. Status/export remain read-only.
- Journal operations get one contender per chain ahead of the shared mutex.
  Separate metrics expose lane, global-lock, health-capacity, storage and Redis waits.
- Solana history lookups batch within one cached cluster/endpoint: at most 256
  outstanding polls and four HTTP requests, with a 5 ms collection window.
- Typed publication uncertainty retries the original work after 2 seconds. It does not
  generate a replacement identity or clear a permanent fence.

## Validation in progress

The instrumented baseline is commit `01d5048`; its unchanged binary is preserved
privately. The full Rust suite, all 44 ignored journal regressions, 73 executor regressions,
33 queue regressions, HTTP tests and eight real HTTP batching tests passed. All
73 model configurations produced their expected outcomes. A later allocation-only
fix bounds the task-local wrapper size and passes real EVM process recovery; CI
will recheck the final source map. Remaining local gates are the other process
scenarios and matched throughput comparisons.

All new experiments use disposable local Redis, Anvil and Agave fixtures. No paid
RPC calls are needed. Raw reports and fixture data stay private under
`/Users/alfongj/.local/share/engine-core/runs/pr2-oct12/`; sanitized reports and model
logs belong on the separate [load-tests branch](https://github.com/alfongj-com/engine-core/tree/load-tests).

## Limits that remain

One Unix host and one retained authoritative journal are required. This does not
recover a lost/rolled-back authority, prove filesystem durability, rebuild every
missing Redis queue, or resolve quarantined unknown transactions automatically.

Fairness covers journal operation opportunities. Global admission/health capacity
and the shared overflow lane can still delay a peer. Solana batching reduces
transport calls without changing finality or retry budgets; it does not qualify
public RPC quotas or actual rollup consensus. Local throughput is not a mainnet
capacity guarantee. Models remain finite abstractions, not whole-service proofs.

See [LOCAL_TESTING.md](LOCAL_TESTING.md), the [protocol design](docs/design/recoverable-projection.md),
[Solana polling design](docs/design/solana-status-batching.md), and
[formal correspondence](formal/disaster-recovery.md).
