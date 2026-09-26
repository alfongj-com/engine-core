# To Alfonso

Updated September 26, 2026.

## Formal verification

Added **TLA+ models for queue ownership, EVM/Solana recovery and request retention**, plus **Kani proofs of the production Rust fee arithmetic**. The fee proofs cover all possible integer inputs within their stated assumptions. Protocol models explore finite combinations of workers, crashes, retries and observations.

**35 model checks pass; five Rust proofs pass all 116 checks.** The model suite includes required counterexamples for broken behavior and unsupported guarantees.

All four Linux CI workflows pass at `1c0bb18`: formal verification, the full Rust/Redis/HTTP/local-chain suite, queue tests and queue coverage. [Exact results](formal/evidence/README.md).

The models also exposed queue cancellation and pruning bugs. Those fixes have real Redis regressions, including checks that fail against the previous behavior.

The correctness fix makes history pruning slower: about **123–127 µs per entry** at default retention in a focused Redis benchmark. A reference index is the next performance improvement; earlier throughput numbers did not exercise pruning. [Measurement and limits](formal/evidence/pruning/README.md).

Start with [formal verification](formal/README.md), [coverage and remaining gaps](formal/coverage.md), and the [verification record](docs/verification.md). These checks do **not** prove the entire service correct. Reorg/finality handling, storage loss and dishonest providers have explicit counterexamples; they remain production work.

## Existing qualification

The [fork](https://github.com/alfongj-com/engine-core/tree/production-hardening) builds without Thirdweb Vault. Everything remains in [draft PR #1](https://github.com/alfongj-com/engine-core/pull/1).

The earlier public round verified **332 transactions on five test networks, with zero duplicate effects**, including crash recovery. Short bursts reached offered rates of 10/s on Ethereum, 20/s on the EVM L2s and 5/s on Solana. They do not establish sustained capacity. [Receipts and results](docs/baselines/public-transactions.md).

This formal-verification work used **no paid RPC calls**. Estimated campaign spending remains **$2.22**, the gateway is stopped, and the persistent $12 ceiling remains. Rotate the shared dRPC key when the campaign ends.

## Next production work

1. Define and implement chain-specific finality/reorg handling and Redis disaster recovery.
2. Add operator recovery, production spending limits and sustained multi-wallet tests.
3. Integrate AWS KMS through credential references and qualify deployed smart-account contracts.

**Nothing is needed from you for these checks.** Before deployment, we still need target traffic, AWS/KMS setup, hosting/Redis choice and resolution of the upstream repository's missing license. Follow the [migration guide](docs/replay-migration.md) before upgrading retained queues.
