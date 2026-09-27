# To Alfonso

Updated September 26, 2026, 10:35 p.m. EDT. [Draft PR #1](https://github.com/alfongj-com/engine-core/pull/1).

## Status

**The campaign is in recovery after the native Nitro node exited with a full VM disk. No production throughput limit is qualified.** The latest tests use one signer per chain, local nodes, a shared durable journal, and exact transaction-by-transaction reconciliation.

| Six-minute screen | Offered TPS | Terminal TPS after warmup | What happened |
|---|---:|---:|---|
| Ethereum-like, 12-second blocks | 60 | 58.0 | Unsigned backlog grew; all 21,600 requests eventually completed. |
| OP execution, 2-second blocks | 60 | 57.6 | Backlog grew; all 21,600 eventually completed. |
| Native Arbitrum Nitro, 32 concurrent sends | 50 | 38.4 | Backlog grew; all 18,000 eventually completed. |
| Native Arbitrum Nitro, 64 concurrent sends | 50 | 27.5 | Worse in this ordered comparison; all 18,000 eventually completed. The default stays 32. |
| Native Solana, 5-second status polling | 60 | 59.9 | The client missed 2 requests; all 21,598 admitted requests completed. Late unsigned backlog needs another measurement. |

These are measured outcomes, **not sustainable-rate claims**. All accepted transactions in the five screens above reconciled exactly. That statement does not include the later failed Nitro run. Increasing concurrency did not establish an improvement. [Full reports, hashes and limitations](docs/baselines/capacity-2026-09-26/final-screen-132b012af02d/README.md).

## Latest findings

- Ethereum-like at **55 TPS**: all 19,800 intents completed, but terminal throughput was 53.1 TPS after warmup and backlog grew. Next candidate: 50 TPS.
- OP execution at **55 TPS**: all 19,800 completed; terminal throughput was 55.4 TPS. Backlog measurements vary with block timing, so this needs a repeat before calling it sustainable.
- Nitro at **40 TPS**: the node exited about two minutes into the run; its VM disk was full. Engine admitted 14,400 intents before the harness stopped. The preserved journal has 4,502 terminal records, 302 signed unresolved intents and 9,596 unsigned intents. This run has no final reconciliation and is not a capacity pass.

The failed run's journal and Redis AOF have verified immutable backups. A cold copy of the original VM disk is also verified. The disk is expanded, Nitro reopened its original database, and all saved terminal blocks plus the 302 unresolved signed receipts match. Engine queue recovery is still pending. Recovery will retain the original chain, transaction IDs and signed bytes; it will not turn this interrupted run into a throughput result. The harness also needs to stop new offers earlier when the node remains unavailable.

## Work remaining

1. Recover the interrupted Nitro workload and reconcile every original intent.
2. Confirm individual rates on the final build. Solana's live observer is fixed and tested; its repeat has not run yet.
3. Run all four chains together at the selected individual rates, then measure a sustainable shared rate if the combined load overloads the journal.
4. Exercise mixed transactions, process kills, lost responses, RPC errors, reorgs and Redis recovery at measured load. Report safety and automatic recovery separately.

The current changes bound queue diagnostic history and fix Solana observer fairness. Workspace compilation, targeted Rust/Redis regressions and 99 distinct Python tests pass. All four existing CI workflows pass at `dd9282a`; newer changes are not pushed yet. All 61 finite formal-model cases pass, with 75 source-file hashes reviewed. These models and the prior fee-arithmetic proofs do not prove the whole service.

## Limits

All measurements share one Mac with the nodes and load generator. EVM/OP use Anvil; Nitro and Solana use native development nodes. Rollup L1 settlement, public RPC quotas, AWS KMS throughput, production storage and multi-host recovery remain unqualified. New capacity-harness CI steps are prepared but publishing workflow changes requires GitHub workflow permission.

No paid RPC credits were used for this local campaign. Nothing is needed from you while this work continues. Rotate the shared dRPC key when the campaign ends.
