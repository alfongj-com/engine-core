# To Alfonso

Updated September 26, 2026, 11:31 p.m. EDT. [Draft PR #1](https://github.com/alfongj-com/engine-core/pull/1).

## Status

**The Nitro disk incident is recovered. Individual capacity, shared load and planned chaos qualification are still in progress; no production throughput limit is qualified.** The latest tests use one signer per chain, local nodes, a shared durable journal, and exact transaction-by-transaction reconciliation.

| Six-minute screen | Offered TPS | Terminal TPS after warmup | What happened |
|---|---:|---:|---|
| Ethereum-like, 12-second blocks | 60 | 58.0 | Unsigned backlog grew; all 21,600 requests eventually completed. |
| OP execution, 2-second blocks | 60 | 57.6 | Backlog grew; all 21,600 eventually completed. |
| Native Arbitrum Nitro, 32 concurrent sends | 50 | 38.4 | Backlog grew; all 18,000 eventually completed. |
| Native Arbitrum Nitro, 64 concurrent sends | 50 | 27.5 | Worse in this ordered comparison; all 18,000 eventually completed. The default stays 32. |
| Native Solana, 5-second status polling | 60 | 59.9 | The client missed 2 requests; all 21,598 admitted requests completed. Late unsigned backlog needs another measurement. |

These are measured outcomes, **not sustainable-rate claims**. All accepted transactions in the five screens above reconciled exactly. That statement does not include the later failed Nitro run. Increasing concurrency did not establish an improvement. [Full reports, hashes and limitations](docs/baselines/capacity-2026-09-26/final-screen-132b012af02d/README.md).

## Latest findings

- Latest Solana at **70 TPS**: 25,155 requests accepted, 45 rejected with HTTP 429; late terminal throughput was 68.9 TPS and unsigned backlog grew. All accepted requests subsequently passed an independent finalized-receipt, effect and fee audit. The disk guard interrupted the original verification; its phase budget and validator history retention are now fixed. **70 TPS is not a clean capacity result.** [Evidence](docs/baselines/capacity-2026-09-26/solana70-guard-incident/README.md).

- Ethereum-like at **55 TPS**: all 19,800 intents completed, but terminal throughput was 53.1 TPS after warmup and backlog grew. Next candidate: 50 TPS.
- OP execution at **55 TPS**: all 19,800 completed; terminal throughput was 55.4 TPS. Backlog measurements vary with block timing, so this needs a repeat before calling it sustainable.
- Nitro at **40 TPS**: the node exited about two minutes into the run; its VM disk was full. Engine admitted 14,400 intents before the harness stopped. At interruption, its journal had 4,502 terminal records, 302 signed unresolved intents and 9,596 unsigned intents. The subsequent recovery below closed all accepted work; the interrupted run remains a failed capacity test.

After verified backups and disk expansion, Nitro rebuilt its original state. Engine then resumed the original queue: **all 14,400 intents reconciled exactly**, with matching nonces, balances and fees, no changed signed identities, and no unresolved work. Queue drain took 181 seconds after restart; disk repair and checks took additional time. No new workload or manual transaction replay was used. The interrupted run remains a failed capacity test. The harness now checks disk reserves, stops intake after a sustained node outage, and preserves interrupted transaction custody.

## Work remaining

1. Running the next individual cohort: EVM and OP at 60 TPS, then Solana at 65. Next is an adjacent old/new Nitro comparison at 50. These are test inputs, not qualified limits.
2. Run all four chains together at the selected individual rates, then measure a sustainable shared rate if the combined load overloads the journal.
3. Exercise mixed transactions, process kills, lost responses, RPC errors, reorgs and Redis recovery at measured load. Report safety and automatic recovery separately.

The dispatch implementation is pushed at `cbdedae`: ordered durable authorization now overlaps bounded EVM sends. Workspace compilation, six new dispatch regressions, four negative controls and two actual Engine reorg tests pass. All 61 finite formal-model cases pass, with 76 source-file hashes reviewed. The updated harness has 139 distinct passing Python tests plus 11 supervisor tests. These models and the prior fee-arithmetic proofs do not prove the whole service. A small shared four-chain smoke also reconciled all 48 intents; it does not measure capacity.

## Limits

All measurements share one Mac with the nodes and load generator. EVM/OP use Anvil; Nitro and Solana use native development nodes. Rollup L1 settlement, public RPC quotas, AWS KMS throughput, production storage and multi-host recovery remain unqualified. New capacity-harness CI steps are prepared but publishing workflow changes requires GitHub workflow permission.

No paid RPC credits were used for this local campaign. Nothing is needed from you while this work continues. Rotate the shared dRPC key when the campaign ends.
