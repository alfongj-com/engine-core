# To Alfonso

Updated September 17, 2026.

## Where we are

The [fork](https://github.com/alfongj-com/engine-core/tree/production-hardening) builds without Vault. The EVM path passed local tests, including a crash/restart with 24 transfers, and all three Linux CI workflows passed. [PR #1](https://github.com/alfongj-com/engine-core/pull/1) contains the changes; [verification](docs/verification.md) contains the evidence.

**Public-chain performance has not been measured.** The queue benchmark reached about 26,600 jobs/second; that does not establish blockchain throughput. Solana execution code exists, but removing Vault removed its only working signer. Replacing that signer is part of the next work.

## Recommended test plan

Test **Ethereum Sepolia, Arbitrum Sepolia, OP Sepolia, Base Sepolia, and Solana Devnet**, one at a time. Devnet is Solana's network for testing applications; its separately named Testnet is mainly for validator testing. Start with ordinary wallet transfers, then contract calls. Smart accounts need separate tests and bundler pricing.

Try **dRPC paid** first: its published price is **$6 per million ordinary RPC calls**, with all five networks listed and no published paid-tier rate cap. It is the cheapest candidate for our short load tests among the plans compared. We still need to measure its actual speed.

| Workload | Estimated RPC demand | dRPC usage cost |
| --- | ---: | ---: |
| 100 EVM transactions/second | Roughly 410–460 calls/second | About $9–10/hour |
| 100 Solana transactions/second; assumed 15-second wait for finality | Roughly 2,000 calls/second before improving polling | About $43/hour |

These are estimates before retries, not benchmark results. The [RPC comparison](docs/design/rpc-test-plan.md) explains the assumptions, prices, quotas, and alternatives.

Ten-minute runs at 10, 50, and 100 transactions/second on each network would cost roughly **$14–30 total in RPC usage** under the documented scenarios. I suggest a **$50 total RPC spending limit** for the first round, including retries. Initial deposits, taxes, and test-wallet funding are separate. No service has been purchased.

## Next steps

1. **Connect other providers.** Add per-chain EVM RPC settings, reuse connections, count requests by method, and remove RPC keys from logs. Solana already has an endpoint setting.
2. **Restore Solana signing and test recovery locally.** Keep keys out of Redis. Test crashes, expired blockhashes, uncertain submissions, and duplicate requests.
3. **Check the provider's speed.** Verify the actual RPC methods, submission limits, errors, and latency. Aim for twice the expected request capacity so throttling does not distort the engine benchmark.
4. **Run the five networks.** Start at one transaction/second and increase in short steps only while the chain and provider keep up. Report successful transactions, missing or duplicate effects, confirmation times, RPC errors, and cost.
5. **Fix the measured bottleneck and repeat.** Solana status batching is an obvious candidate. Then finish finality/reorg recovery, test Redis failures, and connect AWS KMS before discussing production.

## What I need from you

**Nothing to continue the local work.** Public tests will eventually need an RPC account/key and approval of its exact spending limit. I will use fresh test-only wallets, try the faucets, and identify any funding shortfall.

Before release, we also need to resolve the upstream repository's missing license. Remaining issues and retained-job migration instructions are in the [security audit](docs/audit-security.md), [queue audit](docs/audit-queue.md), and [migration guide](docs/replay-migration.md).
