# RPC results — September 17, 2026

## Decision

dRPC handled the tested read mix at 1,000 requests/second on all five networks for 30 seconds. Subsequent [funded Engine transaction tests](public-transactions.md) passed on all five networks. These short checks do not prove sustained Engine throughput. Solana needs further capacity work before a 100-transaction/second target: the planning model allows about 2,000 RPC/s, with a desired margin of 4,000 RPC/s. Those higher rates did not pass this screen; [actual polling measurements](../design/solana-polling-cost.md) now provide a smaller-workload baseline.

## 30-second follow-up

Each network received 30,000 scheduled calls, one method per HTTP request. Every call returned a populated JSON-RPC result. There were **zero upstream errors, null results, timeouts, local concurrency drops or scheduler drops**. Completed RPS includes draining the final requests.

| Network | Completed RPS | p95 latency | p99 latency | Raw report |
| --- | ---: | ---: | ---: | --- |
| Ethereum Sepolia | 997.5 | 83.8 ms | 276.9 ms | [JSON](rpc-11155111-verified.json) |
| Arbitrum Sepolia | 998.1 | 75.6 ms | 171.1 ms | [JSON](rpc-421614-verified.json) |
| OP Sepolia | 998.1 | 73.2 ms | 167.7 ms | [JSON](rpc-11155420-verified.json) |
| Base Sepolia | 997.5 | 78.1 ms | 207.8 ms | [JSON](rpc-84532-verified.json) |
| Solana Devnet | 997.6 | 299.1 ms | 744.9 ms | [JSON](rpc-solana-devnet-verified.json) |

Earlier five-second ramps and a first 30-second run are retained in the files without `-verified`. They used a weaker response classifier; use the stricter rerun above for qualification.

## Solana higher-rate screen

| Offered RPS | Scheduled | Dispatched / successful | Local concurrency drops | p99 latency |
| ---: | ---: | ---: | ---: | ---: |
| 2,000 | 10,000 | 9,354 | 646 | 4.08 s |
| 4,000 | 20,000 | 14,371 | 5,629 | 6.94 s |

The earlier classifier labeled all dispatched calls successful, but rising latency filled the 1,024-request concurrency limit. These stages are **unqualified**, not evidence of a specific provider rate cap: the client stopped admitting some scheduled work. [Raw report](rpc-solana-devnet-high-rate.json).

## Cost and controls

The counters below close the read-screening round. Later writes reuse the same
campaign budget; see the public transaction report for their updated total.

- Campaign observed calls: **367,791**, conservatively priced at $6/million = **$2.206746**. This includes discovery, smoke checks and failed faucet attempts through the gateway.
- Reserved upper bound: **371,000 calls = $2.23**. Reservations persist before dispatch; unused reservations after a restart are forfeited.
- Campaign ceiling: **2,000,000 calls = $12**, below the supplied $50 credit.
- The gateway is stopped. The provider invoice/balance was not queried; another application using this key is outside these counters. [Final counters](rpc-budget-final.json).

Prices follow [dRPC method pricing](https://drpc.org/docs/pricing/compute-units). The [comparison and demand model](../design/rpc-test-plan.md) explains alternatives and transaction costs.

## Method and limits

Strict rerun source: `3aad56d21b479f5b62bd18bd60030d3f22ffcd9c` (probe blob `cc80081218505def5b4ddb58571cb870f13b8fbf`). Earlier screens used `516fd974542bc9a4a0a381c003519f343159d3c8`; Node 22.22.0, macOS arm64, Apple M4 (10 cores), 16 GiB RAM. This was a shared desktop, with local compiles and validators paused during the load screens. Calls passed through the loopback spending gateway to dRPC paid endpoints. The 30-second and higher-rate runs allowed 1,024 in-flight requests; the initial five-second runs allowed 512.

- EVM mix: equal weights of fee history, zero-value transfer gas estimation, latest account nonce and a recent transaction receipt.
- Solana mix: equal weights of confirmed blockhash, recent priority fees, historical signature status and recent transaction details.
- One run per stage, fixed order, no retries or batching. Target sets are small (up to eight recent transactions; one in the OP follow-up), so caching can improve results.
- Probe CPU/event-loop observations are in each report. They exclude the gateway and provider; these runs cannot isolate server-side bottlenecks.
- The generator validates response envelopes and records null results separately. Engine smoke tests additionally decode actual EVM contract reads. Read probes do not exercise transaction sends, inclusion, finality, reorgs or funded balance changes.

The final review found that malformed `error: null/false/0` envelopes could count as success in the first probe. Because raw envelopes were not retained, those earlier summaries cannot rule out misclassification. All five 30-second stages were repeated with the stricter classifier; the table above uses only those reruns. Higher-rate Solana stages remain unqualified and were not repeated.

## Actual Engine checks

[HTTP smoke report](testnet-read-smoke.json): four EVM `getChainId()` contract reads matched the requested chains. Three unauthorized reads were rejected without provider calls. Solana local signing succeeded, but simulation returned `AccountNotFound` for the fresh unfunded payer; no public execution is claimed and no public transaction was broadcast.

## Reproduce

See the [probe README](../../scripts/rpc/README.md). Reuse the campaign budget; do not reset its state. Provider credentials and signer keys remain outside Git. The funded harnesses and their initial rate increases are documented in the [public results](public-transactions.md); further runs must retain the same reconciliation and spending controls.
