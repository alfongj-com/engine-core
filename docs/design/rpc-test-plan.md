# RPC providers and test budget

Date: September 17, 2026. Code reviewed: `037b688`. Prices are public USD rates, using monthly billing where applicable. This is a cost model and proposed experiment, not a measured provider benchmark.

## Recommendation

Use free endpoints for small correctness checks. For short load tests, try **dRPC paid first**, then measure whether it can sustain our workload. Its ordinary methods cost **$6/million calls**; the paid service documents no rate cap while the deposit remains funded. This does not guarantee endpoint speed or transaction inclusion. The required minimum deposit was not established from public documentation. [Method prices](https://drpc.org/docs/pricing/compute-units), [rate limits](https://drpc.org/docs/howitworks/ratelimiting), [payment model](https://drpc.org/docs/pricing/requests).

Start with **Ethereum Sepolia (11155111), Arbitrum Sepolia (421614), OP Sepolia (11155420), Base Sepolia (84532), and Solana Devnet**. dRPC lists all five on its [status page](https://status.drpc.org/). Endpoint availability is not a performance guarantee. Solana recommends Devnet for application development; its separately named Testnet primarily serves validator testing. [Solana networks](https://solana.com/docs/references/clusters).

Alchemy PAYG is a useful alternative for smaller EVM runs. Ankr has higher advertised capacity at a higher per-call price. Recompare after collecting traffic: the cheapest short experiment need not be the cheapest month of continuous use.

## 1. Requests this engine makes

### Ordinary EVM transactions

With automatic fees and gas limits, a successful transaction normally makes one call each to:

- `eth_feeHistory` — choose fees.
- `eth_estimateGas` — estimate gas.
- `eth_sendRawTransaction` — submit.
- `eth_getTransactionReceipt` — retrieve execution result.

Each active wallet also calls `eth_getTransactionCount` once per processing cycle; Base calls it twice. Calls are separate HTTP requests. Concurrent execution does not combine them into a JSON-RPC batch.

For `T` transactions/second, `W` active wallets and a measured cycle time of `P` seconds:

**RPC calls/second ≈ `4T + W/P`; Base ≈ `4T + 2W/P`.**

The requested 200 ms queue delay becomes one integer second in TWMQ. Use approximately one cycle/second for this initial model, then measure it. Receipts are normally requested only after the observed nonce advances; they are not polled for every outstanding transaction each cycle.

| Transactions/second | Calls/second, 10 wallets | Base calls/second | Calls/hour, ordinary EVM | dRPC/hour |
| ---: | ---: | ---: | ---: | ---: |
| 10 | 50 | 60 | 180,000 | $1.08 |
| 50 | 210 | 220 | 756,000 | $4.54 |
| 100 | 410 | 420 | 1,476,000 | $8.86 |

Assumptions: warm caches, `P=1`, one successful receipt lookup, no retries, ordinary EIP-1559 support. Supplying gas and fees reduces `4T` to `2T`. Cold wallets add code/balance reads; null receipts, replacements, crashes and transport retries add calls. Fee estimation is per transaction, not cached per block.

**Wallet capacity also matters.** The server allows 50 outstanding nonces per wallet. At 100 transactions/second and an illustrative 12-second inclusion delay, ten wallets are insufficient. Thirty wallets provide 1,500 slots. Expected occupancy is about 1,250, including half a second of average nonce-detection delay; the estimate becomes roughly 430 RPC calls/second (460 on Base). Record the actual inclusion delay and wallet count. A faster RPC cannot remove this limit or the chain's own capacity limit.

Source locations: [fee/gas preparation](../../executors/src/eoa/worker/transaction.rs), [submission](../../executors/src/eoa/worker/send.rs), [receipts](../../executors/src/eoa/worker/confirm.rs), [nonce handling](../../executors/src/lib.rs), [queue timing](../../twmq/src/lib.rs), [server limits](../../server/src/queue/manager.rs). The pinned Alloy provider implements the fee-history call.

### Solana

After restoring signing, the existing execution path needs:

**Calls/transaction = `3 + F + S + V`.**

- `3`: `getLatestBlockhash`, `sendTransaction`, successful `getTransaction`.
- `F`: one `getRecentPrioritizationFees` for automatic fees; zero for manual/omitted fees.
- `S`: `getSignatureStatuses` polls, currently one signature per request.
- `V`: additional `isBlockhashValid` calls on pending polls after 30 seconds.

Its requested 200 ms retry uses the same integer-second queue scheduling. Expect roughly one status poll per second under light load, with phase and RPC delay affecting the actual count. Default commitment is **finalized**, so use the wait for finality, not first inclusion.

| Assumed time until requested commitment is visible | Calls/transaction with automatic fees | Calls/second at 10 / 50 / 100 transactions/second |
| --- | ---: | ---: |
| 2 seconds | 6–7 | 70 / 350 / 700, using 7 |
| 15 seconds | 19–20 | 200 / 1,000 / 2,000, using 20 |
| 30 seconds | 34–35 | 350 / 1,750 / 3,500, using 35 |

These are scenarios, not observed Solana timing. Beyond 30 seconds, each further pending poll generally adds both status and blockhash-validity requests. SDK retries can add more. Preflight is part of submission; this path makes no separate client `simulateTransaction` call.

Source: [Solana worker](../../executors/src/solana_executor/worker.rs), [default commitment](../../core/src/execution_options/solana.rs). Status batching can query up to 256 signatures per RPC call; the current worker does not use it. Preserve a baseline before optimizing. [Solana RPC specification](https://solana.com/docs/rpc/http/getsignaturestatuses).

## 2. Published prices and limits

RPS means RPC requests/second, not blockchain transactions/second. Credits, compute units (CUs), and request units (RUs) are different currencies. Batching generally still charges each RPC method; a Solana status call containing many signatures is a different optimization.

| Provider / plan | Price and included quota | Rate constraint | Fit for this experiment |
| --- | --- | --- | --- |
| **dRPC paid** | Deposit-based; **$6/M ordinary calls** | No published paid-tier cap | Cheapest short-load-test candidate; measure actual capacity. |
| **Alchemy PAYG** | No monthly minimum; **$0.525/M billed CUs**, charged from first CU | **10,000 throughput CUs/s**, shared across account apps, rolling 10 seconds | Fits smaller runs; default capacity falls just short of 100 EVM transactions/s. |
| **Chainstack Growth / Pro / Business** | **$49 / $199 / $349–499** monthly; **20M / 80M / 200M RUs**; ordinary calls 1 RU | **250 / 400 / 600 RPS** | Growth nominally fits 50 EVM transactions/s; 100 requires Business and still lacks 2× spare capacity. |
| **QuickNode Build / Accelerate / Scale** | **$49 / $249 / $499** monthly; **80M / 450M / 950M credits** | **50 / 125 / 250 RPS** | Build is saturated at our 10 EVM transactions/s estimate. Scale still cannot fit 100. |
| **Ankr Premium PAYG** | **$10 minimum deposit**; **$20/M EVM calls**, **$50/M Solana calls** | Advertised up to **1,500 EVM / 4,000 Solana RPS per endpoint** | More expensive short-test fallback; verify actual testnet capacity. |
| **Infura Developer / Team** | **$50 / $225** monthly; **15M / 75M credits/day** | **4,000 / 40,000 credits/s** | Developer cannot fit even our 10 EVM transactions/s case; Team cannot fit 100. |
| **Helius Developer / Business / Professional** | **$49 / $499 / $999** monthly; **10M / 100M / 200M credits** | RPC **50 / 200 / 500 RPS**; submission **5 / 50 / 100 transactions/s** | Separate submission cap matters. Existing Solana polling can exhaust total RPS first. |
| **Thirdweb Growth / Scale** | **$99 / $499** monthly; first **1M RPC calls** included; then **$8/M** up to 250M | **250 / 1,000 RPS** | Existing routing, but a costly subscription for a short experiment. This excludes hosted wallet/transaction products. |

Primary price sources: [dRPC](https://drpc.org/docs/pricing/compute-units), [Alchemy](https://www.alchemy.com/pricing), [Chainstack](https://chainstack.com/pricing/), [QuickNode](https://www.quicknode.com/pricing), [Ankr](https://www.ankr.com/docs/rpc-service/pricing/), [Infura](https://www.infura.io/pricing), [Helius](https://www.helius.dev/pricing), [Thirdweb](https://thirdweb.com/pricing).

Important details:

- **dRPC free:** 210M CU/30 days, equivalent to 10.5M ordinary calls, over public nodes. The usual limit is about 100 RPS/IP and can decrease under load. Do not use its free allowance to price paid-node traffic. [Limits](https://drpc.org/docs/howitworks/ratelimiting), [tiers](https://drpc.org/docs/pricing/requests).
- **Alchemy:** billed/throughput CUs differ. `sendRawTransaction` costs **40/50**, fee history **10/10**, gas estimate/receipt/nonce **20/20**. Our EVM model is **`90T+200` billed CU/s** and **`100T+200` throughput CU/s**. At 100 transactions/s: **10,200 CUPS**, exceeding 10,000 before retries. An extra 5,000 CUPS costs $160/month. A dashboard Usage Limit can stop usage at a chosen CU or dollar amount. [Usage cap](https://www.alchemy.com/docs/reference/pay-as-you-go-pricing-faq). With 2× spare capacity, the default plan supports about **48 transactions/s** in this model. Free includes 30M CUs, but pricing lists 500 CUPS while docs list 300; verify the dashboard. [Method weights](https://www.alchemy.com/docs/reference/compute-unit-costs), [throughput](https://www.alchemy.com/docs/reference/throughput).
- **Chainstack:** the same pricing page shows Business at $499 on its card and $349 in its comparison table; confirm checkout. Overage is $15/$12.50/$10 per M RUs for Growth/Pro/Business. Disabling extra usage stops service when quota is exhausted. Solana Devnet explicitly lists 25/250 RPS for Developer/Growth; confirm its higher-tier limits at setup. Solana Mainnet has additional restrictions. [RUs](https://docs.chainstack.com/docs/request-units), [network limits](https://docs.chainstack.com/docs/limits), [spending control](https://docs.chainstack.com/docs/manage-your-billing).
- **QuickNode:** relevant ordinary EVM methods cost 20 credits; Solana methods cost 30. Listed overage is $0.62/$0.56/$0.53 per M credits. Paid overages are automatic; no general hard dollar cap was verified. Do not assume each endpoint independently multiplies the plan RPS. The free 30-day trial has 10M credits and 15 RPS. [Weights](https://www.quicknode.com/api-credits), [429 rules](https://support.quicknode.com/articles/2335544672-429-errors-explained).
- **Ankr:** rates above are advertised maxima. Freemium provides 200M credits/month but only about 30 RPS across endpoints; EVM calls use 200 credits and Solana 500. [Plans, deposits and limits](https://www.ankr.com/docs/rpc-service/service-plans/).
- **Infura:** gas estimation costs 300 credits; the other four relevant EVM methods cost 80 each. Our model is **`540T+800` credits/s**: 6,200 / 27,800 / 54,800 at 10 / 50 / 100 transactions/s. Free has 3M credits/day and 500 credits/s. Daily quotas and throughput are separate limits. [Current plan documentation](https://docs.infura.io/get-started/pricing/), [method costs](https://docs.infura.io/get-started/pricing/credit-cost/).
- **Helius:** our standard Solana calls cost one credit each; paid overage is $5/M. Free offers 1M credits, 10 RPC/s and only 1 submission/s. Even Professional's 100-submission/s allowance cannot accommodate the estimated 2,000 RPC/s in the 15-second scenario. [Credits](https://www.helius.dev/docs/billing/credits), [rate limits](https://www.helius.dev/docs/billing/rate-limits).

### Network availability

Alchemy, Chainstack and QuickNode document all five initial networks. QuickNode also lists the separately named Solana Testnet. [Alchemy networks](https://www.alchemy.com/docs/reference/node-supported-chains), [Chainstack networks](https://docs.chainstack.com/docs/protocols-networks), [QuickNode Solana](https://www.quicknode.com/docs/solana).

Helius documents Devnet. Infura lists all four EVM testnets, but Solana access is restricted to selected customers. Ankr documents Solana Devnet; confirm the exact EVM endpoints at setup. [Helius FAQ](https://www.helius.dev/docs/faqs), [Infura endpoints](https://docs.infura.io/get-started/endpoints/), [Ankr Solana](https://www.ankr.com/docs/rpc-service/chains/chains-list/s-t/).

## 3. First-round budget

Proposal: one chain at a time, ten minutes each at 10, 50 and 100 transactions/second, after a successful low-rate check. These are offered-load targets; we do not yet know which the chain, account setup and engine can sustain.

- Four EVM networks: **1.626M RPC calls**, approximately **$9.76** at dRPC. Based on ten wallets, with Base's extra nonce polling. More wallets and delayed receipts increase this.
- Solana: **0.672M / 1.920M / 3.360M calls** for the 2 / 15 / 30-second scenarios above: **$4.03 / $11.52 / $20.16**.
- Combined: roughly **$14 / $21 / $30** before retries, provider probes, funding transactions, and final draining. Propose **$50 total RPC usage**, with a request counter that reserves budget to finish tracking already-submitted transactions. Stop new submissions before the cap; do not abandon pending work.

No paid account or deposit was created. These amounts describe consumed service; the minimum starting balance, taxes and any subscriptions are separate. Faucet balances must cover testnet fees. Do not extrapolate this short budget to continuous operation: 100 EVM transactions/s in the ten-wallet model consumes about **1.063 billion calls per 30 days**, approximately **$6,376 at dRPC's flat rate**.

## 4. Work required before public load tests

1. **Provider configuration and counters.** EVM URLs are currently constructed for Thirdweb, and write routes require Thirdweb credentials. Add explicit endpoint configuration and authentication, pooled clients, and per-method counts/latency/errors. Solana already accepts `APP__SOLANA__DEVNET__HTTP_URL`; its [client cache](../../executors/src/solana_executor/rpc_cache.rs) currently logs the full URL, so remove secret-bearing URL logging before adding provider keys.
2. **Solana signer and recovery.** The [signer](../../core/src/signer.rs) currently rejects Solana signing. Add an Ed25519 backend with secret references, then test actual signed transactions on a local validator. Persist signed bytes through uncertain sends and crashes; fix expiry and terminal-state handling before public tests. Devnet is selectable; the separately named Testnet currently needs additional enum/configuration support.
3. **Calibrate demand.** Use a small run to replace assumed poll cadence, wallet count and confirmation delay with observed values. Keep inclusion, confirmation and finality timings separate. Benchmark ordinary transfers first, then representative contract calls. Bundlers, paymasters, ERC-4337 and EIP-7702 need their own method/cost model.
4. **Check endpoint capacity.** Test the actual method mix and submission path, recording 429s, timeouts, latency and account credit usage. Target at least twice the measured demand: approximately **1,000 RPC/s for 100 EVM transactions/s**, and **4,000 RPC/s for the illustrative Solana 15-second case**. A block-number-only read test does not establish write capacity. Respect purchased limits; if capacity is not verified, mark that run provider-limited.
5. **Run and compare.** Begin at 1 transaction/s. Increase only while queue depth, latency and chain progress remain healthy. Spread work across enough independently funded accounts. Record admitted, submitted, included, finalized and failed transactions; verify unique on-chain effects after drain. Compare with local measurements to distinguish engine, RPC, account-contention and chain limits. Fix what the data shows, then repeat the same workload.

Published quotas establish only whether a plan could fit. **We cannot yet say any provider is fast enough:** no authenticated provider load test has run. This document identifies the cheapest candidate and the measurements needed to decide.
