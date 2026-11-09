# RPC providers and test budget

Updated October 13, 2026 for schema-3 recovery and bounded Solana status batching. The provider comparison retains its **September 17 historical scope**; only dRPC method pricing was rechecked today. Historical planning tables below are not current capacity estimates.

Use the [PR #3 measured method counts and workload provenance](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/retained-recovery-2026-10-12/README.md) as the local baseline, then measure the intended public endpoints. The provider account balance remains unknown.

## Recommendation

Use the existing dRPC account for a capped validation pass, then measure whether it sustains this workload. Rechecked October 13: the published method schedule charges 20 CU per ordinary RPC method at $0.30/million CU, or **$6/million calls**. Counting even zero-cost identity methods at that rate gives a conservative estimate. Price does not establish endpoint capacity. [Method prices](https://drpc.org/docs/pricing/compute-units).

Start with **Ethereum Sepolia (11155111), Arbitrum Sepolia (421614), OP Sepolia (11155420), Base Sepolia (84532), and Solana Devnet**. dRPC lists all five on its [status page](https://status.drpc.org/). Endpoint availability is not a performance guarantee. Solana recommends Devnet for application development; its separately named Testnet primarily serves validator testing. [Solana networks](https://solana.com/docs/references/clusters).

Recompare alternatives after collecting traffic; the historical provider table is a starting point, not a current purchase recommendation.

## 1. Requests this engine makes

### Ordinary EVM transactions

With automatic fees and gas limits, a successful transaction normally makes one call each to:

- `eth_feeHistory` — choose fees.
- `eth_estimateGas` — estimate gas.
- `eth_sendRawTransaction` — submit.
- `eth_getTransactionReceipt` — retrieve execution result.

Each active wallet also calls `eth_getTransactionCount` once per processing cycle. An explicitly enabled sequencer preconfirmation capability adds a second call; Base now defaults to one like the other chains. Calls are separate HTTP requests. Concurrent execution does not combine them into a JSON-RPC batch.

For `T` transactions/second, `W` active wallets and a measured cycle time of `P` seconds:

**RPC calls/second ≈ `4T + W/P`.** With verified preconfirmation explicitly enabled, use `4T + 2W/P`.

Current scheduling depends on progress: useful nondelegated work can rejoin the queue immediately, while polling-only work remains delayed. The separate finality poll uses policy-head nonce hints, canonical block/checkpoint reads and continuity checks. Receipts are not polled for every outstanding transaction each cycle. The formula and table below omit that later work, replay and independent verification; use measured method counts for a new budget.

**Historical EVM planning scenario:** excludes the current full finality/recovery method mix.

| Transactions/second | Calls/second, 10 wallets | With preconfirmation enabled | Calls/hour, ordinary EVM | dRPC/hour |
| ---: | ---: | ---: | ---: | ---: |
| 10 | 50 | 60 | 180,000 | $1.08 |
| 50 | 210 | 220 | 756,000 | $4.54 |
| 100 | 410 | 420 | 1,476,000 | $8.86 |

Assumptions: warm caches, `P=1`, one successful receipt lookup, no retries, ordinary EIP-1559 support. Supplying gas and fees reduces `4T` to `2T`. Cold wallets add code/balance reads; null receipts, replacements, crashes and transport retries add calls. Fee estimation is per transaction, not cached per block.

**Wallet capacity also matters.** The default is 50 outstanding nonces per wallet; `queue.eoa_max_inflight` is configurable from 1 to 4,096. A single signer at 100 transactions/second and an illustrative 12-second inclusion delay needs roughly 1,200 slots plus detection/burst margin; increasing wallet count is not the only way to provide them. Retained finality work has a separate limit. Record actual inclusion/finality delay, window size and signer count. RPC speed cannot remove chain capacity limits.

Source locations: [fee/gas preparation](../../executors/src/eoa/worker/transaction.rs), [submission](../../executors/src/eoa/worker/send.rs), [receipts](../../executors/src/eoa/worker/confirm.rs), [nonce handling](../../executors/src/lib.rs), [queue timing](../../twmq/src/lib.rs), [server limits](../../server/src/queue/manager.rs). The pinned Alloy provider implements the fee-history call.

### Solana

For instruction-built transactions, budget actual HTTP methods rather than logical worker polls:

**HTTP calls/intent ≈ `3 + F + S + H + R`.**

- `3`: initial `getLatestBlockhash`, `sendTransaction`, and successful finalized `getTransaction`.
- `F`: one `getRecentPrioritizationFees` for automatic fees; zero for manual/omitted fees.
- `S`: historical status HTTP calls allocated across participating intents. Eligible concurrent singleton polls are coalesced into **at most 32 signatures per request**, with a 5 ms collection window. Partial batches, isolated endpoints and disabled coalescing increase calls; do not assume every batch fills.
- `H`: actual finalized `getBlockHeight` calls while status is absent, or `isBlockhashValid` for serialized inputs. These results are **not cached by Engine**; cached RPC clients and genesis checks do not remove height calls. A visible status waiting for finality needs no height lookup. Provider caching can affect latency, but forwarded methods still count toward the gateway budget.
- `R`: same-wire retransmissions after uncertainty. The original Redis path allows 20 sends including the initial send; schema 3 adds a separate durable allowance of 20 recovery grants shared across all restorations. Lost grants consume credit. There is no unconditional 40-send lifetime bound because the original Redis counter can roll back.

This shorthand excludes startup checks, failed/repeated receipt reads and independent verification; measure those too. Serialized input uses `isBlockhashValid` instead of the initial `getLatestBlockhash` and preserves its own compute-budget instructions. See [batching and opt-out behavior](solana-status-batching.md) and [retained-attempt recovery limits](retained-solana-recovery.md).

`queue.solana_confirmation_poll_interval_seconds` ranges from 1 to 5 (default 1). Queue/HTTP delays and absent-status retries affect the actual cadence. Settlement requires **finalized** status plus a matching receipt. The 500-check local reconciliation allowance resets after projection reconstruction; it is not a lifetime RPC-read quota. Expired unknown outcomes retain their identity and park.

**Historical unbatched Solana planning scenarios:** these assume one status request per poll and a one-second interval. They are retained for comparison, not for sizing the current implementation.

| Assumed time until requested commitment is visible | Calls/transaction with automatic fees | Calls/second at 10 / 50 / 100 transactions/second |
| --- | ---: | ---: |
| 2 seconds | 6–7 | 70 / 350 / 700, using 7 |
| 15 seconds | 19–20 | 200 / 1,000 / 2,000, using 20 |
| 30 seconds | 34–35 | 350 / 1,750 / 3,500, using 35 |

These assume prompt signature visibility, with no extra height checks or rebroadcasts. Current batching changes the status term; public-provider history latency and partial batches must be measured. The bounded HTTP sender adds no hidden retries. Preflight belongs to submission; the worker makes no separate `simulateTransaction` call.

Source: [Solana worker](../../executors/src/solana_executor/worker.rs), [RPC transport](../../executors/src/solana_executor/rpc_cache.rs), [batch coordinator](../../executors/src/solana_executor/status_batcher.rs). For actual local counts, see the [PR #3 measurements](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/retained-recovery-2026-10-12/README.md).

## 2. Historical provider comparison — September 17, 2026

The rates, quotas, availability and fit assessments in this section are historical, except dRPC method pricing rechecked above. Recheck a provider before purchasing or sizing a run. The Solana fit assessments use the old unbatched scenarios.

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

## 3. Authorized test budget

Alfonso originally supplied an existing dRPC key with $50 in credit; its current provider balance is unknown. The loopback gateway enforces **2,000,000 forwarded methods**, equivalent to $12 at ordinary published prices, allowing only the five testnets and selected methods. That dollar conversion is not an independently verified provider-invoice cap; quorum/routing changes require repricing. The key stays outside Git and out of Engine's configuration.

The gateway counts each method in a batch, including failed requests. It persists reservations before forwarding and preserves the ceiling across restarts. Two million calls is the absolute limit. Its estimates do not replace the provider's invoice or account balance, and do not include another application's use of the same key. The actual counter and reserved upper bound are recorded with the results.

The October 13 validation pass has an additional **100,000-method ceiling**, a conservative **$0.60** estimate at the verified ordinary-method rate, within the existing two-million global limit. Before this pass, the gateway had observed **370,084 forwarded methods**. This is a historical starting count, not the current balance or reserved high-water mark. Persisted reservations remain authoritative; unused reservations are forfeited on restart and must not be reset or reclaimed.

Use short ramps and preserve at least 25% of the pass allowance for reconciliation. Record actual forwards and reserved upper bounds with each result. The [September budget record](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/capacity-2026-09-26/rpc-budget.md) describes the earlier reservation state. Read-only capacity probes do not need funded wallets; submissions do.

For historical scale only, 100 EVM transactions/s in the simplified ten-wallet model consumes about **1.063 billion calls per 30 days**, approximately **$6,376 at dRPC's flat rate**. The cheapest short experiment need not be the cheapest continuous deployment.

## 4. Qualification sequence

1. **Local correctness.** Configured EVM clients, isolated credentials, Solana signing and crash recovery must pass local HTTP/Redis/validator tests. [Implementation and limits](rpc-and-solana-recovery.md).
2. **Live provider checks.** Verify chain identities through Engine and test the actual read-method mix, including gas/fee preparation and real receipts. Record offered versus completed rate, latency, 429s, upstream errors and local saturation separately. Reusing recent receipts can benefit from provider caches; report this.
3. **Funded low-rate submissions.** Start at one transaction/s on each network. Observe confirmation delay, wallet occupancy, method counts and fees. Separate inclusion from finality. A successful signature or simulation is not an executed transaction.
4. **Short throughput ramps.** Increase only while queue depth and chain progress remain healthy. Seek about twice the measured RPC demand as headroom; include status batch sizes, height checks, retries, finality reads and verification. Do not size the current run from the historical tables. If effective capacity is unmeasured, label it unqualified; distinguish RPC throttling from local saturation.
5. **Compare and improve.** Reconcile unique effects after drain. Compare the frozen public baseline with the same workload after cleanup. Report logical status polls, actual HTTP batches, inclusion/finality latency and offered/completed rates separately. A lower RPC count alone is not a throughput result.

The local environment signer currently provides one EVM identity. Configure and qualify its outstanding window first; tests that independently require many funded wallets still need a reviewed key-reference selection mechanism. Smart accounts, bundlers, paymasters, ERC-4337 and EIP-7702 require separate qualification and pricing.

Published quotas only establish whether a plan could fit. See [verification](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/verification.md) and [the handoff](https://github.com/alfongj-com/engine-core/blob/load-tests/TO_ALFONSO.md) for what actually ran and the remaining funding or capacity limits.
