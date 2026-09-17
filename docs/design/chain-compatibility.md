# Chain compatibility and transaction guarantees

Status: proposed design; source review completed 2026-09-15. Code observations refer to upstream `b6b7a0bbdc737b3a2b09611305b71b1bf6aba6e8`, before the fork's changes. This document defines qualification work; it does not certify any production chain.

Update, 2026-09-17: the first public test plan includes Ethereum Sepolia, Arbitrum Sepolia, OP Sepolia, Base Sepolia, and Solana Devnet. See the [RPC test plan](rpc-test-plan.md) for current code limitations, provider prices, request estimates, and the proposed budget.

## Context and goals

Engine pipelines signed transactions through Redis and reports receipts. Its useful unit of serialization is `(chain, sender)`. Throughput depends on the node's admission policy, signer latency, available funds, and chain capacity as well as the queue.

Goals: preserve transaction intent through crashes and RPC uncertainty; distinguish inclusion from finality; qualify Ethereum, Arbitrum Nitro, and OP Stack by actual deployment and endpoint. Non-goals: exhaustive support for every EVM chain, bridge execution, sequencer operation, MEV ordering guarantees, or production certification from local Anvil tests.

## What the code currently assumes

| Area | Upstream implementation | Qualification gap |
| --- | --- | --- |
| Confirmation | `executors/src/eoa/worker/confirm.rs` processes available receipts, advances a cached latest nonce, and explicitly does not revisit already confirmed transactions after nonce regression. | No durable finality/reorg reconciliation contract. A receipt is evidence of execution in a particular block, not permanent success. |
| Preconfirmation | The fork defaults to canonical `latest`; an explicit endpoint capability can enable sequencer preconfirmation. | Endpoint semantics cannot be inferred from chain ID. |
| Sending | `executors/src/eoa/worker/mod.rs` defaults to 100 inflight; transaction preparation/send is concurrent. | Future-nonce limits and admission behavior vary, particularly on Nitro and delegated EOAs. |
| Fees | `worker/transaction.rs` estimates fees/gas, adds a 20% gas buffer, multiplies replacement fees, and includes an Etherlink special case. | Arithmetic bounds, user fee limits, L1/operator costs, replacement rules, and fork limits need explicit tests. |
| Infrastructure | `core/src/chain.rs` constructs Thirdweb URLs; 7702 uses `tw_getDelegationContract`. | Standard RPC availability does not imply bundler, paymaster, or proprietary method availability. |

## Verified protocol facts and implications

All external sources below were accessed 2026-09-15. **Facts** describe the cited protocol/docs; **decisions** in the next section are recommendations for this fork. Network parameters must be rechecked at qualification time.

### Ethereum mainnet

The RPC exposes `latest`, `pending`, `safe`, and `finalized` views. `eth_getTransactionReceipt` may return null before inclusion; a mined receipt includes execution status and block identity. Neither a returned transaction hash nor an advanced nonce alone proves the intended call succeeded. [Ethereum JSON-RPC](https://ethereum.org/developers/docs/apis/json-rpc/), [RPC specification](https://eips.ethereum.org/EIPS/eip-1474).

EIP-1559 transactions cap total fee per gas and priority fee separately; available balance must cover value plus the upfront gas budget. Replacement policy is a client configuration, not a universal EVM rule: Geth documents a configurable 10% ordinary transaction bump and a separate blob policy. Measure the chosen provider; do not assume a 20% multiplication always yields an accepted replacement. [EIP-1559](https://eips.ethereum.org/EIPS/eip-1559), [Geth options](https://geth.ethereum.org/docs/fundamentals/command-line-options).

### Arbitrum Nitro, including Orbit deployments

The sequencer offers rapid provisional ordering; `safe` and `finalized` reflect batch inclusion at the respective parent-chain heads. Assertion confirmation and withdrawability are separate settlement milestones. AnyTrust posts a data-availability certificate instead of full batch data and adds DAC trust assumptions. An L3 inherits its parent's finality; self-managed chains must verify finality-tag support and configuration. [Finality](https://docs.arbitrum.io/how-arbitrum-works/deep-dives/finality), [Finality and reorgs](https://docs.arbitrum.io/how-arbitrum-works/reference/finality-and-reorgs).

Nitro's sequencer retains future nonces only in a short configurable retry buffer, then returns `nonce too high` if the predecessor does not arrive. The documented `pending` nonce equals `latest`; this buffer is not visible through RPC. Engine must persist its outstanding nonces and tolerate out-of-order arrival without discarding intent. [Nonce management](https://docs.arbitrum.io/arbitrum-essentials/arbitrum-vs-ethereum/nonce-management).

Gas covers child execution and estimated parent data cost. `eth_estimateGas` includes the parent component converted into child gas units, so the same call's estimate changes with data pricing. Orbit deployments can use custom native gas tokens; balance/fee labels and funding policy must come from the deployment profile. [Gas and fees](https://docs.arbitrum.io/how-arbitrum-works/deep-dives/gas-and-fees), [Custom gas tokens](https://docs.arbitrum.io/arbitrum-essentials/bridging/custom-gas-token-chains).

Ordering is versioned policy. The current Timeboost docs describe a default 200 ms non-express delay and configurable express-lane access. A separate 2026 governance proposal describes moving Arbitrum One to priority-fee auctions; the proposal is not evidence that activation has happened. Record the active policy and fee collection settings at the benchmark block. Do not encode “Arbitrum always ignores tips” as a permanent rule. [Timeboost](https://docs.arbitrum.io/how-arbitrum-works/timeboost/gentle-introduction), [PGA proposal and activation process](https://forum.arbitrum.foundation/t/constitutional-aip-transition-arbitrum-one-ordering-policy-to-priority-gas-auctions-pga/30942).

A direct sequencer endpoint supports submission methods, not the full read API. Delayed Inbox / force inclusion is a separate parent-chain workflow with configurable delays, not a low-latency failover RPC. [Transaction lifecycle](https://docs.arbitrum.io/how-arbitrum-works/deep-dives/transaction-lifecycle).

### Optimism OP Stack and Base

Standard OP Stack distinguishes sequencer **unsafe**, L1-published **safe**, and L1-finalized **finalized** blocks. The bridge withdrawal challenge period is not the waiting period for ordinary transaction finality. Use heads, not a fixed stopwatch. [OP Stack finality](https://docs.optimism.io/op-stack/transactions/transaction-finality).

If batches are not published in the sequencing window, unsafe history can be replaced through derivation. The default window is approximately 12 hours, but is deployment-specific. Sending through `OptimismPortal` during an outage is an L1 deposit/forced-inclusion operation; it is not equivalent to retrying the original raw L2 transaction at another provider. [Outages](https://docs.optimism.io/op-stack/protocol/outages), [Derivation specification](https://specs.optimism.io/protocol/derivation.html).

Total cost includes L2 execution, L1 data, and, where configured, operator fees. Operator-fee formulas changed between Isthmus and Jovian. An EIP-1559 cap bounds the L2 component; funding checks must include additional costs using the active fork's supported estimators. [OP Stack fees](https://docs.optimism.io/op-stack/transactions/fees).

Base Flashblocks-aware endpoints resolve `pending` against sequencer preconfirmed state and can publish subblock events. The standard Base receipt endpoint documents mined receipts; separate subscriptions / block-receipt APIs expose preconfirmation. OP Mainnet also documents a Flashblock stage, so this is not a permanent Base-only capability. Endpoint, method, and returned evidence must determine the lifecycle stage. [Base Flashblocks API](https://docs.base.org/base-chain/api-reference/flashblocks-api/flashblocks-api-overview), [Base receipt semantics](https://docs.base.org/base-chain/api-reference/ethereum-json-rpc-api/eth_getTransactionReceipt), [OP Mainnet finality](https://docs.optimism.io/op-mainnet/network-information/transaction-finality).

### EIP-7702 and account abstraction

7702 requires chain support for type `0x04` and authorization tuples. Chain ID zero permits cross-chain authorization replay; the authority nonce is distinct from the sponsoring transaction's nonce. Delegation persists even when execution reverts. Delegated execution can advance a nonce more than once, and the EIP recommends restrictive client mempool admission for delegated EOAs. These are reasons to qualify that path independently from ordinary EOA throughput. [EIP-7702](https://eips.ethereum.org/EIPS/eip-7702).

ERC-4337 depends on the account implementation, EntryPoint, bundler and optional paymaster. UserOperation success is distinct from successful execution of its containing transaction. Query supported EntryPoints and chain identity, verify deployed code/version, and test the selected operation schema. Successful ordinary RPC calls cannot establish this capability. [ERC-4337](https://eips.ethereum.org/EIPS/eip-4337), [Bundler RPC specification](https://eips.ethereum.org/EIPS/eip-7769).

### Other chains and Solana

Unknown EVM networks remain unqualified until their fee model, transaction types, finality signal, admission limits, and signer route pass the same gates. “EVM compatible” is insufficient evidence. ZK rollups, chains with native account abstraction, Etherlink, and alternative DA configurations need separate profiles.

Solana requires a separate lifecycle: recent-blockhash expiry, commitment level, signature status, compute budget and account contention. Record `lastValidBlockHeight`; prove expiry before rebuilding with a new blockhash, which changes the signature. Keep preflight and confirmation commitments coherent. Durable nonce transactions have different rules. [Solana confirmation and expiration](https://solana.com/developers/cookbook/transactions/confirmation). The fork now implements local Ed25519 signing and persisted-byte recovery; [its design](rpc-and-solana-recovery.md) states the supported inputs, tests and remaining limits. Public transaction throughput has not yet been established.

## Proposed decisions

1. **Explicit chain/endpoint profiles.** Store chain ID, genesis identity, client/fork versions, native fee asset, read/write endpoints, pending semantics, supported finality tags, transaction types, replacement policy, bounded inflight count, and receipt extensions. Pin contract code hashes and EntryPoint versions for AA. Unknown capabilities disable that path; a failed probe is not positive evidence for a fallback.
2. **Evidence-based lifecycle.** Track accepted, broadcast-unknown, included, safe, finalized, reverted, and replaced distinctly. Store hash, nonce, receipt status, block number/hash, and all signed attempts. Use finality as a configurable completion requirement. A preconfirmed/included notification must state its guarantee; retain enough history to retract it after reorg.
3. **One intent, durable recovery.** Persist signed bytes before send. After a timeout, rebroadcast the identical bytes and inspect all attempts. Never assign that nonce to another intent merely because a receipt is missing. Distinguish proven replacement from provider lag. A reverted call consumes a nonce but is not business success.
4. **Bounded recovery spending.** Replacements preserve sender, nonce, destination, value, calldata and authorization intent. Enforce fee ceilings, maximum attempts and total budget; round upward safely. Do not silently replace a user's action with a no-op. During chain/provider stalls, apply backpressure and check head freshness before increasing fees.
5. **Controlled parallelism.** Parallelize preparation and independent senders. Keep nonce allocation fenced across workers and deployments, and tune per-sender admission to the actual node. Accounts used outside Engine need explicit reconciliation. Start delegated accounts conservatively until measured.

## Qualification matrix

These are required scenarios, not a claim that the tests exist. See [testing and benchmarks](testing-and-benchmarks.md) for levels and execution policy.

| Scenario | Required assertion |
| --- | --- |
| Two workers; crash before/after durable preparation and RPC acceptance | Acknowledged intent remains recoverable; stale owner cannot commit; retries preserve signed intent. |
| Future nonce expires on Nitro; predecessor send is slow | Intent survives `nonce too high`; correct nonce ordering eventually drains. |
| Base ordinary vs Flashblocks endpoint; OP preconfirmation | Mempool pending is never counted as inclusion; preconfirmation is never counted as finality. |
| Receipt delayed/null/error; advanced nonce; competing replacement | No false success or premature nonce reuse; identify the mined attempt. |
| Receipt reverts; receipt block is later orphaned | Execution failure is visible; reorg reconciliation updates lifecycle exactly once per event version. |
| L1 batch delay; stale RPC; sequencer outage | Queue remains bounded, spending stays capped, finality stays pending. |
| L1 data/operator fee spike; custom gas asset; tiny/maximum fees | Funding includes applicable components; arithmetic cannot wrap; user limits hold. |
| 7702 same-account sponsorship, revoked/stale authorization, failed call | Authority nonce/delegation state is correct; no stale cached authorization reused. |
| Bundler success containing failed UserOperation | Operation is reported failed with correct hash/version and receipt evidence. |

## Rollout and unresolved decisions

Qualify local failure recovery first, then one funded testnet per target family and each real provider profile. Run bounded mainnet canaries only after spend/throughput limits and completion semantics are chosen. Promote each chain independently; requalify after RPC, client, fork or signer changes. Roll back by pausing new admission while preserving pending work and reconciliation.

Needed from Alfonso before production qualification: exact chain/provider list; whether API completion means inclusion or finality; exclusive versus shared sender ownership; transaction mix, signer choice, funding budget, and throughput/latency objectives. These do not block local tests and benchmarks.
