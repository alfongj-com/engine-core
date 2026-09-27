# To Alfonso

Updated September 27, 2026. Campaign in progress. [Draft PR #1](https://github.com/alfongj-com/engine-core/pull/1).

## Current measurements

These are local, six-minute tests with one signer per chain and durable SQLite/Redis. **They are measured screens, not confirmed sustainable maxima or public-chain capacity.**

| Chain fixture | Offered TPS | Late completed TPS | Result |
| --- | ---: | ---: | --- |
| Ethereum-style, 12-second blocks |60|59.18|All 21,600 completed; backlog grew. |
| OP execution, 2-second blocks |60|59.48|All 21,600 completed; backlog grew. |
| Native Arbitrum Nitro |50|48.84|All 18,000 completed; stability needs confirmation. |
| Native Solana |65|64.53|All 23,400 completed; small finality-backlog drift needs confirmation. |

The new Arbitrum dispatcher improved the adjacent comparison from 43.84 to 48.84 late TPS; drain ended at 365 rather than 395 seconds from load start. This ordered pair is not a randomized causal result. [Native evidence](docs/baselines/capacity-2026-09-26/native-dispatch-pair/README.md), [other chains](docs/baselines/capacity-2026-09-26/phase-fixed-screen-bd44836d17c9/README.md).

## Failures kept visible

- Solana at 70 TPS rejected 45 of 25,200 requests and built backlog. Every accepted request later reconciled exactly; 70 is not a clean rate.
- The earlier Nitro disk-full incident recovered all 14,400 accepted intents after disk repair.
- A later EVM55 harness run stopped after a host-wide disk-allocation spike. It discarded its disposable node before receipt verification: 3,274 signed intents are preserved, but 1,980 final outcomes remain unverified. This failed run is not repaired or promoted. The harness now preserves interrupted Anvil history and uses sustained disk consumption for long forecasts. [Incident](docs/baselines/capacity-2026-09-26/evm55-projection-incident/README.md).

The latest 155 integrated harness tests passed, including actual Ethereum/OP snapshot restores and cleanup with unresolved transactions. Engine's transaction code and durability settings were unchanged by these harness fixes. The prior Rust, queue, coverage and formal CI checks are green; the finite models do not prove the whole service.

## Shared results and current work

With the production 600-second queue lease:

- **235 TPS offered:** 55,105 transactions accepted and reconciled; 29,495 rejected. Late completion was 41.19 TPS combined, with EVM queues growing while Solana progressed.
- **47 TPS offered (12/12/10/13):** all 16,920 accepted and verified. Late completion was 47.11 TPS, with no Engine/RPC errors. This is a clean finite control, not an indefinite capacity guarantee.

The earlier 10-second benchmark lease caused ownership churn under overload. Correcting it removed those errors but did not solve the shared throughput limit. [Corrected shared evidence](docs/baselines/capacity-2026-09-26/shared-lease600-1c3eec03d6cb/README.md).

Completed fault/control tests:

| Test | Independently verified result |
| --- | --- |
| Lost accepted RPC responses, four chains | All 14,100 completed after 80 lost responses; original signed identities preserved. |
| Mixed transactions, three chains | All 11,100 completed, including 2,400 expected EVM reverts with no persisted storage changes. |
| Mixed transactions with send-RPC errors | All 11,100 completed after 60 pre-forward errors; no extra upstream sends or new identities. |

All three drained completely. Their 100 duplicate-ID probes covered EVM only and produced no new sends. The OP reorg/pool-loss run has closed with a passing oracle; independent review is underway. Next are final rate brackets, then process and Redis failure tests. The harness now preserves owned EVM history for ordinary unresolved outcomes as well as unexpected interruptions.

No paid RPC credits were used. Public RPC quotas, L1 settlement, KMS and production hardware remain unqualified. Nothing is needed from you to continue. Publishing the additional CI workflow steps still requires GitHub workflow permission. Rotate the shared dRPC key when the campaign ends.
