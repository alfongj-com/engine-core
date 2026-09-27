# To Alfonso

Updated September 27, 2026, 12:47 a.m. EDT. [Draft PR #1](https://github.com/alfongj-com/engine-core/pull/1).

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

The latest 147 integrated harness tests passed, including actual Ethereum/OP snapshot restores. Engine's transaction code and durability settings were unchanged by these harness fixes. The prior Rust, queue, coverage and formal CI checks are green; the finite models do not prove the whole service.

## Shared results and current work

With the campaign’s 10-second lease, shared 235 TPS accepted and eventually reconciled 55,742 transactions but rejected 28,858 requests. Shared 47 TPS (12/12/10/13 per chain) accepted and verified all 16,920 with no Engine/RPC errors. Neither is an indefinite capacity guarantee.

The overload exposed a benchmark configuration mismatch: production defaults to a 600-second queue lease. The harness is corrected and the same shared high/low pair is being repeated with that setting. Then come the planned high-load fault tests. Original reports and failures remain visible; the new runs cannot retroactively qualify them.

No paid RPC credits were used. Public RPC quotas, L1 settlement, KMS and production hardware remain unqualified. Nothing is needed from you to continue. Publishing the additional CI workflow steps still requires GitHub workflow permission. Rotate the shared dRPC key when the campaign ends.
