# To Alfonso

Updated September 27, 2026. [Draft PR #1](https://github.com/alfongj-com/engine-core/pull/1).

## How fast it went

Local tests, one signer per chain, durable SQLite and Redis. All offers at the
rates below were accepted and reconciled exactly after drain:

| Chain | Useful measured rate | What happened above it |
|---|---|---|
| EVM, 12-second blocks | **50 TPS for 15 minutes**; 45,000 outcomes | 55 TPS built unsigned work. |
| OP execution, 2-second blocks | **55 TPS for 6 minutes**; 19,800 outcomes | Stability remains uncertain; the earlier 60 TPS screen grew queues. |
| Native Arbitrum Nitro | **55 TPS for 6 minutes**; 19,800 outcomes | 65 TPS clearly overloaded. |
| Solana | **60 TPS for 6 minutes**; 21,600 outcomes | 65 TPS showed backlog drift; 70 TPS rejected requests. |
| All four together | **58.75 total TPS for 15 minutes**; 52,875 outcomes | 235 TPS rejected 29,495 requests and built a large queue. |

The shared working rate was 15 EVM + 15 OP + 12.5 Nitro + 16.25 Solana TPS.
These are measured working points and screens, not proven absolute maxima.
The detailed report preserves different lease settings and the unchanged
stability checks. Public-chain throughput, full rollup settlement and KMS remain
untested by this campaign. **No paid RPC credits were used.**

## What broke

Mixed transactions, 80 lost accepted responses, 60 injected send errors and an
OP reorg/pool loss recovered exactly. The reorg replayed 253 original signed wires.

**Automatic crash recovery is the main blocker.** Killing Engine left SQLite
one checkpoint ahead of Redis; restart refused to proceed. The Redis crash also
required operator recovery. They retained 741 and 261 nonterminal test requests.
The audit verified every recorded terminal proof without changing those states.

Explicit Redis reconstruction worked: 413 completed records stayed completed,
187 uncertain requests were quarantined, and five unsigned requests were retained.
331 API probes produced no new sends. This preserves identity; it does not finish
the 192 outstanding requests or prove resumed execution for that signer.

An older disposable EVM test still has 1,980 unverified outcomes after losing node
history. That evidence gap remains visible. The journal-reader bug found in this
campaign is fixed; all **163 harness regressions pass**. The **61 formal-model
outcomes** are bounded checks, not proof of the whole service.

## Next priorities

1. Make the journal-to-Redis update recoverable after a crash without weakening the fence.
2. Measure storage/Redis wait times, then improve shared-chain fairness and Solana polling.
3. Repeat on production hardware and full rollup stacks before a capped testnet run.

[Results and evidence](docs/baselines/capacity-2026-09-26/RESULTS.md),
[five-area review](docs/baselines/capacity-2026-09-26/final-review/OUTCOME.md),
[RPC sizing](docs/baselines/capacity-2026-09-26/final-review/RPC-SIZING.md).
The additional CI workflow steps still need GitHub workflow permission.
Rotate the shared dRPC key when finished using it.
