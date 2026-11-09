# November 8 follow-up: completed checks and measured limits

Reports identify their own source and executable. Historical passes do not qualify
the current review head. Raw keys, wires, journals, AOF and validator state remain
private; failed and incomplete fixtures are retained.

## Completed runtime #17 checks

Runtime source `4d05bbf`; frozen local executable SHA-256
`68066fe73c0c3fe6597f601f308b80f22a2c59667810359bcdf8454c486ffc1d`.

| Check | Result and scope |
| --- | --- |
| [Rust and Redis/API](runtime-17-rust.json) | Full workspace, both binaries and 196 explicit Redis/API regressions pass. |
| [Process scenarios](runtime-17-process.json) | Fifteen local crash/restart, AOF, rollback, reorg, cross-binary CREATE and queue-reconstruction scenarios pass; no duplicate effects. |
| [Protocol models](runtime-17-formal.json) | 115 finite configurations and 100 source guards pass. No whole-service/refinement proof. |
| [Dependency audit](dependency-audit.json) | Zero vulnerabilities and zero unsoundness warnings; five unmaintained-package warnings remain. No advisory suppression. |
| KMS | Actual SDK/Alloy loopback protocol vectors, independent keys, DER/SPKI and signature boundaries pass. This does not qualify an AWS account, IAM, quotas or HSM operation. |

Exact-head CI summaries are saved separately as `ci-pr*.json`. Later review fixes
require their own CI and measurements; the table above remains pinned to #17.

## Verified review heads, November 9

| PR | Source | CI evidence |
| --- | --- | --- |
| #21 | `0783256` | [Full checks](ci-pr21-07832567ac90.json) |
| #18 | `164b4c5` | [Full checks](ci-pr18-164b4c553156.json) |
| #19 | `7fd8fa7` | [Full checks](ci-pr19-7fd8fa785b43.json) |

All three pass Rust, Redis/API and process scenarios, dependency audit, 122 finite
model cases, 110 source guards and five fee proofs with two required mutation
counterexamples. Artifact digests and model/source hashes are independently
checked. New regressions cover mixed failure refusal, terminal scheduling cleanup,
identical-wire recovery, typed KMS rejection/outage handling and retained-error
serialization. These checks do not qualify throughput or prove the whole service.

## Public testnets

[Both campaigns and reconciliation](public-testnets.json) cover Sepolia, Arbitrum
Sepolia, OP Sepolia, Base Sepolia and Solana Devnet with small transfer workloads.
The first campaign passes on all five networks. In the second campaign, Solana
passes; four EVM runs stop on provider read timeouts after retaining their signed
transactions. All four are subsequently reconciled from their original state
using finalized reads, with no new broadcasts or duplicate effects. The original
failure reports and wire inventories remain unchanged. Reconciliation has its
own recorded source; it does not relabel those failures as uninterrupted passes.

The oracle is separate from Engine; it still trusts the configured RPC's chain
answers. These are correctness/recovery checks, not provider capacity tests.

[Spending record](public-budget.json): **7,998 forwarded requests**, approximately
**$0.048 at the configured accounting rate**. Provider billing was not independently
verified. The shared cumulative ledger was not reset. Owned public services are stopped.

## Capacity and comparison limits

The [first Linux search](linux-capacity-42553585910.json) and
[corrected-clock search](linux-capacity-42556538348.json) use the same frozen
runtime #17 release executable on four-core hosted Linux, SQLite FULL/fullfsync
and Redis `always`. The second search confirms **50 TPS three times** for the
local two-second OP-mode Anvil profile and **25 TPS three times** for local Agave.
The OP-mode 100-TPS run drains but fails the backlog-growth criterion; Agave's
50-TPS screen passes but a confirmation run fails. The other EVM profiles have incomplete or unqualified evidence;
a green job with no qualifying rate does not establish capacity. Those harness
defects are corrected in the review PR before new measurements.

The corrected-clock run also records journal wait observations. For the first
qualified trial of each profile, load-phase counter differences give these means:

| Profile and trial | Admission lane wait | Health lane wait | SQLite commit | Redis continuity read |
| --- | ---: | ---: | ---: | ---: |
| OP-mode 50 TPS, `01-none.json` | 0.50 ms | 1.70 ms | 0.33 ms | 0.12 ms |
| Agave 25 TPS, `00-none.json` | 77.02 ms | 97.93 ms | 0.36 ms | 0.58 ms |

These are overlapping-operation means, not latency percentiles or additive
transaction costs. They show queueing in that Agave trial; they do not establish
one universal bottleneck. The raw time series is private; exported reports retain
its digest and phase summaries.

The [later 600-second search](linux-capacity-42565910045.json), source `83a4df1`,
passes single Agave screens at 25 and 50 TPS, then stops incomplete at 100 TPS.
All 59,897 accepted transactions finalize on chain, but Engine acknowledges only
42,500 before its 240-second drain ends; 17,397 remain in its journal/queue.
Another 103 offers are dropped before HTTP. The safety oracle passes and the
completion check correctly fails. Neither earlier screen receives the three
required confirmations, so this run qualifies no rate.

In its final 480-second load window, the shared journal serial hold occupies
97% of wall time. Mean admission/broadcast/terminal lane waits are about
246/214/293 ms, while FULL commits average 0.37 ms and Redis checkpoint mirrors
0.60 ms. Included and terminal rates trail the offered rate and backlogs grow.
This is an Engine scheduling/serialization limit in that trial; allowing a
longer drain would not make 100 TPS sustainable. The original failed fixture is
retained privately in its verified archive.

The earlier workflow archived fixtures only when the entire job failed. A closed
failed screen followed by successful fallback could therefore retain its report
without its raw fixture. The #18 correction archives those fixtures even when
the job succeeds; it cannot recreate fixtures discarded by older runners.

The 250ms EVM profile is Ethereum-mode Anvil, not Nitro. Small public Arbitrum
checks do not establish Arbitrum throughput. Earlier Nitro results remain
historical and its retained VM is untouched.

The [Mac comparison](mac-comparison-incomplete.json) stops after two of four
planned trials. The first drains all admitted work but misses offered requests;
the second hits the projected disk-reserve guard with unresolved work. The host
was not exclusive. There is no supported performance comparison or sustainable
capacity conclusion from that attempt.

## Current work and operation

The open code stack is **#21 → #18 → #19**. Previously merged stack branches had
not all reached `main`; #21 brings that reviewed code and post-merge corrections
onto `main`. Open-PR fixes live in their own PR, followed by rebasing dependents.
Temporary #20/#22 are closed. Read the code branch's `WORK_IN_PROGRESS.md` and
`LOCAL_TESTING.md` for setup and current scope.

The [current campaign](https://github.com/alfongj-com/engine-core/actions/runs/42573447135)
uses source `7fd8fa7`, a 50-TPS ceiling and 600-second/480-second trial/window
settings. Three individual confirmations are required before shared-chain and
chaos qualification. It is still running. Partial campaign exports contain only
completed jobs; their workflow status remains explicit.

One independent journal and one Engine process on one Unix host remain required.
Intent/replay history is permanent; offline compaction reclaims unused pages.
Queue reconstruction does not resolve unknown outcomes or permit fresh signing
of retained Solana attempts. Production hardware, public-provider maximum TPS,
actual Nitro capacity, physical power loss and account-specific KMS behavior
remain unqualified.
