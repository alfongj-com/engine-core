# Sustaining 50 transactions/second from one signer

**Review:** 2026-09-26. **Reference implementation:** `5baa70b` before this review;
changes below describe the working revision. **Target:** 50 admitted, broadcast
and eventually finalized transactions/second **per chain**, measured separately.
A submission acknowledgment is not terminal throughput.

## Decision

Qualify a concrete transaction workload, RPC provider and finality policy before
claiming this target. Keep canonical finality, exact attempt identity and durable
recovery mandatory. Improve bounded work and admission backpressure first; do
not infer production capacity from a short testnet burst or finite state model.

## Chain constraints

Source facts below were checked against primary documentation on 2026-09-26.
Numbers derived from them are sizing examples, not service-level guarantees.

| Chain | Verified constraint | Consequence for a single signer |
|---|---|---|
| Ethereum | Slots are 12 seconds; an empty slot can occur. Block gas is shared and its limit can change. [Blocks](https://ethereum.org/developers/docs/blocks/) | 50/s requires roughly 600 transactions per full slot: 12.6M gas for 21k-gas transfers, 60M for 100k-gas calls. Network demand, fees, provider per-account pools and proposer inclusion can prevent that rate. A 100-nonce window only permits about 8.3/s if inclusion takes a full slot. |
| Arbitrum Nitro | Out-of-order nonces have a short, operator-configured retry buffer; `pending` count equals `latest`. Parent-chain batch finality is distinct from sequencer inclusion. [Nonce management](https://docs.arbitrum.io/arbitrum-essentials/arbitrum-vs-ethereum/nonce-management), [Finality](https://docs.arbitrum.io/how-arbitrum-works/reference/finality-and-reorgs) | Keep one coordinated allocator and recover gaps promptly; a larger inflight window does not guarantee sequencer acceptance. Orbit/L3 configurations inherit their actual parent/DA assumptions. |
| OP Mainnet | Typical L2 inclusion is about 2s; hard finality is about 15–30 minutes in the documented standard configuration. Safe means L1 data inclusion, finalized means its L1 finality. [OP finality](https://docs.optimism.io/op-stack/transactions/transaction-finality) | A 100-nonce window provides only an ideal 50/s with no latency headroom. Retain roughly 45k–90k attempts at 50/s before polling delay, replacements and outage headroom. |
| Base | The documented stages are ~200ms preconfirmation, ~2s L2 block, ~2min batch and ~20min L1 batch finality. [Base finality](https://docs.base.org/specifications/transactions/transaction-finality) | Flashblocks may free a submission window earlier; they cannot authorize final cleanup. About 60k retained attempts precede extra headroom at 50/s. |
| Solana | `getSignatureStatuses` accepts up to 256 signatures; historical lookup must be requested explicitly. [RPC](https://solana.com/docs/rpc/http/getsignaturestatuses) | Current per-job single-signature polling leaves batching gains unused. A shared writable fee payer also consumes the per-account budget (currently 12M CUs/block); transaction compute/account usage and fee selection require workload-specific measurement. [Account limits](https://solana.com/upgrades/100m-cu-blocks), [writable payer](https://solana.com/docs/core/transactions/transaction-pipeline). No generic 50/s guarantee follows from cluster TPS. |

Finality timings are planning inputs. The implementation uses evidence, not a
wall-clock timer, to finalize. Ethereum/rollup retention can rise sharply during
consensus or batch publication delays. Ethereum gas examples use the standard
21k base cost for a simple transfer. [Gas](https://ethereum.org/developers/docs/gas/)

## Implementation bottlenecks and bounded changes

| Finding at `5baa70b` | Change / remaining constraint |
|---|---|
| 10k retained hashes fill in 200s at 50/s, much earlier than normal L1 settlement. | Retained allocation ceiling is now 100k hashes; pending intake is separately capped at 25k per signer/chain. Hashes include fee replacements. This is only 33m20s of arrivals before any cleanup, not an outage guarantee or a journal disk bound. |
| Effective manager inflight window 50 limits block-paced inclusion (an unused constant elsewhere said 100). | Queue configuration now exposes the EOA inflight window, retaining default 50 with validated range 1–4096. The baseline full-slot Ethereum bound was about 4.17/s; OP 2s inclusion gives 25/s. Size it from observed inclusion latency and provider limits. Delegated-account serialization and custom chain overrides require separate qualification. |
| 256 receipt reads/5s gives only 51.2 checks/s before repeated provisional reads. | Rotating page is 1024/5s, with at most 32 receipt RPCs and 8 distinct-block assessments in flight. 204.8 checks/s is a configured ceiling, not measured terminal TPS. |
| Latest-nonce selection repeatedly queried receipts before the configured finality head covered them. | Candidate cutoff now reads the signer count at `finalized`, or at latest height minus explicit depth. Depth zero can reuse latest count. Unsupported reads stop this cycle; there is no latest fallback. This is only a hint: false-high counts still face full receipt gates. |
| Every page fetched all retained records; every cleanup hydrated the whole eligible range. | Redis returns bounded rank pages; cleanup reads only finalized nonce groups and at most 4096 group records. Pathological replacement fanout exceeding that budget fails closed with evidence retained. |
| Old retained receipts could rewind optimistic nonce below consumed chain count. | Cleanup always includes the consumed-count floor; real Redis regression reproduces 313 consumed / 250 retained. [Finite model](../../formal/nonce-allocator.md) |
| A larger inflight window could keep the send cycle busy with one huge batch. | Each new allocation cycle consumes at most 128 nonce reservations; ordered preparation/send concurrency is 32. Ten preparation refill passes may process up to 1,280 rejected pending jobs. Borrowed recovery still visits all retained borrowed attempts (up to the configured 4,096 window), and recycled recovery reads its retained set. No universal 128-work or elapsed-time bound follows. |
| Even a useful bounded send cycle with unsigned backlog waited behind a 200ms delay rounded to one second by TWMQ. | Successful nondelegated cycles with acknowledged send or recovery progress and remaining unsigned work now rejoin the queue tail immediately. Delegated accounts retain 2s; unknown delegation, no-progress, unknown-only and finality-only cycles retain the rounded 1s delay. Errors retain their existing handling. The [matched six-minute run](../baselines/review-2026-09-26/README.md) raises last-minute attempted TPS from 45.085 to 50.075; public-chain capacity is still unqualified. |
| Repeated identical journal attempts/checkpoints incurred avoidable durable work. | Exact attempt and same policy/checkpoint-head no-ops remain behind health/owner/CAS checks. Terminal receipt anchors still belong to each intent. Disk durability and serialization remain required. |

Block evidence is shared only within one worker cycle and exact block number/hash;
each receipt still validates its expected hash, execution status and durable
sender/nonce/attempt identity. There is no persistent/global checkpoint cache.
Pagination is advisory: deletions shift ranks, but never remove evidence. A static
backlog is fully visited; no formal fairness guarantee covers unbounded churn.

## RPC and storage budget

Let `P` be returned receipts in a page and `B` their distinct block identities.
With a prior journal checkpoint and coherent successful responses, the current
EOA cycle uses approximately `P + 3B + 2` RPCs when receipts remain provisional,
or `P + 5B + 2` when finality is reached under the finalized policy. Receipt lookup actually costs one call
for every selected hash, including null results; replace `P` by the selected-hash
count for that term. The final `2` covers active-poll continuity and the
finalized-nonce hint. Explicit positive depth adds a block-number query; depth
zero reuses the already observed latest count and omits the hint RPC. CAS races
can add continuity reads. Nonce, fee, estimation and broadcast calls are extra.

At a fully saturated 1024/5s receipt budget, receipt calls alone reach 17.69M/day
per signer/chain. At dRPC's current 20 CU per ordinary call and $0.30/M CU, that
is about **$106/day** before other RPCs. This is a budget ceiling illustration,
not an observed bill or recommendation to poll that aggressively continuously.
[Official compute-unit pricing](https://drpc.org/docs/pricing/compute-units)

The policy-head cutoff should make healthy steady-state receipt demand track
newly settled hashes rather than the whole premature backlog. At 50/s and one
receipt lookup per intent, receipt reads alone are 4.32M/day (~$25.92 at that
price), plus block evidence and all submission calls. This is an estimate;
missing receipts, page boundaries, replacement fanout and RPC lag add reads.

Solana's present unbatched status rate is approximately `T × F / I`: transaction
rate `T`, time awaiting finality `F`, effective poll interval `I`. For example,
50/s × hypothetical 20s / 1s = 1000 status calls/s, plus blockhash, send and final
transaction reads. The example's 20s is not a promised finality time. Existing
[public measurements](solana-polling-cost.md) show actual intervals can be longer.

Sizing rule: retain at least `T × (finality delay + scan delay + outage margin)`
attempts, multiplied by measured replacement fanout. A 100k cap may still bind
at long OP finality delays once scan delay is included. HTTP pending capacity
and admission permits do not bound permanent journal growth; provision and
monitor disk, Redis memory, oldest unresolved age, rejection rate and checkpoint
lag. Confirmation work still shares the EOA worker cycle: RPC RTT can delay its
next send batch even with bounded concurrency. Unknown borrowed attempts may
retry indefinitely with one receipt read and one broadcast per unresolved attempt.
Unknown-only cycles keep the rounded one-second delay; mixed cycles making useful
send/recovery progress with unsigned backlog can immediately rejoin the queue.
Shared provider throttling and lifetime EOA retry budgets are not implemented.

## Qualification and next work

- Run sustained local Engine loads with real Redis/SQLite durability, one signer,
  exact effect/fee reconciliation, delayed finalized head and missing receipts.
  Measure actual admission/broadcast/finalization rates and latency separately.
- Exercise at least one complete retention/finality window before calling an
  Ethereum/L2 workload sustained; a fast-finality local run tests different work.
- Collect RPC method counts, rejected admissions, p95/p99 latency, retained depth,
  allocator progress, memory and journal fsync time. Preserve old/new run scope.
- Next optimization candidates: a finalized-head coordinator and a provisional
  block index, and Solana signature batching. Keys must include endpoint/policy;
  absent/stale observations cannot erase or renew identity. These are proposals,
  not implemented claims.
- Finite models check identity/finality/failure transitions. Pagination,
  bounded-concurrency and backlog regressions exercise implementation. None
  proves 50 TPS, resource bounds under arbitrary traffic, or model composition.

## Durable writer capacity qualification

The [separate unoptimized journal probe](../baselines/review-2026-09-26/JOURNAL.md)
on this host measured an equivalent 53–57 three-stage intents/second, derived
from separately timed admission, attempt and terminal phases. This was not mixed
Engine traffic. That isolated measurement cannot support a claim of
50 TPS on several chains simultaneously: all chains share the serial durable
writer. Per-chain send/receipt improvements must therefore be qualified together
under the intended aggregate load, including SQLite FULL-sync and Redis persistence.
The final Engine workload reports take precedence over this component measurement.
