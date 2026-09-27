# Capacity experiment resource and outage guards

## Purpose

A dead native node previously left Engine intake running through the remaining offer window. The sampler logged errors while the supervisor waited for whole-job completion. The guard now stops new workload promptly, preserves custody, and prevents a failed infrastructure run from becoming a capacity result.

## Decisions

1. **Reserve before starting.** Every profile checks host space; native Nitro also checks guest root bytes/inodes through read-only Lima commands. The supervisor checks before native unpause, and the campaign checks before owned services start. Different host filesystems require separate budgets.
2. **Budget the entire job.** Host floor is `max(8 GiB, 2% total)`, plus twice a provisional 2 MiB/s allowance over setup, offers, permitted drain and verification, with observation/cleanup headroom. Native guest floor is `max(4 GiB, 10%)`, with 1 MiB/s growth; inode floor is `max(100,000, 5%)`, with 4/s growth. Budget inputs and measured values are retained.
3. **Separate setup from sustained growth.** Pre-reserve 2 GiB when Solana is included, otherwise 256 MiB. Setup samples still enforce absolute floors and future-run reserves, but do not extrapolate one-time allocation. A fresh end-of-setup sample establishes the growth baseline before the offer clock starts. Later depletion spikes count fully. Resource samples run every 60 seconds without holding the admission/receipt sampler lock.
4. **Detect provider unavailability separately.** Direct loopback reads every five seconds bypass the fault proxy. Three failures spanning at least ten seconds latch an infrastructure stop. EVM probes check chain identity; Solana probes use node health. Planned proxy send errors/lost responses do not themselves count as provider death.
5. **Stop without erasing uncertainty.** Abort future slots and queued-but-not-started HTTP work. Finish already-dispatched responses, fence future chaos/restart actions, and join the lifecycle worker before custody capture. Skip retries and impossible long draining. Already-entered operations may finish; this is an eligibility fence, not an instantaneous network cancellation guarantee.
6. **Keep independent evidence honest.** Stop Engine, take a bounded private SQLite backup including WAL, retain exact dispatched-ID responses and proxy evidence, record fixed queue-index counts, and retain original AOF paths. If chain reconciliation is unavailable, the oracle remains incomplete, with unproven safety rather than a fabricated pass or zero drain. An incident during reconciliation cannot restore capacity flags to true.
7. **Require review.** The supervisor preserves its active/global-review state across resume, including native pause failures alongside the original incident. No automatic restart, reset, reattach, replay, deletion, or latch clearing occurs.

## Limits

Growth allowances are provisional planning budgets, not established retention limits. A 60-second monitor cannot prevent arbitrary sudden disk consumption; peak-based forecasts can conservatively reject unrelated host churn. Reachability is not advancing blocks, write availability, or throughput. An owned Anvil chain remains in memory: preserving journal/AOF does not establish resumable chain custody. Prior terminal ledger records remain historical proofs until independently reconciled.

The final authorized read-only preflight passed with the larger 2 GiB setup reserve: host 32.204 GiB available versus 21.953 required; guest 13.408 versus 9.977 GiB. No node mutation occurred. Finite mocked-outage/real-SQLite regressions cover transient recovery, sustained outage, late HTTP 202 custody, cleanup failures, chaos lifecycle boundaries, setup allocation and load spikes. The two live smokes establish normal integration only; they do not recreate a live disk-full incident or establish sustained capacity.
