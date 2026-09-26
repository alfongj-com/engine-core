# To Alfonso

Updated September 26, 2026. Work is in [draft PR #1](https://github.com/alfongj-com/engine-core/pull/1).

## What the review found

**The implementation is safer and faster, but 50 TPS per chain in production is not yet qualified.** I reviewed all five areas, fixed the concrete findings, and repeated the affected tests and measurements.

| Area | Result |
|---|---|
| Performance | The final six-minute EVM run reached 50.1 attempted and 50.5 terminal TPS in its last minute, after fixing a scheduling delay. Solana reached roughly 50 terminal TPS after startup. Public chains and concurrent chains sharing one journal remain unqualified. |
| Security | Final outcomes must match that request's durable signed attempt. Added legacy API authentication and Solana cluster checks. Bundled EIP-7702 is disabled until independent execution attribution exists; direct EOA type-4 transactions remain supported. |
| Reliability | Fixed nonce-counter rollback and recycling after ambiguous send errors. Original bytes and nonce survive uncertainty. Reorg and Solana crash/lost-response tests produced no duplicate effects. Redis recovery retains its independent journal and quarantine rules. |
| Readability | Split streaming export into its own module, named the journal operations, simplified send outcomes and bounded queue work. Clippy completes, but substantial warnings remain, mostly large error types. The whole repository is not yet clean. |
| Tests and proofs | Added failure-path, identity, capacity and recovery regressions. All 56 finite model cases pass; five production fee-arithmetic proofs pass and two deliberate mutations are rejected. Queue coverage now includes Redis regressions: 64.2%, up from 41.2%. This is not workspace coverage or a proof of the whole service. |

All four Linux CI workflows passed at `f309177`, including the real-chain recovery/load scenarios. [Exact CI results](docs/baselines/review-2026-09-26/ci/SUMMARY.md).

## Actual load results

All runs used one signer, local nodes, SQLite FULL sync and Redis AOF every-second sync. EVM used an inflight window of 1,024; the default remains 50. They completed with the exact expected effects. “Terminal” means Engine recorded the outcome after the configured finality checks.

- **Earlier delayed-finality EVM:** 12,000 requests over four minutes; about 49.7 included TPS near the end. All finalized after releasing the test checkpoint. Premature receipt queries fell from 43,489 to zero.
- **Final EVM, 12-second blocks:** 18,000 requests over six minutes; all completed by 375.1 seconds. The scheduling change raised last-minute attempted TPS from 45.1 to 50.1 and terminal TPS from 47.7 to 50.5. Unsigned backlog fell from 505 to 12; admission p99 stayed below 24ms.
- **Final Solana:** 3,000 requests over one minute; 49.7 terminal TPS over the roughly 40-second interval after startup, all completed by 80.1 seconds. Admission p99 was 248ms.

[Measurements, limitations and exact build hashes](docs/baselines/review-2026-09-26/README.md).

## Next work before production

1. Qualify production storage; if durable writes still limit capacity, design and test batching or independent journal partitions. A separate debug-build probe measured 53–57 three-stage intents/second, excluding other Engine work; it is not a production capacity limit. Qualify complete finality windows, real RPC quotas, realistic transaction gas/compute, and concurrent chains.
2. Replace legacy KMS credential payloads with immutable key references and worker-role credentials. Current legacy credentials can remain in journal history and backups.
3. Add evidence-based operator reconciliation for quarantined/parked attempts. Rejected NOOPs can require manual reconciliation. Multi-host operation and loss of both journal and Redis remain unsupported.

No paid RPC credits were used for this review. Nothing is needed from you to finish it. Deployment still needs storage, endpoint and signer qualification, plus resolution of the upstream repository's missing license. Rotate the shared dRPC key when the campaign ends.
