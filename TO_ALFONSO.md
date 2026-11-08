# To Alfonso

Updated November 8, 2026. Code and reviewed evidence are saved locally and pushed.

## PRs

Seven fixes are stacked on the open [PR #4](https://github.com/alfongj-com/engine-core/pull/4):

1. [#5](https://github.com/alfongj-com/engine-core/pull/5): EOA creation, legacy estimation and gas overflow.
2. [#6](https://github.com/alfongj-com/engine-core/pull/6): cancellation/broadcast race.
3. [#7](https://github.com/alfongj-com/engine-core/pull/7): bounded HTTP batches and signing/ABI work.
4. [#8](https://github.com/alfongj-com/engine-core/pull/8): 11 compatible dependency security fixes.
5. [#9](https://github.com/alfongj-com/engine-core/pull/9): native environment-key signing and upgrade checks.
6. [#10](https://github.com/alfongj-com/engine-core/pull/10): Solana crash-lock recovery with stale-worker fencing, found during the final load test.
7. [#11](https://github.com/alfongj-com/engine-core/pull/11): strict public-testnet resume provenance and wallet-lock cleanup.

Merge order: **#4 → #5 → #6 → #7 → #8 → #9 → #10 → #11**. No PR was merged in this review.

Code: **`/Users/alfongj/Code/engine-core-review-stack`**, branch
**`pin-testnet-resume-provenance`**, commit **`8039b2d`**.
Runtime source is **`4dadaf5`**; the last PR changes harnesses/tests/documentation only.
README and `LOCAL_TESTING.md` contain setup, commands and another agent's prompt.
The older `/Users/alfongj/Code/engine-core` checkout is unchanged.

Evidence: **`/Users/alfongj/Code/engine-core-review-evidence`**, pushed to **`load-tests`**.
Read the [full review and measured limits](docs/baselines/code-review-2026-11-08/README.md).
Private logs/fixtures: `/Users/alfongj/.local/share/engine-core/runs/code-review-2026-11-08/`.
Completed steady validator ledgers have verified compressed archives there;
the failed crash fixture, old incident journals and Nitro environment remain intact.

## Results

- Full runtime workflow, **186 Redis/API tests** and **12 process scenarios** pass; all **41 offline harness tests** pass at the review head.
- Final CI passes functional gates, **107 model cases / 95 source guards**, five fee proofs and two mutation checks. This is not a whole-service proof.
- Ten five-minute runs complete **96,000 transfers at 8 + 8 + 16 TPS**. Latency varies; sustainable capacity is unconfirmed. No 50 TPS-per-chain claim.
- The original shared-chain crash run exposed the lock delay and ended incomplete. The fixed binary passes the same 2,880-transfer fault workload, independent safety/liveness checks and final drain. Both results are retained.
- **Dependency audit stays red:** one crypto-bigint vulnerability, one LRU unsoundness warning and five unmaintained warnings. Native signing mitigates environment-key exposure; the upstream dependency needs migration/backport.
- **$0 public RPC credits spent.** Owned test services are stopped.

## Remaining work

1. Resolve the upstream cryptographic dependency for a clean audit.
2. Qualify production storage/workloads, public providers, rollup settlement and KMS. Durable writes are a measured constraint on this Mac.
3. Design journal retention and missing-job reconstruction. Keep the latest independent journal and one active Engine process per deployment on one Unix host. Missing Solana history never authorizes re-signing unknown work.

No access or credentials are needed from you to review the stack. Drain old workers
gracefully before upgrading; legacy Solana locks retain their TTL. Follow the
[upgrade/rollback runbook](https://github.com/alfongj-com/engine-core/blob/pin-testnet-resume-provenance/docs/design/redis-disaster-recovery.md#schema-upgrade-and-rollback)
before opening deployed state. Schema 3 has no in-place downgrade.
