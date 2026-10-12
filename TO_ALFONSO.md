# To Alfonso

Updated October 12, 2026.

## Saved and pushed

[PR #2](https://github.com/alfongj-com/engine-core/pull/2) contains the recovery,
fairness and polling changes. Code is pushed at `319035e`; your main local folder,
`/Users/alfongj/Code/engine-core`, is current. Its README and `LOCAL_TESTING.md`
include setup commands and a prompt for another agent. PR #1 is merged.
Detailed test evidence stays on this `load-tests` branch.

## Review comments addressed

- Separate HTTP health checks from the shared fallback lane; scope all 4337/7702
  workers and reclaim idle EVM lanes safely.
- Install metrics before workers start.
- Isolate malformed Solana statuses within each batch, reduce batches to 32,
  and provide a switch to disable batching.
- Fix shutdown lock ordering and permanently reject delayed repair registration
  after the final drain.
- Document planned Redis restarts and schema rollback. Pending transitions still
  require recovery and quarantine after Redis restarts; the fence is unchanged.

## Validation and performance

The updated code passed the five-minute shared load at **32 TPS combined**:
8 EVM + 8 OP-like + 16 Solana. All 9,600 transactions completed and all queues
drained. Admission p99 was 69 / 64 / 710 ms respectively. The earlier run measured
70 / 63 / 638 ms, so this does not establish a latency improvement.

A forced Engine crash under the same offered rates also passed: all 2,880
transactions completed, all queues drained, and four same-ID retries caused no
extra sends. This fault test used a 10-second queue lease; the steady run used
the 600-second default. The strict capacity screen remains unconfirmed.

All five CI checks and automated review passed on `319035e`, including the full
Rust correctness suite, dependency audit, queue coverage, 73 bounded model
configurations and five fee proofs with two deliberate mutation rejections.

The tests cover journal cancellation, shutdown, Redis loss, reorgs, response loss,
duplicate effects, lane reclamation and malformed Solana statuses. The evidence
separates the earlier full suite from targeted checks on the final revision.

[Review results and exact source hashes](docs/baselines/pr2-review-2026-10-12/README.md).
[Earlier comparisons and failed overload runs](docs/baselines/pr2-2026-10-12/README.md)
remain available. These local Anvil/Agave fixtures do not qualify real rollup
settlement or public RPC capacity. No paid RPC credits were used.

## Remaining before production

1. Improve durable journal throughput and qualify the intended hardware, RPC,
   workload and real chain stack. There is no sustainable maximum or 50 TPS per
   chain claim; the earlier longer 40 TPS shared run accumulated backlog.
2. Add explicit recovery for missing signed Redis projections and quarantined
   unknown effects. Checkpoint repair alone cannot reconstruct them.
3. Complete KMS/account and deployment qualification, including archival-provider
   latency with batching enabled and disabled.

The old incident journals and retained Nitro environment were not changed.
Unrecorded schema-1 mismatches do not become repairable just by upgrading.
One host and one retained authority are still required; finite models do not
prove the whole service.
