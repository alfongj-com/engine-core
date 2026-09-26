# Capacity and failure testing

## Question

How much work can one signer complete on each chain, how does the service recover
from failures at that rate, and what happens when those chains share one Engine
process and durable journal?

The result is a measured operating range for this machine, workload, node and
durability configuration. It is not a network-wide maximum. Native rollup runs,
public-testnet checks and local timing simulations must be labeled separately.

## Method

1. Screen individual chains above the previous 50 TPS result, including 100 TPS.
   Increase load until backlog, latency, rejection or node limits bind. Narrow
   the interval between the highest sustainable rate and the first failure.
2. Confirm candidates over a longer steady interval after finality startup.
   Record admission, attempted wire, on-chain execution and durable terminal
   rates separately. Preserve unsuccessful runs.
3. At the selected rate, mix transfers with multiple-instruction/contract work
   and expected failures. Inject bounded RPC faults and process crashes while
   work is active. Compare original identities, chain effects and terminal
   outcomes after recovery.
4. Run the individually selected rates together through one Engine process,
   one Redis and one SQLite journal. Measure per-chain fairness and aggregate
   capacity. If that overloads the service, reduce aggregate load to identify a
   usable shared configuration. Repeat representative faults under shared load.

## Measurement rules

- Use open-loop arrival schedules. Record missed arrival slots, client queue
  pressure, HTTP errors and overload responses. Do not hide overload by retrying
  requests with new IDs or measuring only the drain.
- A capacity candidate needs near-offered throughput after startup, bounded
  admission latency, no unexplained rejection, and no growing unsigned backlog.
  Block-batched rates fluctuate; examine the series over complete block cycles.
  A short screening pass requires longer confirmation before selection.
- End the offered-load window before waiting for outstanding HTTP responses.
  Include client queue time in latency. Keep sampling off the producer thread.
- Report drain separately. Reconcile every admitted or uncertain HTTP request
  using its original ID; distinguish expected execution failures from lost work.
- Successful value transfers, expected reverts and duplicate retries must match
  independent chain balances, contract state, transaction status and durable
  attempts. Aggregate counts alone cannot detect substituted terminal outcomes.
- Keep SQLite FULL synchronization enabled and record Redis AOF policy. Redis
  loss may require a halt and quarantine; an unsafe automatic replay is a failed
  safety test even if throughput recovers.
- Only one timed local workload runs at once. Record node versions, binary/source
  hashes, worker/inflight settings, host resources and faults with timestamps.

## Budget and limits

Local tests use disposable keys and loopback nodes. Existing public-testnet
balances are small. Any public RPC use must preserve the existing persisted
campaign spending cap; do not reset its counters or infer account credit from
the local estimate. Public bursts must respect provider quotas and are separate
from local saturation tests.

Finite-duration fault tests can expose defects; passing them does not prove
recovery from every timing, disk failure or dishonest provider. Existing formal
models retain their stated assumptions and scope.
