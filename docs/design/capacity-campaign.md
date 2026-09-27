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
- A short screening candidate needs near-offered throughput after startup,
  bounded admission latency, clean intake/RPCs, and no growing unsigned backlog.
  The harness's 5% rate tolerance is a screening threshold, not evidence that a
  terminal queue growing at 5% of intake is sustainable. Apply the stricter
  confirmation assessment below before selecting an operating rate.
- End the offered-load window before waiting for outstanding HTTP responses.
  Include client queue time in latency. Keep sampling off the producer thread.
- Report drain separately. Reconcile every admitted or uncertain HTTP request
  using its original ID; distinguish expected execution failures from lost work.
  Anvil drain counts both pending and queued transactions through `txpool_status`.
  Native Nitro exposes only a weaker pending-nonce observation: a zero nonce
  delta cannot prove the pool is empty behind a gap. Each chain's source/counts
  are recorded separately; exact per-ID evidence remains the execution authority.
- Successful value transfers, expected reverts and duplicate retries must match
  independent chain balances, contract state, transaction status and durable
  attempts. Aggregate counts alone cannot detect substituted terminal outcomes.
- Keep SQLite FULL synchronization enabled and record Redis AOF policy. Redis
  loss may require a halt and quarantine; an unsafe automatic replay is a failed
  safety test even if throughput recovers.
- Only one timed local workload runs at once. Record node versions, binary/source
  hashes, worker/inflight settings, host resources and faults with timestamps.

### Tuning controls

Capacity runs use `APP__QUEUE__EOA_MAX_INFLIGHT=4096`; the service default is
50. This is the allowance for unconsumed nonces, not RPC concurrency. A signer
offering 50 TPS across a 12-second block interval needs room for roughly 600
transactions before inclusion, plus headroom. The measured rates therefore do
not describe the default configuration. The campaign also uses one EOA worker
per EVM chain, 100 Solana workers, a 20ms queue poll interval and local keys.
AWS KMS signing throughput is a separate qualification.

`APP__QUEUE__EOA_BROADCAST_CONCURRENCY` accepts 1–128 concurrent broadcast tasks
per signer/chain worker; the default remains 32. It does not enlarge the nonce
window, preparation concurrency, borrowed-receipt lookup limit, or sequential
gap recovery. It is not a global RPC rate limit. The harness sets it explicitly
with `--eoa-broadcast-concurrency` and records the setting. Compare settings on
the same build before attributing any throughput change to this control.

Solana's experimental confirmation polling interval accepts 1–5 seconds, with
the existing one-second default. Longer intervals may reduce status traffic at
the cost of detection latency; they do not relax commitment or signed-identity
checks. Record it with `--solana-confirmation-poll-seconds`.

The final rate search uses an explicit 100ms scheduling-lag allowance, following
initial trials with the unchanged 25ms harness default. Absolute offer times,
bounded client concurrency and missed-slot counts remain authoritative. Eligible
late offers can produce bounded microbursts; this is not a hard real-time load
generator. Reports include scheduled-to-HTTP-start lateness, distinct from
response latency. Preserve both rounds; a later allowance does not repair an
earlier missed offer.

The final client permits 128 concurrent HTTP requests. Earlier screens used 64;
both limits are bounded, and old client-capacity drops remain failed offers.
Neither limit changes Engine's worker or RPC concurrency.

Solana's live observer keeps a FIFO list of unresolved signatures. It rotates
each selected batch before querying, removes finalized signatures, and appends
new signatures behind waiting work. Each sample still queries at most 2,048
signatures in batches of 256. Failures retain the pending work and disqualify
the measurement. This fixes an older observer that could follow new arrivals
and fail to revisit old signatures until load stopped. Full post-drain
reconciliation remains independent of live sampling.

### Confirmation assessment

Run [`capacity_assess.py`](../../scripts/capacity_assess.py) on the original full
JSON report, preserving that report and writing a new assessment:

```sh
python3 scripts/capacity_assess.py /tmp/capacity-run.json \
  --output /tmp/capacity-run-assessment.json
```

The assessment records both source hashes and requires:

- At least 180 seconds after the configured warmup and three complete
  aggregation groups. Choose warmup to cover the chain's finality startup;
  180 seconds of total offered load is usually only a screen.
- Admission, unique durable attempt, observed inclusion and durable terminal
  rates each within one transaction per analyzed window of the offered rate.
  A durable attempt records permission to send; it does not prove RPC acceptance.
- Unsigned and terminal backlogs that grow by at most one transaction between
  endpoints, with aligned mean backlog slopes no greater than 0.01 transaction
  per second. These are explicit finite measurement tolerances, not permission
  for continuing queue growth.
- Clean intake, the configured admission p99 limit, no unexpected RPC or
  execution errors, and exact final reconciliation. A later successful drain
  cannot repair an overloaded offered-load window or a missed arrival slot.

Groups span at least 30 seconds and whole nominal block/sample cycles. The
analyzer integrates observed samples linearly. Existing reports do not record a
head timestamp at each sample, so alignment is nominal; endpoint disagreement
without persistent growth remains unconfirmed. Large, sustained unsigned
growth is overload even when a conservative classification also reports cadence
ambiguity. Solana's bounded inclusion observer may lag, which requires inspecting
observer backlog before attributing a deficient observed rate to execution.

A clean result is named `steady_window_evidence_requires_repetition`. Repeat it
with the same binary, workload and durability settings, bracket a higher failing
rate, then test the selected rates together. Neither this finite assessment nor
the highest tested passing rate proves an absolute or indefinitely sustainable
maximum. Fault runs are assessed for recovery and safety separately from nominal
capacity.

## Budget and limits

Local tests use disposable keys and loopback nodes. Existing public-testnet
balances are small. Any public RPC use must preserve the existing persisted
campaign spending cap; do not reset its counters or infer account credit from
the local estimate. Public bursts must respect provider quotas and are separate
from local saturation tests.

Finite-duration fault tests can expose defects; passing them does not prove
recovery from every timing, disk failure or dishonest provider. Existing formal
models retain their stated assumptions and scope.
