# PR #2 status

Updated October 12, 2026. [PR #1](https://github.com/alfongj-com/engine-core/pull/1)
merged into `main`; [PR #2](https://github.com/alfongj-com/engine-core/pull/2)
implements the follow-up against `main`.

## Delivered

- **Crash-recoverable checkpoints.** Schema 2 records the exact journal-to-Redis
  transition in the authority commit. Startup or health repair can finish it after
  a crash or lost reply. A second FULL SQLite commit acknowledges publication
  before permission returns; a fresh observation still rejects rollback afterward.
- **Unchanged safety boundaries.** Wrong tokens, Redis process/role changes,
  corrupt records and durable halts stop execution. Healthy schema-1 journals
  migrate under exclusive ownership. Old binaries reject schema 2; status/export
  stay read-only. Missing signed Redis attempts still require reconciliation.
- **Shared-chain scheduling.** One contender per chain reaches the journal mutex.
  Health checks take their chain lane before shared health capacity, preventing a
  busy chain's waiting polls from reserving all 32 slots. Metrics separate lane,
  capacity, mutex, SQLite and Redis waits.
- **Bounded Solana polling.** Eligible history lookups coalesce per cached
  cluster/endpoint, with at most 256 outstanding polls, 32 signatures per batch,
  four HTTP batches and a 5 ms collection window. Invalid entries fail only their
  matching callers; a config switch disables coalescing. Finalized receipt and
  signed-identity checks remain.
- **Shutdown under Redis outage.** Stalled queue acquisition is cancellable.
  Already-running handlers and owned journal repairs still drain before exit.

## Review follow-up

Unscoped HTTP/health checks have separate bounded scheduling capacity; 4337 and
7702 workers now carry their chain scope. Idle EVM lanes can be reclaimed without
splitting live overflow work across lanes. Registration fails promptly when a
shutdown drain is waiting, avoiding the health/mutation lock-order deadlock.
Queue and executor metrics initialize before any startup work.

The [restart and rollback runbook](docs/design/redis-disaster-recovery.md#redis-restart-with-intact-data)
now covers pending transitions after Redis restart and one-way schema migration.
Exact-target pending reattachment remains refused; this is a documented recovery
limit, not a relaxed fence. Earlier measurements below predate these review fixes;
their source hashes identify what was measured.

The [review validation record](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/pr2-review-2026-10-12/README.md)
keeps new tests, load and crash results separate from the original PR measurements.

## Validation and measurements

[Versioned results and failures](https://github.com/alfongj-com/engine-core/blob/load-tests/docs/baselines/pr2-2026-10-12/README.md)
are kept on `load-tests`, outside this implementation PR. They include binary/source
provenance, baseline comparisons, latency/backlog, exact effect reconciliation,
model outcomes and CI links. The PR checks qualify their own source commit.

The local checks cover the workspace suite, 47 journal regressions, 73 executor
regressions, 36 queue regressions, HTTP/admission tests, real HTTP batching, process
crashes, Redis data loss, reorgs and the live-outage shutdown regression. The formal
suite has 73 bounded configurations, including deliberate faults that must produce
specific counterexamples; fee arithmetic is checked separately with Kani.

The schema-2 acknowledgement adds real disk cost. The first corrected 10/10/20 TPS
shared run passed its finite-run gates, but the journal was nearly fully occupied
and Solana latency increased. This does **not** establish 50 TPS per chain or a
sustainable maximum. See the evidence for repeats, overload and chaos outcomes;
no baseline capacity figure transfers automatically to the new protocol.

All new experiments use disposable local Redis, Anvil and Agave fixtures. No paid
RPC calls are needed. Raw reports and fixture data stay private under
`/Users/alfongj/.local/share/engine-core/runs/pr2-oct12/`.

## Remaining production work

- Qualify sustained capacity on the intended storage, RPC and actual chain stack.
  Further journal throughput work needs fewer durable commits per batch or faster
  durable storage, with the same crash and rollback guarantees.
- Add an explicit recovery procedure for missing signed Redis projections. Marker
  repair alone cannot recreate them or resolve quarantined unknown transactions.
- Keep the existing one-host, one-owner authority assumption. Lost or rolled-back
  authority, filesystem guarantees, provider honesty and actual rollup settlement
  remain outside these local tests and finite models.

Fairness allocates journal operations, not equal TPS. Global admission capacity
and overflow lanes can still delay peers. No whole-service proof is claimed.

See [local commands and agent prompt](LOCAL_TESTING.md), the
[checkpoint protocol](docs/design/recoverable-projection.md),
[Solana polling design](docs/design/solana-status-batching.md), and
[formal correspondence](formal/disaster-recovery.md).
