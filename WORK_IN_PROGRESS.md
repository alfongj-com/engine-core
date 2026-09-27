# Saved work: recovery and chain fairness

Saved September 27, 2026 at Alfonso's request to conserve the weekly compute budget.
Branch: `crash-recovery-and-chain-fairness`, stacked on `production-hardening` (PR #1).
This is unfinished work, not a release or a qualified performance improvement.

## Implemented but not compiled

- Journal serial-lock wait/hold, blocking-pool wait, SQLite mutex/execution/commit,
  and Redis timing instrumentation; server metrics registration.
- Queue and EOA phase/outcome timing, bounded chain labels, backlog/transition
  measurements, and Solana RPC/handler timing. Existing scheduling/polling behavior
  is unchanged. Regression tests are authored but have not been executed.
- `scripts/capacity_metrics.py` runs the existing local campaign with bounded
  Prometheus snapshots. Its three parsing tests pass.

## Recovery draft: not connected to production yet

`core/src/recovery/projection.rs` and `projection_tests.rs` are deliberately not
included by `recovery.rs`. Production still uses schema 1 and the old fail-closed
checkpoint behavior. The draft records an exact pending transition, applies Redis
CAS, then commits a SQLite acknowledgement before returning permission.

Remaining wiring includes schema 2 and safe legacy migration, startup reconciliation,
transport ambiguity, owner retention through cancelled blocking work, offline
recovery/reattach rules, status/export, and every mutation's pending record. Add the
remaining migration, corrupt-marker, ambiguous-response and acknowledgement tests.
The drafted process-kill/cancellation tests have not been compiled or run.

The revised disaster-recovery models and new projection model are drafts. The
standalone agent run reports 22 expected outcomes; full-suite validation and reviewed
runtime source correspondence are still pending. The source map is intentionally
not refreshed. Do not present these models as verification of the unwired Rust draft.

## Resume order

1. Resolve any compile errors and update Cargo.lock for the added Prometheus
   dependencies. Run the new timing tests. Save an instrumentation-only binary
   before changing recovery or scheduling semantics.
2. Measure an unchanged local workload, including storage/Redis wait breakdowns.
3. Wire and review the recovery protocol; run adversarial crash, rollback,
   migration and cancellation regressions before throughput measurements.
4. Use the baseline to choose a fairness change. Implement bounded Solana status
   coalescing only after reviewing `docs/design/solana-status-batching.md` and
   validating positional mapping, history/endpoint isolation and cancellation.
5. Repeat matched measurements, review all changes, refresh formal correspondence
   only after review, then run the required checks.

No new load test or paid RPC call ran in this work period. Prior failed-campaign
custody remains untouched and Nitro remains paused. The original PR #1 executable
is preserved privately at `/tmp/engine-stacked-pr2/engine-pr1`; SHA-256
`35e307e16cafa7cbb17caafcf5ed90a840d58dddd2454542566619266e7a4c07`.
Historical load reports and generated evidence are preserved on the
[load-tests branch](https://github.com/alfongj-com/engine-core/tree/load-tests).
They were removed from PR #1 and are not prerequisites for a normal build.
The [targeted model WIP evidence](https://github.com/alfongj-com/engine-core/blob/load-tests/formal/evidence/recoverable-projection-wip/README.md) is archived separately.
