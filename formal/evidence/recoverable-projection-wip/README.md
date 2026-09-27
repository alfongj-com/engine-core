# Paused schema2 formal work

The user requested stopping feature work to preserve compute and reorganize PR1.
No TLC/check processes remain. Source-map refresh and further model work are paused.

## Completed

- Identity/effect model now retains pending across crashes and separates Redis publication, SQL acknowledgement and caller return.
- New focused projection model covers schema2 pending repair, schema1 migration, transport uncertainty, cancellation, process/role changes and explicit recovery.
- All 22 targeted cases reached their expected outcomes: three exhaustive positive cases and 19 named fault/boundary/witness counterexamples. These are **not 22 safety proofs**. The new family contributes two positives, six mutations and four reachability witnesses.
- Positive distinct states: identity 1,608,200; projection 220,530; legacy migration 622,118.

## Not completed

- Independent final review of the new model/source correspondence.
- Final runtime implementation, its Rust/process tests and performance measurements.
- Refreshed `formal/source-map.json`, full repository model run and runner safeguards.
- No production-readiness, general liveness or composition/refinement claim.

`report.json`, `suite.log` and individual logs preserve exact outcomes. The recorded runner (`targeted_check.py`) intentionally exercised only model files while runtime source was changing; its omission of the source-map check must not be used for final qualification. `source-hashes.json` pins the reviewed draft files. Existing evidence was not overwritten. The stop signal raced the last case's completed named counterexample; all recorded final exit codes are 0 or the expected 12, not interruption/resource failures.

Next, review the final runtime against both models, update only reviewed source-map entries, then run the official `formal/check.py` and safeguards in a coordinated quiet window.
