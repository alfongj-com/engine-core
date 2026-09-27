# Final combined validation and custody follow-up

This supplements the immutable phase-only evidence in [README.md](README.md) and [source-map.json](source-map.json). Their campaign hash `ac297f23…` is the **intermediate phase-only stage**. The final combined hashes are in [final-source-map.json](final-source-map.json).

Root then added an explicit local validator limit of 1,000,000 ledger shreds and recorded it in the initial fixture metadata. The [retention-only patch](retention-only.patch) captures exactly that addition after the phase fix. It addresses default-history pruning when a retained local validator ledger is reopened; it is a bounded fixture setting, not indefinite archival retention or a public Solana guarantee. The host resource guard remains active. Rust and Engine were unchanged by this addition.

The [combined 94-case suite](combined-94-pass.log) passed after that change. These repeat the same 94 cases, with no extra unique tests claimed. Original phase logs, patch, review and source hashes remain unchanged; [intermediate-manifest.json](intermediate-manifest.json) preserves their original manifest.

## Original accepted custody now verified

A separate read-only reconciliation verified all 25,155 accepted original intents against actual finalized receipts, effects, fees and the retained ledger. It used no Engine, Redis, funding, or sends. The [follow-up summary](accepted-custody-followup.json) records the exact original-report/evidence hashes and outcome `accepted_custody_pass`.

The original 25,200-offer campaign still failed capacity qualification: 45 known 429 responses, its infrastructure interruption, and its recorded backlog behavior remain. The successful custody review does not change original intake statistics or establish a new sustainable offered window. The [full accepted-custody report](../solana70-guard-incident/accepted-custody-report.json) and [incident archive](../solana70-guard-incident/README.md) preserve the independent evidence; no original report was overwritten here.
