# Formal validation

Commit: `04d15d86ad960f40345d7dddb8c84505ba3e96ba`. Local and CI checks below are complete. CI artifacts were downloaded and their report/source hashes independently checked against this commit.

| Validation | Result | Scope |
|---|---|---|
| Local guarded protocol suite | Pass | All 100 expected outcomes; 91 source hashes and 100 model/config pairs match this commit. |
| CI guarded protocol suite | Pass | All 100 expected outcomes; 91 source hashes and 100 model/config pairs match this commit. |
| CI production fee arithmetic | Pass | Five Kani harnesses proved; both deliberate source mutations rejected at the required assertion. |

The protocol inventory comprises 22 positive, 49 fault, 12 boundary, and 17 witness configurations. Expected counterexamples are required for fault, boundary, and witness cases; their presence does not mean the positive models failed. New cases cover retained Solana identity, bounded durable recovery grants, exact-target offline acknowledgement, schema migration, and retry after committed acknowledgement.

## Limits

These are finite models with explicit bounds. The source hashes guard review correspondence; they do not establish a refinement or composition theorem. The durable recovered-send pool is bounded independently. The combined original-plus-recovered send bound assumes the original Redis counter remains intact until recovery mode activates; the original-counter rollback boundary has its own expected counterexample. Clocks, provider honesty, physical durability, and sustained throughput require separate evidence.

See [formal.json](formal.json) for exact report/source hashes and proof names, and [CI run](https://github.com/alfongj-com/engine-core/actions/runs/38272433716) for job provenance.

## Final-head validation

Final PR head `146de9d78fa494a73d03bcc80dc7bc9410bdb733` changes only `README.md`, `LOCAL_TESTING.md`, and `WORK_IN_PROGRESS.md` after the validated implementation commit. Executable code, models, and the source map are identical.

The separate [final-head CI run](https://github.com/alfongj-com/engine-core/actions/runs/38274782927) passed. Its downloaded artifacts independently confirm all 100 expected protocol outcomes, 91 matching source hashes, 100 matching model/config pairs, five production fee proofs, and both required mutation counterexamples. Final-head report and artifact digests are recorded under `final_head` in [formal.json](formal.json); earlier results above retain their original commit provenance.

The intermediate documentation head `cfd7d07` and [its CI run](https://github.com/alfongj-com/engine-core/actions/runs/38274306942) were superseded. GitHub canceled that run when the newer documentation head was pushed; this is not a failed proof result.
