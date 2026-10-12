# PR #2 final-source formal checks

Source: `0ad0453e0dcadee57a40870ec5830f9b20089959`.
[CI run](https://github.com/alfongj-com/engine-core/actions/runs/38209152202).

- [TLC report](protocols/report.json): all **73 expected outcomes** match, including deliberately violated invariants and witness cases. All **88 mapped source files** match the reviewed source map.
- [Kani report](fees/summary.json): five production fee-arithmetic harnesses proved; both deliberate source mutations produce the required counterexample.

The archived logs contain synthetic model/proof executions, not live-chain requests or credentials. Runtime benchmark and failure reports are separately sanitized in the [measurement report](../../../docs/baselines/pr2-2026-10-12/README.md).

These are bounded protocol models, selected arithmetic proofs and source-change review guards. They do not establish Rust refinement, model composition, filesystem durability, fair scheduling, throughput, provider honesty or correctness of the entire service.
