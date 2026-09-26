# Throughput review: formal evidence

The [report](report.json) and 56 TLC logs are the final local rerun against the
65 reviewed source hashes in test-only commit `9537ac6ade9bd1acafe4d2e8bd1f2ceb919d1725`.
All 56 expected outcomes pass. Thirteen positive configurations exhaust
3,164,046 states summed across separate state spaces; the 43 other configurations
produce their required fault, boundary or reachability counterexamples.

Production runtime is unchanged from `025a3d154c23f26e8e5bc6a8f7ef7c3a8294e90b`,
the source used for the measured Engine binaries. The follow-up changes only
Redis fixture seeding and its reviewed test-file hash; it changes no model
transitions. The earlier run remains in the formal CI artifacts for `025a3d1`.
See the [fixture follow-up](../fixture-batching/README.md) for the cancelled Linux
run, bounded setup correction and exact local serial test result.

The [summary](summary.json) labels the overlapping implementation checks and
source revisions. The EOA and parser logs were produced during the initial
runtime review; the latest executor serial log is in `../fixture-batching/`.
No formal result proves Rust/model refinement, model composition, throughput or
public-chain behavior. Linux separately passed the corrected test fixture and
full runtime gates at `9537ac6`; links are in the fixture follow-up. Those results
precede the later progress-scheduling runtime experiment.
