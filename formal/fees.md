# Production fee arithmetic proofs

## Scope and code connection

`executors/src/eoa/worker/transaction.rs` calls the dependency-free [fee_math.rs](../executors/src/eoa/worker/fee_math.rs). The [Kani harness](fees/src/lib.rs) imports that exact production file with `#[path]`; it does not maintain a second implementation as its verification target. The extraction preserves the existing arithmetic and transaction behavior.

The specification is `min(cap, 2^128−1, floor(value × multiplier / 100))`, with multiplication over natural numbers and absent caps interpreted as `2^128−1`. Dynamic priority fees are additionally limited to the resulting total fee. The current stalled-recovery caller uses multiplier **120**.

| Property | Symbolic domain / assumptions |
| --- | --- |
| Both caps hold; priority ≤ total | Every `u128` fee/priority/cap, present or absent caps, every `u32` multiplier; no assumptions |
| Scalar fees do not decrease | Every `u128` value/cap and `u32` multiplier, assuming multiplier > 100 and cap ≥ old value |
| Both dynamic fees do not decrease | Multiplier 120; old priority ≤ old total and each cap ≥ its corresponding old value |
| Exact floor, saturation and cap | Multiplier 120; every `u128` input and optional cap, through the two compositional proofs below |

The five proof harnesses include all automatically generated arithmetic safety checks. They contain no loops requiring a chosen application unwind bound and no stubs or unchecked proof contracts.

## Exact arithmetic argument

1. **Division:** verify the production `division_parts(value)` returns `q,r` with `r < 100` and `100q + r = value`, without overflow, for every `u128` input.
2. **Saturation:** verify the production `capped_increase_parts` for every `u128 q`, optional cap, and `r < 100`. Set `f = floor(120r/100)`. The mathematical result exceeds `M = 2^128−1` exactly when `q > floor(M/120)`, or `q = floor(M/120)` and `f > M mod 120`. The reference uses this pre-multiplication threshold and ordinary checked arithmetic, without saturating operations.

**Composition (reviewed source):** the production `capped_increase` consists of calling `division_parts(value)` and passing its outputs, the multiplier, and the cap to `capped_increase_parts`. This transparent adapter is manually reviewed, not a separately machine-checked refinement proof.

Euclidean division gives `floor(120 × value / 100) = 120q + f`; the verified pieces therefore cover every original input. Splitting the proof avoids expensive monolithic wide-product equivalence queries. Those exploratory queries timed out and are not counted as evidence. A separate unit test compares selected boundary cases with a 192-bit multiply-then-divide reference; that test is not the symbolic proof.

## Reproduce and detect regressions

[Kani 0.68.0](https://github.com/model-checking/kani/releases/tag/kani-0.68.0) supports Linux x86-64 and both Intel and Apple Silicon macOS. Installation follows its [official guide](https://model-checking.github.io/kani/install-guide.html):

```sh
cargo install --locked kani-verifier --version 0.68.0
cargo kani setup
python3 formal/fees/check.py --output-dir formal/fees/results
```

The runner pins the verifier version, checks the exact five-harness inventory, and records source hashes, commands, checks and timings. Defaults: three parallel harnesses and a 300-second timeout per harness. Generated logs/results and build products are ignored.

Two negative controls mutate temporary copies of the **production** module: remove the cap, and replace saturating multiplication with wrapping multiplication. They must produce the specific cap and nondecrease assertion failures. Installation errors, compilation errors, timeouts, missing JSON and unrelated failures are rejected. The working production file is never mutated by the runner.

## Recorded verification

On September 26, 2026, the complete runner passed on Apple Silicon macOS with Kani 0.68.0 / CBMC 6.11.0: **five positive harnesses, 116 checks, zero failures; both mutation counterexamples detected**. Total wall time was approximately 108 seconds, including negative controls. The generated summary records exact source hashes; future source revisions must rerun the gate. The existing three signed-wire/fee unit tests and the separate wide-reference boundary test also passed; the Redis integration test was not rerun for this arithmetic-only extraction.

## Limits and tool choice

This establishes arithmetic properties, not correctness of fee estimation, request parsing, transaction field mapping, signing, provider behavior, nonce recovery, total transaction cost, or the rule that unchanged replacements are skipped. Existing signed-wire and Redis regression tests cover several of those boundaries. Explicit fees can deliberately lower a previously invalid value; nondecrease therefore requires the stated assumptions. Estimated fees still have no spending ceiling, and L2 data fees are outside these per-gas limits.

Kani is the useful first tool here: it verifies the Rust implementation and its finite-width overflow behavior directly. A Lean proof could express a broader mathematical specification and machine-check its derivation, but a separate Lean model would also require a maintained connection to this Rust implementation. That is additional work, not an automatic extension of these results. See [Kani's arithmetic checks](https://model-checking.github.io/kani/tutorial-kinds-of-failure.html) and the [Lean language reference](https://lean-lang.org/doc/reference/latest/Introduction/).
