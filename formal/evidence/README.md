# Formal verification evidence — September 26, 2026

The reports bind results to exact model, configuration and Rust source hashes.
No paid RPC or blockchain transactions were used. This is finite protocol
verification plus proofs of the production fee arithmetic, not an end-to-end
proof of Engine.

## Protocol models

[Full TLC report](tlc/report.json): **35/35 configured checks pass**. Ten positive
configurations exhaust **2,463,657 states** across four models; 18 fault configurations
produce their required counterexamples, six boundary configurations disprove
unsupported guarantees, and one witness reaches Solana's twentieth dispatch.
State counts are summed across separate configurations, not one service-wide
state space. Logs retain model/configuration hashes, collision estimates,
coverage and counterexample traces. Total local runner time: about
134 seconds using pinned TLC 1.7.4 and Temurin 21 on macOS arm64.

## Rust fee arithmetic

[Summary](fees/summary.json): five Kani 0.68.0 harnesses, 116 checks, zero failures.
Both mutations of the production source fail their required assertion. JSON
and logs (trailing whitespace normalized) in `fees/` retain the assumptions, tool results and source digests.
Positive verification took 105.5 seconds locally; both mutations took 2.6 seconds
combined. The [arithmetic document](../fees.md) explains the manually reviewed
composition step and the exact scope of each proof.

## Queue regressions

All 26 library regressions pass against disposable Redis 7.4.2. Twelve new tests
reproduce cancellation, delayed/terminal reuse, zero retention and orphan
cancellation defects. [Final log](queue-redis-final.log); before-fix logs are
retained alongside it. Some individual tests cover several history orderings.

## Development integration checks

[`development-gates/report.json`](development-gates/report.json) records a
passing workspace run, 37 executor Redis regressions, 4 Solana admission tests,
2 HTTP tests and formatting. Queue fixes continued during this development run,
so **the final immutable-source CI run is the final runtime qualification**.
The final queue regressions above ran after the last runtime edit.

The first HTTP attempt failed because the rebuilt Redis executable was outside
PATH; [the failure](http-missing-redis-binary.log) is retained. Setting
`REDIS_SERVER_BIN` and `REDIS_CLI_BIN` to the disposable tool paths resolved it;
no HTTP code change was needed.

## Limits

Read [coverage](../coverage.md) and the per-model documents before using these
results as a release claim. TLA+ models are manually mapped to Rust/Redis tests;
there is no mechanically checked refinement or composition of all four models.
Exploratory incomplete searches and solver timeouts are not counted as passes.
Historical public-network and throughput results retain their earlier source
scope. In particular, the previous queue benchmark did not exercise pruning;
the added retained-history scans need a separate throughput measurement.
