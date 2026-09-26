# Formal verification evidence — September 26, 2026

The reports bind results to exact model, configuration and Rust source hashes.
No paid RPC or blockchain transactions were used. This is finite protocol
verification plus proofs of the production fee arithmetic, not an end-to-end
proof of Engine. Trailing blank lines in stored test logs are normalized.

## Latest gap-recovery candidate

The [gap-recovery review](capacity-gap-review/README.md) passes all 61 expected
model outcomes and 69 reviewed source hashes. It adds source correspondence for
the journal's original-wire getter and bounded replay helper, plus an actual
Redis/HTTP/SQLite regression with 11 isolated scenarios. Models are unchanged:
repeated same-identity dispatch is a stutter, not a proof of mempool-eviction
recovery, cooldown, scheduler fairness or elapsed progress. Distinct runtime
suites, exact source/release hashes and raw traces are preserved there.

## Previous capacity review candidate

The [capacity review record](capacity-review/README.md) passes 61 expected model
outcomes and all 67 reviewed candidate source hashes. The new DepthCheckpoint
family distinguishes observed tip from qualified history: shallow replacement
must not falsely halt, while a detected qualified-boundary conflict still must.
The 14 positive configurations exhaust 3,164,944 states across separate state
spaces. Targeted Rust/Redis/HTTP regressions and exact counterexamples are linked
there. These results qualify the recorded working-tree bytes, not the earlier
base commit or a measured throughput maximum. Prior evidence remains unchanged.

## Previous scheduling follow-up

The [progress scheduling record](progress-scheduling/README.md) covers runtime
`f309177`: 56 expected model outcomes, 66 reviewed source hashes, 40 EOA tests
including a nine-scenario real Redis scheduling regression. Immediate requeue
requires useful progress and rejoins the queue tail. Unknown-only work remains
delayed. Model transitions are unchanged; elapsed time, scheduler fairness,
per-wire retry rates and throughput are not proved.

## Throughput and recovery review (preceding runtime)

The [56-case protocol report](throughput-review/report.json) passes on checked
source `9537ac6ade9bd1acafe4d2e8bd1f2ceb919d1725`, with **65 reviewed source hashes**.
That follow-up changes only Redis test-fixture setup and its reviewed hash.
Production runtime and measured binaries remain at
`025a3d154c23f26e8e5bc6a8f7ef7c3a8294e90b`.
Thirteen positive configurations exhaust **3,164,046 distinct states** in total;
29 fault, ten boundary and four reachability configurations produce the exact
required counterexamples. State counts are summed across separate configurations,
not one combined service state space. Pinned TLC 1.7.4 took about 186 seconds
locally; all raw logs are beside the report.

The extension adds the consumed-nonce allocator floor and terminal-attribution
mutation. Source review also maps bounded finality polling, per-attempt membership,
post-dispatch EOA uncertainty, pre-dispatch NOOP retention and Solana's independent
reservation before Redis. These mappings do not prove Rust/model refinement,
model composition, automated NOOP recovery or 50 TPS.

[Implementation evidence](throughput-review/summary.json) records 39 passing EOA
tests (including real Redis/HTTP/SQLite) and a final targeted rerun after import-only
cleanup. The new rejection fixture verifies unchanged signed bytes/nonce, no
uncertain send webhook, receipt reconciliation and rejected NOOP preservation.
Three formal-runner parser tests also pass. After the test-only fixture change,
the [exact serial executor command](fixture-batching/README.md) passes 57/57 tests
in 4.83 seconds locally; these overlap the earlier EOA tests. The earlier Linux
run was cancelled after its fixture stopped advancing; Linux confirmation of
the correction subsequently passed at `9537ac6` ([Rust](https://github.com/alfongj-com/engine-core/actions/runs/36271173982),
[formal](https://github.com/alfongj-com/engine-core/actions/runs/36271174009)).
These CI results precede the later progress-scheduling runtime experiment.
Hosted CI and measured Engine load remain separate qualification records. Fee arithmetic
source is unchanged; the earlier direct Rust proof retains its stated source scope.

## Finality and disaster-recovery extension

The later finality/recovery implementation has a separate
[52-case protocol report](finality-recovery/report.json), with all 58 reviewed
source hashes and 3,163,401 distinct states across positive cases. The new models
add independent chain state and journal/Redis failure cuts. See the
[round's verification record](../../docs/verification.md#finality-and-independent-redis-recovery--2026-09-26)
for runtime tests and hosted evidence. The older results below qualify only their
named source commit.

## Initial formal round: Linux CI

All four workflows passed for source
`1c0bb18acd0f92ecb3fe275202f03517aef68bc5`:

| Workflow | Result |
| --- | --- |
| [Formal verification](https://github.com/alfongj-com/engine-core/actions/runs/36247988038) | Both protocol and production Rust proof jobs pass. |
| [Rust correctness](https://github.com/alfongj-com/engine-core/actions/runs/36247987990) | Workspace, Redis, HTTP, local Anvil/Solana recovery and dependency audit pass. |
| [Queue tests](https://github.com/alfongj-com/engine-core/actions/runs/36247988026) | Pass. |
| [Queue coverage](https://github.com/alfongj-com/engine-core/actions/runs/36247988027) | Pass; execution of a coverage tool is not complete invariant coverage. |

[Workflow metadata](linux-ci/workflows.json) records every step and source commit.
The [runtime results](linux-ci/runtime-results.json) preserve test summaries and
four local-chain recovery reports extracted from the successful CI log. The
dependency audit passes with five informational warnings; compiler warnings
also remain in the log.
The downloaded [protocol report](linux-ci/protocol-report.json) and
[fee summary](linux-ci/fee-summary.json) match the committed model, configuration
and mapped Rust hashes. Linux reproduces all 35 model checks, 2,463,657 positive
states, five fee harnesses and 116 checks. Raw Linux logs remain attached to the
formal workflow. Subsequent commits in this round add documentation and evidence;
they do not change the runtime, models or proof runners.

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
there is no mechanically checked refinement or composition of the separate models.
Exploratory incomplete searches and solver timeouts are not counted as passes.
Historical public-network and throughput results retain their earlier source
scope. In particular, the previous queue benchmark did not exercise pruning;
the [separate pruning benchmark](pruning/README.md) measures that added cost.
At default retention, a single-entry prune takes about 123–127 µs of Redis
service time, compared with 6–7 µs before the correctness fix. Larger histories
increase it further. This isolated measurement is not end-to-end throughput.
