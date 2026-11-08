# Code review — November 8, 2026

## Changes

Seven PRs are pushed on the open [PR #4](https://github.com/alfongj-com/engine-core/pull/4).
Merge order: **#4 → #5 → #6 → #7 → #8 → #9 → #10 → #11**. No PR was merged during this review.

| PR | Finding and fix |
| --- | --- |
| [#5](https://github.com/alfongj-com/engine-core/pull/5) | Missing recipients did not become CREATE envelopes. Set CREATE/CALL explicitly, preserve legacy gas estimation and reject gas arithmetic overflow. |
| [#6](https://github.com/alfongj-com/engine-core/pull/6) | Cancellation raced durable broadcast reservation. Cancel unsigned intents in the existing journal transaction; reuse one payload snapshot and typed errors. |
| [#7](https://github.com/alfongj-com/engine-core/pull/7) | Signing/ABI work fanned out without a per-request bound. Limit batches to 100 items and preparation to 16 polled operations, preserving order. This is no global request limit. |
| [#8](https://github.com/alfongj-com/engine-core/pull/8) | Compatible dependency updates remove 11 of 12 vulnerabilities and 11 unsoundness warnings. No advisory is suppressed. |
| [#9](https://github.com/alfongj-com/engine-core/pull/9) | Environment-key operations used affected RustCrypto arithmetic. Select native secp256k1; verify published wire bytes, signature boundaries and cross-binary recovery. Update local-agent instructions. |
| [#10](https://github.com/alfongj-com/engine-core/pull/10) | A dead Solana processing lock could outlive a signed transaction after job reassignment. Reclaim only after Redis confirms the recorded queue owner lost its lease; fence stale attempt access and pre-RPC checks. |
| [#11](https://github.com/alfongj-com/engine-core/pull/11) | A resumed public run could retain old provenance while using changed harness code. Reject missing/drifting provenance before provider calls or manifest updates; release the wallet lock on validation failure. |

Review head: **`8039b2d`**, branch **`pin-testnet-resume-provenance`**.
Runtime source remains **`4dadaf5`**; the final PR changes only public-testnet helpers, their tests and documentation.
[Frozen binary manifest](processing-binary-manifest.json); [local paths and commands](../../../TO_ALFONSO.md).
Legacy UUID processing locks retain release/expiry behavior: drain old workers
gracefully where possible. Queue payloads, job IDs and journal schema do not change.

## Verification

- Full local workspace workflow passes, including **186 explicit Redis/API tests**.
  [Commands, counts and source](processing-rust.json).
- **12 process scenarios pass**: Engine/Redis crashes, live Redis outage, flush/rollback
  quarantine, reverted calls, successful/reverted reorgs and both Solana recovery
  variants. The old/new binary scenario verifies four contracts with original
  transaction hashes, exact code/value and no duplicates. [Results](processing-process.json).
- All **41 offline public-testnet harness tests** pass on `8039b2d`; changes to every provenance field, missing records, actual CLI refusal, saved-state preservation and wallet-lock release are checked. [Evidence](resume-provenance-tests.json).
- Native signing matches the independent [EIP-155 fixture](https://eips.ethereum.org/EIPS/eip-155).
  Checks cover wrong-chain refusal, configured chain identity, ordinary signatures and
  curve-order digest boundaries. Native and k256 signatures may differ for prehashes
  at/above the order; both must recover the correct signer. The initial universal-byte-equality
  test assumption was corrected. [Signature evidence](native-compatibility.json).
- Final CI passes **107 bounded cases / 95 source guards**, five production fee
  harnesses and two mutation counterexamples. Cases: **24 positive, 52 faults,
  12 assumption boundaries and 19 reachability witnesses**. These do not establish
  refinement, composition, cryptographic timing or whole-service correctness.
  The earlier [local model report](formal-local.json) retains its own source scope.

Original arithmetic and cancellation regressions fail before their fixes.
[Reproduction hashes](reproductions.json). Real-Redis lock tests cover dead-owner
takeover before TTL, stale mutation/release, competing claimants, live-owner protection,
mismatched keys and legacy refusal. Model preparation failures never count as passing
counterexamples.

## CI

All functional gates and formal workflows pass. **Overall Rust CI remains red on
dependency advisories.** Summaries verify exact heads, artifact digests, model/config
hashes, guarded sources and proof outcomes. PRs #5–#7 retain old lockfile findings;
#8–#11 retain one vulnerability and six warnings.

| PR | Head | Cases / guards | Functional and formal | Overall Rust |
| --- | --- | --- | --- | --- |
| [#5](ci-pr5.json) | `3faf3b0` | 100 / 92 | Pass | Failed: dependency audit |
| [#6](ci-pr6.json) | `66cc7c3` | 103 / 93 | Pass | Failed: dependency audit |
| [#7](ci-pr7.json) | `ad7e760` | 103 / 93 | Pass | Failed: dependency audit |
| [#8](ci-pr8.json) | `883d7d2` | 103 / 93 | Pass | Failed: dependency audit |
| [#9](ci-pr9.json) | `1e62dd7` | 103 / 94 | Pass | Failed: dependency audit |
| [#10](ci-pr10.json) | `4dadaf5` | 107 / 95 | Pass | Failed: dependency audit |
| [#11](ci-pr11.json) | `8039b2d` | 107 / 95 | Pass | Failed: dependency audit |

## Load comparison

Ten sequential five-minute runs each offer **9,600 transfers**: two local EVM chains
at 8 TPS each and Solana at 16 TPS, one signer per chain on an Apple M4 host.
All **96,000** offers were dispatched, accepted and drained; functional safety and
liveness checks pass. Local Rust builds, automated test suites and model checkers ran outside
timed load; OS/background activity was uncontrolled. SQLite FULL/fullfsync and Redis AOF/everysec remain enabled.

Admission p99, milliseconds:

| Run | EVM 0.25 s | EVM 2 s | Solana | Mean SQL commit ms | Supervisor exit |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline-steady | 84 | 62 | 607 | 3.29 | 0 |
| candidate-steady | 93 | 77 | 989 | 3.51 | 2 |
| candidate-repeat | 54 | 58 | 629 | 3.40 | 0 |
| baseline-repeat | 67 | 58 | 657 | 3.47 | 0 |
| native-steady | 125 | 102 | 1034 | 4.01 | 2 |
| baseline-control | 104 | 78 | 783 | 3.62 | 0 |
| native-repeat | 54 | 66 | 749 | 3.46 | 0 |
| processing-steady | 87 | 92 | 988 | 3.78 | 0 |
| baseline-final-control | 72 | 66 | 659 | 3.44 | 0 |
| processing-repeat | 51 | 60 | 661 | 3.46 | 0 |

Baseline: PR #4 `4e262c8`; candidate: `883d7d2`; native: `5ae3601`; processing: `4dadaf5`. A new baseline control and final-runtime repeat follow the first processing run.
Each frozen executable has separate provenance. First candidate/native runs had worse
tails, repeats improved and the unchanged baseline also varied. **Latency equivalence,
regression freedom across workloads and sustainable maximum throughput are unproven.**
All outliers remain included.
The last adjacent pair measured Solana admission p99 **659 ms baseline / 661 ms final**,
with mean SQL commits **3.438 / 3.463 ms**. The first final-runtime run measured
988 ms and remains included. This small sample does not establish latency equivalence.

Exit 2 preserves the failed capacity screen despite passing functional checks; exit 0
is no sustainable-capacity certificate. Stricter posthoc classifications and reasons
are retained. Attempt/terminal percentiles are sampled upper bounds including drain.
[All ten runs](load-review-final.json), [first seven](load-repeats.json), [first final-runtime run](processing-steady.sanitized.json),
[final strict screen](processing-repeat.strict.json), [original comparison](load-comparison.json).

SQL commit means are several milliseconds versus roughly 0.08 ms for Redis mirrors,
with about six journal commits per admission. Phase timings overlap and cannot be
added as wall-time utilization. Durable storage/serialization remains a scaling
constraint. There is **no 50 TPS-per-chain claim** and no reason here to weaken durability.

## Crash finding and rerun

Both runs offer **2,880 transfers at 8 + 8 + 16 TPS over 90 seconds**, kill Engine
after 200 accepted Solana sends and restart with the same identities and 10-second
queue lease.

The original run is **incomplete**. Logs show three jobs blocked by a dead worker's
120-second processing lock, followed by an expired-blockhash unknown outcome. The
run later stopped at its projected disk-reserve guard. Reconciliation did not finish:
**safety is null and liveness is false**. Zero recorded safety failures is not a pass.
Its fixture, journal and validator ledger remain intact. [Original](native-crash.sanitized.json).

The fixed binary passes the same workload/fault, independent safety and liveness
checks and final drain. [Rerun](processing-crash.sanitized.json). It uses fresh state;
the old unknown outcome was not re-signed or discarded. Its supervisor exits 2 because
the capacity screen remains unconfirmed; the fault, safety and drain checks pass. Long leases, outages or
slow restarts can still exceed blockhash validity. This fix gives no recovery deadline
and never authorizes re-signing unknown work.

## Remaining architecture and security limits

The review followed admission, signing, nonce/attempt binding, queue completion,
cancellation, finality, Redis recovery, Solana polling, webhook destinations and API
work expansion. Other defects may remain.

The independent journal is needed to detect Redis rollback. Cancellation and lock
recovery use existing authorities without another store or journal schema. The service
still needs **the latest independent journal and one active process on one Unix host**,
serializes durable writes, retains intent history indefinitely and does not reconstruct
missing queue jobs. Retention/compaction and job reconstruction need a recovery design.
Production KMS, public providers and real rollup settlement remain unqualified.

RustSec database `309392b`: vulnerabilities **12 → 1**, unsoundness warnings **12 → 1**,
five unmaintained warnings remain. [Audit](dependency-audit.json), [native audit](native-dependency-audit.json).
The [crypto-bigint timing advisory](https://rustsec.org/advisories/RUSTSEC-2026-0387.html)
remains through Alloy/k256. Native signing mitigates environment-key operations;
the development credential still constructs k256 keys. The package is not declared
patched or unused. A reviewed upstream migration/backport remains required. The LRU
warning requires a panicking key destructor; inspected provider keys are integers/B256,
limiting demonstrated exposure without patching it.
[Dependency analysis](https://github.com/alfongj-com/engine-core/blob/4dadaf5995ad22468fd32e37074a7f5f51602f1f/docs/design/dependency-security.md).
The Rust `local_signer()` return type changes to `Secp256k1Signer`; HTTP/queue
serialization and signer identity remain stable. Rust callers using a `BorrowedJob`
struct literal must use its constructor after the private queue-identity field was added.

## Evidence handling

These are allowlisted aggregates. Raw keys, wallet addresses, signed wires, journals
and provider URLs stay private. **$0 public RPC credits spent.** Owned fixture services
are stopped. To recover disk reserve, completed, verified steady-run validator ledgers
were compressed and checked against every original file's SHA-256 before removal of
the original directory. Archives/restore manifests remain in private `fixture-archives/`.
No failed fixture or incident state was removed. Old incident journals and the retained
Nitro environment are untouched. [October public-chain results](../testnet-cleanup-2026-10-13/README.md)
qualify only their recorded binaries.
