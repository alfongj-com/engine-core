# Frozen runtime validation

Release SHA-256: `c0014d2aae01a59b78f34d76d0288b9addfe49e2a1ee7d65555d7ff7373bcce4`. The original baseline binary is preserved. Exact commands, file/log hashes and separate test counts are in `runtime-validation.json`.

## Completed gates

| Gate | Result |
|---|---|
| Every workspace test and binary | Compiled |
| Core finality | 9 passed |
| Server library, including health and actual config wiring | 23 passed; 6 ignored |
| Executor Redis/recovery regressions | 60 passed |
| Executor normal library suite | 37 passed; 60 ignored |
| Release build | Passed, 41.05 seconds |

These are separate suites, not a summed unique-test count. Redis26479 was stopped after verifying ownership.

## Read-only review

No new safety blocker found. Four health-check tasks retain their permits through journal completion after HTTP timeout/cancellation; a fresh successful fence is required for200. Storage stalls can keep all four occupied, intentionally returning503. The tests inject a fence and prove cancellation bounds; they do not simulate a stuck physical SQLite device.

The Solana setting defaults to1 second and changes ordinary pending delays only. Longer intervals can delay a same-wire retry until blockhash expiry and extend the wall time of the500-check budget; they do not permit a new identity. Configuration tests deserialize the real EngineConfig from checked-in base+production sources.

The256 EOA setting caps new nonce consumption per cycle, including uncertain sends, at concurrency32. It is not a total RPC-work cap: ten preparation iterations and separate recovered/recycled work remain. Sustained load and multiple-wallet fairness need measurement.

Depth now anchors latest-minus-depth; positive-depth assessment costs two extra block reads. Old journals are retained conservatively and may delay progress until the new qualified boundary catches up. No ledger reset or automatic proof reinterpretation is authorized.

## CI integration sequence

1. Keep the pinned baseline and this release digest for the paired campaigns. Complete new-binary reorg and selected sustained comparisons first.
2. The final full draft is `/tmp/engine-capacity-review/ci-hardening.yaml`; its shared smoke rejects nonzero per-method RPC errors as well as transport/proxy failures. `git apply --check /tmp/engine-capacity-review/ci.patch` passes.
3. Parent applies the isolated CI patch after harness/runtime source freeze. Extract Cast from the existing checksum-pinned Foundry archive; run all three capacity unittest modules.
4. Run the short18-intent mixed shared smoke only after Anvil, Agave and the debug Engine binary exist. Require exact effects, empty queues, no proxy/RPC errors, stopped children and exit2 because three seconds cannot qualify a sustainable rate. Preserve report and compressed observations.
5. Run the full Linux workflow on the final commit and inspect its40-minute job budget. The draft has not run on Linux yet; no workflow was changed in this review.

The shared smoke covers local Anvil profiles and Solana, not native Nitro or performance. Formal results and actual process/load reports are separate evidence; this record does not claim their completion.
