# Native Nitro dispatcher comparison

Both runs offered 50 TPS for 360 seconds, with one signer, send concurrency 32, SQLite FULL and Redis AOF every second. Both reconciled all 18,000 transactions exactly and drained all work. **Neither is a confirmed sustained maximum.**

| Metric | Old 300db | Updated 35e |
| --- | ---: | ---: |
| Late attempted TPS | 42.667 | 49.848 |
| Late completed TPS | 43.839 | 48.837 |
| Late unsigned backlog | 63→1,382 | 5→32 |
| Drain complete, seconds from load start | 395.271 | 365.083 |
| Late send-RPC mean, ms | 243.741 | 163.269 |
| Late Engine RPC/s | 186.244 | 222.144 |

The updated implementation performed better in this ordered pair. The old run precedes the new run on the same retained native database, so storage history and timing remain confounders. This is not a randomized causal experiment. The new result's block-aligned backlog comparisons straddle zero and need repetition. Native Nitro includes gas estimation; this development node has no L1 settlement, batch poster or public RPC quota. Completion uses the explicitly configured two-block policy.

Per-run full reports, sanitized per-ID observations, independent journal audits, digest-only RPC audits, resource logs and unchanged strict assessments are retained with hashes. Original private journals, raw signed payloads, AOF and node data remain outside Git. Frozen hashes in the state files identify actual tested bytes. The new cohort state also records the subsequent failed EVM55 guard incident; that separate failure does not invalidate the preceding closed Native audit.
