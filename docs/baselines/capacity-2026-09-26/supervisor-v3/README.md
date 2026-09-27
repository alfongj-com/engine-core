# Sequential supervisor v3

Adds the approved `--http-concurrency` job option, bounded to **1–1,024**. New manifests can explicitly select **128**; the default remains 64. The state schema is named v3. No Engine or campaign source changed. [Exact diff](v2-to-v3.patch), [source](engine-capacity-sequential-v3.py), and [validation metadata](validation.json).

All **11 tests passed**: the prior ten stop/latch/timeout/native-pause tests plus a test of supervisor bounds and the actual `capacity_campaign.arguments` parser. The generated command accepts 128, retains the 100 ms scheduling deadline, 1,000 ms admission p99 threshold, 4,096 outstanding limit and 256 proxy concurrency, and rejects out-of-bounds or duplicate job options. These are control/argument tests, not throughput measurements.

[Test source](test_sequential_v3.py), [output](capacity-sequential-v3-tests.log), and [existing five-job fixture](engine-capacity-final-screen-jobs.json) are archived unchanged. That fixture is used by inherited tests; it is **not** the next campaign manifest and does not request 128. Tests retain their original local import/fixture paths in the source.

Create a fresh v3 state from the next explicit manifest and current Engine digest. Do not modify or reuse a v2 frozen state: v2 source/state/results remain unchanged. The supervisor continues to stop globally on unresolved or unsafe work; the narrow safe missing-offer continuation does not promote a capacity result.
