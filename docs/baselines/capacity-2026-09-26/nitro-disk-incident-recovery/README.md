# Native Nitro disk incident: original-intent recovery

**Recovery passed; this is not a capacity result.** The frozen Engine completed the original 14,400 intents after the coordinator restored the same Nitro container. The independent offline review found one canonical successful execution per ID, exact nonce/effect/fee accounting, no new IDs or replacement signatures, and zero remaining queue counters. The failed load report and supervisor review latch remain unchanged.

## Incident and custody

The 40 TPS screen lost its native node about 120 seconds into its 360-second offer window. The coordinator reported exit code 2 and a full 11 GiB VM root filesystem. The final failed report has no per-ID oracle or drain result. Admissions continued to 14,400 while the node was unavailable.

At backup: 4,502 durable success records, 302 signed unresolved intents, and 9,596 unsigned intents. All 4,804 signed attempts matched the proxy's accepted hashes and wire digests. Signed bytes, SQLite/WAL, Redis AOF, configuration and original reports remain in an immutable private custody backup; they are not published here. See [incident summary](incident-summary.json), [failed report](original-failure-report.json.gz) and [failure log](original-failure.log).

## Recovery controls and result

The recovery used mutable copies of the original SQLite/AOF state, immutable frozen harness imports/configuration and Engine binary `300db978…858b23`. It created no HTTP admissions and made no manual Engine-wallet broadcasts. Before workers started, it rechecked all 4,502 prior terminal receipts, 297 canonical blocks, the original genesis and retained checkpoint 4083; cloned Redis passed exact checkpoint reattachment. A proxy permitted only original signed wires or one first signature for an original unsigned ID. The bounded ticker used the separate existing development account to advance block depth; the runner did not restart or reconfigure the native node.

| Check | Result |
| --- | ---: |
| Original IDs / canonical successful effects | 14,400 / 14,400 |
| Original signed-attempt rows preserved exactly | 4,804 |
| Original terminal proofs preserved exactly | 4,502 |
| First signatures for original unsigned IDs | 9,596 |
| Recovery sends / accepted unique wires | 9,596 / 9,596 |
| Replacement identities / new IDs / guard rejections | 0 / 0 / 0 |
| Nonces | 89,999–104,398, unique and contiguous |
| Recipient increase | 14,400 wei |
| Receipt fees | 1,366,447,200,000,000,000 wei |
| Sender debit | 1,366,447,200,000,014,400 wei |
| Remaining drain counters | All nine zero |
| Recovery drain duration | 181.202 seconds |

The independent review checked every [per-ID observation](recovery-observations.jsonl.gz) against original and completed private journals: immutable payload/fingerprint, exact old wire/proof rows, wire digest, observed intent digest, hash/outcome/block, nonce, effect and aggregate balance/fee conservation. The three owned PIDs were absent. See [review result](independent-audit.json), [review source](independent-audit.py), [full recovery report](recovery-report.json.gz), [RPC audit](recovery-rpc-audit.jsonl.gz) and [runner](recovery-runner.py). The coordinator paused the same native container after cleanup; this review made no node/RPC calls.

Preparation attempts 1 and 2 stopped before Engine execution because the cloned WAL-mode database needed writable sidecar initialization and its existing private owner-lock file had not been copied. These were recovery-runner setup issues; no original custody state changed. [Attempt 1 log](preparation-1.log), [attempt 2 report](preparation-2.json.gz) and [successful preparation 3](preparation-3.json.gz) are retained.

## Qualification limits

The generic oracle field `eligible_for_rate_assessment: true` only reflects reconciled ledger effects. This continuation had **no new offered workload or steady-state rate windows**; top-level `all_chain_capacity_candidate` is false. Its drain time must not be promoted to a throughput ceiling or a successful 40 TPS screen.

Evidence assumes the restored local RPC truthfully served the retained chain. The offline review checks captured observations, not a fresh chain query after pause. Native Nitro used explicit depth-2 finality and no L1 settlement qualification. The original capacity campaign remains stopped for separate operator review.

[Manifest](MANIFEST.json) records exact source and archived hashes, including the original uncompressed report hash `2d43b862…7b549e`. [Frozen runtime manifest](frozen-runtime-manifest.json) identifies the independently copied imports, configuration and binary. Private signed transaction and credential material is excluded.
