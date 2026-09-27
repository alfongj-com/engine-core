# Round 2: offered-load assessment

All admitted transactions reconciled safely. None of these offered rates qualifies
as steady capacity. Completion during the later drain is a separate result.

These read-only assessments preserve the full report's SHA-256 and analyzer SHA-256
`b21a97c6580fe0c7ee48804732b4840c58164ac037d97db559819e246cfef3dd`.
The analyzer is [`scripts/capacity_assess.py`](../../../scripts/capacity_assess.py).
Each JSON was copied byte-for-byte from the original analysis; its report hash
matches the decompressed adjacent `capacity-*.full.json.gz` evidence. The original
`/tmp` paths are provenance, not required locations for reproducing the analysis.

| Profile / offered TPS | Post-warmup admission / attempt / inclusion / terminal TPS | Unsigned backlog growth | Terminal backlog growth | Intake and completion |
| --- | --- | ---: | ---: | --- |
| [EVM 12s / 100](round2-evm12-100-candidate-assessment.json) | 99.94 / 42.67 / 42.67 / 54.66 | +6,872 | +5,434 | 17,999 admitted; one scheduled slot dropped; admitted work settled |
| [OP execution / 100](round2-op-100-candidate-assessment.json) | 100.00 / 42.02 / 40.54 / 40.54 | +6,957 | +7,135 | All 18,000 admitted and settled by 310.99s |
| [Native Nitro dev / 100](round2-nitro-100-candidate-assessment.json) | 100.01 / 34.14 / 34.14 / 34.61 | +7,904 | +7,847 | All 18,000 admitted and settled by 401.15s |
| [Solana / 75, poll 2s](round2-solana-75-candidate-poll2-assessment.json) | 74.59 / 71.59 / 63.77 / 71.80 | +361 | +335 | 13,478 admitted; 22 client-capacity drops; admitted work settled by 205.65s |

Rates and growth use approximately 60–180s of offered load, not the drain. Every
run offered work for 180s, so only about 120s remained after warmup: shorter than
the strict 180s confirmation requirement. No proxy transport errors, proxy
overloads or RPC method errors occurred in these four runs.

## Interpretation

- EVM12 has only two complete 60s aggregation groups. Its conservative automatic
  label includes cadence ambiguity, but thousands of additional unsigned intents
  and a roughly 47 TPS increase in mean unsigned backlog demonstrate overload;
  twelve-second block batching cannot explain it away.
- OP execution and native Nitro show increasing unsigned and terminal backlog in
  all four 30s groups. These are clear overload screens, not close endpoint calls.
- Solana poll2 shows a later onset: mean unsigned backlog progresses
  2.7 → 4.9 → 54.3 → 181.9. Inclusion observation also lags, so its lower reported
  inclusion rate alone cannot diagnose execution capacity. Durable attempts,
  terminal backlog and the 22 client drops independently disqualify this run.
- The EVM12 run used binary `c0014d2a…`; the other three used `a3d360eb…` after the
  bounded exact-wire gap replay fix. They are not one identical-binary combined
  qualification. Full hashes and profile labels remain in the source reports.

The next probes start EVM12/OP/Nitro at 50 TPS and Solana at 60 TPS with its default
one-second confirmation poll. Reduced intake also reduces shared journal work:
observing 34–42 completions per second under 100 TPS intake does **not** establish
a 34–42 TPS steady ceiling. This is a reason to measure the next rate, not a proven
storage-performance model. The two-second Solana poll remains a separate experiment.

Longer trials preserve the automatic strict assessment. Bounded block-phase
jitter may warrant a separately documented repeat and manual interpretation;
it must not silently turn positive backlog growth into a pass. Native Nitro's
zero pending-nonce delta is explicitly a weak pool observation, while exact
per-ID canonical effects and terminal outcomes provide the execution evidence.
These local profiles do not establish public-chain or indefinite maximum capacity.
