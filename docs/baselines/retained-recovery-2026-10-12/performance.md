# Matched local performance comparison

**All four local runs pass safety, liveness and final drain. The initial fast-EVM latency increase did not repeat, but all 12 strict rate/backlog screens remain unconfirmed.**

The steady-run sequence is baseline → candidate → candidate → baseline, with a separate 90-second candidate crash/recovery campaign between the candidate runs. Each steady run offers 8/8/16 TPS for 300 seconds, with 60 seconds of warmup, on the same local Anvil/Agave fixtures. Baseline executable: `319035e` (the same source tree as merged `918d7d9`); candidate executable: `04d15d8`. The two candidate runs use identical binary bytes. Checkout and executable provenance remain separate in [comparison.json](comparison.json).

Each of the 4 completed runs accepted and drained all 9,600 intents, with safety and liveness passing and no reported execution errors. This describes completed work; it does not qualify sustained capacity.

## Latency

Cells show admission p99 in milliseconds / terminal p99 in seconds. Terminal values are sampled upper bounds over the whole completed campaign, including drain.

| Fixture | Baseline 1 | Candidate 1 | Candidate 2 | Baseline 2 |
| --- | --- | --- | --- | --- |
| Fast EVM | 61.038 / 11.383 | 87.820 / 18.006 | 51.413 / 11.008 | 60.604 / 14.134 |
| Slow EVM | 63.329 / 15.719 | 68.882 / 16.471 | 58.761 / 15.720 | 58.186 / 16.461 |
| Solana | 676.286 / 23.901 | 680.810 / 23.788 | 633.381 / 23.591 | 595.855 / 23.658 |

The first candidate increased fast-EVM admission p99 by 44% and terminal p99 by 58%. HTTP service p99 also increased, so client scheduling alone does not explain it. With the same candidate binary, the repeat fell to 51.413 ms admission and 11.008 s terminal, both below the first baseline. The initial increase therefore did not repeat; these observations do not establish its cause or prove absence of regression. Against the reversed baseline, candidate 2 fast-EVM admission/terminal p99 is 15%/22% lower, while Solana admission p99 is 6% higher. There is no consistent improvement across the measured tails.

## Full post-warmup rates

Rates use the approximately 240-second post-warmup load interval; drain is excluded. All completed run/chain results remain `unconfirmed_growth_or_cadence_ambiguity` under the unchanged strict screen. No persistent-growth stage is detected. Reasons and backlog endpoints are retained in the JSON.

| Run | Fixture | Admitted TPS | Attempted TPS | Included TPS | Terminal TPS |
| --- | --- | ---: | ---: | ---: | ---: |
| Baseline 1 | Fast EVM | 8.000 | 8.004 | 7.996 | 8.034 |
| Baseline 1 | Slow EVM | 8.000 | 7.988 | 7.996 | 8.017 |
| Baseline 1 | Solana | 16.001 | 15.934 | 15.934 | 15.901 |
| Candidate 1 | Fast EVM | 7.996 | 8.005 | 8.005 | 8.034 |
| Candidate 1 | Slow EVM | 8.000 | 7.971 | 7.967 | 8.134 |
| Candidate 1 | Solana | 15.984 | 15.926 | 15.930 | 15.967 |
| Candidate 2 | Fast EVM | 7.996 | 8.005 | 8.005 | 7.934 |
| Candidate 2 | Slow EVM | 8.000 | 8.009 | 8.025 | 7.934 |
| Candidate 2 | Solana | 16.001 | 15.997 | 15.959 | 16.113 |
| Baseline 2 | Fast EVM | 8.000 | 8.017 | 8.005 | 7.934 |
| Baseline 2 | Slow EVM | 8.000 | 8.005 | 7.992 | 8.059 |
| Baseline 2 | Solana | 16.001 | 16.001 | 15.980 | 15.955 |

The first fast-EVM late-window terminal rate changed from 8.334 to 7.875 TPS, while its full post-warmup terminal rate stayed 8.034 TPS in both runs. Different backlog endpoints explain why the late-window comparison alone exaggerates a throughput change. The repeat’s lower latency also does not imply a higher terminal rate; completion timing and endpoint backlog must remain visible.

## Journal time

Seconds accumulated in each approximately 240-second timing window:

| Measure | Baseline 1 | Candidate 1 | Candidate 2 | Baseline 2 |
| --- | ---: | ---: | ---: | ---: |
| Serialization held | 206.066 | 207.546 | 203.154 | 210.782 |
| SQL commits | 161.073 | 163.399 | 159.252 | 166.903 |
| Acknowledgement | 82.929 | 83.171 | 82.095 | 84.909 |

Acknowledgement includes commits already counted in SQL commit time; totals overlap. The first candidate increased serialization-held time by 0.72%, while the repeat fell below the first baseline. Cumulative waiter time is not wall-clock utilization.

## Limits

The strict screen is unchanged and all runs remain unconfirmed. There are only two executions per binary, without randomized repetitions or statistical equivalence bounds. Nominal block-cadence grouping lacks per-sample block-head timestamps, and sampled durable latencies include observation delay. These local fixtures do not qualify production RPC providers, OP/Arbitrum sequencing or settlement, public Solana throughput, or an absolute maximum.
