# Supplemental finite-window interpretation (v1)

Proposed before inspecting the first 360-second result. This does not change the
strict analyzer, its thresholds, or any report. All raw classifications remain.

## Eligibility

Every offered ID must be admitted exactly once and settle with exact effects;
zero missed/client-capacity slots, unexpected RPC/HTTP errors, unsafe outcomes or
retained work. Same binary, workload, workers, durability and polling setting for
repeats. An eventual drain alone is never a capacity pass. Solana inclusion
observer lag is shown separately; it cannot excuse durable unsigned growth.

## Fixed measurements

Use the configured post-warmup window, with nonoverlapping complete nominal
cycles: 60-second groups for EVM12, 30-second groups for OP, native Nitro and
Solana. For unsigned and terminal backlog report:

1. Every group mean, p10, median and p90; group minima/maxima separately.
2. Descriptive least-squares slope of group means, and first-to-last change in
   each quantile. Do not present an IID significance test over autocorrelated
   samples as a confidence bound.
3. First/last values and last-180-second min/max. Phase-align endpoint checks by
   sweeping observed start offsets across one aggregation group, pairing each
   with an endpoint an integral number of groups later. Report the full range
   and median of these paired growth values, not only a favorable phase.

Existing reports lack sampled block-head timestamps. This is nominal phase
accounting, not proof of actual identical block phase. Native/Solana have no
fixed block cadence; their groups are time windows only.

## Interpretation and repetition

- **Observable growth:** all consecutive aligned mean groups rise by more than
  one transaction, with positive mean slope and rising median/p90; or even the
  least-growth paired endpoint shows growth greater than one transaction.
  Do not explain this as endpoint phase noise. Lower the next offered rate.
- **Bounded-jitter hypothesis:** the paired-growth range straddles zero, its
  median is at most one transaction, quantile bands overlap, and group means
  are not persistently rising. A single positive endpoint or fitted slope is
  then ambiguous, not an automatic failure or pass. Repeat the same rate.
- **Unconfirmed:** incomplete telemetry, contradictory mean/quantile/phase
  evidence, or positive shifts not explained by paired phase. Repeat once;
  do not declare a ceiling from this class.

Two eligible independent 360-second runs are the initial repeat requirement.
If strict results still reject endpoints but the bounded-jitter hypothesis
survives both, run one 900-second confirmation with the same 120-second warmup.
That gives at least13 EVM12 mean groups. Require clean offered/accepted/settled
counts, no observable-growth rule, and the same bounded-jitter evidence. A
positive mean/median/p90 shift that persists across repeats or the long run
remains unresolved growth, regardless of apparent final drain. Do not keep
rerunning until a favorable random endpoint appears.

Only rates with repeated finite-window evidence are candidates for the table
label **highest repeatedly sustained tested rate**. Show the tested duration,
raw achieved rates, backlog range and slope, strict verdict and any explicit
bounded-phase interpretation alongside it. If the long result remains
ambiguous, use **highest cleanly completed tested rate; stability unconfirmed**
instead. The next higher tested overload brackets the experiment; it does not
prove an absolute ceiling. No positive percentage-of-intake allowance is used,
and finite observation cannot exclude arbitrarily slow long-term growth.
