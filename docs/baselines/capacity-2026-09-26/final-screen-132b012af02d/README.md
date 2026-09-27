# Closed five-run screen: 132b012af02d

**No rate qualified in this screen.** All 100,798 admitted intents reconciled to
one exact finalized effect each. Solana missed two of 100,800 total scheduled
intents at the client's concurrency limit, so its offered-load qualification is
incomplete. Eventual drain is separate from sustained capacity.

All runs used Engine `bb34deee…77ce`, source `dd9282a`, 360 seconds of arrivals,
120 seconds of warmup, HTTP concurrency 64 and a 100ms scheduling deadline.
[The index](index.json) records complete binary/source/harness hashes, original
file hashes, archive hashes, configuration, and links to each artifact. Full
reports, per-ID evidence, complete proxy audits, original strict assessments,
and descriptive backlog summaries are retained; gzip round-trips were checked
against every original byte. Prior screens remain unchanged.

| Local profile | Broadcast setting | Offered / admitted | Post-warmup terminal TPS | Drain reached | Strict result |
|---|---:|---:|---:|---:|---|
| Nitro dev, 50 TPS | 32 | 18,000 / 18,000 | 38.392 | 455.30s | Persistent backlog growth |
| Nitro dev, 50 TPS | 64 | 18,000 / 18,000 | 27.455 | 510.42s | Persistent backlog growth |
| Anvil, 12s blocks, 60 TPS | 64 | 21,600 / 21,600 | 58.019 | 410.38s | Persistent backlog growth |
| Anvil OP execution, 2s blocks, 60 TPS | 64 | 21,600 / 21,600 | 57.632 | 380.20s | Persistent backlog growth |
| Agave, 60 TPS, 5s polling | Irrelevant | 21,600 / 21,598 | 59.881 | 385.45s | Unconfirmed growth; intake incomplete |

Times start at the offered phase. Nitro is native dev L2 execution with depth-2
confirmation and no L1 settlement. Anvil profiles simulate cadence; the OP
profile does not qualify native sequencing/derivation or L1 fee behavior. Agave
is local, not public-cluster capacity. These ordered finite runs establish no
absolute maximum or causal index/concurrency/disk effect. See the
[paired Native analysis](native-broadcast-comparison.md).

## Solana: distinguish three observations

- **Client drops:** missing indices 20312 and 20313 were scheduled at 338.533s
  and 338.550s. These are nominal times, not measured drop timestamps: the drop
  counter changed between samples at 335.009s and 340.014s. All 64 client slots
  were occupied; there were zero schedule-lag drops and maximum HTTP start lag
  was 40.754ms. No accepted intent was lost.
- **Backlog:** terminal backlog was approximately stable, 1,180→1,171 after
  warmup. Unsigned backlog rose 1→95; the final 30s mean was 59.83 versus means
  below 11 in the preceding groups. This late disturbance, plus two client
  drops, prevents promoting the run. It does not establish a sustained Engine
  limit at 60 TPS. A repeat with HTTP concurrency 128 must retain the scheduling
  and latency gates; the old result remains incomplete.
- **Observer lag:** the live Solana status cursor can follow newly appended
  signatures without revisiting older ones while arrivals continue. Its
  `finalized` counter stayed zero throughout arrivals despite durable finalized
  terminal results; at 360s, observed inclusion (19,282) lagged terminal
  completion (20,392). The reported 52.927 observed-inclusion TPS is therefore
  not chain execution throughput. After drain, independent per-ID reconciliation
  checked all 21,598 admitted signatures and exact effects.

[Drop and observer evidence](solana-drop-and-observer-analysis.json) includes
exact counts and limitations. Window analysis files use cumulative RPC/CPU
counter differences over actual sample times. CPU percentages use one core as
100%; low values do not identify a storage bottleneck. Descriptive summaries
supplement, and never override, the preserved strict verdicts.
