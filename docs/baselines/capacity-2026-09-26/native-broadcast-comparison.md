# Native Nitro: broadcast 32 versus 64

**64 showed no capacity improvement in this ordered pair. Neither run sustained
50 TPS.** Both admitted and independently reconciled all 18,000 intents with
zero RPC/proxy errors and zero final drain counters. Both had substantial backlog
growth during arrivals. These are Nitro dev L2-only results, not public Arbitrum
or L1-settlement capacity.

The runs use the same `bb34deee…77ce` binary, `dd9282a` source commit and identical
harness hashes. Queue configuration differs only in broadcast concurrency and
the isolated namespace. Each offered 50 TPS for 360s; preparation remains 32.
[Exact data, source hashes and method](native-broadcast-comparison-data.json).

| Measurement | Broadcast 32 | Broadcast 64 |
|---|---:|---:|
| Clean offered/admitted/settled intents | 18,000 | 18,000 |
| HTTP start lag, maximum | 26.932ms | 17.509ms |
| Post-warmup terminal rate, approximately 120–360s | 38.392 TPS | 27.455 TPS |
| Last-180s terminal rate | 34.116 TPS | 27.940 TPS |
| Last-180s unsigned backlog | 449 → 3,432 | 2,463 → 6,621 |
| Drain target reached after offered start | 455.30s | 510.42s |
| Full-run send RPC mean / maximum | 243.328 / 633.610ms | 241.357 / 656.654ms |
| Observed peak forwarded-but-unaccepted sends | 32 | 64 |
| Last-180s mean outstanding sends | 8.212 | 6.286 |
| Last-180s time without an outstanding send | 72.49% | 88.58% |
| Last-180s Engine RPC rate | 146.538/s | 112.977/s |

The complete audits each contain 36,001 events and pair every one of 18,000
unique forwards with its accepted identity; none are unmatched. Thus the higher
setting reached the proxy. Send latency did not materially rise. Instead,
post-warmup intervals without outstanding sends lasting at least 0.5s grew from
4.826s to 7.835s on average; their descriptive p95 grew from 7.104s to 10.623s.
Other work occurs during these intervals. They are not proof of an idle Engine,
a disk bottleneck, or a particular slow journal operation.

## Earlier 32-run context

The earlier `a3d360e` run is **not** a matched control: runtime, wall-clock time,
native history and harness intake reporting differ. One of its 18,000 scheduled
slots was missed; all 17,999 admitted intents reconciled. Its mean send RTT was
242.520ms, versus 243.328ms for the new 32 run. Estimate/fee-history means were
4.296/4.484ms versus 4.309/4.445ms; receipt/block-read means were 6.068/1.470ms
versus 6.306/1.556ms. Both opened 34 proxy upstream connections without failures.
The new run's lower CPU-time deltas accompany less completed work; they do not
establish storage or index causality. Native VM CPU is outside those counters.

Keep the default at 32. A same-harness old-binary control and reversed/repeated
settings are more informative than asserting a gain or immediately testing 128.
Finite, ordered trials cannot establish an absolute ceiling. Raw reports and
audits are identified by path/hash in the data file; large archival compression
is deferred until the agreed quiet window.
