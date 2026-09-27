# EOA terminal preflight: paired release probe

September 27, 2026 UTC. **The extra preflight cost is small in this probe; these
results do not justify changing the production terminal API in this campaign.**

Four paired experiments compare the existing bare `record_terminal` probe with
the current EOA caller sequence: admission lookup, independent replay-key
comparison, attempt-identity validation, chain-health check and terminal commit.
Both retain every guard inside the journal. This is a test-only benchmark change.

## Result

Each run has 768 synthetic intents and concurrency 1. Values below are terminal
phase elapsed time divided by intent count; each pair is one experimental repeat.

| Pair | Run order | Bare ms/intent | EOA caller ms/intent | Added ms/intent | Three-stage rate: bare / caller |
|---|---|---:|---:|---:|---:|
| 1 | Bare, caller | 4.078 | 4.292 | +0.213 | 80.85 / 79.01 |
| 2 | Caller, bare | 4.213 | 4.428 | +0.215 | 76.79 / 77.82 |
| 3 | Bare, caller | 4.216 | 4.429 | +0.213 | 80.59 / 79.76 |
| 4 | Caller, bare | 4.243 | 4.146 | −0.097 | 79.49 / 80.44 |

The median paired increment is **0.213 ms/intent**, with a range of −0.097 to
+0.215 ms. The median terminal-time ratio is 1.051. The fourth pair reverses
direction, and admission/attempt control phases also vary. The median paired
change in total three-stage time is −0.010 ms/intent: these few runs do not
establish an overall throughput gain from removing the preflight calls.

All eight runs reconcile exactly: 768 admissions, 768 attempts, 768 terminal
records and 2,304 journal checkpoint advances per run; the journal stays healthy.
There are **6,144 synthetic intents** across the experiment, not blockchain
transactions. The descriptive per-operation quantiles are in each report; they
are not thousands of independent experimental repeats.

## Setup and evidence

- Apple M4, 10 CPUs, 16 GiB RAM, macOS 26.6.2, APFS.
- One precompiled release test executable for all runs, fresh private journal and
  Redis namespace per invocation, identical payloads, no warmups.
- Journal WAL, synchronous FULL and fullfsync; disposable Redis 7.4.2 with AOF
  everysec, snapshots disabled and no other workload using that Redis.
- The preceding capacity campaign had ended. No load campaign or compiler ran
  during the probe. Ordinary host background activity remained; load averages
  are recorded, not interpreted as a CPU or disk attribution.
- Engine's production executable stayed unchanged at
  `a3d360eba3dc470bd830ec9d3a32e0be8e11b6764f9bbf6efeb319f080eb8515` and was
  separately preserved. The release test build took 42.62s before measurement.

[Summary and paired calculations](summary.json), [metadata](metadata.json),
[build artifact](build-artifacts.json), [build log](build.log),
[runner](run_probe.py), [runner log](runner.log) and the
[applied test-only patch](benchmark-applied.patch) preserve the exact commands,
source/build hashes, host/Redis settings and results. All eight JSON reports and
test logs are alongside this file. Redis stopped cleanly after the final run.
The metadata preserves and explains one empty-string parsing correction for
Redis's disabled snapshot setting; [the original metadata](metadata-raw.json)
is retained. No server setting or measurement changed.

## Interpretation limits

The paired delta includes extra SQL/Redis health reads, key/JSON construction,
locking and async scheduling. It **does not isolate fsync cost**. The three-stage
rate measures sequential journal admission, attempt and terminal operations;
it is not Engine transaction TPS. Finality RPC/checkpoint commits, Redis queue
work, webhooks and blockchain execution are excluded from both variants.

These results cover one attempt per identity and one concurrent caller on this
host. They do not establish long-history, multi-chain or production capacity.
The four pairs do not support a statistical significance claim. Any future
terminal API consolidation would still need exact replay-key binding inside the
terminal transaction, unchanged durability and halt checks, and independent
correctness tests; this experiment implements no such change.
