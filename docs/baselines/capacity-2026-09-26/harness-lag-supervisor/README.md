# Harness timing and bounded campaign supervision

These are offline and loopback fixture checks, not throughput measurements. Exact commands, counts and source digests are in [validation.json](validation.json).

## Validation

- **84/84 Python tests passed; zero skips**, with the pinned Cast binary. This includes the capacity oracle, scheduler, scenarios, pool accounting, strict assessment, reorg oracle and public-write guards.
- **10/10 supervisor tests passed**, covering global stop across subsets/resume, missing-offer versus unresolved accepted work, private no-overwrite state, timeout cleanup, native pause on failure and explicit per-job settings.
- **3/3 Nitro hook tests passed**, using a fake Cast process only: duration bounds, bounded tick cadence and reaping a blocked subprocess after an owner signal. No Nitro RPC was called by these tests.

The first full suite exposed a timing-dependent test assumption: rejecting a browser request before consuming its HTTP body can produce either EOF or a TCP reset. The isolated unchanged guard suite passed on retry. The corrected test accepts those two closure forms and additionally checks the exact Origin/Host/path rejection reasons, zero forwarded calls and zero billed RPC calls. Both the initial failure and final passing logs are retained. Production guard behavior was unchanged.

## Timing contract

The campaign still defaults to a 25 ms offer tolerance. The next explicit job list uses **100 ms**, with absolute deadlines, bounded client concurrency and no failed-POST retries. Eligible late offers can arrive in a bounded microburst; there is no unbounded catch-up queue. Reports distinguish scheduled-to-HTTP-call-start lateness from scheduled-to-response latency and exclude duplicate-ID retry samples from the original-offer distribution. HTTP-call start is not a measurement of the first socket byte.

The new broadcast setting is explicit in the child environment and report, overriding ambient configuration. Defaults remain 32; the separate jobs compare 32 and 64. This changes the experiment configuration and must not relabel earlier runs.

## Supervisor and drain hook

The archived supervisor is a historical copy of `/tmp/engine-capacity-sequential-v2.py`; the live external script is the executable entry point. It requires an explicit Engine binary digest when preparing a new frozen manifest, hashes all executable experiment inputs, refuses existing evidence paths, and runs one job per invocation unless a finite larger count is requested. It does not choose a capacity maximum or change the strict assessment thresholds.

An active trial, persistent review latch or historical unsafe/unresolved result blocks every later job, including a different subset. There is no automatic latch-clearing option. A dropped offer may permit a subsequent independent trial only when every actual admission is independently reconciled, every numeric drain counter is zero and no safety, execution or cleanup error exists. Such a run remains ineligible for a clean offered-rate claim.

The whole-child limit is offered duration + drain allowance + 120 seconds for setup + 600 seconds for verification (2,880 seconds for the supplied jobs). Timeout or owner termination first requests harness cleanup, then terminates its owned process group, persists the review stop and pauses caller-owned Nitro. It never resets or restarts Nitro.

Hook v2 accepts `--max-seconds` from 1 through 1,800 (default 600), initiates at most one tick per second and uses the same fixed loopback chain and **public synthetic Nitro development wallet** as v1. The embedded fixture key is not a user credential. Cast now inherits the campaign process group so forced supervisor cleanup covers it; ordinary hook signals also terminate and reap the known direct Cast child. Hook tests use shared wall-clock observations of separate fake subprocesses because macOS Python 3.9 monotonic epochs are process-local.

Example preparation only, after the operator selects the exact final binary:

```sh
python3 /tmp/engine-capacity-sequential-v2.py \
  --state /tmp/engine-capacity-final-screen-state.json \
  --jobs /tmp/engine-capacity-final-screen-jobs.json \
  --expect-engine-sha <exact-binary-sha256> --prepare
```

Execution is a separate explicit invocation with `--resume ... --execute`; default one job allows review between measurements. The supplied five-job list is an experiment plan, not evidence that any listed rate is sustainable.
