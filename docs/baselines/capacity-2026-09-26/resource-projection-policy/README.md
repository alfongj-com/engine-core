# Disk projection policy

Based on `capacity_resource_guard.py` SHA256 `3982ac9a2917545dc5307ebbef9d6dfd050264268508118850664219f2969c99`. Changes are confined to the resource policy and synthetic tests; no campaign/node/lifecycle or frozen-state changes.

## Decision

Keep fixed floors, setup reservation, explicit declared rates, finite original/one-way reconciliation deadlines, and sticky stop decisions. Split observed depletion into two uses:

- **Full remaining horizon:** `factor × max(declared rate, sustained observed rate)`. The observed rate is the time-weighted average of positive interval depletion over the trailing180seconds, available only after180seconds and at least3observation intervals. Thirty-second sampling therefore needs6intervals. A partially overlapping first interval is clipped. Space freed in an interval contributes zero rather than cancelling previous consumption.
- **Observation + shutdown reserve:** `factor × max(declared rate, recent peak)` for the configured sample interval plus cleanup grace (normally60+120seconds). This catches a sufficiently fast decline on the first observed interval. The peak covers the same trailing180secondwindow.

Required free space is **floor + full-horizon reserve + emergency reserve + remaining setup allowance**. The lifetime peak remains diagnostic only. Evidence reports lifetime/recent/sustained rates, window coverage/intervals, readiness, both applied rates and emergency duration separately. A phase transition preserves observed history; nothing clears an existing stop.

## Evidence

`tests.log`:35tests pass (existing18resource/provider +8phase/campaign-boundary +9newtrace regressions). Synthetic only; no disk filling, nodes, RPC or process lifecycle. The phase fixture now asserts the narrower reconciliation deadline still reduces reserve; its old single-burst false-stop expectation is intentionally removed. Existing stop-retention scenario instead uses an actual floor breach.

`old-policy-negative.log`: the new one-minute burst regression fails against the unmodified3982policy at the first60second sample, demonstrating the original full-horizon peak extrapolation.

New traces cover: isolated24MiB/s×60s burst ages out; sustained24MiB/s stops at180s under regular60s sampling with emergency cleanup room still available;300MiB/s first interval stops before reaching floor; declared24MiB/s cannot bypass preflight;30s sampling still needs180s; a single delayed sample cannot impersonate3intervals; cleanup cannot cancel positive depletion; partial interval weighting; independent guest pressure.

## Limits

The sample measures entire-volume availability, so observed consumption is not attributed to Engine. A single ordered trace does not prove the cause of either prior incident. This is a conservative policy, not a disk-capacity guarantee: sudden consumption between probes or faster-than-declared growth before a sustained window can exceed assumptions. Delayed probes delay full-window eligibility; the recent observed rate still funds emergency reserve immediately. Regular60second observations are assumed by the180second detection test. The source was integrated after independent review and all35 targeted tests passed again. Later workloads require a new frozen state; no prior report was changed.
