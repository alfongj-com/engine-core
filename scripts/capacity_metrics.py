#!/usr/bin/env python3
"""Run the unchanged capacity oracle with bounded, independent timing snapshots.

Uses capacity_campaign's arguments and local-only fixtures. Histogram totals are
cumulative within one Engine process; restart boundaries must never be subtracted
as though they belonged to the same process. Missing snapshots stay visible.
"""
import hashlib
import json
import math
from pathlib import Path
import re
import threading
import time
import sys

from capacity_campaign import Campaign, LOCAL_HTTP, arguments

LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)=("(?:[^"\\]|\\.)*")(?:,|$)')
ALLOWED_LABELS = {"operation", "phase", "chain_id", "method", "cluster", "outcome", "queue_type", "le", "executor_type", "state", "transition"}


def parse_metrics(raw):
    """Keep bounded timing counters; omit all identity-bearing label families."""
    if len(raw) > 1024 * 1024:
        raise ValueError("metrics response exceeds bound")
    values = {}
    for line in raw.decode("utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        key, value = line.rsplit(" ", 1)
        name = key.split("{", 1)[0]
        if not name.startswith(("tw_engine_", "twmq_")):
            continue
        labels = {}
        if "{" in key:
            if not key.endswith("}"):
                raise ValueError("malformed metric labels")
            encoded = key[key.index("{") + 1:-1]
            position = 0
            for match in LABEL.finditer(encoded):
                if match.start() != position or match[1] in labels:
                    raise ValueError("malformed metric label mapping")
                labels[match[1]] = json.loads(match[2])
                position = match.end()
            if position != len(encoded):
                raise ValueError("incomplete metric labels")
        if not set(labels) <= ALLOWED_LABELS:
            continue
        number = float(value)
        if not math.isfinite(number) or number < 0:
            raise ValueError("invalid cumulative metric value")
        canonical = name + json.dumps(labels, sort_keys=True, separators=(",", ":"))
        if canonical in values:
            raise ValueError("duplicate metric series")
        values[canonical] = number
    return values


def summarize_timing_window(report):
    """Average histogram observations within one uninterrupted load window.

    These are accumulated waits/service times, often nested or concurrent. They
    cannot be summed into a wall-clock utilization or latency percentage.
    """
    samples = [row for row in report.get("timing_observations", [])
               if row["phase"] == "load" and row["seconds"] >= report["warmup_seconds"]]
    if len(samples) < 2:
        raise ValueError("at least two post-warmup load samples are required")
    if any(row.get("http_status") != 200 or "values" not in row for row in samples):
        raise ValueError("load window contains a missing metrics observation")
    pids = {row.get("engine_pid") for row in samples}
    if None in pids or len(pids) != 1:
        raise ValueError("cannot subtract counters across Engine processes")
    times = [row["seconds"] for row in samples]
    if any(not math.isfinite(t) for t in times) or any(a >= b for a, b in zip(times, times[1:])):
        raise ValueError("sample times must be finite and strictly increasing")
    first, last = samples[0], samples[-1]
    scrapes = [row["scrape_seconds"] for row in samples]
    if any(not math.isfinite(value) or value < 0 for value in scrapes):
        raise ValueError("scrape durations must be finite and non-negative")
    histogram_keys = {key for row in samples for key in row["values"] if "_sum{" in key}
    if any(key not in last["values"] for key in histogram_keys):
        raise ValueError("histogram disappeared during the load window")
    metrics = []
    for key, total in sorted(last["values"].items()):
        name, separator, labels = key.partition("{")
        if not separator or not name.endswith("_sum"):
            continue
        count_key = name[:-4] + "_count{" + labels
        if count_key not in last["values"]:
            raise ValueError("histogram sum has no count")
        previous_count = previous_sum = 0
        for row in samples:
            values = row["values"]
            count, amount = values.get(count_key, 0), values.get(key, 0)
            if (not math.isfinite(count) or not math.isfinite(amount)
                    or count < previous_count or amount < previous_sum):
                raise ValueError("histogram counters decreased or became non-finite")
            previous_count, previous_sum = count, amount
        count = last["values"][count_key] - first["values"].get(count_key, 0)
        amount = total - first["values"].get(key, 0)
        if count:
            metrics.append({"metric": name[:-4], "labels": json.loads("{" + labels),
                            "observations": count, "sum": amount, "mean": amount / count})
    return {"scope": "One Engine process, post-warmup load only; overlapping durations are not additive",
            "from_seconds": times[0], "to_seconds": times[-1],
            "scrape_mean_seconds": sum(scrapes) / len(samples),
            "metrics": metrics}


class InstrumentedCampaign(Campaign):
    def __init__(self, args, profiles):
        super().__init__(args, profiles)
        self.metrics_lock = threading.Lock()
        self.report["timing_observations"] = []
        self.report["timing_source_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.report["timing_scope"] = "Cumulative process counters; scrape cost is separate from chain observer duration. No subtraction across Engine PIDs."

    def sample(self):
        row = super().sample()
        with self.metrics_lock:
            started = time.monotonic()
            process = self.children.get("engine")
            sample = {"seconds": started - self.started, "phase": self.phase,
                      "engine_pid": process.pid if process is not None else None}
            try:
                status, raw = LOCAL_HTTP.request(self.base + "/metrics", timeout=2, max_response_bytes=1024 * 1024)
                sample["http_status"] = status
                if status == 200:
                    sample["values"] = parse_metrics(raw)
                else:
                    sample["unavailable"] = "http_status"
            except Exception as error:
                sample["unavailable"] = type(error).__name__
            sample["scrape_seconds"] = time.monotonic() - started
            self.report["timing_observations"].append(sample)
        return row


def main():
    args, profiles = arguments()
    campaign = InstrumentedCampaign(args, profiles)
    campaign.run()
    if campaign.report["outcome"] != "pass":
        raise SystemExit(1)
    if not campaign.report.get("all_chain_capacity_candidate"):
        raise SystemExit(2)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--summarize":
        print(json.dumps(summarize_timing_window(json.loads(Path(sys.argv[2]).read_text())), indent=2))
    else:
        main()
