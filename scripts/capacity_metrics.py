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
    main()
