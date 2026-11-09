#!/usr/bin/env python3
"""Bounded local-node rate search; three repeats and shared-chain fault trials.

This runs the existing campaign in isolated process groups, preserves every
trial, and never contacts public RPCs. Rates are measured candidates on this
host/workload, not production or public-network limits.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
PROFILES = ("evm12", "evm2", "evm025", "solana")
RATES = (25, 50, 100, 200, 400, 800)


def eligible(report):
    """A drained run, incomplete sample, or a safety failure cannot qualify."""
    chains = report.get("per_chain", {})
    return (report.get("outcome") == "pass"
            and report.get("owned_children_stopped") is True
            and report.get("oracle", {}).get("safety_pass") is True
            and report.get("oracle", {}).get("liveness_pass") is True
            and not report.get("errors") and not report.get("infrastructure_stop")
            and report.get("all_chain_capacity_candidate") is True
            and bool(chains)
            and all(row.get("late_window", {}).get("capacity_candidate") is True
                    for row in chains.values()))


def selected_rates(summaries, binary_sha):
    selected = {}
    for summary in summaries:
        profile = summary.get("profile")
        rate = summary.get("qualified_tps")
        trials = summary.get("confirmation_reports", [])
        if (profile not in PROFILES or profile in selected
                or summary.get("engine_binary_sha256") != binary_sha
                or type(rate) is not int or not 0 < rate <= RATES[-1]
                or len(trials) != 3 or len(set(trials)) != 3):
            raise ValueError("Four distinct profiles with matching binaries and three confirmations required")
        selected[profile] = rate
    if set(selected) != set(PROFILES):
        raise ValueError("All four individual profiles must qualify before the shared trial")
    return selected


class Sweep:
    def __init__(self, args):
        self.args = args
        self.output = args.output.resolve()
        self.output.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.binary_sha = hashlib.file_digest(args.engine_bin.open("rb"), "sha256").hexdigest()
        self.summary = {"scope": "local-node host/workload measurements; no public-chain or power-loss qualification",
                        "engine_binary_sha256": self.binary_sha, "source_commit": subprocess.check_output(
                            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                        "platform": platform.platform(), "logical_cpus": os.cpu_count(),
                        "seconds_per_trial": 300, "redis_fsync": "always", "trials": []}

    def save(self):
        temporary = self.output / ".summary.tmp"
        temporary.write_text(json.dumps(self.summary, indent=2) + "\n")
        temporary.replace(self.output / "summary.json")

    def trial(self, rates, chaos="none"):
        index = len(self.summary["trials"])
        report = self.output / f"{index:02d}-{chaos}.json"
        command = [sys.executable, str(ROOT / "scripts/capacity_metrics.py"),
                   "--engine-bin", str(self.args.engine_bin), "--seconds", "300",
                   "--drain-seconds", "240", "--warmup-seconds", "60", "--late-window-seconds", "60",
                   "--redis-fsync", "always", "--max-intents", "500000", "--report", str(report)]
        for name, rate in rates.items():
            command += ["--chain", f"{name}={rate}"]
        if "solana" in rates:
            command += ["--solana-bin-dir", str(self.args.solana_bin_dir)]
        if chaos != "none":
            command += ["--chaos", chaos, "--chaos-after", "100", "--fault-count", "10",
                        "--retry-unknown", "--duplicate-count", "20", "--mixed"]
            if chaos == "reorg":
                command += ["--chaos-chain", "evm2"]
        started = time.monotonic()
        with report.with_suffix(".log").open("x") as log:
            child = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                     start_new_session=True)
            try:
                code = child.wait(timeout=1200)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                raise RuntimeError("Trial deadline exceeded; preserve its fixture and stop the sweep")
        value = json.loads(report.read_text())
        if value.get("engine_binary_sha256") != self.binary_sha:
            raise RuntimeError("Trial used a different binary")
        row = {"report": report.name, "rates": rates, "chaos": chaos, "exit": code,
               "seconds": round(time.monotonic() - started, 3), "outcome": value.get("outcome"),
               "capacity_candidate": eligible(value)}
        self.summary["trials"].append(row)
        self.save()
        print(json.dumps(row), flush=True)
        # Never hide an infrastructure/safety failure by trying a lower rate.
        # The metrics wrapper returns 2 for a safely drained rate that fails
        # the capacity screen, including intentionally disrupted chaos trials.
        if (code not in (0, 2) or value.get("outcome") != "pass" or value.get("errors")
                or value.get("oracle", {}).get("safety_pass") is not True
                or value.get("oracle", {}).get("liveness_pass") is not True
                or value.get("owned_children_stopped") is not True):
            raise RuntimeError("Trial did not close safely; preserved evidence needs review")
        return row

    def individual(self):
        self.summary.update(profile=self.args.profile, qualified_tps=None, confirmation_reports=[])
        candidates = []
        for rate in RATES:
            row = self.trial({self.args.profile: rate})
            if not row["capacity_candidate"]:
                break
            candidates.append((rate, row))
        if not candidates:
            for rate in (12, 6):
                row = self.trial({self.args.profile: rate})
                if row["capacity_candidate"]:
                    candidates.append((rate, row))
                    break
        for rate, screen in reversed(candidates):
            repeats = [screen]
            for _ in range(2):
                row = self.trial({self.args.profile: rate})
                repeats.append(row)
                if not row["capacity_candidate"]:
                    break
            if len(repeats) == 3 and all(row["capacity_candidate"] for row in repeats):
                self.summary.update(qualified_tps=rate, confirmation_reports=[row["report"] for row in repeats],
                                    search_ceiling_reached=rate == RATES[-1])
                break
        self.save()

    def shared(self):
        summaries = []
        for path in self.args.individuals.glob("**/summary.json"):
            summary = json.loads(path.read_text())
            for filename in summary.get("confirmation_reports", []):
                if not isinstance(filename, str) or Path(filename).name != filename:
                    raise ValueError("Invalid individual report path")
                value = json.loads((path.parent / filename).read_text())
                profile = summary.get("profile")
                if (not eligible(value) or value.get("engine_binary_sha256") != self.binary_sha
                        or value.get("duration_seconds") != 300
                        or set(value.get("per_chain", {})) != {profile}
                        or value["per_chain"][profile].get("offered_tps") != summary.get("qualified_tps")):
                    raise ValueError("Individual confirmation evidence does not qualify")
            summaries.append(summary)
        original = selected_rates(summaries, self.binary_sha)
        self.summary.update(individual_rates=original, qualified_shared_rates=None, confirmation_reports=[])
        for factor in (1, .5, .25):
            rates = {name: rate * factor for name, rate in original.items()}
            # Keep the existing campaign's explicit bounded inventory limit.
            if sum(rates.values()) * 300 > 500000:
                raise RuntimeError("Combined maximum exceeds the bounded campaign inventory")
            repeats = []
            for _ in range(3):
                row = self.trial(rates)
                repeats.append(row)
                if not row["capacity_candidate"]:
                    break
            if len(repeats) == 3 and all(row["capacity_candidate"] for row in repeats):
                self.summary.update(qualified_shared_rates=rates,
                                    confirmation_reports=[row["report"] for row in repeats])
                for fault in ("engine-crash", "lost-send", "rpc-errors", "redis-restart", "reorg"):
                    self.trial(rates, fault)
                break
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("individual", "shared"))
    parser.add_argument("--profile", choices=PROFILES)
    parser.add_argument("--individuals", type=Path)
    parser.add_argument("--engine-bin", required=True, type=Path)
    parser.add_argument("--solana-bin-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if (args.mode == "individual" and not args.profile) or (args.mode == "shared" and not args.individuals):
        parser.error("individual needs --profile; shared needs --individuals")
    sweep = Sweep(args)
    try:
        getattr(sweep, args.mode)()
    except Exception as error:
        sweep.summary["stopped"] = type(error).__name__
        sweep.save()
        raise


if __name__ == "__main__":
    main()
