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


class NoQualifiedRate(RuntimeError):
    """The bounded search completed without three passing confirmations."""


def validate_settings(args):
    if type(args.max_screen_rate) is not int or args.max_screen_rate not in RATES:
        raise ValueError("Screening ceiling must be one of the existing rate candidates")
    if type(args.seconds) is not int or not 300 <= args.seconds <= 900:
        raise ValueError("Trial duration must be an integer from 300 through 900 seconds")
    if (type(args.late_window_seconds) is not int
            or not 180 <= args.late_window_seconds <= args.seconds - 60):
        raise ValueError("Late window must be at least 180 seconds and leave the fixed 60-second warmup")


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


def closed_trial(report):
    """Only pre-dispatch misses may continue a safely closed failed screen.

    They remain an incomplete, disqualified workload. An unresolved admission,
    lost HTTP outcome, missing observation or failed safety check stops the run.
    """
    oracle, drain = report.get("oracle", {}), report.get("drain", {})
    required = {"client_pending", "http_inflight", "journal_unresolved", "node_pending",
                "redis_active", "redis_borrowed", "redis_delayed", "redis_pending", "redis_submitted"}
    if (report.get("owned_children_stopped") is not True or oracle.get("safety_pass") is not True
            or report.get("errors") or report.get("infrastructure_stop") or report.get("journal_halted")
            or report.get("client_worker_failures") or oracle.get("safety_failures")
            or report.get("durable_chain_halts") or report.get("operator_recovery_required")
            or not required <= set(drain) or any(type(n) is not int or n != 0 for n in drain.values())):
        return False
    if report.get("outcome") == "pass":
        return oracle.get("liveness_pass") is True and not oracle.get("liveness_failures")
    if report.get("outcome") != "incomplete" or oracle.get("liveness_pass") is not False:
        return False
    if (report.get("requested_fault_validated") is not True
            or any(event.get("event") == "observer_error" for event in report.get("events", []))):
        return False
    timing = [row for row in report.get("timing_observations", []) if row.get("phase") == "load"]
    if (len(timing) < 2 or any(row.get("http_status") != 200 or row.get("unavailable")
                             or not isinstance(row.get("values"), dict) for row in timing)):
        return False
    dispatched = offered = missed = 0
    missing_by_chain = {}
    for name, row in report.get("per_chain", {}).items():
        client = row.get("client", {})
        sent, planned = client.get("dispatched", 0), row.get("offered")
        drops = [client.get("dropped_schedule_lag", 0), client.get("dropped_client_capacity", 0)]
        if (any(type(n) is not int or n < 0 for n in (sent, planned, *drops))
                or planned != sent + sum(drops)
                or row.get("responses", {}) != ({"202": sent} if sent else {})
                or any(value for key, value in client.items()
                       if key.startswith("dropped_") and key not in ("dropped_schedule_lag", "dropped_client_capacity"))):
            return False
        dispatched += sent
        offered += planned
        missed += sum(drops)
        missing_by_chain[name] = dict(zip(("schedule_lag", "client_capacity"), drops))
    drops = report.get("predispatch_drops", [])
    if not isinstance(drops, list) or len(drops) != missed:
        return False
    missing_ids = set()
    for drop in drops:
        if not isinstance(drop, dict):
            return False
        txid, chain, reason = drop.get("id"), drop.get("chain"), drop.get("reason")
        if (not isinstance(txid, str) or not txid or txid in missing_ids
                or not isinstance(chain, str) or chain not in missing_by_chain
                or not isinstance(reason, str) or reason not in missing_by_chain[chain]):
            return False
        missing_ids.add(txid)
        missing_by_chain[chain][reason] -= 1
    failures = oracle.get("liveness_failures")
    if (any(count for reasons in missing_by_chain.values() for count in reasons.values())
            or not isinstance(failures, list) or len(failures) != missed
            or any(not isinstance(failure, str) for failure in failures)
            or set(failures) != {txid + ": offered intent not admitted" for txid in missing_ids}):
        return False
    return (missed > 0 and oracle.get("observed_intents") == dispatched
            and oracle.get("offered_intents") == offered and offered == dispatched + missed)


class Sweep:
    def __init__(self, args):
        validate_settings(args)
        self.args = args
        self.output = args.output.resolve()
        self.output.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.binary_sha = hashlib.file_digest(args.engine_bin.open("rb"), "sha256").hexdigest()
        self.summary = {"scope": "local-node host/workload measurements; no public-chain or power-loss qualification",
                        "engine_binary_sha256": self.binary_sha, "source_commit": subprocess.check_output(
                            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                        "platform": platform.platform(), "logical_cpus": os.cpu_count(),
                        "seconds_per_trial": args.seconds, "late_window_seconds": args.late_window_seconds,
                        "warmup_seconds": 60, "redis_fsync": "always", "trials": []}
        if args.mode == "individual":
            self.summary["configured_search_ceiling_tps"] = args.max_screen_rate
            self.summary["search_scope"] = "Highest qualifying tested candidate within the configured ceiling; not an absolute maximum"

    def save(self):
        temporary = self.output / ".summary.tmp"
        temporary.write_text(json.dumps(self.summary, indent=2) + "\n")
        temporary.replace(self.output / "summary.json")

    def inventory_allows(self, rates, factor=None):
        offered = sum(int(rate * self.args.seconds) for rate in rates.values())
        if offered <= 500000:
            return True
        limited = {"rates": rates, "offered_intents": offered,
                   "max_intents": 500000, "reason": "inventory_limit"}
        if factor is not None:
            limited["factor"] = factor
        self.summary.setdefault("inventory_limited_candidates", []).append(limited)
        self.save()
        return False

    def trial(self, rates, chaos="none"):
        if not self.inventory_allows(rates):
            raise RuntimeError("Trial exceeds the bounded campaign inventory")
        index = len(self.summary["trials"])
        report = self.output / f"{index:02d}-{chaos}.json"
        command = [sys.executable, str(ROOT / "scripts/capacity_metrics.py"),
                   "--engine-bin", str(self.args.engine_bin), "--seconds", str(self.args.seconds),
                   "--drain-seconds", "240", "--warmup-seconds", "60", "--late-window-seconds", str(self.args.late_window_seconds),
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
                code = child.wait(timeout=self.args.seconds + 900)
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
        if (value.get("duration_seconds") != self.args.seconds
                or value.get("minimum_late_window_seconds") != self.args.late_window_seconds
                or value.get("warmup_seconds") != 60):
            raise RuntimeError("Trial used different duration or observation settings")
        row = {"report": report.name, "rates": rates, "chaos": chaos, "exit": code,
               "seconds": round(time.monotonic() - started, 3), "outcome": value.get("outcome"),
               "capacity_candidate": eligible(value)}
        self.summary["trials"].append(row)
        self.save()
        print(json.dumps(row), flush=True)
        # Never hide an infrastructure/safety failure by trying a lower rate.
        # The wrapper returns 2 for a complete failed capacity screen and 1 for
        # an incomplete workload. Only independently closed pre-dispatch misses
        # may continue at a lower rate; neither result can qualify a rate.
        if code not in (0, 1, 2) or not closed_trial(value):
            raise RuntimeError("Trial did not close safely; preserved evidence needs review")
        return row

    def individual(self):
        self.summary.update(profile=self.args.profile, qualified_tps=None, confirmation_reports=[],
                            configured_search_ceiling_tps=self.args.max_screen_rate)
        candidates = []
        for rate in RATES:
            if rate > self.args.max_screen_rate:
                break
            if not self.inventory_allows({self.args.profile: rate}):
                break
            row = self.trial({self.args.profile: rate})
            if not row["capacity_candidate"]:
                break
            candidates.append((rate, row))
        for rate, screen in reversed(candidates):
            if self.confirm_individual(rate, screen):
                self.save()
                return
        # Confirm each fallback before deciding whether to try the next. A
        # passing 12-TPS screen alone cannot suppress the 6-TPS fallback.
        for rate in (12, 6):
            screen = self.trial({self.args.profile: rate})
            if screen["capacity_candidate"] and self.confirm_individual(rate, screen):
                self.save()
                return
        self.save()
        raise NoQualifiedRate("No individual rate passed three confirmations")

    def confirm_individual(self, rate, screen):
        repeats = [screen]
        for _ in range(2):
            row = self.trial({self.args.profile: rate})
            repeats.append(row)
            if not row["capacity_candidate"]:
                return False
        self.summary.update(qualified_tps=rate, confirmation_reports=[row["report"] for row in repeats],
                            search_ceiling_reached=rate == self.args.max_screen_rate)
        return True

    def shared(self):
        summaries = []
        for path in self.args.individuals.glob("**/summary.json"):
            summary = json.loads(path.read_text())
            if (summary.get("seconds_per_trial") != self.args.seconds
                    or summary.get("late_window_seconds") != self.args.late_window_seconds
                    or summary.get("warmup_seconds") != 60):
                raise ValueError("Individual duration and observation settings must match the shared trial")
            for filename in summary.get("confirmation_reports", []):
                if not isinstance(filename, str) or Path(filename).name != filename:
                    raise ValueError("Invalid individual report path")
                value = json.loads((path.parent / filename).read_text())
                profile = summary.get("profile")
                if (not eligible(value) or value.get("engine_binary_sha256") != self.binary_sha
                        or value.get("duration_seconds") != self.args.seconds
                        or value.get("minimum_late_window_seconds") != self.args.late_window_seconds
                        or value.get("warmup_seconds") != 60
                        or set(value.get("per_chain", {})) != {profile}
                        or value["per_chain"][profile].get("offered_tps") != summary.get("qualified_tps")):
                    raise ValueError("Individual confirmation evidence does not qualify")
            summaries.append(summary)
        original = selected_rates(summaries, self.binary_sha)
        self.summary.update(individual_rates=original, qualified_shared_rates=None, confirmation_reports=[],
                            inventory_limited_candidates=[])
        for factor in (1, .5, .25):
            rates = {name: rate * factor for name, rate in original.items()}
            # Keep the existing campaign's explicit bounded inventory limit.
            if not self.inventory_allows(rates, factor):
                continue
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
                self.save()
                return
        self.save()
        raise NoQualifiedRate("No shared rate passed three confirmations")


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("individual", "shared"))
    parser.add_argument("--profile", choices=PROFILES)
    parser.add_argument("--individuals", type=Path)
    parser.add_argument("--engine-bin", required=True, type=Path)
    parser.add_argument("--solana-bin-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-screen-rate", type=int, choices=RATES, default=RATES[-1],
                        help="Individual screening ceiling in TPS; a bounded search, not an absolute maximum")
    parser.add_argument("--seconds", type=int, default=300,
                        help="Offered duration per trial, 300 through 900 seconds")
    parser.add_argument("--late-window-seconds", type=int, default=180,
                        help="Late observation window, at least 180 seconds with 60 seconds left for warmup")
    args = parser.parse_args(argv)
    if (args.mode == "individual" and not args.profile) or (args.mode == "shared" and not args.individuals):
        parser.error("individual needs --profile; shared needs --individuals")
    try:
        validate_settings(args)
    except ValueError as error:
        parser.error(str(error))
    return args


def main():
    args = arguments()
    sweep = Sweep(args)
    try:
        getattr(sweep, args.mode)()
    except Exception as error:
        sweep.summary["stopped"] = type(error).__name__
        sweep.save()
        raise


if __name__ == "__main__":
    main()
