#!/usr/bin/env python3
"""Bounded local capacity/chaos campaign using one Engine, Redis and SQLite ledger.

Built-in EVM profiles use Anvil (including its OP execution backend), with
simulated cadence/finality. External dev-node execution is labeled separately;
these experiments do not qualify public networks or rollup L1 settlement.
A local Solana validator is optional. No public endpoint or production key is
accepted. A drained run and a sustainable offered rate are separate results.

Example (root/operator runs measurements after source freeze):
  python3 scripts/capacity_campaign.py --chain evm12=100 --chain evm2=100 \
    --chain evm025=100 --chain solana=100 --solana-bin-dir /path/to/bin \
    --seconds 300 --redis-fsync everysec --report /tmp/campaign.json
"""
import argparse
import base64
from collections import Counter
import concurrent.futures
from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import functools
import hashlib
import heapq
import http.client
import json
import math
import os
from pathlib import Path
import shutil
import shlex
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import uuid

from local_eoa_recovery import FROM, TO, ROOT, stop
from local_solana_recovery import port
from capacity_faults import KeepAliveHttp

# Separate from the RPC proxy client so transport pressure is attributable.
LOCAL_HTTP = KeepAliveHttp()

PROFILES = {
    "evm12": {"family": "evm", "chain_id": 31337, "block_seconds": 12.0},
    "evm2": {"family": "evm", "chain_id": 31338, "block_seconds": 2.0},
    "evm025": {"family": "evm", "chain_id": 31339, "block_seconds": 0.25},
    "solana": {"family": "solana", "chain_id": "solana:local"},
}


def percentile(values, fraction):
    if not values:
        return None
    return round(sorted(values)[min(len(values) - 1, int(len(values) * fraction))], 3)


def distribution(values):
    return {"count": len(values), **{f"p{q}": percentile(values, q / 100) for q in (50, 95, 99)},
            "max": round(max(values), 3) if values else None}


def check_loopback(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.username or parsed.password:
        raise ValueError("Only explicit HTTP IPv4 loopback endpoints are supported")
    return url


def request(url, payload=None, headers=None, timeout=10):
    check_loopback(url)
    body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    status, raw = LOCAL_HTTP.request(url, body=body, headers={"Content-Type": "application/json", **(headers or {})}, timeout=timeout)
    if not 200 <= status < 300:
        # Do not interpret arbitrary non-JSON error pages or follow redirects.
        return status, None
    return status, json.loads(raw)


def transport_error_details(error):
    """Keep actionable connection diagnostics without URLs, headers or messages."""
    cause = getattr(error, "reason", error)
    if not isinstance(cause, BaseException): cause = error
    return {"type": type(error).__name__, "cause_type": type(cause).__name__,
            "errno": getattr(cause, "errno", None)}


def wait_until(check, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, http.client.HTTPException, ValueError, RuntimeError, urllib.error.URLError):
            pass
        time.sleep(.1)
    raise RuntimeError("Local service readiness deadline exceeded")


def private_json(path, value):
    """Publish a complete, private report atomically without replacing evidence."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".campaign-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        os.unlink(temporary)


def redis_command(port_number, *args):
    encoded = [str(arg).encode() for arg in args]
    packet = b"*%d\r\n" % len(encoded) + b"".join(b"$%d\r\n%s\r\n" % (len(arg), arg) for arg in encoded)
    with socket.create_connection(("127.0.0.1", port_number), timeout=3) as connection:
        connection.sendall(packet)
        reader = connection.makefile("rb")
        def read():
            line = reader.readline()
            kind, data = line[:1], line[1:-2]
            if kind == b":": return int(data)
            if kind == b"+": return data.decode()
            if kind == b"$":
                length = int(data)
                if length < 0: return None
                value = reader.read(length)
                if reader.read(2) != b"\r\n": raise RuntimeError("Invalid Redis reply")
                return value.decode()
            if kind == b"*": return [read() for _ in range(int(data))]
            raise RuntimeError("Redis command failed")
        return read()


@dataclass(frozen=True)
class Offer:
    chain: str
    index: int
    scheduled: float


def schedule_phases(rates):
    """A deterministic fraction of each period separates equal-rate arrivals."""
    return {chain: index / (len(rates) * rates[chain]) for index, chain in enumerate(sorted(rates))}


def campaign_outcome(oracle, report, execution_error=False):
    """Keep a proven safety violation and durable fences visible after faults."""
    if oracle.get("safety_pass") is False:
        return "unsafe"
    if report.get("durable_chain_halts") or report.get("finality_checkpoint_conflict"):
        return "finality_checkpoint_conflict"
    if report.get("journal_halted") or report.get("operator_recovery_required"):
        return "fail_closed_recovery_required"
    if report.get("offline_projection_recovered") and oracle.get("safety_pass") and not execution_error:
        return "recovered_with_quarantine"
    if report.get("chaos_qualification", {}).get("qualified") is False and oracle.get("safety_pass"):
        return "unqualified_fault_scenario"
    if execution_error:
        return "error"
    return oracle.get("outcome", report.get("outcome", "error"))


def open_loop(rates, duration, start, max_lag, dispatch, dropped, clock=time.monotonic, sleep=time.sleep, cancelled=lambda: False):
    """No unbounded catch-up queue: one heap item per chain, bounded dispatch.

    Eligible late offers can microburst up to the configured lag tolerance.

    A slot older than that tolerance is discarded. A full client drops the slot. Neither path
    retries it. Dispatch returns immediately and must enforce a nonblocking bound.
    """
    phases = schedule_phases(rates)
    heap = [(start + phases[chain], chain, 0) for chain in sorted(rates)]
    heapq.heapify(heap)
    counts = {chain: int(math.floor(rate * duration)) for chain, rate in rates.items()}
    while heap:
        deadline, chain, index = heapq.heappop(heap)
        if index >= counts[chain]:
            continue
        if cancelled():
            dropped(Offer(chain, index, deadline), "campaign_aborted")
            index += 1
            if index < counts[chain]: heapq.heappush(heap, (start + phases[chain] + index / rates[chain], chain, index))
            continue
        now = clock()
        if deadline > now:
            sleep(deadline - now)
            now = clock()
        offer = Offer(chain, index, deadline)
        if now - deadline > max_lag:
            dropped(offer, "schedule_lag")
        elif not dispatch(offer):
            dropped(offer, "client_capacity")
        index += 1
        if index < counts[chain]:
            heapq.heappush(heap, (start + phases[chain] + index / rates[chain], chain, index))
    # Keep the offered phase at its declared length, including the last interval.
    remaining = start + duration - clock()
    if remaining > 0 and not cancelled():
        sleep(remaining)


class BoundedPool:
    """At most limit submitted/running futures; no hidden executor backlog."""
    def __init__(self, limit):
        self.permits = threading.BoundedSemaphore(limit)
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=limit)
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.failures = []

    def submit(self, function, *args):
        if not self.permits.acquire(blocking=False):
            return False
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        def run():
            try:
                return function(*args)
            except Exception as error:
                with self.lock:
                    self.failures.append(type(error).__name__)
            finally:
                with self.lock:
                    self.active -= 1
                self.permits.release()
        try:
            self.pool.submit(run)
        except BaseException:
            with self.lock:
                self.active -= 1
            self.permits.release()
            raise
        return True

    def close(self):
        self.pool.shutdown(wait=True)


def rate_assessment(samples, chain, end_seconds, offered_rate, minimum_window=60, tolerance=.05):
    """Drain is excluded. No throughput claim without a complete late window."""
    rows = [row for row in samples if row.get("phase") == "load" and chain in row.get("chains", {})]
    target_start = max(0, end_seconds - minimum_window)
    starts = [row for row in rows if row["seconds"] <= target_start + .05]
    ends = [row for row in rows if row["seconds"] <= end_seconds + .05]
    if not starts or not ends:
        return {"sustainable": False, "reason": "insufficient late-window samples"}
    first, last = starts[-1], ends[-1]
    elapsed = last["seconds"] - first["seconds"]
    if elapsed < minimum_window - 1:
        return {"sustainable": False, "reason": "late window shorter than required", "window_seconds": elapsed}
    before, after = first["chains"][chain], last["chains"][chain]
    fields = ("admitted", "attempted", "included", "terminal")
    rates = {field: (after[field] - before[field]) / elapsed for field in fields}
    backlog_before = before["admitted"] - before["terminal"]
    backlog_after = after["admitted"] - after["terminal"]
    growth = (backlog_after - backlog_before) / elapsed
    unsigned_before = before["admitted"] - before["attempted"]
    unsigned_after = after["admitted"] - after["attempted"]
    window = [row for row in rows if first["seconds"] <= row["seconds"] <= last["seconds"]]
    xs = [row["seconds"] for row in window]
    ys = [row["chains"][chain]["admitted"] - row["chains"][chain]["attempted"] for row in window]
    mean_x, mean_y = sum(xs) / len(xs), sum(ys) / len(ys)
    variance = sum((x - mean_x) ** 2 for x in xs)
    trend = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / variance if variance else 0
    monotonic_growth = len(ys) >= 3 and all(b >= a for a, b in zip(ys, ys[1:])) and ys[-1] > ys[0]
    # A positive unsigned trend is evidence of saturation, even when the
    # terminal rate happens to fit a broad block-batching tolerance.
    unsigned_growing = trend > .1 and unsigned_after > unsigned_before + 1
    return {"sustainable": all(value >= offered_rate * (1 - tolerance) for value in rates.values())
            and growth <= offered_rate * tolerance and not unsigned_growing and not monotonic_growth,
            "from_seconds": first["seconds"], "to_seconds": last["seconds"], "window_seconds": elapsed,
            "rates_tps": {key: round(value, 3) for key, value in rates.items()},
            "terminal_backlog_start": backlog_before, "terminal_backlog_end": backlog_after,
            "terminal_backlog_growth_per_second": round(growth, 3), "relative_tolerance": tolerance,
            "unsigned_backlog_start": unsigned_before, "unsigned_backlog_end": unsigned_after,
            "unsigned_backlog_linear_trend_per_second": round(trend, 3),
            "unsigned_monotonic_growth": monotonic_growth, "unsigned_growth_detected": unsigned_growing,
            "unsigned_trend_threshold_per_second": .1}


def qualify_capacity(assessment, oracle, proxy, execution_error=False):
    """Eventual recovery cannot turn a contaminated transport run into a ceiling."""
    result = dict(assessment)
    reasons = []
    if not result.get("capacity_candidate"):
        reasons.append("rate_or_admission_threshold_not_met")
    if not oracle.get("safety_pass") or not oracle.get("liveness_pass"):
        reasons.append("exact_reconciliation_failed")
    if proxy.get("proxy_overloads", 0):
        reasons.append("rpc_proxy_connection_limit")
    if proxy.get("proxy_failures") or proxy.get("proxy_failure_details") or proxy.get("http_transport", {}).get("failures"):
        reasons.append("rpc_proxy_transport_errors")
    if any(method.get("errors", 0) for method in proxy.get("methods", {}).values()):
        reasons.append("rpc_method_errors")
    if any(proxy.get(field, 0) for field in ("dropped_responses", "lost_http_responses", "accepted_responses_lost", "injected_preflight_bypass")):
        reasons.append("rpc_fault_injection")
    if execution_error:
        reasons.append("campaign_or_journal_error")
    result["disqualifiers"] = reasons
    result["capacity_candidate"] = not reasons
    return result


class Rpc:
    """Observer/setup requests bypass Engine's measured proxy; count separately."""
    def __init__(self, url):
        self.url = check_loopback(url)
        self.lock = threading.Lock()
        self.calls = Counter()
        self.errors = Counter()

    def call(self, method, params):
        with self.lock: self.calls[method] += 1
        status, value = request(self.url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        if status != 200 or not isinstance(value, dict) or "error" in value or "result" not in value:
            with self.lock: self.errors[method] += 1
            raise RuntimeError("Local observer RPC failed: " + method)
        return value["result"]

    @functools.lru_cache(maxsize=8192)
    def block(self, number):
        # Reconciliation only: per-height full transaction-hash membership.
        return self.call("eth_getBlockByNumber", [number, False])


class JournalObserver:
    """Incremental read-only observer; no repeated full-history COUNT scans."""
    def __init__(self, path, id_chain, start, submission_times):
        self.path, self.id_chain, self.start, self.submission_times = path, id_chain, start, submission_times
        self.cursors = {"admissions": 0, "attempts": 0, "terminal_evidence": 0}
        self.admitted, self.attempted, self.terminal = set(), set(), set()
        self.signatures = {}
        self.outcomes = Counter()
        self.latencies = {chain: {"attempt_observed_upper_ms": [], "terminal_observed_upper_ms": []} for chain in set(id_chain.values())}
        self.unknown_ids = set()
        self.counts = {chain: Counter(admitted=0, attempted=0, terminal=0) for chain in self.latencies}

    def poll(self):
        observed = time.monotonic()
        with sqlite3.connect(f"file:{self.path}?mode=ro", uri=True, timeout=2) as db:
            db.execute("BEGIN")
            for table, target in (("admissions", self.admitted), ("attempts", self.attempted), ("terminal_evidence", self.terminal)):
                column = "rowid" if table == "admissions" else "sequence"
                payload = "payload" if table == "attempts" else "evidence" if table == "terminal_evidence" else "kind"
                cursor = db.execute(f"SELECT {column},id,{payload} FROM {table} WHERE {column}>? ORDER BY {column}", (self.cursors[table],))
                for sequence, txid, data in cursor:
                    self.cursors[table] = sequence
                    chain = self.id_chain.get(txid)
                    if chain is None:
                        self.unknown_ids.add(txid)
                        continue
                    if txid not in target:
                        target.add(txid)
                        self.counts[chain][{"admissions": "admitted", "attempts": "attempted", "terminal_evidence": "terminal"}[table]] += 1
                        if table != "admissions" and txid in self.submission_times:
                            field = "attempt_observed_upper_ms" if table == "attempts" else "terminal_observed_upper_ms"
                            self.latencies[chain][field].append((time.monotonic() - self.submission_times[txid]) * 1000)
                    if table == "attempts":
                        value = json.loads(data)
                        if "signature" in value:
                            self.signatures[value["signature"]] = txid
                    elif table == "terminal_evidence":
                        self.outcomes[(chain, json.loads(data).get("outcome", "unknown"))] += 1
            db.rollback()
        return {chain: dict(counts) for chain, counts in self.counts.items()}


COUNTER_ADDRESS = "0x2222222222222222222222222222222222222222"
REVERT_ADDRESS = "0x3333333333333333333333333333333333333333"


def observe_evm_pool(node, profile, pinned_nonce, pending_nonce):
    """Count Anvil's whole pool; a native nonce delta cannot see queued gaps."""
    nonce_delta = max(0, pending_nonce - pinned_nonce)
    observation = {"nonce_pending_delta": nonce_delta,
                   "nonce_reference": "pinned reconciliation block",
                   "pending_count": None, "queued_count": None, "total_pool_count": None}
    if profile.get("external_url"):
        # Native Nitro does not expose Anvil's txpool API. Keep this weak
        # observation distinct from the authoritative per-ID receipt oracle.
        observation.update({"source": "pending_nonce_minus_pinned_nonce",
            "drain_count": nonce_delta, "complete_pool_observation": False,
            "empty_pool_observed": None,
            "limitation": "Nonce delta excludes queued transactions behind gaps; zero does not prove an empty pool."})
        return observation
    status = node.call("txpool_status", [])
    if not isinstance(status, dict):
        raise ValueError("Malformed txpool status")
    counts = {}
    for field in ("pending", "queued"):
        value = status.get(field)
        if isinstance(value, str) and value.startswith("0x"):
            value = int(value, 16)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError("Malformed txpool count")
        counts[field] = value
    total = counts["pending"] + counts["queued"]
    observation.update({"source": "txpool_status", "pending_count": counts["pending"],
        "queued_count": counts["queued"], "total_pool_count": total, "drain_count": total,
        "complete_pool_observation": True, "empty_pool_observed": total == 0,
        "scope": "All accounts on the caller-owned local Anvil node at observation time."})
    return observation


def record_pool_observation(report, drain, chain, observation):
    """Keep drain's numeric-counter schema separate from explanatory evidence."""
    drain["node_pending"] += observation["drain_count"]
    report.setdefault("node_pool_observations", {})[chain] = observation


def summarize_pool(pool, sender, orphaned_hashes):
    """Redacted identity inventory; separate RPC snapshots are not atomic with drop."""
    selected, all_entries = [], []
    for state in ("pending", "queued"):
        accounts = pool.get(state)
        if not isinstance(accounts, dict): raise ValueError("Malformed pool account map")
        for address, transactions in accounts.items():
            if not isinstance(transactions, dict): raise ValueError("Malformed pool nonce map")
            for _, transaction in transactions.items():
                if not isinstance(transaction, dict): raise ValueError("Malformed pool transaction")
                identity, nonce = transaction.get("hash"), transaction.get("nonce")
                if not isinstance(identity, str) or not identity.startswith("0x") or len(bytes.fromhex(identity[2:])) != 32:
                    raise ValueError("Malformed pool hash")
                nonce = int(nonce, 16) if isinstance(nonce, str) else nonce
                if not isinstance(nonce, int) or isinstance(nonce, bool) or nonce < 0:
                    raise ValueError("Malformed pool nonce")
                row = (identity.lower(), nonce, state)
                all_entries.append(row)
                if address.lower() == sender.lower(): selected.append(row)
    hashes = {row[0] for row in selected}
    if len(hashes) != len(selected): raise ValueError("Duplicate pool hash")
    nonces = sorted(row[1] for row in selected)
    orphaned = {h.lower() for h in orphaned_hashes}
    payload = json.dumps(sorted(all_entries), separators=(",", ":")).encode()
    summary = {"all_transaction_count": len(all_entries), "signer_transaction_count": len(selected),
        "signer_pending_count": sum(row[2] == "pending" for row in selected),
        "signer_queued_count": sum(row[2] == "queued" for row in selected),
        "signer_nonce_min": min(nonces) if nonces else None,
        "signer_nonce_max": max(nonces) if nonces else None,
        "signer_nonce_sha256": hashlib.sha256(json.dumps(nonces, separators=(",", ":")).encode()).hexdigest(),
        "signer_hashes_sha256": hashlib.sha256(json.dumps(sorted(hashes), separators=(",", ":")).encode()).hexdigest(),
        "all_entries_sha256": hashlib.sha256(payload).hexdigest(),
        "orphaned_target_hashes_present": len(hashes & orphaned),
        "other_signer_hashes_present": len(hashes - orphaned)}
    return summary, hashes


class ScenarioUnqualified(RuntimeError):
    """The requested fixture was not observed; this is not a safety violation."""


def evenly_spaced(values, limit):
    """Bounded deterministic probes include both ends of a large ID inventory."""
    if len(values) <= limit:
        return list(values)
    if limit == 1:
        return [values[0]]
    return [values[index * (len(values) - 1) // (limit - 1)] for index in range(limit)]


def validate_recovery_inventory(before, after, namespace):
    """Validate the real CLI transition, not a reconstructed or edited ledger."""
    old, new = before["control"], after["control"]
    if (new["deployment"] != old["deployment"] or new["epoch"] != old["epoch"] + 1
            or new["namespace"] != namespace or new["namespace"] == old["namespace"]
            or new["checkpoint"] != 0 or new["halted"]):
        raise RuntimeError("Recovery deployment/epoch/projection metadata mismatch")
    if set(before["admissions"]) != set(after["admissions"]):
        raise RuntimeError("Recovery added or lost immutable admissions")
    if before["attempts"] != after["attempts"] or before["table_sha256"] != after["table_sha256"]:
        raise RuntimeError("Recovery changed immutable attempts or finality/terminal evidence")
    attempted = {row["id"] for row in before["attempts"].values()}
    counts = Counter(terminal=0, quarantined=0, unsent=0)
    for txid, original in before["admissions"].items():
        expected = dict(original)
        if original["state"] == "terminal":
            counts["terminal"] += 1
        elif txid in attempted:
            expected["state"] = "quarantined"
            counts["quarantined"] += 1
        else:
            counts["unsent"] += 1
        if after["admissions"][txid] != expected:
            raise RuntimeError("Recovery changed payload/replay identity or failed quarantine")
    return dict(counts)


class Campaign:
    def __init__(self, args, profiles):
        self.args, self.profiles = args, profiles
        self.logs = Path(tempfile.mkdtemp(prefix="engine-capacity-"))
        os.chmod(self.logs, 0o700)
        self.namespace = uuid.uuid4().hex
        self.projection_namespace = self.namespace
        self.projection_recovered = False
        self.orphaned = {}
        self.finality_halted = False
        self.token = uuid.uuid4().hex + uuid.uuid4().hex
        self.children, self.streams, self.proxies = {}, [], {}
        self.nodes, self.initial = {}, {}
        self.redis_port, self.engine_port = port(), port()
        self.base = f"http://127.0.0.1:{self.engine_port}"
        self.journal = self.logs / "recovery.sqlite"
        self.lock = threading.Lock()
        self.samples, self.events, self.errors = [], [], []
        self.stats = {name: Counter() for name in profiles}
        self.responses = {name: Counter() for name in profiles}
        self.transport_errors = {name: Counter() for name in profiles}
        self.http_latency = {name: [] for name in profiles}
        self.scheduled_latency = {name: [] for name in profiles}
        self.http_start_lateness = {name: [] for name in profiles}
        self.times, self.statuses = {}, {}
        self.id_chain, self.expected = {}, {}
        self.created = time.monotonic()
        self.started = self.created
        self.phase = "setup"
        self.stop_sampler = threading.Event()
        self.abort = threading.Event()
        self.recovery_required = False
        self.sample_lock = threading.Lock()
        self.included_signatures = set()
        self.finalized_signatures = set()
        self.sig_rotation = 0
        self.pool = BoundedPool(args.http_concurrency)
        self.report = {
            "campaign_started_utc": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1, "scope": "local single-host shared Engine/Redis/SQLite capacity experiment",
            "public_chain_qualification": False, "profiles": profiles, "duration_seconds": args.seconds,
            "warmup_seconds": args.warmup_seconds, "minimum_late_window_seconds": args.late_window_seconds,
            "arrival_phase_seconds": schedule_phases({name: profile["rate"] for name, profile in profiles.items()}),
            "http_concurrency": args.http_concurrency, "rpc_proxy_concurrency": args.proxy_concurrency,
            "max_schedule_lag_ms": args.max_schedule_lag_ms,
            "eoa_max_inflight_per_wallet": args.max_inflight, "solana_workers": args.solana_workers,
            "eoa_broadcast_concurrency": args.eoa_broadcast_concurrency,
            "solana_confirmation_poll_seconds": args.solana_confirmation_poll_seconds,
            "mixed": args.mixed, "chaos": args.chaos, "log_directory": str(self.logs),
            "durability": {"sqlite": "WAL synchronous=FULL/fullfsync=ON", "redis_appendfsync": args.redis_fsync},
            "engine_binary_sha256": hashlib.file_digest(args.engine_bin.open("rb"), "sha256").hexdigest() if hasattr(hashlib, "file_digest") else hashlib.sha256(args.engine_bin.read_bytes()).hexdigest(),
            "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "source_status_porcelain": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).splitlines(),
            "harness_sha256": {name: hashlib.sha256((Path(__file__) if name == "capacity_campaign.py" else ROOT / "scripts" / name).read_bytes()).hexdigest()
                               for name in ("capacity_campaign.py", "capacity_faults.py")},
            "resources_scope": "Owned Engine/Redis/local nodes plus combined campaign+RPC proxy process; external node/VM CPU and memory require separate observation. ps CPU is its reported lifetime average.",
            "latency_note": "HTTP service/deadline latency is measured directly; HTTP-start lateness is measured immediately before the request call (not first socket byte), excluding retries. Scheduler tolerance is not an OS real-time guarantee. Durable attempt/terminal timestamps are observer upper bounds, sampled independently.",
        }

    def event(self, event_name, **fields):
        with self.lock:
            self.events.append({"campaign_elapsed_seconds": round(time.monotonic() - self.created, 6),
                                "load_elapsed_seconds": None if self.phase == "setup" else round(time.monotonic() - self.started, 6),
                                "phase": self.phase, "event": event_name, **fields})

    def spawn(self, name, command, env=None, cwd=None):
        path = self.logs / f"{name}-{len(self.streams)}.log"
        stream = path.open("x")
        os.chmod(path, 0o600)
        self.streams.append(stream)
        child = subprocess.Popen([str(x) for x in command], stdout=stream, stderr=subprocess.STDOUT, env=env, cwd=cwd)
        self.children[name] = child
        self.event("spawn", name=name, argv=[str(x) for x in command], pid=child.pid)
        return child

    def engine_command(self, flag, required=True):
        result = subprocess.run([str(self.args.engine_bin), flag], cwd=ROOT / "server", env=self.env,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
        path = self.logs / (flag.lstrip("-") + f"-{len(self.events)}.log")
        path.write_bytes(result.stdout)
        os.chmod(path, 0o600)
        if result.returncode and required:
            raise RuntimeError("Engine recovery CLI failed; inspect private log")
        return result.returncode == 0

    def start_engine(self):
        self.spawn("engine", [self.args.engine_bin], self.env, ROOT / "server")
        wait_until(lambda: request(self.base + "/health")[0] == 200, 60)

    def start_redis(self):
        directory = self.logs / "redis"
        directory.mkdir(mode=0o700, exist_ok=True)
        self.spawn("redis", [self.args.redis_bin, "--bind", "127.0.0.1", "--port", self.redis_port,
                              "--dir", directory, "--save", "", "--appendonly", "yes", "--appendfsync", self.args.redis_fsync])
        wait_until(lambda: redis_command(self.redis_port, "PING") == "PONG")

    def setup(self):
        from capacity_faults import RpcFaultProxy, FaultPlan, EVM_COUNTER_CODE, EVM_REVERT_CODE
        self.start_redis()
        # Avoid inheriting arbitrary configured RPC/header/key overrides.
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith(("APP__", "ENGINE_"))}
        self.env.update({
            "APP_ENVIRONMENT": "production", "RUST_LOG": "warn", "ENGINE_SIGNING_TOKEN": self.token,
            "APP__REDIS__URL": f"redis://127.0.0.1:{self.redis_port}/", "APP__SERVER__HOST": "127.0.0.1",
            "APP__SERVER__PORT": str(self.engine_port), "APP__RECOVERY__JOURNAL_PATH": str(self.journal),
            "APP__QUEUE__EXECUTION_NAMESPACE": self.namespace, "APP__QUEUE__LOCAL_CONCURRENCY": "4",
            "APP__QUEUE__POLLING_INTERVAL_MS": "20", "APP__QUEUE__LEASE_DURATION_SECONDS": "10",
            "APP__QUEUE__EOA_MAX_INFLIGHT": str(self.args.max_inflight),
            "APP__QUEUE__EOA_BROADCAST_CONCURRENCY": str(self.args.eoa_broadcast_concurrency),
            "APP__QUEUE__EOA_EXECUTOR_WORKERS": str(sum(p["family"] == "evm" for p in self.profiles.values()) or 1),
            "APP__QUEUE__SOLANA_EXECUTOR_WORKERS": str(self.args.solana_workers),
            "APP__QUEUE__SOLANA_CONFIRMATION_POLL_INTERVAL_SECONDS": str(self.args.solana_confirmation_poll_seconds),
        })
        for name in ("WEBHOOK_WORKERS", "EXTERNAL_BUNDLER_SEND_WORKERS", "USEROP_CONFIRM_WORKERS"):
            self.env[f"APP__QUEUE__{name}"] = "1"
        for name, profile in self.profiles.items():
            if profile["family"] == "evm":
                self.env["ENGINE_PRIVATE_KEY"] = f"{1:064x}"
                upstream = profile.get("external_url")
                if upstream is None:
                    node_port = port()
                    upstream = f"http://127.0.0.1:{node_port}"
                    command = [self.args.anvil_bin, "--host", "127.0.0.1", "--port", node_port,
                               "--chain-id", profile["chain_id"], "--block-time", profile["block_seconds"],
                               "--gas-limit", "100000000", "--silent"]
                    if profile["network"] == "optimism": command += ["--network", "optimism"]
                    self.spawn(name, command)
                node = Rpc(upstream)
                wait_until(lambda: int(node.call("eth_chainId", []), 16) == profile["chain_id"])
                if not profile.get("external_url"):
                    node.call("anvil_setBalance", [FROM, hex(10**25)])
                    if self.args.mixed:
                        node.call("anvil_setCode", [COUNTER_ADDRESS, EVM_COUNTER_CODE])
                        node.call("anvil_setCode", [REVERT_ADDRESS, EVM_REVERT_CODE])
                initial = {"client_version": node.call("web3_clientVersion", []),
                           "genesis": {key: value for key, value in node.call("eth_getBlockByNumber", ["0x0", False]).items() if key in ("hash", "gasLimit", "baseFeePerGas", "timestamp")},
                           "sender_nonce": int(node.call("eth_getTransactionCount", [FROM, "latest"]), 16),
                           "sender_balance": int(node.call("eth_getBalance", [FROM, "latest"]), 16),
                           "recipient_balance": int(node.call("eth_getBalance", [TO, "latest"]), 16)}
                if int(node.call("eth_getTransactionCount", [FROM, "pending"]), 16) != initial["sender_nonce"]:
                    raise RuntimeError("External/local signer already has pending work")
                if initial["sender_balance"] < 10**18:
                    raise RuntimeError("Local EVM signer needs at least one dev ETH before campaign")
                if profile["network"] == "optimism":
                    initial["op_predeploy_code_bytes"] = {address: (len(node.call("eth_getCode", [address, "latest"])) - 2) // 2
                        for address in ("0x4200000000000000000000000000000000000015", "0x420000000000000000000000000000000000000F")}
            else:
                upstream, node, initial = self.setup_solana()
            self.nodes[name], self.initial[name] = node, initial
            send_method = "eth_sendRawTransaction" if profile["family"] == "evm" else "sendTransaction"
            plan = FaultPlan(drop_accepted_sends=self.args.fault_count if self.args.chaos == "lost-send" else 0,
                             rpc_errors={send_method: self.args.fault_count} if self.args.chaos == "rpc-errors" else {},
                             after_accepted=self.args.chaos_after, max_inflight=self.args.proxy_concurrency,
                             max_unique_wires=self.args.max_intents * 4)
            proxy = RpcFaultProxy(upstream, plan, audit_path=self.logs / f"{name}-rpc.jsonl")
            self.proxies[name] = proxy
            if profile["family"] == "evm":
                prefix = f"APP__EVM_RPC__ENDPOINTS__{profile['chain_id']}"
                self.env[prefix + "__URL"] = proxy.url
                self.env[prefix + "__FINALITY__MODE"] = "depth"
                self.env[prefix + "__FINALITY__CONFIRMATIONS"] = str(profile["depth"])
            else:
                self.env["APP__SOLANA__LOCAL__HTTP_URL"] = proxy.url
                self.env["APP__SOLANA__LOCAL__WS_URL"] = initial["ws_url"]
        self.report["queue_settings"] = {key: value for key, value in self.env.items() if key.startswith("APP__QUEUE__")}
        self.engine_command("--initialize-recovery")
        self.start_engine()
        self.report["initial"] = self.initial
        for chain, profile in self.profiles.items():
            for index in range(int(math.floor(profile["rate"] * self.args.seconds))):
                txid = f"{self.namespace}-{chain}-{index}"
                self.id_chain[txid] = chain
                _, expected = self.fixture(chain, index)
                self.expected[txid] = expected

    def setup_solana(self):
        rpc_port, faucet, gossip = port(True), port(), port()
        binary = self.args.solana_bin_dir
        addresses = {}
        for name in ("payer", "recipient"):
            key = self.logs / f"{name}.json"
            subprocess.run([str(binary / "solana-keygen"), "new", "--no-bip39-passphrase", "--silent", "--outfile", str(key)],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30)
            os.chmod(key, 0o600)
            addresses[name] = subprocess.check_output([str(binary / "solana-keygen"), "pubkey", str(key)], text=True, timeout=30).strip()
        self.payer, self.recipient = addresses["payer"], addresses["recipient"]
        self.spawn("solana", [binary / "solana-test-validator", "--quiet", "--reset", "--ledger", self.logs / "solana-ledger",
                              "--rpc-port", rpc_port, "--faucet-port", faucet, "--gossip-port", gossip,
                              "--dynamic-port-range", self.args.solana_dynamic_ports, "--bind-address", "127.0.0.1"])
        upstream = f"http://127.0.0.1:{rpc_port}"
        node = Rpc(upstream)
        wait_until(lambda: node.call("getHealth", []) == "ok", 120)
        self.env["ENGINE_SOLANA_KEYPAIR_FILE"] = str(self.logs / "payer.json")
        for address, amount in ((self.payer, 100_000_000_000), (self.recipient, 1_000_000_000)):
            signature = node.call("requestAirdrop", [address, amount, {"commitment": "confirmed"}])
            wait_until(lambda: (node.call("getSignatureStatuses", [[signature], {"searchTransactionHistory": True}])["value"][0] or {}).get("confirmationStatus") == "finalized", 90)
        return upstream, node, {"payer": self.payer, "recipient": self.recipient,
            "payer_balance": node.call("getBalance", [self.payer, {"commitment": "finalized"}])["value"],
            "recipient_balance": node.call("getBalance", [self.recipient, {"commitment": "finalized"}])["value"],
            "ws_url": f"ws://127.0.0.1:{rpc_port + 1}", "version": node.call("getVersion", [])}

    def fixture(self, chain, index):
        from capacity_faults import evm_fixture, solana_fixture
        txid = f"{self.namespace}-{chain}-{index}"
        profile = self.profiles[chain]
        if profile["family"] == "evm":
            kind = ("transfer", "storage", "revert")[index % 3] if self.args.mixed else "transfer"
            payload, expected = evm_fixture(txid, profile["chain_id"], FROM, TO, COUNTER_ADDRESS, REVERT_ADDRESS, index, kind)
            if profile.get("external_url"):
                payload["params"][0].pop("gasLimit", None)
            return payload, expected
        kind = "multi_transfer" if self.args.mixed and index % 2 else "transfer"
        if self.args.solana_park_every and (index + 1) % self.args.solana_park_every == 0:
            kind = "preflight_reject"
        return solana_fixture(txid, self.payer, self.recipient, kind)

    def submit(self, offer, retry=False):
        txid = f"{self.namespace}-{offer.chain}-{offer.index}"
        payload, _ = self.fixture(offer.chain, offer.index)
        path = "/v1/write/transaction" if self.profiles[offer.chain]["family"] == "evm" else "/v1/solana/transaction"
        started = time.monotonic()
        with self.lock:
            self.times.setdefault(txid, started)
            self.stats[offer.chain]["retry_dispatched" if retry else "dispatched"] += 1
        http_started = time.monotonic()
        try:
            status, _ = request(self.base + path, payload, {"x-engine-signing-token": self.token}, timeout=self.args.http_timeout)
        except (OSError, http.client.HTTPException, ValueError, RuntimeError, urllib.error.URLError) as error:
            status = "transport_error"
            detail = transport_error_details(error)
            with self.lock:
                self.transport_errors[offer.chain][json.dumps(detail, sort_keys=True)] += 1
        finished = time.monotonic()
        with self.lock:
            if retry:
                self.stats[offer.chain]["retry_status_" + str(status)] += 1
            else:
                self.responses[offer.chain][str(status)] += 1
                self.http_latency[offer.chain].append((finished - started) * 1000)
                self.scheduled_latency[offer.chain].append((finished - offer.scheduled) * 1000)
                self.http_start_lateness[offer.chain].append((http_started - offer.scheduled) * 1000)
            self.statuses[txid] = status

    def dropped(self, offer, reason):
        with self.lock: self.stats[offer.chain]["dropped_" + reason] += 1

    def resources(self):
        pids = [process.pid for process in self.children.values() if process.poll() is None] + [os.getpid()]
        text = subprocess.check_output(["ps", "-o", "pid=,pcpu=,rss=,time=", "-p", ",".join(map(str, pids))], text=True, timeout=3)
        processes = []
        names = {process.pid: name for name, process in self.children.items()}
        names[os.getpid()] = "campaign"
        for line in text.splitlines():
            pid, cpu, rss, cpu_time = line.split()
            processes.append({"name": names.get(int(pid), "unknown"), "pid": int(pid), "cpu_percent_ps": float(cpu),
                              "rss_bytes": int(rss) * 1024, "cpu_time": cpu_time})
        return {"processes": processes, "host_load_average": os.getloadavg(),
                "free_disk_bytes": shutil.disk_usage(self.logs).free,
                "journal_bytes": sum(path.stat().st_size for path in self.logs.glob("recovery.sqlite*"))}

    def sample(self):
        # Only this lock is held during I/O; the scheduler never takes it.
        with self.sample_lock:
            stamp = time.monotonic()
            chains = self.observer.poll()
            for name, profile in self.profiles.items():
                node = self.nodes[name]
                if profile["family"] == "evm":
                    chains[name]["included"] = int(node.call("eth_getTransactionCount", [FROM, "latest"]), 16) - self.initial[name]["sender_nonce"]
                else:
                    pending = [sig for sig in self.observer.signatures if sig not in self.finalized_signatures]
                    # Finite observer work; record lag instead of perturbing the sender.
                    selected = pending[self.sig_rotation:self.sig_rotation + 2048]
                    if not selected: selected, self.sig_rotation = pending[:2048], 0
                    self.sig_rotation += len(selected)
                    for offset in range(0, len(selected), 256):
                        batch = selected[offset:offset + 256]
                        statuses = node.call("getSignatureStatuses", [batch, {"searchTransactionHistory": True}])["value"]
                        for signature, status in zip(batch, statuses):
                            if status and status.get("confirmationStatus") in ("confirmed", "finalized"):
                                self.included_signatures.add(signature)
                            if status and status.get("confirmationStatus") == "finalized":
                                self.finalized_signatures.add(signature)
                    chains[name]["included"] = len(self.included_signatures)
                    chains[name]["observer_pending_signatures"] = len(pending)
                    chains[name]["finalized"] = len(self.finalized_signatures)
                with self.lock:
                    chains[name]["client"] = dict(self.stats[name])
                    chains[name]["responses"] = dict(self.responses[name])
                snapshot = self.proxies[name].snapshot()
                # Only compact cumulative counters belong in the time series.
                chains[name]["rpc"] = {key: snapshot.get(key) for key in ("calls", "accepted_unique_wires", "forwarded_sends", "proxy_overloads", "peak_inflight", "dropped_responses", "http_requests", "active", "methods", "http_transport")}
            row = {"seconds": round(stamp - self.started, 3), "phase": self.phase, "chains": chains,
                   "observer_duration_ms": round((time.monotonic() - stamp) * 1000, 3), "resources": self.resources()}
            self.samples.append(row)
            return row

    def sampling(self):
        deadline = time.monotonic()
        while not self.stop_sampler.is_set():
            try:
                self.sample()
            except Exception as error:
                self.event("observer_error", **transport_error_details(error))
            deadline += self.args.sample_seconds
            self.stop_sampler.wait(max(0, deadline - time.monotonic()))
            if time.monotonic() - deadline > self.args.sample_seconds:
                deadline = time.monotonic()

    def verify_nonterminal(self, ids):
        """Provisional execution may never cross the durable terminal boundary."""
        with sqlite3.connect(f"file:{self.journal}?mode=ro", uri=True, timeout=2) as db:
            db.execute("BEGIN")
            for offset in range(0, len(ids), 500):
                batch = ids[offset:offset + 500]
                placeholders = ",".join("?" for _ in batch)
                rows = dict(db.execute(f"SELECT id,state FROM admissions WHERE id IN ({placeholders})", batch))
                if len(rows) != len(batch) or any(state != "admitted" for state in rows.values()):
                    raise RuntimeError("Provisional reorg batch became terminal or lost durable admission")
                if db.execute(f"SELECT 1 FROM terminal_evidence WHERE id IN ({placeholders}) LIMIT 1", batch).fetchone():
                    raise RuntimeError("Provisional reorg batch acquired terminal evidence")
            db.rollback()

    def durable_states(self, ids):
        states = {}
        with sqlite3.connect(f"file:{self.journal}?mode=ro", uri=True, timeout=2) as db:
            db.execute("BEGIN")
            for offset in range(0, len(ids), 500):
                batch = ids[offset:offset + 500]
                placeholders = ",".join("?" for _ in batch)
                states.update(db.execute(f"SELECT id,state FROM admissions WHERE id IN ({placeholders})", batch))
            db.rollback()
        return states

    def chain_finality_inventory(self, chain):
        with sqlite3.connect(f"file:{self.journal}?mode=ro", uri=True, timeout=2) as db:
            db.execute("BEGIN")
            chain_id = self.profiles[chain]["chain_id"]
            checkpoint = db.execute("SELECT evidence FROM chain_checkpoints WHERE chain_id=?", (chain_id,)).fetchone()
            halted = db.execute("SELECT reason FROM chain_halts WHERE chain_id=?", (chain_id,)).fetchone()
            db.rollback()
        return {"checkpoint": json.loads(checkpoint[0]) if checkpoint else None,
                "halted": bool(halted), "halt_reason": halted[0] if halted else None}

    def block_attempts(self, chain, block):
        hashes = set(block["transactions"])
        found = {}
        with sqlite3.connect(f"file:{self.journal}?mode=ro", uri=True, timeout=2) as db:
            for txid, replay, encoded in db.execute("SELECT id,replay_key,payload FROM attempts"):
                attempt = json.loads(encoded)
                identity = attempt.get("transactionHash")
                if identity in hashes and self.id_chain.get(txid) == chain:
                    found[txid] = {"identity": identity, "replay_key": replay}
        for proof in found.values():
            receipt = self.nodes[chain].call("eth_getTransactionReceipt", [proof["identity"]])
            if not receipt or receipt["blockHash"] != block["hash"]:
                raise RuntimeError("Actual reorg fixture receipt missing from expected block")
            proof.update({"block_hash": receipt["blockHash"], "block_number": int(receipt["blockNumber"], 16),
                          "outcome": "success" if int(receipt["status"], 16) else "revert"})
        return found

    def reorg(self, chain):
        """Expose shallow-reorg continuity after real older terminal traffic."""
        node, profile = self.nodes[chain], self.profiles[chain]
        interval, depth = int(profile["block_seconds"]), profile["depth"]
        paused = False
        # Do not evade tip-pinning by injecting only before the first terminal.
        def prior_terminal_count():
            with sqlite3.connect(f"file:{self.journal}?mode=ro", uri=True, timeout=2) as db:
                return sum(self.id_chain.get(txid) == chain for txid, in db.execute("SELECT id FROM admissions WHERE state='terminal'"))
        wait_until(lambda: prior_terminal_count() >= self.args.reorg_min_transactions, max(60, self.args.seconds))
        prior_count = prior_terminal_count()
        try:
            node.call("evm_setIntervalMining", [0])
            node.call("evm_setAutomine", [False])
            paused = True
            pending = lambda: int(node.call("eth_getTransactionCount", [FROM, "pending"]), 16) - int(node.call("eth_getTransactionCount", [FROM, "latest"]), 16)
            wait_until(lambda: pending() >= self.args.reorg_min_transactions, 60)
            node.call("evm_mine", [])
            anchor_block = node.call("eth_getBlockByNumber", ["latest", False])
            anchor_number = int(anchor_block["number"], 16)
            anchors = self.block_attempts(chain, anchor_block)
            if len(anchors) < self.args.reorg_min_transactions:
                raise RuntimeError("Anchor block did not include requested actual intents")
            # Parent is one block short of granting the anchor depth eligibility.
            # These manually advanced heights are fault setup, not capacity data.
            for _ in range(depth - 1):
                node.call("evm_mine", [])
            parent = node.call("eth_getBlockByNumber", ["latest", False])
            parent_number = int(parent["number"], 16)
            if parent_number != anchor_number + depth - 1:
                raise RuntimeError("Reorg parent depth setup is inconsistent")
            self.verify_nonterminal(list(anchors))
            snapshot = node.call("evm_snapshot", [])
            wait_until(lambda: pending() >= self.args.reorg_min_transactions, 60)
            before_receipt_calls = self.proxies[chain].snapshot()["calls"].get("eth_getTransactionReceipt", 0)
            node.call("evm_mine", [])
            provisional = node.call("eth_getBlockByNumber", ["latest", False])
            if int(provisional["number"], 16) != parent_number + 1:
                raise RuntimeError("Mining pause failed to bound provisional finality head")
            affected = self.block_attempts(chain, provisional)
            if len(affected) < self.args.reorg_min_transactions:
                raise RuntimeError("Reorg did not include requested minimum actual intents")
            ids = list(affected)
            self.event("reorg_provisional_batch", chain=chain, prior_terminals=prior_count,
                       anchor_number=anchor_number, parent=parent_number,
                       block_hash=provisional["hash"], affected_ids=len(ids))
            deadline = time.monotonic() + self.args.reorg_hold_seconds
            while time.monotonic() < deadline:
                if int(node.call("eth_blockNumber", []), 16) != parent_number + 1:
                    raise RuntimeError("Provisional head advanced during finality safety check")
                self.verify_nonterminal(ids)
                time.sleep(.2)
            self.verify_nonterminal(ids)
            # Candidate-nonce filtering can intentionally skip provisional receipt
            # queries. Older anchors becoming terminal at this held head proves
            # that a real confirmation cycle ran without forcing such a query.
            anchor_states = self.durable_states(list(anchors))
            terminal_anchors = [txid for txid, state in anchor_states.items() if state == "terminal"]
            if not terminal_anchors:
                raise ScenarioUnqualified("anchor_terminal_not_observed_during_hold")
            finality_before = self.chain_finality_inventory(chain)
            if not finality_before["checkpoint"] or finality_before["halted"]:
                raise RuntimeError("No healthy durable checkpoint before reorg")
            self.report["reorg"] = {"chain": chain, "prior_terminal_count": prior_count,
                "anchor_block_number": anchor_number, "anchor_block_hash": anchor_block["hash"],
                "terminal_anchor_ids": terminal_anchors, "parent_number": parent_number,
                "parent_hash": parent["hash"], "orphaned_block_hash": provisional["hash"],
                "affected": affected, "finality_before_rollback": finality_before,
                "engine_receipt_calls_during_hold": self.proxies[chain].snapshot()["calls"].get("eth_getTransactionReceipt", 0) - before_receipt_calls,
                "provisional_hold_seconds": self.args.reorg_hold_seconds,
                "engine_process_restarted": False, "manual_signed_wire_replay": False}
            if node.call("evm_revert", [snapshot]) is not True:
                raise RuntimeError("Anvil did not restore actual parent snapshot")
            # The fixture deliberately combines a shallow reorg with total pool
            # loss. Capture later accepted transactions too; they can create many
            # more nonce holes than the small orphaned mined batch.
            before_drop_at = time.monotonic() - self.started
            sends_before = self.proxies[chain].snapshot()["forwarded_sends"]
            pool_before, hashes_before = summarize_pool(node.call("txpool_content", []), FROM,
                [proof["identity"] for proof in affected.values()])
            node.call("anvil_dropAllTransactions", [])
            drop_returned_at = time.monotonic() - self.started
            pool_after, hashes_after = summarize_pool(node.call("txpool_content", []), FROM,
                [proof["identity"] for proof in affected.values()])
            self.report["reorg"]["pool_loss"] = {
                "scope": "all pending and queued node pool transactions, not only orphaned mined batch",
                "before": pool_before, "after": pool_after,
                "before_rpc_started_load_seconds": before_drop_at,
                "drop_rpc_returned_load_seconds": drop_returned_at,
                "after_rpc_returned_load_seconds": time.monotonic() - self.started,
                "forwarded_sends_before": sends_before,
                "forwarded_sends_after": self.proxies[chain].snapshot()["forwarded_sends"],
                "signer_hashes_not_seen_after": len(hashes_before - hashes_after),
                "signer_new_hashes_seen_after": len(hashes_after - hashes_before),
                "atomic_with_drop": False,
                "concurrency_note": "Engine stays live. Separate snapshots cannot prove the exact removed set when broadcasts interleave; counts describe the observed RPC snapshots."}

            for proof in affected.values():
                if node.call("eth_getTransactionReceipt", [proof["identity"]]) is not None:
                    raise RuntimeError("Orphaned receipt remained canonical after rollback")
            self.verify_nonterminal(ids)
            node.call("evm_setNextBlockTimestamp", [int(provisional["timestamp"], 16) + 2])
            node.call("evm_mine", [])
            replacement = node.call("eth_getBlockByNumber", [hex(parent_number + 1), False])
            if replacement["hash"] == provisional["hash"]:
                raise RuntimeError("Reorg did not produce different canonical branch")
            self.orphaned.update(affected)
            self.report["reorg"]["replacement_block_hash"] = replacement["hash"]
            self.event("reorg_applied", chain=chain, orphaned_intents=len(ids), engine_alive=True)
        finally:
            if paused:
                node.call("evm_setIntervalMining", [interval])
                self.event("interval_mining_restored", chain=chain, seconds=interval)
        # Current tip-pinning code is expected to halt conservatively here. Keep
        # the failure evidence and return promptly; never call it recovered.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            inventory = self.chain_finality_inventory(chain)
            if inventory["halted"]:
                self.finality_halted = True
                self.abort.set()
                self.report["reorg"]["finality_after_rollback"] = inventory
                self.event("reorg_checkpoint_conflict", chain=chain, safety="fail_closed", liveness="blocked")
                return
            time.sleep(.2)
        self.report["reorg"]["finality_after_rollback"] = self.chain_finality_inventory(chain)

    def recovery_inventory_snapshot(self):
        """Hash full immutable columns; never export stored payload credentials."""
        with sqlite3.connect(f"file:{self.journal}?mode=ro", uri=True, timeout=2) as db:
            db.execute("BEGIN")
            fields = ("deployment", "epoch", "namespace", "checkpoint", "halted")
            control = dict(zip(fields, db.execute("SELECT deployment,epoch,namespace,checkpoint,halted FROM control").fetchone()))
            admissions = {txid: {"kind": kind, "fingerprint": fingerprint,
                "payload_sha256": hashlib.sha256(payload.encode()).hexdigest(), "state": state, "replay_key": replay}
                for txid, kind, fingerprint, payload, state, replay in db.execute("SELECT id,kind,fingerprint,payload,state,replay_key FROM admissions")}
            attempts = {sequence: {"id": txid, "replay_key": replay, "digest": digest,
                "payload_sha256": hashlib.sha256(payload.encode()).hexdigest()}
                for sequence, txid, replay, digest, payload in db.execute("SELECT sequence,id,replay_key,digest,payload FROM attempts")}
            tables = {}
            for table, columns, order in (("terminal_evidence", "sequence,id,evidence", "sequence"),
                    ("chain_checkpoints", "chain_id,evidence", "chain_id"), ("chain_halts", "chain_id,reason", "chain_id")):
                digest = hashlib.sha256()
                for row in db.execute(f"SELECT {columns} FROM {table} ORDER BY {order}"):
                    digest.update(json.dumps(row, separators=(",", ":")).encode() + b"\n")
                tables[table] = digest.hexdigest()
            db.rollback()
        return {"control": control, "admissions": admissions, "attempts": attempts, "table_sha256": tables}

    def recover_redis_projection(self):
        """Explicit offline quarantine recovery; never label unknown IDs done."""
        self.abort.set()  # Further scheduled offers are counted as aborted, not retried.
        if redis_command(self.redis_port, "FLUSHDB") != "OK":
            raise RuntimeError("Disposable Redis flush failed")
        self.event("owned_redis_flushed")
        wait_until(lambda: request(self.base + "/health", timeout=3)[0] == 503, 30)
        chain = self.args.chaos_chain or next(iter(self.profiles))
        payload, _ = self.fixture(chain, 0)
        path = "/v1/write/transaction" if self.profiles[chain]["family"] == "evm" else "/v1/solana/transaction"
        status, _ = request(self.base + path, payload, {"x-engine-signing-token": self.token})
        if status != 503:
            raise RuntimeError("Live Redis loss did not fence admission")
        stop(self.children["engine"])
        before = self.recovery_inventory_snapshot()
        if not before["control"]["halted"]:
            raise RuntimeError("Redis loss did not durably latch halt")
        if self.engine_command("--reattach-recovery", required=False):
            raise RuntimeError("Unsafe reattach accepted a flushed projection")
        old_namespace = self.projection_namespace
        new_namespace = old_namespace + "_recovered"
        self.env["APP__QUEUE__EXECUTION_NAMESPACE"] = new_namespace
        self.engine_command("--recover-redis")
        after = self.recovery_inventory_snapshot()
        integrity = validate_recovery_inventory(before, after, new_namespace)
        if integrity["quarantined"] < 1:
            raise ScenarioUnqualified("no_attempted_nonterminal_intent_quarantined")
        self.projection_namespace = new_namespace
        self.report["offline_recovery"] = {"old_namespace": old_namespace, "new_namespace": new_namespace,
            "unsafe_reattach_rejected": True, "live_health_and_admission_503": True,
            "inventory_check": integrity, "original_completion_claimed": False,
            "availability_note": "A quarantined EOA nonce not consumed on-chain can safely block new same-signer execution; recovery does not free or reuse it."}
        inventory_path = self.args.report.with_suffix(".pre-recovery-inventory.json")
        private_json(inventory_path, before)
        self.report["offline_recovery"]["before_inventory"] = str(inventory_path)
        self.start_engine()
        self.event("offline_projection_recovered", namespace=new_namespace, **integrity)
        before_sends = {name: proxy.snapshot()["forwarded_sends"] for name, proxy in self.proxies.items()}
        probes = []
        for name in self.profiles:
            for state, expected_status in (("terminal", 202), ("quarantined", 503)):
                candidates = sorted(txid for txid, row in after["admissions"].items()
                                    if row["state"] == state and self.id_chain.get(txid) == name)
                for txid in evenly_spaced(candidates, self.args.recovery_probe_count):
                    index = int(txid.rsplit("-", 1)[1])
                    payload, _ = self.fixture(name, index)
                    path = "/v1/write/transaction" if self.profiles[name]["family"] == "evm" else "/v1/solana/transaction"
                    status, _ = request(self.base + path, payload, {"x-engine-signing-token": self.token})
                    if status != expected_status:
                        raise RuntimeError("Recovered terminal/quarantine admission guard returned wrong status")
                    probes.append({"id": txid, "state": state, "status": status})
        if not any(probe["state"] == "quarantined" for probe in probes):
            raise ScenarioUnqualified("no_quarantined_original_id_probed")
        # No unsigned admission is replayed here: it could legitimately allocate
        # a fresh wire and hide a forbidden send from an unresolved old intent.
        # Availability is proven by healthy service; signer liveness stays explicit.
        time.sleep(1)
        after_sends = {name: proxy.snapshot()["forwarded_sends"] for name, proxy in self.proxies.items()}
        after_probes = self.recovery_inventory_snapshot()
        if before_sends != after_sends or after_probes["admissions"] != after["admissions"] or after_probes["attempts"] != after["attempts"]:
            raise RuntimeError("Guard-only recovered retries created new work or changed immutable inventory")
        self.report["offline_recovery"].update({"api_probes": probes, "api_probe_count": len(probes),
            "new_rpc_sends": 0, "immutable_inventory_after_probes": True,
            "quarantined_count": integrity["quarantined"], "unsent_retained_count": integrity["unsent"]})
        self.projection_recovered = True
        self.event("chaos_recovered", kind="redis-recover", quarantined=integrity["quarantined"])

    def chaos(self):
        if self.args.chaos not in ("engine-crash", "redis-restart", "redis-recover", "reorg"):
            return
        proxy = self.proxies[self.args.chaos_chain or next(iter(self.profiles))]
        try:
            proxy.wait_for_accepted(self.args.chaos_after, timeout=self.args.seconds)
            self.event("chaos_trigger", kind=self.args.chaos, accepted=proxy.snapshot()["accepted_unique_wires"])
            if self.args.chaos == "reorg":
                self.reorg(self.args.chaos_chain or next(iter(self.profiles)))
                if not self.finality_halted:
                    self.event("reorg_recovery_started", kind="reorg")
                return
            if self.args.chaos == "redis-recover":
                self.recover_redis_projection()
                return
            if self.args.chaos == "engine-crash":
                stop(self.children["engine"], crash=True)
                self.event("engine_sigkill")
            else:
                stop(self.children["redis"], crash=True)
                self.event("redis_sigkill")
                wait_until(lambda: request(self.base + "/health", timeout=3)[0] == 503, 30)
                chain = self.args.chaos_chain or next(iter(self.profiles))
                payload, _ = self.fixture(chain, 0)
                path = "/v1/write/transaction" if self.profiles[chain]["family"] == "evm" else "/v1/solana/transaction"
                status, _ = request(self.base + path, payload, {"x-engine-signing-token": self.token}, timeout=5)
                if status != 503: raise RuntimeError("Redis loss did not fail closed at mutation boundary")
                self.event("redis_loss_fail_closed_verified", health_status=503, mutation_status=status)
                # Ordinary startup/reattach remain fail closed on any journal/CAS gap.
                stop(self.children["engine"], crash=True)
                self.start_redis()
                if not self.engine_command("--reattach-recovery", required=False):
                    self.recovery_required = True
                    self.abort.set()
                    self.event("redis_reattach_refused", classification="fail_closed_operator_recovery_required")
                    return
                self.event("explicit_exact_checkpoint_reattach")
            self.start_engine()
            self.event("chaos_recovered", kind=self.args.chaos)
        except Exception as error:
            if isinstance(error, ScenarioUnqualified):
                self.report["chaos_qualification"] = {"qualified": False, "reason": str(error)}
                self.event("chaos_unqualified", reason=str(error))
            self.errors.append({"phase": "chaos", "error_type": type(error).__name__})
            self.event("chaos_failed", error_type=type(error).__name__)

    def redis_drain(self):
        result = {"redis_pending": 0, "redis_borrowed": 0, "redis_submitted": 0, "redis_active": 0, "redis_delayed": 0}
        for profile in self.profiles.values():
            if profile["family"] == "evm":
                for label, key, command in (("pending", "pending_txs", "ZCARD"), ("borrowed", "borrowed_txs", "HLEN"), ("submitted", "submitted_txs", "ZCARD")):
                    # Alloy's Address display is EIP-55, but Redis names use that
                    # exact Display. Discover this single disposable namespace's
                    # keys rather than guessing its checksum representation.
                    cursor = "0"
                    pattern = f"{self.projection_namespace}:eoa_executor:{key}:{profile['chain_id']}:*"
                    while True:
                        cursor, keys = redis_command(self.redis_port, "SCAN", cursor, "MATCH", pattern, "COUNT", 1000)
                        for name in keys:
                            result["redis_" + label] += redis_command(self.redis_port, command, name)
                        if cursor == "0": break
        for family in ("eoa_executor", "solana_executor"):
            prefix = f"twmq:{self.projection_namespace}_{family}"
            for label, suffix, command in (("pending", "pending", "LLEN"), ("active", "active", "HLEN"), ("delayed", "delayed", "ZCARD")):
                result["redis_" + label] += redis_command(self.redis_port, command, prefix + ":" + suffix)
        return result

    def wait_drain(self):
        deadline = time.monotonic() + self.args.drain_seconds
        parked = {txid for txid, expected in self.expected.items() if expected.get("expected_to_park")}
        while time.monotonic() < deadline:
            row = self.sample()
            if self.args.chaos == "reorg" and self.report.get("reorg"):
                inventory = self.chain_finality_inventory(self.report["reorg"]["chain"])
                if inventory["halted"]:
                    self.finality_halted = True
                    self.report["reorg"]["finality_after_rollback"] = inventory
                    self.event("reorg_checkpoint_conflict", safety="fail_closed", liveness="blocked")
                    return row
            admitted = self.observer.admitted.copy()
            unresolved = admitted - self.observer.terminal
            attempted_parked = parked & self.observer.attempted
            if unresolved <= attempted_parked and self.pool.active == 0:
                counters = self.redis_drain()
                if sum(counters.values()) <= len(attempted_parked):
                    self.event("drain_target_reached", parked=len(attempted_parked))
                    return row
            time.sleep(min(self.args.sample_seconds, max(0, deadline - time.monotonic())))
        self.event("drain_deadline_reached")
        return self.sample()

    def retry_phase(self):
        ids = []
        if self.args.retry_unknown:
            ids.extend(txid for txid, status in self.statuses.items() if status != 202)
        ids.extend(sorted(self.observer.terminal)[:self.args.duplicate_count])
        ids = sorted(set(ids))
        if not ids: return
        self.phase = "same_id_retry"
        before = {name: proxy.snapshot()["forwarded_sends"] for name, proxy in self.proxies.items()}
        self.event("same_id_retry_started", count=len(ids), unknown_enabled=self.args.retry_unknown,
                   duplicate_limit=self.args.duplicate_count)
        for txid in ids:
            chain = self.id_chain[txid]
            index = int(txid.rsplit("-", 1)[1])
            self.submit(Offer(chain, index, time.monotonic()), retry=True)
        after = {name: proxy.snapshot()["forwarded_sends"] for name, proxy in self.proxies.items()}
        self.event("same_id_retry_completed", send_delta={name: after[name] - before[name] for name in before})
        # A duplicate phase cannot create a new id. Completion evidence and nonce/
        # balance reconciliation below cover any duplicate broadcasts/effects.
        self.phase = "retry_drain"
        self.wait_drain()

    def load(self):
        self.started = time.monotonic()
        self.phase = "load"
        self.report["offered_started_utc"] = datetime.now(timezone.utc).isoformat()
        self.observer = JournalObserver(self.journal, self.id_chain, self.started, self.times)
        self.sample()
        sampler = threading.Thread(target=self.sampling, name="campaign-observer", daemon=True)
        sampler.start()
        chaos = threading.Thread(target=self.chaos, name="campaign-chaos", daemon=True)
        chaos.start()
        try:
            open_loop({name: p["rate"] for name, p in self.profiles.items()}, self.args.seconds, self.started,
                      self.args.max_schedule_lag_ms / 1000, lambda offer: self.pool.submit(self.submit, offer), self.dropped, cancelled=self.abort.is_set)
            # End sample's timestamp, rather than the request completion time,
            # defines the end of capacity measurement. No late-response catchup.
            self.report["offered_phase_end_seconds"] = time.monotonic() - self.started
            self.sample()
            self.phase = "client_drain"
            self.pool.close()
            self.event("client_drained", peak_inflight=self.pool.peak)
            chaos.join(timeout=max(0, self.args.seconds + 90 - (time.monotonic() - self.started)))
            if chaos.is_alive():
                raise RuntimeError("Chaos worker did not finish within its bounded deadline")
            self.phase = "drain"
            for name, profile in self.profiles.items():
                if profile.get("drain_hook"):
                    self.spawn("drain-hook-" + name, profile["drain_hook"])
                    self.event("external_drain_hook_started", chain=name)
            for proxy in self.proxies.values(): proxy.release()
            if not self.recovery_required and not self.projection_recovered and not self.finality_halted:
                self.wait_drain()
                self.retry_phase()
        finally:
            self.stop_sampler.set()
            sampler.join(timeout=30)
            if sampler.is_alive(): raise RuntimeError("Observer did not stop")
        self.report["samples"] = self.samples
        self.report["events"] = self.events
        self.report["client_worker_failures"] = self.pool.failures
        self.report["per_chain"] = {}
        for chain, profile in self.profiles.items():
            counts = dict(self.stats[chain])
            assessment = rate_assessment(self.samples, chain, self.report["offered_phase_end_seconds"], profile["rate"], self.args.late_window_seconds)
            client_complete = counts.get("dispatched", 0) == int(math.floor(profile["rate"] * self.args.seconds)) and sum(self.responses[chain].values()) == counts.get("dispatched", 0) and set(self.responses[chain]) == {"202"}
            if self.args.seconds < self.args.warmup_seconds + self.args.late_window_seconds:
                assessment.update({"sustainable": False, "reason": "warmup plus late-window requirement not met"})
            assessment["admission_p99_limit_ms"] = self.args.max_admission_p99_ms
            admission_p99 = percentile(self.scheduled_latency[chain], .99)
            latency_bounded = admission_p99 is not None and admission_p99 <= self.args.max_admission_p99_ms
            assessment["capacity_candidate"] = assessment.pop("sustainable", False) and client_complete and latency_bounded and not self.args.solana_park_every and self.args.chaos == "none"
            assessment["requires_repeated_confirmation"] = True
            self.report["per_chain"][chain] = {"offered": int(math.floor(profile["rate"] * self.args.seconds)),
                "offered_tps": profile["rate"], "client": counts, "responses": dict(self.responses[chain]),
                "transport_errors": [{**json.loads(detail), "count": count} for detail, count in self.transport_errors[chain].items()],
                "http_service_latency_ms": distribution(self.http_latency[chain]),
                "scheduled_to_response_latency_ms": distribution(self.scheduled_latency[chain]),
                "scheduled_to_http_start_lateness_ms": distribution(self.http_start_lateness[chain]),
                "durable_latency_upper_bounds_ms": {key: distribution(value) for key, value in self.observer.latencies[chain].items()},
                "late_window": assessment}

    def capture_durable_fences(self):
        """Read durable classifications even if a fault interrupts reconciliation."""
        if not self.journal.is_file():
            return
        with sqlite3.connect(f"file:{self.journal}?mode=ro", uri=True, timeout=2) as db:
            db.execute("BEGIN")
            self.report["journal_halted"] = bool(db.execute("SELECT halted FROM control").fetchone()[0])
            # Identifiers alone are sufficient; raw stored diagnostics may contain
            # unqualified upstream text and do not belong in public measurements.
            self.report["durable_chain_halts"] = [row[0] for row in db.execute("SELECT chain_id FROM chain_halts ORDER BY chain_id")]
            db.rollback()

    def read_journal(self):
        observations = {}
        with sqlite3.connect(f"file:{self.journal}?mode=ro", uri=True, timeout=2) as db:
            db.execute("BEGIN")
            control = db.execute("SELECT halted,reason FROM control").fetchone()
            self.report["journal_halted"] = bool(control[0])
            self.report["durable_chain_halts"] = [row[0] for row in db.execute("SELECT chain_id FROM chain_halts ORDER BY chain_id")]
            for txid, kind, state, replay in db.execute("SELECT id,kind,state,replay_key FROM admissions"):
                observations[txid] = {"admitted": True, "kind": kind, "state": state, "replay_key": replay, "attempts": [], "executions": []}
            for txid, replay, data in db.execute("SELECT id,replay_key,payload FROM attempts ORDER BY sequence"):
                value = json.loads(data)
                observations[txid]["attempts"].append({"payload": value, "replay_key": replay})
            for txid, data in db.execute("SELECT id,evidence FROM terminal_evidence"):
                proof = json.loads(data)
                observations[txid]["terminal"] = {"identity": proof.get("transactionHash", proof.get("signature")),
                    "outcome": "revert" if proof.get("outcome") == "reverted" else proof.get("outcome")}
            db.rollback()
        return observations

    def reconcile_evm(self, chain, txid, actual):
        from capacity_faults import verify_evm_node_wire, decode_evm_wire
        node, profile = self.nodes[chain], self.profiles[chain]
        expected = self.expected.get(txid)
        executions = []
        for attempt in actual["attempts"]:
            payload = attempt.pop("payload")
            identity = payload.get("transactionHash")
            wire = bytes.fromhex(payload["signedTransaction"].removeprefix("0x"))
            attempt.update({"identity": identity, "wire_digest": hashlib.sha256(wire).hexdigest()})
            transaction = node.call("eth_getTransactionByHash", [identity])
            receipt = node.call("eth_getTransactionReceipt", [identity])
            decoded = verify_evm_node_wire(payload["signedTransaction"], transaction, identity) if transaction else decode_evm_wire(payload["signedTransaction"], self.args.cast_bin)
            if decoded["identity"].lower() != identity.lower():
                raise RuntimeError("EVM signed wire identity differs from journal")
            attempt.update({key: decoded[key] for key in ("wire_digest", "intent_digest", "wire_replay_key")})
            if decoded["wire_replay_key"] != attempt["replay_key"]:
                raise RuntimeError("Actual EVM wire differs from durable replay binding")
            if receipt is None or int(receipt["blockNumber"], 16) > int(self.chain_heads[chain], 16): continue
            if not transaction or receipt.get("transactionHash", "").lower() != identity.lower():
                raise RuntimeError("EVM receipt identity inconsistent with signed transaction")
            if int(transaction["gas"], 16) < int(receipt["gasUsed"], 16):
                raise RuntimeError("Receipt gas exceeds signed transaction gas limit")
            block = node.block(receipt["blockNumber"])
            latest = int(self.chain_heads[chain], 16)
            canonical = bool(block and block["hash"].lower() == receipt["blockHash"].lower() and identity.lower() in {tx.lower() for tx in block["transactions"]})
            outcome = "success" if int(receipt["status"], 16) == 1 else "revert"
            effects = {}
            if outcome == "success":
                if transaction["to"].lower() == TO.lower():
                    effects["balance:" + TO.lower()] = int(transaction["value"], 16)
                elif transaction["to"].lower() == COUNTER_ADDRESS.lower():
                    slot = int(transaction["input"], 16)
                    effects[f"storage:{COUNTER_ADDRESS.lower()}:{slot}"] = int(node.call("eth_getStorageAt", [COUNTER_ADDRESS, hex(slot), self.chain_heads[chain]]), 16)
                else:
                    effects["unexpected_recipient"] = transaction["to"]
            if expected and expected["fixture"] == "revert":
                if int(node.call("eth_getStorageAt", [REVERT_ADDRESS, hex(expected["slot"]), self.chain_heads[chain]]), 16) != 0:
                    effects["reverted_storage_persisted"] = 1
            fee = int(receipt["gasUsed"], 16) * int(receipt["effectiveGasPrice"], 16)
            # Native OP fees are receipt-specific. Anvil OP execution may expose
            # absent/artificial L1 values; exact balance check below catches an
            # unaccounted component instead of silently blessing a fee model.
            components = {}
            for field in ("l1Fee", "operatorFee"):
                value = receipt.get(field)
                if value is not None:
                    components[field] = int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)
                    fee += components[field]
            executions.append({"identity": identity, "outcome": outcome, "effects": effects, "fee": fee,
                               "canonical": canonical, "finalized": canonical and latest >= int(receipt["blockNumber"], 16) + profile["depth"],
                               "block_number": int(receipt["blockNumber"], 16), "block_hash": receipt["blockHash"],
                               "nonce": int(transaction["nonce"], 16), "fee_components": components})
        original = self.orphaned.get(txid)
        if original:
            if actual["replay_key"] != original["replay_key"]:
                raise RuntimeError("Reorg recovery changed immutable replay reservation")
            if any(e["block_hash"] == original["block_hash"] for e in executions):
                raise RuntimeError("Orphaned block became terminal execution evidence")
            actual["orphaned_execution"] = original
        actual["executions"] = executions

    def reconcile_solana(self, chain, txid, actual):
        from capacity_faults import decode_solana_wire
        node = self.nodes[chain]
        for attempt in actual["attempts"]:
            payload = attempt.pop("payload")
            identity = payload["signature"]
            wire = payload["attempt"]["signed_transaction"]
            decoded = decode_solana_wire(wire, txid)
            attempt.update({"identity": identity, "wire_digest": decoded["wire_digest"],
                            "intent_digest": decoded["intent_digest"],
                            "wire_replay_key": "solana:solana:local:" + decoded["signature"]})
            if decoded["signature"] != identity:
                raise RuntimeError("Solana wire signature differs from journal identity")
            transaction = node.call("getTransaction", [identity, {"commitment": "finalized", "encoding": "json", "maxSupportedTransactionVersion": 0}])
            if transaction is None: continue
            if transaction["transaction"]["signatures"][0] != identity:
                raise RuntimeError("Wrong Solana signature in receipt")
            meta = transaction["meta"]
            keys = transaction["transaction"]["message"]["accountKeys"]
            index = keys.index(self.recipient)
            effect = meta["postBalances"][index] - meta["preBalances"][index]
            effects = {"balance:" + self.recipient: effect} if effect else {}
            status = node.call("getSignatureStatuses", [[identity], {"searchTransactionHistory": True}])["value"][0]
            canonical = bool(status and status["slot"] == transaction["slot"] and status.get("err") == meta.get("err"))
            actual["executions"].append({"identity": identity, "outcome": "success" if meta["err"] is None else "revert",
                "effects": effects, "fee": meta["fee"], "canonical": canonical,
                "finalized": canonical and status.get("confirmationStatus") == "finalized", "slot": transaction["slot"]})
        if self.expected.get(txid, {}).get("expected_to_park"):
            queue = f"twmq:{self.projection_namespace}_solana_executor"
            memberships = [("redis_pending", redis_command(self.redis_port, "LPOS", queue + ":pending", txid)),
                           ("redis_active", redis_command(self.redis_port, "HEXISTS", queue + ":active", txid)),
                           ("redis_delayed", redis_command(self.redis_port, "ZSCORE", queue + ":delayed", txid))]
            queue_state = next((name for name, value in memberships if value is not None and (name != "redis_active" or value == 1)), None)
            retained = redis_command(self.redis_port, "GET", f"{self.projection_namespace}:solana_tx_attempt:{txid}")
            retained = json.loads(retained) if retained else {}
            signed_attempt = bool(retained.get("signed_transaction")) and any(
                a["identity"] == retained.get("signature") and a["wire_digest"] == hashlib.sha256(
                    base64.b64decode(retained["signed_transaction"], validate=True)).hexdigest()
                for a in actual["attempts"])
            actual["retained"] = {"signed_attempt": signed_attempt, "journal_state": actual["state"], "queue_state": queue_state}

    def reconcile(self):
        from capacity_faults import evaluate_campaign
        self.phase = "reconciliation"
        stop(self.children["engine"])
        self.event("engine_stopped_for_consistent_reconciliation")
        self.chain_heads = {name: self.nodes[name].call("eth_blockNumber", []) for name, p in self.profiles.items() if p["family"] == "evm"}
        observations = self.read_journal()
        ids = list(observations)
        # Bounded batches prevent a large hidden future queue during verification.
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.reconcile_concurrency) as pool:
            def one(txid):
                chain = self.id_chain.get(txid)
                if chain is None:
                    return  # Oracle reports unexpected durable intents, including NOOPs.
                method = self.reconcile_evm if self.profiles[chain]["family"] == "evm" else self.reconcile_solana
                method(chain, txid, observations[txid])
            for offset in range(0, len(ids), 256):
                list(pool.map(one, ids[offset:offset + 256]))
        drain = {"client_pending": self.pool.active, "http_inflight": self.pool.active,
                 "journal_unresolved": sum(row["state"] != "terminal" for row in observations.values()),
                 "node_pending": 0, **self.redis_drain()}
        balances = {}
        fees = Counter()
        aggregate_effects = Counter()
        for txid, actual in observations.items():
            chain = self.id_chain.get(txid)
            for execution in actual.get("executions", []):
                if execution["canonical"]:
                    fees[chain] += execution["fee"]
                    for effect, amount in execution["effects"].items():
                        if isinstance(amount, int): aggregate_effects[(chain, effect)] += amount
        balance_errors = []
        for name, profile in self.profiles.items():
            node, initial = self.nodes[name], self.initial[name]
            if profile["family"] == "evm":
                nonce = int(node.call("eth_getTransactionCount", [FROM, self.chain_heads[name]]), 16) - initial["sender_nonce"]
                pending_nonce = int(node.call("eth_getTransactionCount", [FROM, "pending"]), 16) - initial["sender_nonce"]
                pool = observe_evm_pool(node, profile, nonce, pending_nonce)
                record_pool_observation(self.report, drain, name, pool)
                sender = int(node.call("eth_getBalance", [FROM, self.chain_heads[name]]), 16)
                recipient = int(node.call("eth_getBalance", [TO, self.chain_heads[name]]), 16)
                recipient_delta = recipient - initial["recipient_balance"]
                observed_count = sum(len([e for e in row["executions"] if e["canonical"]]) for txid, row in observations.items() if self.id_chain.get(txid) == name)
                balances[name] = {"nonce_delta": nonce, "canonical_executions": observed_count,
                    "recipient_delta": recipient_delta, "sender_spent": initial["sender_balance"] - sender, "receipt_fees": fees[name]}
                if nonce != observed_count or recipient_delta != aggregate_effects[(name, "balance:" + TO.lower())] or initial["sender_balance"] - sender != recipient_delta + fees[name]:
                    balance_errors.append(name + ": exact nonce/balance/fee reconciliation failed")
            else:
                payer = node.call("getBalance", [self.payer, {"commitment": "finalized"}])["value"]
                recipient = node.call("getBalance", [self.recipient, {"commitment": "finalized"}])["value"]
                delta = recipient - initial["recipient_balance"]
                balances[name] = {"recipient_delta": delta, "payer_spent": initial["payer_balance"] - payer, "receipt_fees": fees[name]}
                if delta != aggregate_effects[(name, "balance:" + self.recipient)] or initial["payer_balance"] - payer != delta + fees[name]:
                    balance_errors.append(name + ": exact balance/fee reconciliation failed")
        oracle = evaluate_campaign(list(self.expected.values()), observations, drain)
        if balance_errors:
            oracle["safety_failures"].extend(balance_errors)
            oracle.update({"safety_pass": False, "outcome": "unsafe"})
        fault_validated = True
        if self.args.chaos in ("engine-crash", "redis-restart", "reorg", "redis-recover"):
            fault_validated = any(event["event"] == "chaos_recovered" for event in self.events) or self.recovery_required
        elif self.args.chaos == "lost-send":
            fault_validated = all(proxy.snapshot()["dropped_responses"] == self.args.fault_count for proxy in self.proxies.values())
        elif self.args.chaos == "rpc-errors":
            fault_validated = all(not any(proxy.errors_left.values()) for proxy in self.proxies.values())
        if self.args.chaos == "reorg":
            fault_validated = bool(self.orphaned) and not self.finality_halted and all(
                (observations.get(txid, {}).get("terminal") or {}).get("identity") in {
                    item["identity"] for item in observations.get(txid, {}).get("executions", [])
                    if item["canonical"] and item["finalized"] and item["block_hash"] != proof["block_hash"]}
                for txid, proof in self.orphaned.items())
            if self.report.get("reorg"):
                self.report["reorg"]["all_orphaned_intents_recovered"] = fault_validated
        if not fault_validated:
            oracle["liveness_failures"].append("requested chaos did not fully trigger and recover")
            oracle["liveness_pass"] = False
            if oracle["safety_pass"]: oracle["outcome"] = "incomplete"
        self.report["requested_fault_validated"] = fault_validated
        self.report["operator_recovery_required"] = self.recovery_required
        self.report["oracle"], self.report["balances"], self.report["drain"] = oracle, balances, drain
        self.report["rpc"] = {name: proxy.snapshot(include_wires=True) for name, proxy in self.proxies.items()}
        self.report["observer_rpc"] = {name: {"calls": dict(node.calls), "errors": dict(node.errors)} for name, node in self.nodes.items()}
        evidence = self.args.report.with_suffix(".observations.jsonl.gz")
        evidence.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(evidence, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb") as compressed:
                for txid, observation in observations.items():
                    # Full signed payload was replaced by digests above; credentials
                    # and the private SQLite payload never enter public evidence.
                    if txid not in self.id_chain:
                        observation = {"admitted": True, "state": observation["state"], "kind": observation["kind"], "unexpected": True}
                    compressed.write((json.dumps({"id": txid, **observation}, sort_keys=True) + "\n").encode())
        self.report["observation_evidence"] = str(evidence)
        self.report["offline_projection_recovered"] = self.projection_recovered
        self.report["finality_checkpoint_conflict"] = self.finality_halted
        self.report["outcome"] = campaign_outcome(oracle, self.report, bool(self.errors or self.pool.failures))
        for chain, value in self.report["per_chain"].items():
            value["late_window"] = qualify_capacity(value["late_window"], oracle, self.report["rpc"][chain],
                bool(self.errors or self.pool.failures or self.report["journal_halted"] or self.report.get("durable_chain_halts")
                     or any(event["event"] == "observer_error" for event in self.events)))
        self.report["all_chain_capacity_candidate"] = all(value["late_window"]["capacity_candidate"] for value in self.report["per_chain"].values())

    def run(self):
        try:
            self.setup()
            self.load()
            self.reconcile()
        except BaseException as error:
            self.report.update({"outcome": "error", "error_type": type(error).__name__})
            self.errors.append({"phase": self.phase, "error_type": type(error).__name__})
            raise
        finally:
            self.stop_sampler.set()
            self.pool.close()
            for child in reversed(list(self.children.values())):
                stop(child)
            # Preserve diagnostic state if the workload/observer failed before
            # the ordinary reconciliation path could record it.
            if "rpc" not in self.report:
                self.report["rpc"] = {name: proxy.snapshot(include_wires=True) for name, proxy in self.proxies.items()}
            try:
                self.capture_durable_fences()
            except (sqlite3.Error, OSError, TypeError):
                self.report["durable_fence_read_unavailable"] = True
            self.report["operator_recovery_required"] = self.recovery_required
            self.report["finality_checkpoint_conflict"] = self.finality_halted
            self.report["outcome"] = campaign_outcome(self.report.get("oracle", {}), self.report, bool(self.errors or self.pool.failures))
            for proxy in self.proxies.values(): proxy.close()
            LOCAL_HTTP.close()
            self.report["campaign_http"] = LOCAL_HTTP.snapshot()
            for stream in self.streams: stream.close()
            self.report["errors"] = self.errors
            self.report["events"] = self.events
            self.report.setdefault("samples", self.samples)
            self.report["owned_children_stopped"] = all(p.poll() is not None for p in self.children.values())
            private_json(self.args.report, self.report)
            print(json.dumps({"report": str(self.args.report), "outcome": self.report["outcome"],
                              "capacity_candidate": self.report.get("all_chain_capacity_candidate", False), "logs": str(self.logs)}))


def assignments(values):
    result = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key or not item or key in result:
            raise ValueError("Expected unique NAME=VALUE arguments")
        result[key] = item
    return result


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain", action="append", required=True, help="PROFILE=TPS, repeated; evm12/evm2/evm025/solana or external name")
    parser.add_argument("--external-evm", action="append", default=[], help="NAME=http://127.0.0.1:PORT (caller-owned, funded dev node)")
    parser.add_argument("--external-drain-hook", action="append", default=[], help="NAME=/absolute/executable; starts only during drain; caller-owned local chain head ticker")
    parser.add_argument("--chain-id", action="append", default=[], help="NAME=ID, required for each external dev node")
    parser.add_argument("--network", action="append", default=[], help="PROFILE=ethereum|optimism; evm2 defaults optimism")
    parser.add_argument("--block-seconds", action="append", default=[], help="PROFILE=SECONDS; local Anvil only")
    parser.add_argument("--engine-bin", type=Path, default=ROOT / "target/release/thirdweb-engine")
    parser.add_argument("--cast-bin", default=os.environ.get("CAST_BIN", "cast"), help="Offline signed-wire decoding only when node no longer exposes an EVM attempt")
    parser.add_argument("--anvil-bin", default=os.environ.get("ANVIL_BIN", "anvil"))
    parser.add_argument("--redis-bin", default=os.environ.get("REDIS_SERVER_BIN", "redis-server"))
    parser.add_argument("--solana-bin-dir", type=Path)
    parser.add_argument("--solana-dynamic-ports", default="25000-25200")
    parser.add_argument("--seconds", type=float, default=300)
    parser.add_argument("--drain-seconds", type=float, default=300)
    parser.add_argument("--warmup-seconds", type=float, default=60)
    parser.add_argument("--late-window-seconds", type=float, default=60)
    parser.add_argument("--sample-seconds", type=float, default=5)
    parser.add_argument("--depth", action="append", default=[], help="PROFILE=BLOCKS (default2); modeled depth finality, per EVM profile")
    parser.add_argument("--max-inflight", type=int, default=4096)
    parser.add_argument("--eoa-broadcast-concurrency", type=int, default=32, help="Bounded concurrent EOA RPC sends1..128; preparation stays32")
    parser.add_argument("--solana-workers", type=int, default=100)
    parser.add_argument("--solana-confirmation-poll-seconds", type=int, default=1, help="Experimental runtime setting; accepted-send/status polling interval1..5s")
    parser.add_argument("--http-concurrency", type=int, default=64)
    parser.add_argument("--proxy-concurrency", type=int, default=256)
    parser.add_argument("--reconcile-concurrency", type=int, default=16)
    parser.add_argument("--http-timeout", type=float, default=10)
    parser.add_argument("--max-schedule-lag-ms", type=float, default=25)
    parser.add_argument("--max-admission-p99-ms", type=float, default=1000, help="Explicit scheduled-to-response p99 ceiling for capacity candidate")
    parser.add_argument("--max-intents", type=int, default=250000)
    parser.add_argument("--redis-fsync", choices=("always", "everysec"), default="always")
    parser.add_argument("--mixed", action="store_true", help="EVM transfer/storage/revert; Solana transfer/two transfers")
    parser.add_argument("--solana-park-every", type=int, default=0, help="Sparse invalid System instructions; expected retained/parked, never a capacity pass")
    parser.add_argument("--chaos", choices=("none", "engine-crash", "redis-restart", "redis-recover", "reorg", "rpc-errors", "lost-send"), default="none")
    parser.add_argument("--reorg-min-transactions", type=int, default=10, help="Minimum actually included intents orphaned by local Anvil reorg")
    parser.add_argument("--reorg-hold-seconds", type=float, default=7, help="Observe provisional outcomes withheld for at least one confirmation cycle")
    parser.add_argument("--recovery-probe-count", type=int, default=20, help="Bounded API terminal/quarantine probes per chain/state; entire ledger verified")
    parser.add_argument("--chaos-chain", help="Accepted-send trigger chain for shared process crash")
    parser.add_argument("--chaos-after", type=int, default=100)
    parser.add_argument("--fault-count", type=int, default=10)
    parser.add_argument("--retry-unknown", action="store_true", help="Separate post-load same-ID retry; never changes capacity counts")
    parser.add_argument("--duplicate-count", type=int, default=0, help="Post-drain original terminal IDs to replay, bounded")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    rates, external, ids, networks, cadences, depths, hooks = (assignments(x) for x in (args.chain, args.external_evm, args.chain_id, args.network, args.block_seconds, args.depth, args.external_drain_hook))
    profiles = {}
    for name, rate in rates.items():
        if not name.replace("_", "").isalnum(): raise ValueError("Profile names must be alphanumeric/underscore")
        if name in external:
            if name not in ids: raise ValueError("External profile requires explicit --chain-id")
            profile = {"family": "evm", "chain_id": int(ids[name]), "external_url": check_loopback(external[name]),
                       "network": "external", "label": "caller-owned local dev execution; no public qualification"}
            if name in hooks:
                command = shlex.split(hooks[name])
                hook = Path(command[0]) if command else Path("")
                if not hook.is_absolute() or not hook.is_file() or not os.access(hook, os.X_OK): raise ValueError("Drain hook must start with an existing absolute executable")
                profile["drain_hook"] = command
            if args.mixed: raise ValueError("External EVM currently supports transfer-only; mixed needs native fixture deployment")
        elif name in PROFILES:
            profile = dict(PROFILES[name])
            if profile["family"] == "evm":
                profile["network"] = networks.get(name, "optimism" if name == "evm2" else "ethereum")
                if profile["network"] not in ("ethereum", "optimism"): raise ValueError("Invalid Anvil network")
                profile["block_seconds"] = float(cadences.get(name, profile["block_seconds"]))
                if not .01 <= profile["block_seconds"] <= 60: raise ValueError("Invalid block cadence")
                profile["label"] = "OP execution on Anvil; simulated cadence; no sequencer/derivation/L1 settlement; L1 fee predeploy state may be artificial" if profile["network"] == "optimism" else "Anvil EVM execution with simulated block cadence; not Nitro or public-chain capacity"
            else: profile["label"] = "local Agave validator; not public-cluster capacity"
        else: raise ValueError("Unknown profile")
        if profile["family"] == "evm":
            profile["depth"] = int(depths.get(name, 2))
            if not 0 <= profile["depth"] <= 10000: raise ValueError("Invalid depth")
        profile["rate"] = float(rate)
        if not 0 < profile["rate"] <= 5000: raise ValueError("TPS must be in (0,5000]")
        profiles[name] = profile
    if len({p["chain_id"] for p in profiles.values()}) != len(profiles): raise ValueError("Distinct chain IDs required")
    if set(external) - set(profiles) or set(ids) - set(external) or set(networks) - set(profiles) or set(cadences) - set(profiles) or set(depths) - set(profiles) or set(hooks) - set(external): raise ValueError("Unused profile override")
    if args.chaos == "redis-recover" and any(p["family"] != "evm" for p in profiles.values()):
        raise ValueError("Offline quarantine reconciliation currently requires EVM pinned block snapshots")
    if args.chaos == "reorg":
        target = profiles.get(args.chaos_chain or next(iter(profiles)))
        if not target or target["family"] != "evm" or target.get("external_url") or target["depth"] < 2:
            raise ValueError("Reorg requires owned Anvil EVM and explicit depth>=2")
        if not float(target["block_seconds"]).is_integer():
            raise ValueError("Reorg restores Anvil whole-second interval RPC; choose evm12/evm2 or override cadence")
    if args.chaos_chain and args.chaos_chain not in profiles: raise ValueError("Unknown chaos chain")
    if args.chaos == "redis-restart" and args.redis_fsync != "always": raise ValueError("Redis crash scenario requires appendfsync=always; no rollback assumption")
    if "solana" in profiles and not args.solana_bin_dir: raise ValueError("Solana binaries required")
    if args.solana_park_every and "solana" not in profiles: raise ValueError("Solana sparse rejection requires Solana profile")
    ranges = [(args.seconds, 1, 3600), (args.drain_seconds, 1, 3600), (args.sample_seconds, .1, 60),
              (args.http_timeout, .1, 60), (args.max_inflight, 1, 4096), (args.solana_workers, 1, 1000),
              (args.eoa_broadcast_concurrency, 1, 128), (args.http_concurrency, 1, 1024), (args.proxy_concurrency, 1, 1024), (args.reconcile_concurrency, 1, 128),
              (args.chaos_after, 1, 250000), (args.fault_count, 0, 250000),
              (args.duplicate_count, 0, 250000), (args.max_intents, 1, 500000), (args.max_schedule_lag_ms, 0, 1000),
              (args.solana_confirmation_poll_seconds, 1, 5), (args.reorg_min_transactions, 1, 10000), (args.reorg_hold_seconds, 6, 60), (args.recovery_probe_count, 1, 1000), (args.max_admission_p99_ms, 1, 60000), (args.solana_park_every, 0, 250000), (args.late_window_seconds, 1, 600), (args.warmup_seconds, 0, 1800)]
    if any(not low <= value <= high for value, low, high in ranges): raise ValueError("Argument outside bounded range")
    if sum(int(p["rate"] * args.seconds) for p in profiles.values()) > args.max_intents: raise ValueError("Total offered intents exceed explicit bounded memory limit")
    if args.report.exists() or args.report.with_suffix(".observations.jsonl.gz").exists(): raise ValueError("Evidence path already exists")
    args.engine_bin = args.engine_bin.resolve()
    if not args.engine_bin.is_file(): raise ValueError("Engine binary missing")
    return args, profiles


def main():
    args, profiles = arguments()
    campaign = Campaign(args, profiles)
    campaign.run()
    if campaign.report["outcome"] != "pass": raise SystemExit(1)
    if not campaign.report.get("all_chain_capacity_candidate"): raise SystemExit(2)


if __name__ == "__main__":
    main()
