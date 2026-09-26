#!/usr/bin/env python3
"""Compare actual Queue pruning Lua from two git commits on disposable local Redis.

No third-party Python dependency, live RPC, FLUSHDB, or server configuration change.
Only one randomly owned namespace is mutated and cleaned between trials.
"""
import argparse
import collections
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import socket
import statistics
import subprocess
import time
import uuid


class Redis:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.sock.settimeout(30)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.reader = self.sock.makefile("rb")

    @staticmethod
    def encode(parts):
        parts = [str(p).encode() if not isinstance(p, bytes) else p for p in parts]
        return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n" % len(p) + p + b"\r\n" for p in parts)

    def read(self):
        head = self.reader.readline()
        if not head:
            raise RuntimeError("Redis disconnected")
        kind, body = head[:1], head[1:-2]
        if kind == b"+":
            return body.decode()
        if kind == b"-":
            raise RuntimeError(body.decode())
        if kind == b":":
            return int(body)
        if kind == b"$":
            count = int(body)
            if count == -1:
                return None
            data = self.reader.read(count)
            if len(data) != count or self.reader.read(2) != b"\r\n":
                raise RuntimeError("Truncated bulk reply")
            return data.decode()
        if kind == b"*":
            count = int(body)
            return None if count == -1 else [self.read() for _ in range(count)]
        raise RuntimeError("Unexpected RESP reply")

    def cmd(self, *parts):
        self.sock.sendall(self.encode(parts))
        return self.read()

    def pipe(self, commands):
        self.sock.sendall(b"".join(self.encode(c) for c in commands))
        return [self.read() for _ in commands]

    def scan(self, pattern):
        cursor, found = "0", set()
        while True:
            cursor, keys = self.cmd("SCAN", cursor, "MATCH", pattern, "COUNT", 1000)
            found.update(keys)
            if cursor == "0":
                return found

    def close(self):
        self.reader.close()
        self.sock.close()


def info(redis, section):
    return dict(line.split(":", 1) for line in redis.cmd("INFO", section).splitlines()
                if line and not line.startswith("#") and ":" in line)


def eval_stats(redis):
    fields = info(redis, "commandstats").get("cmdstat_evalsha", "calls=0,usec=0")
    fields = dict(piece.split("=", 1) for piece in fields.split(","))
    return int(fields["calls"]), int(fields["usec"])


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True)


def extract(repo, ref, outcome):
    source = git(repo, "show", f"{ref}:twmq/src/lib.rs")
    name = "post_success_completion" if outcome == "success" else "post_fail_completion"
    start = source.index(f"    async fn {name}(")
    block = source[start:]
    script = block.split('r#"', 1)[1].split('"#', 1)[0]
    # The actual Lua declaration determines its positional KEYS contract.
    positions = re.findall(r"local (\w+) = KEYS\[(\d+)\]", script)
    assert positions and sorted(int(n) for _, n in positions) == list(range(1, len(positions) + 1))
    return script, sorted(positions, key=lambda entry: int(entry[1]))


def chunks(items, size=500):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)]


class Case:
    def __init__(self, redis, name, success, failed):
        self.redis, self.name = redis, name
        self.prefix = f"twmq:{name}:"
        self.limits = {"success": success, "failed": failed}
        self.history = {kind: collections.deque() for kind in self.limits}
        self.payload = json.dumps({"body": "x" * 256}, separators=(",", ":"))
        self.error = json.dumps({"attempt": 1, "error": "deterministic fixture failure"})

    def key(self, suffix):
        return self.prefix + suffix

    def clear(self):
        keys = sorted(self.redis.scan(self.key("*")))
        for batch in chunks(keys):
            self.redis.cmd("DEL", *batch)
        assert not self.redis.scan(self.key("*"))

    def record(self, job_id, outcome):
        commands = [
            ("HSET", self.key("jobs:data"), job_id, self.payload),
            ("HSET", self.key(f"job:{job_id}:meta"), "created_at", 1700000000,
             "processed_at", 1700000000, "finished_at", 1700000000, "attempts", 1),
            ("SADD", self.key("dedup"), job_id),  # Default Permanent idempotency.
        ]
        if outcome == "success":
            commands.append(("HSET", self.key("jobs:result"), job_id, json.dumps({"id": job_id, "ok": True})))
        else:
            commands.append(("LPUSH", self.key(f"job:{job_id}:errors"), self.error))
        return commands

    def setup(self):
        for outcome, count in self.limits.items():
            ids = [("S" if outcome == "success" else "F") + f"{i:035d}" for i in range(count)]
            self.history[outcome] = collections.deque(ids)
            for batch in chunks(ids, 250):
                commands = []
                for job_id in batch:
                    commands.extend(self.record(job_id, outcome))
                commands.append(("RPUSH", self.key(outcome), *batch))
                self.redis.pipe(commands)

    def overflow(self, outcome, index):
        job_id = "N" + f"{index:035d}"
        commands = self.record(job_id, outcome)
        commands.append(("LPUSH", self.key(outcome), job_id))
        self.redis.pipe(commands)
        self.history[outcome].appendleft(job_id)
        self.history[outcome].pop()

    def keys(self, positions, outcome):
        opposite = "failed" if outcome == "success" else "success"
        values = {
            "queue_id": self.name, "list_name": self.key(outcome),
            "job_data_hash": self.key("jobs:data"), "results_hash": self.key("jobs:result"),
            "dedupe_set_name": self.key("dedup"), "active_hash": self.key("active"),
            "pending_list": self.key("pending"), "delayed_zset": self.key("delayed"),
            "other_terminal_list": self.key(opposite),
            "pending_cancellations": self.key("pending_cancellations"),
        }
        return [values[key] for key, _ in positions]

    def reconcile(self):
        expected = set(self.history["success"]) | set(self.history["failed"])
        for outcome in self.history:
            actual = self.redis.cmd("LRANGE", self.key(outcome), 0, -1)
            assert actual == list(self.history[outcome]), (outcome, "history mismatch")
        actual = self.redis.cmd("HGETALL", self.key("jobs:data"))
        data = dict(zip(actual[::2], actual[1::2]))
        assert set(data) == expected, "live/deleted data IDs mismatch"
        assert all(value == self.payload for value in data.values()), "payload changed"
        assert set(self.redis.cmd("SMEMBERS", self.key("dedup"))) == expected, "dedup mismatch"
        meta = self.redis.scan(self.key("job:*:meta"))
        assert meta == {self.key(f"job:{job_id}:meta") for job_id in expected}, "metadata mismatch"
        errors = self.redis.scan(self.key("job:*:errors"))
        assert errors == {self.key(f"job:{job_id}:errors") for job_id in self.history["failed"]}, "errors mismatch"
        actual = self.redis.cmd("HGETALL", self.key("jobs:result"))
        results = dict(zip(actual[::2], actual[1::2]))
        assert set(results) == set(self.history["success"]), "results mismatch"
        assert all(json.loads(value) == {"id": job_id, "ok": True} for job_id, value in results.items())
        # Sample retained metadata contents as well as checking every key's existence.
        for outcome in self.history:
            for job_id in (self.history[outcome][0], self.history[outcome][-1]):
                assert self.redis.cmd("HGET", self.key(f"job:{job_id}:meta"), "finished_at") == "1700000000"
        for suffix in ["pending", "active", "delayed", "pending_cancellations"]:
            assert self.redis.cmd("EXISTS", self.key(suffix)) == 0
        return {"retained_success": len(self.history["success"]), "retained_failed": len(self.history["failed"]),
                "retained_data_records": len(data), "exact_ids_payloads_results_and_metadata_error_key_sets": True,
                "sampled_metadata_finished_at": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redis-port", type=int, default=6385)
    parser.add_argument("--old-ref", default="224b638")
    parser.add_argument("--new-ref", default="1c0bb18acd0f92ecb3fe275202f03517aef68bc5")
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--warmup", type=int, default=50)
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--sizes", default="small,default,large")
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("results.json"))
    args = parser.parse_args()
    if not 1024 <= args.redis_port <= 65535 or not 1 <= args.iterations <= 2000 or not 0 <= args.warmup <= 200 or not 2 <= args.rounds <= 6:
        parser.error("bounded iterations/warmup/rounds/port required")
    if not __debug__:
        parser.error("assertions must be enabled")
    profiles = {"small": (100, 100), "default": (1000, 10000), "large": (10000, 100000)}
    selected = args.sizes.split(",")
    if not selected or len(set(selected)) != len(selected) or any(size not in profiles for size in selected):
        parser.error("sizes must be unique small/default/large names")
    repo = Path(__file__).resolve().parents[3]
    refs = {"old": git(repo, "rev-parse", args.old_ref).strip(), "new": git(repo, "rev-parse", args.new_ref).strip()}
    redis = Redis(args.redis_port)
    owned_name = "prune-benchmark-" + uuid.uuid4().hex
    case = Case(redis, owned_name, 1, 1)
    assert not redis.scan(case.key("*")), "owned namespace collision"
    scripts = {}
    for version, ref in refs.items():
        for outcome in ["success", "failed"]:
            script, positions = extract(repo, ref, outcome)
            digest = redis.cmd("SCRIPT", "LOAD", script)
            assert digest == hashlib.sha1(script.encode()).hexdigest()
            scripts[(version, outcome)] = (digest, positions)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            (args.output.parent / f"{version}-{outcome}.lua").write_text(script)
    report = {
        "scope": "Queue pruning Lua service time; not Engine, queue worker, or blockchain TPS",
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "machine": {"platform": platform.platform(), "python": platform.python_version(),
                    "cpu_count": os.cpu_count(), "load_before": os.getloadavg()},
        "redis": {k: v for k, v in info(redis, "server").items() if k in ["redis_version", "os", "arch_bits", "process_id", "io_threads_active"]},
        "redis_persistence": {key: redis.cmd("CONFIG", "GET", key)[1] for key in ["appendonly", "save"]},
        "refs": refs, "script_sha1": {f"{version}-{outcome}": value[0] for (version, outcome), value in scripts.items()},
        "settings": {"iterations": args.iterations, "warmup": args.warmup, "rounds": args.rounds,
                     "profiles": {name: profiles[name] for name in selected}, "record_payload_bytes": len(case.payload.encode()),
                     "job_id_bytes": 36, "idempotency_mode": "Permanent", "live_queue_members": 0},
        "method": "Identical namespace/IDs/data reset per trial; populated disjoint terminal histories; one unique completion and one overflow prune per iteration. Setup and completion writes excluded from EVALSHA service time. Reversed old/new order each round. INFO commandstats EVALSHA delta supplies server mean; individual loopback round trips supply client quantiles. No Redis config/stat reset.",
        "trials": [], "status": "running",
    }
    try:
        for size in selected:
            for outcome in ["success", "failed"]:
                for round_index in range(args.rounds):
                    order = ["old", "new"] if round_index % 2 == 0 else ["new", "old"]
                    for position, version in enumerate(order):
                        case.clear()
                        case = Case(redis, owned_name, *profiles[size])
                        memory_before = int(info(redis, "memory")["used_memory"])
                        case.setup()
                        memory_populated = int(info(redis, "memory")["used_memory"])
                        digest, positions = scripts[(version, outcome)]
                        keys = case.keys(positions, outcome)
                        command = ("EVALSHA", digest, len(keys), *keys, case.limits[outcome])
                        for i in range(args.warmup):
                            case.overflow(outcome, i)
                            assert redis.cmd(*command) == 1
                        calls0, usec0 = eval_stats(redis)
                        elapsed = []
                        for i in range(args.warmup, args.warmup + args.iterations):
                            case.overflow(outcome, i)
                            start = time.perf_counter_ns()
                            deleted = redis.cmd(*command)
                            elapsed.append((time.perf_counter_ns() - start) / 1000)
                            assert deleted == 1, "each prune must delete exactly one old record"
                        calls1, usec1 = eval_stats(redis)
                        assert calls1 - calls0 == args.iterations, "another EVALSHA client interfered; discard trial"
                        reconciled = case.reconcile()
                        trial = {"profile": size, "outcome": outcome, "round": round_index + 1,
                                 "version": version, "order_position": position + 1,
                                 "server_evalsha_calls": calls1 - calls0, "server_evalsha_us": usec1 - usec0,
                                 "server_mean_us": (usec1 - usec0) / args.iterations,
                                 "client_mean_us": statistics.mean(elapsed), "client_p50_us": percentile(elapsed, .5),
                                 "client_p95_us": percentile(elapsed, .95), "client_max_us": max(elapsed),
                                 "client_samples_us": elapsed, "redis_populated_bytes_delta": memory_populated - memory_before,
                                 "reconciliation": reconciled, "load_average": os.getloadavg()}
                        report["trials"].append(trial)
                        args.output.write_text(json.dumps(report, indent=2) + "\n")
                        print(json.dumps({key: trial[key] for key in ["profile", "outcome", "round", "version", "server_mean_us", "client_p95_us"]}), flush=True)
        report["summary"] = []
        for size in selected:
            for outcome in ["success", "failed"]:
                means = {version: [t["server_mean_us"] for t in report["trials"] if t["profile"] == size and t["outcome"] == outcome and t["version"] == version] for version in refs}
                old, new = (statistics.median(means[version]) for version in ["old", "new"])
                report["summary"].append({"profile": size, "outcome": outcome, "old_server_mean_us_median": old,
                                          "new_server_mean_us_median": new, "ratio": new / old,
                                          "added_us": new - old, "old_run_means_us": means["old"], "new_run_means_us": means["new"]})
        report["status"] = "pass"
    except BaseException as exc:
        report["status"] = "failed"
        report["failure_type"] = type(exc).__name__
        report["failure"] = str(exc)
        raise
    finally:
        try:
            case.clear()
            report["namespace_cleanup_verified"] = True
        except Exception as cleanup_error:
            report["namespace_cleanup_verified"] = False
            report["cleanup_failure"] = f"{type(cleanup_error).__name__}: {cleanup_error}"
            if report["status"] != "failed":
                report["status"] = "failed"
                report["failure_type"] = "CleanupFailure"
                report["failure"] = report["cleanup_failure"]
        report["machine"]["load_after"] = os.getloadavg()
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        redis.close()
    if report["status"] != "pass":
        raise RuntimeError(report["failure"])



if __name__ == "__main__":
    main()
