#!/usr/bin/env python3
"""Measure one signer's real HTTP -> durable journal -> Redis -> Anvil path.

This is a local engine capacity test, not public-chain or provider qualification.
Admission, inclusion and terminal throughput are reported separately. Anvil mines
at a fixed cadence; depth models a delayed finality backlog. No public RPCs.
"""
import argparse
from collections import Counter
import concurrent.futures
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sqlite3
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import uuid

from local_eoa_recovery import FROM, TO, ROOT, ports, request, rpc, stop, until


def percentile(values, quantile):
    return round(sorted(values)[min(len(values) - 1, int(len(values) * quantile))], 3) if values else None


def redis_ready(redis_port):
    with socket.create_connection(("127.0.0.1", redis_port), timeout=1) as connection:
        connection.sendall(b"*1\r\n$4\r\nPING\r\n")
        return connection.recv(32) == b"+PONG\r\n"


def observed_rates(samples, end):
    """Use the last available minute of actual work, excluding the drain."""
    candidates = [row for row in samples if max(0, end["seconds"] - 60) <= row["seconds"] < end["seconds"]]
    if not candidates:
        return {}
    start = min(candidates, key=lambda row: row["seconds"])
    elapsed = end["seconds"] - start["seconds"]
    return {"from_seconds": start["seconds"], "to_seconds": end["seconds"], **{
        key + "_tps": round((end[key] - start[key]) / elapsed, 3)
        for key in ["admitted", "attempted", "included", "terminal", "finalized_transfers"]
        if key in start and key in end}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-bin", type=Path, default=ROOT / "target/release/thirdweb-engine")
    parser.add_argument("--rate", type=int, default=50)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--block-seconds", type=int, default=1)
    parser.add_argument("--depth", type=int, default=2)
    parser.add_argument("--max-inflight", type=int, default=1024)
    parser.add_argument("--redis-fsync", choices=["always", "everysec"], default="always")
    parser.add_argument("--drain-seconds", type=int, default=180)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    assert 1 <= args.rate <= 500 and 1 <= args.seconds <= 3600
    assert 1 <= args.block_seconds <= 60 and 0 <= args.depth <= 10000
    redis_port, engine_port, node_port, proxy_port = ports(4)
    node_url, base = f"http://127.0.0.1:{node_port}", f"http://127.0.0.1:{engine_port}"
    logs = Path(tempfile.mkdtemp(prefix="engine-eoa-throughput-"))
    os.chmod(logs, 0o700)
    namespace, token = uuid.uuid4().hex, uuid.uuid4().hex + uuid.uuid4().hex
    env = os.environ.copy()
    env.update({
        "APP_ENVIRONMENT": "production", "RUST_LOG": "warn", "ENGINE_PRIVATE_KEY": f"{1:064x}",
        "ENGINE_SIGNING_TOKEN": token, "APP__REDIS__URL": f"redis://127.0.0.1:{redis_port}/",
        "APP__SERVER__HOST": "127.0.0.1", "APP__SERVER__PORT": str(engine_port),
        "APP__EVM_RPC__ENDPOINTS__31337__URL": f"http://127.0.0.1:{proxy_port}",
        "APP__EVM_RPC__ENDPOINTS__31337__FINALITY__MODE": "depth",
        "APP__EVM_RPC__ENDPOINTS__31337__FINALITY__CONFIRMATIONS": str(args.depth),
        "APP__RECOVERY__JOURNAL_PATH": str(logs / "recovery.sqlite"),
        "APP__QUEUE__EXECUTION_NAMESPACE": namespace, "APP__QUEUE__LOCAL_CONCURRENCY": "4",
        "APP__QUEUE__POLLING_INTERVAL_MS": "20", "APP__QUEUE__LEASE_DURATION_SECONDS": "600",
        "APP__QUEUE__EOA_MAX_INFLIGHT": str(args.max_inflight),
    })
    for name in ["WEBHOOK_WORKERS", "EXTERNAL_BUNDLER_SEND_WORKERS", "USEROP_CONFIRM_WORKERS", "EOA_EXECUTOR_WORKERS", "SOLANA_EXECUTOR_WORKERS"]:
        env[f"APP__QUEUE__{name}"] = "1"
    calls, call_lock = Counter(), threading.Lock()

    class Proxy(BaseHTTPRequestHandler):
        def log_message(self, *unused):
            pass

        def do_POST(self):
            try:
                data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                with call_lock:
                    calls.update(item["method"] for item in (data if isinstance(data, list) else [data]))
                status, result = request(node_url, data)
                encoded = json.dumps(result).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
            except (OSError, ValueError):
                self.send_error(502)

    class Server(ThreadingHTTPServer):
        request_queue_size = 2048
        daemon_threads = True

    proxy = Server(("127.0.0.1", proxy_port), Proxy)
    threading.Thread(target=proxy.serve_forever, daemon=True).start()
    children, streams, samples, latencies, responses = [], [], [], [], Counter()
    sample_stop = threading.Event()
    started = time.monotonic()

    def spawn(command, name, cwd=None, child_env=None):
        stream = (logs / name).open("w")
        streams.append(stream)
        child = subprocess.Popen(command, cwd=cwd, env=child_env, stdout=stream, stderr=subprocess.STDOUT)
        children.append(child)
        return child

    def sample():
        # Read-only SQL is observability; never bypass engine writes or recovery.
        with sqlite3.connect(f"file:{logs / 'recovery.sqlite'}?mode=ro", uri=True, timeout=2) as db:
            terminal = db.execute("SELECT COUNT(*) FROM admissions WHERE state='terminal'").fetchone()[0]
            admitted = db.execute("SELECT COUNT(*) FROM admissions").fetchone()[0]
            attempted = db.execute("SELECT COUNT(DISTINCT id) FROM attempts").fetchone()[0]
        return {"seconds": round(time.monotonic() - started, 3),
                "admitted": admitted, "attempted": attempted,
                "included": int(rpc(node_url, "eth_getTransactionCount", [FROM, "latest"]), 16),
                "terminal": terminal}

    def sampler():
        while not sample_stop.wait(5):
            try:
                samples.append(sample())
            except (OSError, sqlite3.Error):
                pass

    def submit(index):
        # Include client-side queue delay, not just time once a thread starts.
        start = started + index / args.rate
        payload = {"executionOptions": {"chainId": 31337, "type": "EOA", "from": FROM,
                   "idempotencyKey": f"{namespace}-{index}"},
                   "params": [{"to": TO, "value": "0x1", "data": "0x", "gasLimit": 21000}]}
        try:
            status, _ = request(base + "/v1/write/transaction", payload, {"x-engine-signing-token": token})
        except urllib.error.HTTPError as error:
            status = error.code
        except OSError:
            status = "transport_error"
        return status, (time.monotonic() - start) * 1000

    report = {"scope": "local EOA; one signer; fixed-cadence Anvil; no public chain qualification",
              "offered_tps": args.rate, "duration_seconds": args.seconds,
              "block_seconds": args.block_seconds, "finality_depth": args.depth,
              "requested_max_inflight": args.max_inflight,
              "durability": f"SQLite WAL synchronous=FULL/fullfsync=ON; Redis AOF appendfsync={args.redis_fsync}",
              "engine_binary": str(args.engine_bin.resolve()),
              "engine_binary_sha256": hashlib.sha256(args.engine_bin.read_bytes()).hexdigest(),
              "log_directory": str(logs)}
    try:
        redis_dir = logs / "redis"
        redis_dir.mkdir(mode=0o700)
        spawn([os.environ.get("REDIS_SERVER_BIN", "redis-server"), "--bind", "127.0.0.1", "--port", str(redis_port),
               "--dir", str(redis_dir), "--save", "", "--appendonly", "yes", "--appendfsync", args.redis_fsync], "redis.log")
        until(lambda: redis_ready(redis_port))
        spawn([os.environ.get("ANVIL_BIN", "anvil"), "--host", "127.0.0.1", "--port", str(node_port), "--chain-id", "31337",
               "--block-time", str(args.block_seconds), "--gas-limit", "100000000", "--silent"], "anvil.log")
        until(lambda: rpc(node_url, "eth_chainId", []) == "0x7a69")
        rpc(node_url, "anvil_setBalance", [FROM, hex(10**24)])
        subprocess.run([str(args.engine_bin), "--initialize-recovery"], cwd=ROOT / "server", env=env, check=True, capture_output=True)
        spawn([str(args.engine_bin)], "engine.log", ROOT / "server", env)
        until(lambda: request(base + "/health")[0] == 200)
        started = time.monotonic()
        threading.Thread(target=sampler, daemon=True).start()
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
            for index in range(args.rate * args.seconds):
                remaining = started + index / args.rate - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)
                futures.append(pool.submit(submit, index))
            offered_end = sample()
            for future in futures:
                status, latency = future.result()
                responses[str(status)] += 1
                latencies.append(latency)
        samples.append(offered_end)
        report["during_load"] = offered_end
        report["observed_rates"] = observed_rates(samples, offered_end)
        report["admission_completion_seconds"] = round(time.monotonic() - started, 3)
        report["admission_responses"] = dict(responses)
        report["admission_latency_ms"] = {"p50": percentile(latencies, .5), "p95": percentile(latencies, .95), "p99": percentile(latencies, .99)}
        report["rpc_calls_during_load"] = dict(calls)
        # Release the synthetic finality delay to measure catch-up separately.
        # Continue advancing depth while draining transactions admitted late.
        deadline = time.monotonic() + args.drain_seconds
        accepted = responses["202"]
        while time.monotonic() < deadline:
            rpc(node_url, "anvil_mine", [hex(args.depth + 1), "0x1"])
            final = sample()
            if final["terminal"] == accepted and final["included"] == accepted:
                break
            time.sleep(5)
        samples.append(final)
        value = int(rpc(node_url, "eth_getBalance", [TO, "latest"]), 16)
        report.update({"final": final, "recipient_value_wei": value,
                       "outcome": "pass" if final["terminal"] == final["included"] == accepted == value == args.rate * args.seconds else "incomplete",
                       "rpc_calls_total": dict(calls), "samples": samples,
                       "journal_bytes": sum(p.stat().st_size for p in logs.glob("recovery.sqlite*"))})
    except Exception as error:
        report.update({"outcome": "error", "error": str(error)})
        raise
    finally:
        sample_stop.set()
        for child in reversed(children):
            stop(child)
        proxy.shutdown()
        proxy.server_close()
        for stream in streams:
            stream.close()
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    if report.get("outcome") != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
