#!/usr/bin/env python3
"""Single payer local Solana throughput with the real server and durable stores.

No public RPC or faucet is used. Distinguishes admitted, sent and finalized work.
"""
import argparse
import base64
from collections import Counter
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import struct
import subprocess
import tempfile
import time
import urllib.error
import uuid

from local_solana_recovery import ROOT, port, request, until, stop
from local_eoa_throughput import percentile, redis_ready, observed_rates


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-bin", type=Path, default=ROOT / "target/release/thirdweb-engine")
    parser.add_argument("--solana-bin-dir", type=Path, required=True)
    parser.add_argument("--rate", type=int, default=50)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--workers", type=int, default=100)
    parser.add_argument("--redis-fsync", choices=["always", "everysec"], default="always")
    parser.add_argument("--drain-seconds", type=int, default=180)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    assert 1 <= args.rate <= 200 and 1 <= args.seconds <= 3600 and 1 <= args.workers <= 1000
    logs = Path(tempfile.mkdtemp(prefix="engine-solana-throughput-"))
    os.chmod(logs, 0o700)
    rpc_port, redis_port, engine_port, faucet_port, gossip_port = port(True), port(), port(), port(), port()
    upstream, base = f"http://127.0.0.1:{rpc_port}", f"http://127.0.0.1:{engine_port}"
    children, streams, samples, latencies, responses = [], [], [], [], Counter()
    namespace, token = uuid.uuid4().hex, uuid.uuid4().hex + uuid.uuid4().hex
    started = time.monotonic()

    def spawn(command, name, cwd=None, child_env=None):
        stream = (logs / name).open("w")
        streams.append(stream)
        child = subprocess.Popen(command, cwd=cwd, env=child_env, stdout=stream, stderr=subprocess.STDOUT)
        children.append(child)
        return child

    def rpc(method, params):
        _, response = request(upstream, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        if "error" in response:
            raise AssertionError(f"{method}: {response['error']}")
        return response["result"]

    def sample():
        with sqlite3.connect(f"file:{logs / 'recovery.sqlite'}?mode=ro", uri=True, timeout=2) as db:
            terminal = db.execute("SELECT COUNT(*) FROM admissions WHERE state='terminal'").fetchone()[0]
            attempted = db.execute("SELECT COUNT(DISTINCT id) FROM attempts").fetchone()[0]
            admitted = db.execute("SELECT COUNT(*) FROM admissions").fetchone()[0]
        return {"seconds": round(time.monotonic() - started, 3), "admitted": admitted, "attempted": attempted, "terminal": terminal,
                "finalized_transfers": (rpc("getBalance", [recipient, {"commitment": "finalized"}])["value"] - initial_balance) // 1000}

    report = {"scope": "local Agave validator, one payer, real Engine and durable stores; no public-chain qualification",
              "offered_tps": args.rate, "duration_seconds": args.seconds, "workers": args.workers,
              "durability": f"SQLite WAL synchronous=FULL/fullfsync=ON; Redis AOF appendfsync={args.redis_fsync}",
              "engine_binary": str(args.engine_bin.resolve()),
              "engine_binary_sha256": hashlib.sha256(args.engine_bin.read_bytes()).hexdigest(), "log_directory": str(logs)}
    try:
        addresses = {}
        for name in ["payer", "recipient"]:
            path = logs / f"{name}.json"
            subprocess.run([str(args.solana_bin_dir / "solana-keygen"), "new", "--no-bip39-passphrase", "--silent", "--outfile", str(path)],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            os.chmod(path, 0o600)
            addresses[name] = subprocess.check_output([str(args.solana_bin_dir / "solana-keygen"), "pubkey", str(path)], text=True).strip()
        payer, recipient = addresses["payer"], addresses["recipient"]
        spawn([str(args.solana_bin_dir / "solana-test-validator"), "--quiet", "--reset", "--ledger", str(logs / "ledger"),
               "--rpc-port", str(rpc_port), "--faucet-port", str(faucet_port), "--gossip-port", str(gossip_port),
               "--dynamic-port-range", "25000-25200", "--bind-address", "127.0.0.1"], "validator.log")
        until(lambda: rpc("getHealth", []) == "ok", timeout=120)
        report["validator_version"] = rpc("getVersion", [])
        for address, amount in [(payer, 10_000_000_000), (recipient, 1_000_000_000)]:
            signature = rpc("requestAirdrop", [address, amount, {"commitment": "confirmed"}])
            until(lambda: (rpc("getSignatureStatuses", [[signature], {"searchTransactionHistory": True}])["value"][0] or {}).get("confirmationStatus") == "finalized")
        initial_balance = rpc("getBalance", [recipient, {"commitment": "finalized"}])["value"]
        redis_dir = logs / "redis"
        redis_dir.mkdir(mode=0o700)
        spawn([os.environ.get("REDIS_SERVER_BIN", "redis-server"), "--bind", "127.0.0.1", "--port", str(redis_port),
               "--dir", str(redis_dir), "--save", "", "--appendonly", "yes", "--appendfsync", args.redis_fsync], "redis.log")
        until(lambda: redis_ready(redis_port))
        env = os.environ.copy()
        env.pop("ENGINE_PRIVATE_KEY", None)
        env.update({"APP_ENVIRONMENT": "production", "RUST_LOG": "warn", "ENGINE_SOLANA_KEYPAIR_FILE": str(logs / "payer.json"),
                    "ENGINE_SIGNING_TOKEN": token, "APP__REDIS__URL": f"redis://127.0.0.1:{redis_port}/",
                    "APP__SERVER__HOST": "127.0.0.1", "APP__SERVER__PORT": str(engine_port),
                    "APP__RECOVERY__JOURNAL_PATH": str(logs / "recovery.sqlite"),
                    "APP__QUEUE__EXECUTION_NAMESPACE": namespace, "APP__QUEUE__LOCAL_CONCURRENCY": str(args.workers),
                    "APP__QUEUE__POLLING_INTERVAL_MS": "20", "APP__QUEUE__LEASE_DURATION_SECONDS": "600"})
        for network in ["LOCAL", "DEVNET", "MAINNET"]:
            env[f"APP__SOLANA__{network}__HTTP_URL"] = upstream
            env[f"APP__SOLANA__{network}__WS_URL"] = f"ws://127.0.0.1:{rpc_port + 1}"
        for name in ["WEBHOOK_WORKERS", "EXTERNAL_BUNDLER_SEND_WORKERS", "USEROP_CONFIRM_WORKERS", "EOA_EXECUTOR_WORKERS", "SOLANA_EXECUTOR_WORKERS"]:
            env[f"APP__QUEUE__{name}"] = str(args.workers) if name == "SOLANA_EXECUTOR_WORKERS" else "1"
        subprocess.run([str(args.engine_bin), "--initialize-recovery"], cwd=ROOT / "server", env=env, check=True, capture_output=True)
        spawn([str(args.engine_bin)], "engine.log", ROOT / "server", env)
        until(lambda: request(base + "/health")[0] == 200)
        started = time.monotonic()

        def submit(index):
            start = started + index / args.rate
            payload = {"idempotencyKey": f"{namespace}-{index}", "instructions": [{"programId": "11111111111111111111111111111111",
                       "accounts": [{"pubkey": payer, "isSigner": True, "isWritable": True}, {"pubkey": recipient, "isSigner": False, "isWritable": True}],
                       "data": base64.b64encode(struct.pack("<IQ", 2, 1000)).decode(), "encoding": "base64"}],
                       "executionOptions": {"signerAddress": payer, "chainId": "solana:local", "commitment": "finalized"}}
            try:
                status, _ = request(base + "/v1/solana/transaction", payload, {"x-engine-signing-token": token})
            except urllib.error.HTTPError as error:
                status = error.code
            except OSError:
                status = "transport_error"
            return status, (time.monotonic() - start) * 1000

        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
            for index in range(args.rate * args.seconds):
                remaining = started + index / args.rate - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)
                futures.append(pool.submit(submit, index))
                if index and index % (args.rate * 5) == 0:
                    samples.append(sample())
            report["during_load"] = sample()
            for future in futures:
                status, latency = future.result()
                responses[str(status)] += 1
                latencies.append(latency)
        report["admission_completion_seconds"] = round(time.monotonic() - started, 3)
        report["observed_rates"] = observed_rates(samples, report["during_load"])
        report["admission_responses"] = dict(responses)
        report["admission_latency_ms"] = {"p50": percentile(latencies, .5), "p95": percentile(latencies, .95), "p99": percentile(latencies, .99)}
        deadline = time.monotonic() + args.drain_seconds
        while time.monotonic() < deadline:
            final = sample()
            samples.append(final)
            if final["terminal"] == responses["202"]:
                break
            time.sleep(5)
        recipient_delta = rpc("getBalance", [recipient, {"commitment": "finalized"}])["value"] - initial_balance
        report.update({"final": final, "samples": samples, "recipient_delta_lamports": recipient_delta,
                       "outcome": "pass" if final["terminal"] == final["finalized_transfers"] == responses["202"] == args.rate * args.seconds and recipient_delta == 1000 * args.rate * args.seconds else "incomplete",
                       "journal_bytes": sum(p.stat().st_size for p in logs.glob("recovery.sqlite*"))})
    except Exception as error:
        report.update({"outcome": "error", "error": str(error)})
        raise
    finally:
        for child in reversed(children):
            stop(child)
        for stream in streams:
            stream.close()
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    if report.get("outcome") != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
