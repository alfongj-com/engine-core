#!/usr/bin/env python3
"""Prove a reported unsigned EOA failure cannot execute after Redis loss/rebuild.

Uses an actual Engine, disposable Redis and Anvil, and public local test key 1.
The formerly reverting recipient becomes payable before rebuild. A successful
control transfer after restart proves that the worker and chain are live.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
import uuid

from local_eoa_recovery import FROM, ROOT, TO, ports, request, rpc, stop, until


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-bin", type=Path, default=ROOT / "target/debug/thirdweb-engine")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    binary = args.engine_bin.resolve(strict=True)
    assert binary.is_file() and os.access(binary, os.X_OK)
    redis_port, server_port, anvil_port = ports(3)
    base, rpc_url = f"http://127.0.0.1:{server_port}", f"http://127.0.0.1:{anvil_port}"
    namespace, token = uuid.uuid4().hex, uuid.uuid4().hex + uuid.uuid4().hex
    failed_id, control_id = f"{namespace}:rejected", f"{namespace}:control"
    control_to = "0x2222222222222222222222222222222222222222"
    logs = Path(tempfile.mkdtemp(prefix="engine-eoa-preparation-failure-"))
    journal = logs / "recovery.sqlite"
    env = os.environ.copy()
    env.update({
        "APP_ENVIRONMENT": "production", "RUST_LOG": "warn", "ENGINE_PRIVATE_KEY": f"{1:064x}",
        "ENGINE_SIGNING_TOKEN": token, "APP__REDIS__URL": f"redis://127.0.0.1:{redis_port}/",
        "APP__SERVER__HOST": "127.0.0.1", "APP__SERVER__PORT": str(server_port),
        "APP__SERVER__DIAGNOSTIC_ACCESS_PASSWORD": uuid.uuid4().hex,
        "APP__EVM_RPC__ENDPOINTS__31337__URL": rpc_url,
        "APP__EVM_RPC__ENDPOINTS__31337__FINALITY__MODE": "depth",
        "APP__EVM_RPC__ENDPOINTS__31337__FINALITY__CONFIRMATIONS": "0",
        "APP__RECOVERY__JOURNAL_PATH": str(journal),
        "APP__QUEUE__EXECUTION_NAMESPACE": namespace, "APP__QUEUE__LOCAL_CONCURRENCY": "2",
        "APP__QUEUE__POLLING_INTERVAL_MS": "20", "APP__QUEUE__LEASE_DURATION_SECONDS": "2",
    })
    for name in ["WEBHOOK_WORKERS", "EXTERNAL_BUNDLER_SEND_WORKERS", "USEROP_CONFIRM_WORKERS", "EOA_EXECUTOR_WORKERS", "SOLANA_EXECUTOR_WORKERS"]:
        env[f"APP__QUEUE__{name}"] = "1"
    headers = {"x-engine-signing-token": token}
    cli = [os.environ.get("REDIS_CLI_BIN", "redis-cli"), "-p", str(redis_port), "--json"]
    children, streams = [], []

    def spawn(command, name, child_env=None):
        stream = (logs / name).open("w")
        streams.append(stream)
        process = subprocess.Popen(command, cwd=ROOT / "server", env=child_env, stdout=stream, stderr=subprocess.STDOUT)
        children.append(process)
        return process

    def engine(name):
        process = spawn([str(binary)], name, env)
        until(lambda: request(base + "/health")[0] == 200)
        return process

    def redis(*command):
        return json.loads(subprocess.check_output(cli + list(command), text=True))

    def failed_authority():
        with sqlite3.connect(f"file:{journal}?mode=ro", uri=True) as db:
            state = db.execute("SELECT state,replay_key FROM admissions WHERE id=?", (failed_id,)).fetchone()
            attempts = db.execute("SELECT count(*) FROM attempts WHERE id=?", (failed_id,)).fetchone()[0]
            witness = db.execute("SELECT evidence FROM terminal_evidence WHERE id=? ORDER BY sequence", (failed_id,)).fetchall()
        assert state == ("terminal", None), "Reported failure must be durably terminal without replay authority"
        assert attempts == 0 and len(witness) == 1, "Rejected intent must never authorize an attempt"
        evidence = json.loads(witness[0][0])
        assert evidence["kind"] == "eoa_preparation_failure"
        return evidence

    def send(payload):
        status, body = request(base + "/v1/write/transaction", payload, headers)
        assert status == 202, body

    failed = {"executionOptions": {"chainId": 31337, "type": "EOA", "from": FROM, "idempotencyKey": failed_id},
              "params": [{"to": TO, "value": "0x1", "data": "0x"}]}
    control = {"executionOptions": {"chainId": 31337, "type": "EOA", "from": FROM, "idempotencyKey": control_id},
               "params": [{"to": control_to, "value": "0x1", "data": "0x", "gasLimit": 21000}]}
    started = time.monotonic()
    report = {"scenario": "unsigned EOA failure, Redis projection loss, offline rebuild, same-ID retry",
              "engine_binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "log_directory": str(logs)}
    try:
        spawn([os.environ.get("REDIS_SERVER_BIN", "redis-server"), "--bind", "127.0.0.1", "--port", str(redis_port),
               "--save", "", "--appendonly", "no"], "redis.log")
        until(lambda: redis("PING") == "PONG")
        spawn([os.environ.get("ANVIL_BIN", "anvil"), "--host", "127.0.0.1", "--port", str(anvil_port),
               "--chain-id", "31337", "--no-mining", "--silent"], "anvil.log")
        until(lambda: rpc(rpc_url, "eth_chainId", []) == "0x7a69")
        rpc(rpc_url, "anvil_setBalance", [FROM, hex(10**21)])
        rpc(rpc_url, "anvil_setCode", [TO, "0x60006000fd"])
        initial_balance = int(rpc(rpc_url, "eth_getBalance", [TO, "latest"]), 16)
        control_balance = int(rpc(rpc_url, "eth_getBalance", [control_to, "latest"]), 16)
        subprocess.run([str(binary), "--initialize-recovery"], cwd=ROOT / "server", env=env, check=True, capture_output=True, timeout=30)
        process = engine("engine-before.log")
        send(failed)
        key = f"{namespace}:eoa_executor:tx_data:{failed_id}"
        until(lambda: redis("HGET", key, "status") == "failed")
        original = failed_authority()
        assert int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "pending"]), 16) == 0
        stop(process, crash=True)
        # Make the formerly rejected call executable, then lose all executor
        # status/queue data while retaining the independent journal and marker.
        rpc(rpc_url, "anvil_setCode", [TO, "0x"])
        removed = 0
        for pattern in [f"{namespace}:eoa_executor:*", f"twmq:{namespace}_eoa_executor:*"]:
            keys = redis("KEYS", pattern)
            if keys:
                removed += redis("DEL", *keys)
        assert removed > 0
        for _ in range(2):
            subprocess.run([str(binary), "--rebuild-queues"], cwd=ROOT / "server", env=env, check=True, capture_output=True, timeout=30)
            assert redis("KEYS", f"{namespace}:eoa_executor:*") == [], "Rebuild must not recreate rejected work"
        assert failed_authority() == original
        process = engine("engine-after.log")
        send(failed)
        assert redis("EXISTS", key) == 0, "Terminal same-ID retry cannot create executor work"
        send(control)
        until(lambda: int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "pending"]), 16) >= 1)
        rpc(rpc_url, "evm_mine", [])
        until(lambda: redis("HGET", f"{namespace}:eoa_executor:tx_data:{control_id}", "status") == "confirmed")
        send(failed)
        assert redis("EXISTS", key) == 0
        assert failed_authority() == original
        assert int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "latest"]), 16) == 1
        assert int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "pending"]), 16) == 1
        assert int(rpc(rpc_url, "eth_getBalance", [TO, "latest"]), 16) == initial_balance
        assert int(rpc(rpc_url, "eth_getBalance", [control_to, "latest"]), 16) == control_balance + 1
        report.update({"outcome": "pass", "rejected_intent_attempts": 0, "rejected_intent_chain_effects": 0,
                       "control_transfers": 1, "offline_rebuilds": 2, "same_id_retries": 2,
                       "original_failure_retained": True, "elapsed_seconds": round(time.monotonic() - started, 3),
                       "redis_persistence": "disabled; selective projection-loss regression, not host power loss"})
    except Exception as error:
        report.update({"outcome": "fail", "error": str(error)})
        raise
    finally:
        for process in reversed(children):
            stop(process)
        for stream in streams:
            stream.close()
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
