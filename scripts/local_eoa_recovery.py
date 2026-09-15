#!/usr/bin/env python3
"""HTTP -> Redis -> environment signer -> Anvil crash/restart regression.

Requires a built server, redis-server and anvil. Uses disposable state and the
public test key 1, funded only in the disposable local node. Never contacts a public RPC.
"""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
FROM = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
TO = "0x1111111111111111111111111111111111111111"

def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]

def request(url, data=None, headers=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=5) as response:
        return response.status, json.load(response)

def rpc(method, params):
    _, result = request("http://127.0.0.1:8545", {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if "error" in result:
        raise AssertionError(f"{method}: {result['error']}")
    return result["result"]

def until(check, timeout=45):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.1)
    raise AssertionError(f"Condition not met within {timeout}s; last result={last}")

def stop(proc, crash=False):
    if proc.poll() is None:
        proc.send_signal(signal.SIGKILL if crash else signal.SIGINT)
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transactions", type=int, default=24)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    assert 1 <= args.transactions <= 40
    # The inherited local-chain adapter uses a fixed URL. Refuse to touch an existing node.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 8545))
    redis_port, server_port = port(), port()
    run_id = uuid.uuid4().hex
    logs = Path(tempfile.mkdtemp(prefix="engine-eoa-recovery-"))
    token, diagnostic = uuid.uuid4().hex + uuid.uuid4().hex, uuid.uuid4().hex
    env = os.environ.copy()
    env.update({
        "APP_ENVIRONMENT": "production", "RUST_LOG": "warn", "ENGINE_PRIVATE_KEY": f"{1:064x}",
        "ENGINE_SIGNING_TOKEN": token, "APP__REDIS__URL": f"redis://127.0.0.1:{redis_port}/",
        "APP__SERVER__HOST": "127.0.0.1", "APP__SERVER__PORT": str(server_port),
        "APP__SERVER__DIAGNOSTIC_ACCESS_PASSWORD": diagnostic,
        "APP__QUEUE__EXECUTION_NAMESPACE": run_id, "APP__QUEUE__LOCAL_CONCURRENCY": "4",
        "APP__QUEUE__POLLING_INTERVAL_MS": "20", "APP__QUEUE__LEASE_DURATION_SECONDS": "2",
    })
    for name in ["WEBHOOK_WORKERS", "EXTERNAL_BUNDLER_SEND_WORKERS", "USEROP_CONFIRM_WORKERS", "EOA_EXECUTOR_WORKERS", "SOLANA_EXECUTOR_WORKERS"]:
        env[f"APP__QUEUE__{name}"] = "1"
    headers = {"x-engine-signing-token": token, "x-thirdweb-secret-key": "local-test"}
    base = f"http://127.0.0.1:{server_port}"
    children, streams = [], []
    def spawn(command, name, cwd=None, child_env=None):
        stream = (logs / name).open("w")
        streams.append(stream)
        proc = subprocess.Popen(command, cwd=cwd, env=child_env, stdout=stream, stderr=subprocess.STDOUT)
        children.append(proc)
        return proc
    def engine(name):
        proc = spawn([str(ROOT / "target/debug/thirdweb-engine")], name, ROOT / "server", env)
        until(lambda: request(base + "/health")[0] == 200)
        return proc
    report = {"scenario": "local EOA HTTP admission, crash with pending transactions, restart, mine, duplicate admission", "transactions": args.transactions, "log_directory": str(logs)}
    started = time.monotonic()
    try:
        spawn([os.environ.get("REDIS_SERVER_BIN", "redis-server"), "--bind", "127.0.0.1", "--port", str(redis_port), "--save", "", "--appendonly", "no"], "redis.log")
        spawn([os.environ.get("ANVIL_BIN", "anvil"), "--host", "127.0.0.1", "--port", "8545", "--chain-id", "31337", "--no-mining", "--silent"], "anvil.log")
        until(lambda: rpc("eth_chainId", []) == "0x7a69")
        rpc("anvil_setBalance", [FROM, hex(10**21)])
        initial_balance = int(rpc("eth_getBalance", [TO, "latest"]), 16)
        process = engine("engine-before.log")
        payloads = [{"executionOptions": {"chainId": 31337, "type": "EOA", "from": FROM, "idempotencyKey": f"{run_id}-{i}"},
            "params": [{"to": TO, "value": "0x1", "data": "0x", "gasLimit": 21000}]} for i in range(args.transactions)]
        # A valid signing payload with missing/wrong authentication must never reach the queue.
        for index, invalid_headers in enumerate([
            {"x-thirdweb-secret-key": "local-test"},
            {"x-thirdweb-secret-key": "local-test", "x-engine-signing-token": "wrong-token-with-at-least-32-bytes"},
        ]):
            rejected = json.loads(json.dumps(payloads[0]))
            rejected["executionOptions"]["idempotencyKey"] = f"{run_id}-unauthorized-{index}"
            try:
                request(base + "/v1/write/transaction", rejected, invalid_headers)
                raise AssertionError("Unauthenticated signing request was accepted")
            except urllib.error.HTTPError as error:
                assert error.code == 400, error.code
        def send(payload):
            status, body = request(base + "/v1/write/transaction", payload, headers)
            assert status == 202, body
            return body
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(send, payloads))
        until(lambda: int(rpc("eth_getTransactionCount", [FROM, "pending"]), 16) == args.transactions)
        assert int(rpc("eth_getTransactionCount", [FROM, "latest"]), 16) == 0
        stop(process, crash=True)
        process = engine("engine-after.log")
        rpc("evm_mine", [])
        until(lambda: int(rpc("eth_getBalance", [TO, "latest"]), 16) == initial_balance + args.transactions)
        def confirmations():
            found = 0
            for payload in payloads:
                txid = payload["executionOptions"]["idempotencyKey"]
                _, body = request(base + f"/admin/executors/eoa/{FROM}:31337/transaction/{txid}", headers={"x-diagnostic-access-password": diagnostic})
                data = body.get("result", {}).get("transactionData")
                if data and data.get("receipt"):
                    assert data["receipt"]["status"] == "0x1", data["receipt"]
                    found += 1
            return found == args.transactions
        until(confirmations)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(send, payloads))
        time.sleep(2)
        rpc("evm_mine", [])
        balance = int(rpc("eth_getBalance", [TO, "latest"]), 16)
        assert balance == initial_balance + args.transactions, f"Duplicate effects: recipient gained {balance - initial_balance} wei for {args.transactions} one-wei intents"
        assert int(rpc("eth_getTransactionCount", [FROM, "pending"]), 16) == args.transactions
        report.update({"outcome": "pass", "unique_chain_effects": args.transactions, "duplicate_chain_effects": 0, "unauthorized_requests_rejected": 2,
            "elapsed_seconds": round(time.monotonic() - started, 3), "redis_persistence": "disabled; process-restart test, not Redis power-loss test"})
    except Exception as error:
        report.update({"outcome": "fail", "error": str(error)})
        raise
    finally:
        for child in reversed(children):
            stop(child)
        for stream in streams:
            stream.close()
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
