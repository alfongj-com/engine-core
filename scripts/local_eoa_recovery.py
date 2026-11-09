#!/usr/bin/env python3
"""HTTP -> Redis -> environment signer -> Anvil crash/restart regression.

Requires a built server, redis-server and anvil. Uses disposable state and the
public test key 1, funded only in the disposable local node. Never contacts a public RPC.
"""
import argparse
import concurrent.futures
import contextlib
import hashlib
import json
import os
import re
from pathlib import Path
import signal
import sqlite3
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
CREATED_CODE = "0x600160005260206000f3"
CREATE_INPUT = "0x600a600c600039600a6000f3" + CREATED_CODE[2:]

def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]

def ports(count):
    # Hold reservations together so the OS cannot return the same free port twice.
    with contextlib.ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(count)]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]

def request(url, data=None, headers=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=5) as response:
        return response.status, json.load(response)

def rpc(url, method, params):
    _, result = request(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
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
    parser.add_argument("--redis-crash", action="store_true", help="Also SIGKILL Redis and replay its appendfsync=always AOF before restarting Engine")
    parser.add_argument("--live-redis-restart", action="store_true", help="Kill Redis while Engine stays running; require a durable halt, SIGTERM shutdown, and exact-checkpoint reattach")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--revert", action="store_true", help="Use a reverting local contract; prove terminal failure and no duplicate retry")
    mode.add_argument("--create", action="store_true", help="Deploy fixed contracts through Engine; verify predicted addresses, runtime, value and duplicate retries")
    parser.add_argument("--engine-bin", type=Path, default=ROOT / "target/debug/thirdweb-engine")
    parser.add_argument("--restart-engine-bin", type=Path, help="Restart with a different binary against the same journal and Redis state")
    parser.add_argument("--rebuild-jobs-only", action="store_true", help="Rebuild only missing worker jobs; keep EOA intent/attempt projections")
    parser.add_argument("--rebuild-queues", action="store_true", help="Delete stopped EOA projections/jobs, rebuild twice from the journal, then reconcile exact original hashes")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    args.rebuild_queues = args.rebuild_queues or args.rebuild_jobs_only
    args.engine_bin = args.engine_bin.resolve(strict=True)
    args.restart_engine_bin = (args.restart_engine_bin or args.engine_bin).resolve(strict=True)
    for binary in (args.engine_bin, args.restart_engine_bin):
        assert binary.is_file() and os.access(binary, os.X_OK), "Engine binary must be an executable file"
    args.redis_crash = args.redis_crash or args.live_redis_restart
    assert 1 <= args.transactions <= 40
    redis_port, server_port, anvil_port = ports(3)
    rpc_url = f"http://127.0.0.1:{anvil_port}"
    run_id = uuid.uuid4().hex
    logs = Path(tempfile.mkdtemp(prefix="engine-eoa-recovery-"))
    token, diagnostic = uuid.uuid4().hex + uuid.uuid4().hex, uuid.uuid4().hex
    env = os.environ.copy()
    env.update({
        "APP_ENVIRONMENT": "production", "RUST_LOG": "warn", "ENGINE_PRIVATE_KEY": f"{1:064x}",
        "ENGINE_SIGNING_TOKEN": token, "APP__REDIS__URL": f"redis://127.0.0.1:{redis_port}/",
        "APP__SERVER__HOST": "127.0.0.1", "APP__SERVER__PORT": str(server_port),
        "APP__SERVER__DIAGNOSTIC_ACCESS_PASSWORD": diagnostic,
        "APP__EVM_RPC__ENDPOINTS__31337__URL": rpc_url,
        "APP__EVM_RPC__ENDPOINTS__31337__FINALITY__MODE": "depth",
        "APP__EVM_RPC__ENDPOINTS__31337__FINALITY__CONFIRMATIONS": "0",
        "APP__RECOVERY__JOURNAL_PATH": str(logs / "recovery.sqlite"),
        "APP__QUEUE__EXECUTION_NAMESPACE": run_id, "APP__QUEUE__LOCAL_CONCURRENCY": "4",
        "APP__QUEUE__POLLING_INTERVAL_MS": "20", "APP__QUEUE__LEASE_DURATION_SECONDS": "2",
    })
    for name in ["WEBHOOK_WORKERS", "EXTERNAL_BUNDLER_SEND_WORKERS", "USEROP_CONFIRM_WORKERS", "EOA_EXECUTOR_WORKERS", "SOLANA_EXECUTOR_WORKERS"]:
        env[f"APP__QUEUE__{name}"] = "1"
    headers = {"x-engine-signing-token": token}
    base = f"http://127.0.0.1:{server_port}"
    children, streams = [], []
    def spawn(command, name, cwd=None, child_env=None):
        stream = (logs / name).open("w")
        streams.append(stream)
        proc = subprocess.Popen(command, cwd=cwd, env=child_env, stdout=stream, stderr=subprocess.STDOUT)
        children.append(proc)
        return proc
    def engine(name, binary):
        proc = spawn([str(binary)], name, ROOT / "server", env)
        until(lambda: request(base + "/health")[0] == 200)
        return proc
    redis_dir = logs / "redis"
    redis_dir.mkdir(mode=0o700)
    def start_redis(name):
        return spawn([os.environ.get("REDIS_SERVER_BIN", "redis-server"), "--bind", "127.0.0.1", "--port", str(redis_port),
            "--dir", str(redis_dir), "--save", "", "--appendonly", "yes" if args.redis_crash else "no",
            "--appendfsync", "always"], name)
    report = {"scenario": "local EOA HTTP admission, crash with pending transactions, restart, mine, duplicate admission", "transactions": args.transactions, "log_directory": str(logs),
              "configured_rpc": True, "redis_crash": args.redis_crash, "live_redis_restart": args.live_redis_restart, "reverted_execution": args.revert, "contract_creation": args.create,
              "binary_upgrade": args.engine_bin != args.restart_engine_bin,
              "engine_binary_sha256": hashlib.sha256(args.engine_bin.read_bytes()).hexdigest(),
              "restart_engine_binary_sha256": hashlib.sha256(args.restart_engine_bin.read_bytes()).hexdigest()}
    started = time.monotonic()
    try:
        redis_process = start_redis("redis-before.log")
        spawn([os.environ.get("ANVIL_BIN", "anvil"), "--host", "127.0.0.1", "--port", str(anvil_port), "--chain-id", "31337", "--no-mining", "--silent"], "anvil.log")
        until(lambda: rpc(rpc_url, "eth_chainId", []) == "0x7a69")
        rpc(rpc_url, "anvil_setBalance", [FROM, hex(10**21)])
        if args.revert:
            # PUSH1 0, PUSH1 0, REVERT. Real EVM execution consumes the nonce and
            # gas but rolls back the value transfer.
            rpc(rpc_url, "anvil_setCode", [TO, "0x60006000fd"])
        initial_balance = int(rpc(rpc_url, "eth_getBalance", [TO, "latest"]), 16)
        subprocess.run([str(args.engine_bin), "--initialize-recovery"],
            cwd=ROOT / "server", env=env, check=True, capture_output=True)
        process = engine("engine-before.log", args.engine_bin)
        payloads = [{"executionOptions": {"chainId": 31337, "type": "EOA", "from": FROM, "idempotencyKey": f"{run_id}:invoice:{i}" if args.rebuild_queues else f"{run_id}-{i}"},
            "params": [{"to": TO, "value": "0x1", "data": "0x", "gasLimit": 40000 if args.revert else 21000}]} for i in range(args.transactions)]
        created_addresses = []
        if args.create:
            for nonce, payload in enumerate(payloads):
                # Exercise both gas paths. No supplied gas means Engine must
                # estimate a CREATE envelope rather than an unfinished request.
                payload["params"] = [{"to": None, "value": "0x1", "data": CREATE_INPUT}]
                if nonce % 2:
                    payload["params"][0]["gasLimit"] = 300000
                computed = subprocess.check_output([
                    os.environ.get("CAST_BIN", "cast"), "compute-address", FROM, "--nonce", str(nonce),
                ], text=True)
                matches = re.findall(r"0x[0-9a-fA-F]{40}", computed)
                assert len(matches) == 1, "Cannot derive CREATE address independently"
                created_addresses.append(matches[0].lower())
                assert rpc(rpc_url, "eth_getCode", [created_addresses[-1], "latest"]) == "0x"
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
        until(lambda: int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "pending"]), 16) == args.transactions)
        assert int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "latest"]), 16) == 0
        prior_hashes = None
        if report["binary_upgrade"] or args.rebuild_queues:
            pending = rpc(rpc_url, "eth_getBlockByNumber", ["pending", True])
            prior_hashes = {tx["hash"].lower() for tx in pending["transactions"]}
            assert len(prior_hashes) == args.transactions
        if args.rebuild_queues:
            locked = subprocess.run([str(args.restart_engine_bin), "--rebuild-queues"],
                cwd=ROOT / "server", env=env, capture_output=True, timeout=30)
            assert locked.returncode != 0 and b"already owned" in locked.stderr
            report["live_journal_owner_refused_rebuild"] = True
        if args.live_redis_restart:
            stop(redis_process, crash=True)
            def live_halted():
                assert process.poll() is None, "Engine must remain alive during Redis failure"
                with sqlite3.connect(f"file:{logs / 'recovery.sqlite'}?mode=ro", uri=True) as db:
                    return db.execute("SELECT halted,reason FROM control").fetchone() == (1, "Redis continuity unavailable")
            until(live_halted, timeout=30)
            try:
                request(base + "/v1/write/transaction", payloads[0], headers)
                raise AssertionError("Live Redis outage did not fence admission")
            except urllib.error.HTTPError as error:
                assert error.code == 503, error.code
            process.send_signal(signal.SIGTERM)
            process.wait(timeout=15)
            assert process.returncode == 0, "Engine must handle SIGTERM cleanly"
            report["live_engine_fenced"] = True
            report["sigterm_exit_code"] = process.returncode
        else:
            stop(process, crash=True)
        if args.redis_crash:
            if not args.live_redis_restart:
                stop(redis_process, crash=True)
            redis_process = start_redis("redis-after.log")
            until(lambda: subprocess.run([os.environ.get("REDIS_CLI_BIN", "redis-cli"), "-p", str(redis_port), "PING"], capture_output=True, text=True).stdout.strip() == "PONG")
            subprocess.run([str(args.restart_engine_bin), "--reattach-recovery"],
                cwd=ROOT / "server", env=env, check=True, capture_output=True)
        if args.rebuild_queues:
            cli = [os.environ.get("REDIS_CLI_BIN", "redis-cli"), "-p", str(redis_port), "--json"]
            removed = []
            patterns = [f"twmq:{run_id}_eoa_executor:*"]
            if not args.rebuild_jobs_only:
                patterns.append(f"{run_id}:eoa_executor:*")
            for pattern in patterns:
                keys = json.loads(subprocess.check_output(cli + ["KEYS", pattern], text=True))
                if keys:
                    subprocess.run(cli + ["DEL"] + keys, check=True, capture_output=True)
                    removed.extend(keys)
            assert removed
            for _ in range(2):
                rebuilt = subprocess.run([str(args.restart_engine_bin), "--rebuild-queues"],
                    cwd=ROOT / "server", env=env, check=True, capture_output=True)
            report["missing_projection_rebuilt_twice"] = True
        process = engine("engine-after.log", args.restart_engine_bin)
        if args.live_redis_restart:
            with sqlite3.connect(f"file:{logs / 'recovery.sqlite'}?mode=ro", uri=True) as db:
                assert db.execute("SELECT count(*) FROM admissions WHERE state='quarantined'").fetchone() == (0,)
                assert db.execute("SELECT namespace FROM control").fetchone() == (run_id,)
            report["reattached_without_quarantine_or_namespace_change"] = True
        rpc(rpc_url, "evm_mine", [])
        expected_transfer = 0 if args.revert or args.create else args.transactions
        until(lambda: int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "latest"]), 16) == args.transactions)
        assert int(rpc(rpc_url, "eth_getBalance", [TO, "latest"]), 16) == initial_balance + expected_transfer
        def confirmations():
            found = 0
            seen_creations = set()
            for payload in payloads:
                txid = payload["executionOptions"]["idempotencyKey"]
                _, body = request(base + f"/admin/executors/eoa/{FROM}:31337/transaction/{txid}", headers={"x-diagnostic-access-password": diagnostic})
                data = body.get("result", {}).get("transactionData")
                if data and data.get("receipt"):
                    assert data["receipt"]["status"] == ("0x0" if args.revert else "0x1"), data["receipt"]
                    if prior_hashes is not None:
                        assert data["receipt"]["transactionHash"].lower() in prior_hashes, "Upgrade replaced an original signed transaction"
                    if args.create:
                        # Concurrent admissions need not receive nonces in
                        # caller order. Derive the expected address from the
                        # actual mined transaction, independently of Engine.
                        mined = rpc(rpc_url, "eth_getTransactionByHash", [data["receipt"]["transactionHash"]])
                        nonce = int(mined["nonce"], 16)
                        assert mined["to"] is None and mined["input"] == CREATE_INPUT
                        assert int(mined["value"], 16) == 1 and 0 <= nonce < args.transactions
                        address = created_addresses[nonce]
                        assert address not in seen_creations
                        seen_creations.add(address)
                        assert data["receipt"]["contractAddress"].lower() == address
                        assert rpc(rpc_url, "eth_getCode", [address, "latest"]) == CREATED_CODE
                        assert int(rpc(rpc_url, "eth_getBalance", [address, "latest"]), 16) == 1
                    status = subprocess.check_output([os.environ.get("REDIS_CLI_BIN", "redis-cli"), "-p", str(redis_port),
                        "--raw", "HGET", f"{run_id}:eoa_executor:tx_data:{txid}", "status"], text=True).strip()
                    assert status == ("failed" if args.revert else "confirmed"), status
                    found += 1
            return found == args.transactions
        until(confirmations)
        if prior_hashes is not None:
            report["retained_transaction_hashes_verified"] = True
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(send, payloads))
        time.sleep(2)
        rpc(rpc_url, "evm_mine", [])
        balance = int(rpc(rpc_url, "eth_getBalance", [TO, "latest"]), 16)
        assert balance == initial_balance + expected_transfer, f"Recipient gained {balance - initial_balance} wei; expected {expected_transfer}"
        assert int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "pending"]), 16) == args.transactions
        for address in created_addresses:
            assert rpc(rpc_url, "eth_getCode", [address, "latest"]) == CREATED_CODE
            assert int(rpc(rpc_url, "eth_getBalance", [address, "latest"]), 16) == 1
        report.update({"outcome": "pass", "unique_chain_effects": args.transactions, "duplicate_chain_effects": 0, "unauthorized_requests_rejected": 2,
            "successful_transfers": expected_transfer, "contract_creations": len(created_addresses), "reverted_transactions": args.transactions if args.revert else 0,
            "recipient_value_received_wei": expected_transfer, "terminal_status": "failed" if args.revert else "confirmed",
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "redis_persistence": "appendonly=yes, appendfsync=always; Redis SIGKILL and AOF replay, not host power loss or failover" if args.redis_crash else "disabled; Engine process-restart test, not Redis power-loss test"})
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
