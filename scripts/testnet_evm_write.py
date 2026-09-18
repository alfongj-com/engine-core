#!/usr/bin/env python3
"""Bounded public EOA qualification. Only funded testnets, via the budget gateway.

Retains the manifest, signed wires and fsync-always Redis AOF outside Git on both
success and failure. Never rerun an uncertain intent under a new request ID.
The forwarding guard independently limits destination, value, gas, fees, nonces,
wire variants and RPC count. It is test infrastructure, not an Engine fee policy.
"""
import argparse
import concurrent.futures
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import threading
import time
import urllib.request
import uuid

from local_eoa_recovery import ports, stop

ROOT = Path(__file__).resolve().parents[1]
STATE = Path.home() / ".config/engine-core"
GATEWAY = "http://127.0.0.1:8788"
CHAINS = {11155111: "sepolia", 421614: "arbitrum-sepolia", 11155420: "optimism-sepolia", 84532: "base-sepolia"}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise RuntimeError("Redirect refused")


HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


def durable(path, value):
    temp = path.with_suffix(".tmp")
    with temp.open("w") as stream:
        os.chmod(temp, 0o600)
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temp.replace(path)
    fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def request(url, data=None, headers=None):
    raw = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=raw, headers={"Content-Type": "application/json", **(headers or {})})
    with HTTP.open(req, timeout=20) as response:
        body = response.read(16 * 1024 * 1024 + 1)
        if len(body) > 16 * 1024 * 1024:
            raise RuntimeError("Response exceeds harness limit")
        return response.status, json.loads(body)


def rpc(url, method, params):
    _, result = request(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if "error" in result:
        raise RuntimeError(f"RPC {method} failed: {result['error']}")
    return result["result"]


def wait(check, timeout=300, interval=1):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return
        time.sleep(interval)
    raise RuntimeError("Timed out; retain run state and reconcile existing request IDs")


def rlp(data):
    """Decode one bounded RLP value, rejecting truncation and trailing bytes."""
    def item(offset, depth=0):
        if depth > 8 or offset >= len(data):
            raise ValueError("Invalid RLP bounds")
        tag = data[offset]
        if tag < 128:
            return data[offset:offset+1], offset+1
        if tag <= 183:
            start, size, is_list = offset+1, tag-128, False
        elif tag <= 191:
            n = tag-183
            if offset+1+n > len(data):
                raise ValueError("Truncated RLP length")
            start, size, is_list = offset+1+n, int.from_bytes(data[offset+1:offset+1+n], "big"), False
        elif tag <= 247:
            start, size, is_list = offset+1, tag-192, True
        else:
            n = tag-247
            if offset+1+n > len(data):
                raise ValueError("Truncated RLP length")
            start, size, is_list = offset+1+n, int.from_bytes(data[offset+1:offset+1+n], "big"), True
        end = start + size
        if end > len(data):
            raise ValueError("Truncated RLP payload")
        if not is_list:
            return data[start:end], end
        values = []
        while start < end:
            value, start = item(start, depth+1)
            values.append(value)
        if start != end:
            raise ValueError("Invalid RLP list boundary")
        return values, end
    value, end = item(0)
    if end != len(data):
        raise ValueError("Trailing RLP bytes")
    return value


class Guard:
    def __init__(self, manifest, directory, port, crash=False):
        self.m, self.directory = manifest, directory
        self.upstream = GATEWAY + "/" + str(manifest["chain_id"])
        self.lock = threading.Lock()
        self.calls, self.sends, self.dropped = 0, 0, 0
        self.wires, self.errors = {}, []
        self.accepted = {}
        self.hide = crash
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                try:
                    assert not self.headers.get("Origin"), "Browser requests refused"
                    assert self.headers.get("Host") == f"127.0.0.1:{port}", "Non-loopback Host refused"
                    assert self.path == "/", "Unexpected guard path"
                    size = int(self.headers.get("Content-Length", "0"))
                    assert 0 < size <= 65536
                    body = json.loads(self.rfile.read(size))
                    method = body["method"]
                    assert isinstance(body, dict) and body["jsonrpc"] == "2.0"
                    with owner.lock:
                        owner.calls += 1
                        assert owner.calls <= 20000, "Per-run RPC cap"
                    if method == "eth_sendRawTransaction":
                        owner.validate(body["params"][0])
                    with owner.lock:
                        hide = owner.hide
                    if hide and method == "eth_getTransactionReceipt":
                        response = {"jsonrpc": "2.0", "id": body["id"], "result": None}
                    else:
                        _, response = request(owner.upstream, body)
                    if method == "eth_sendRawTransaction":
                        with owner.lock:
                            owner.sends += 1
                            nonce = str(owner.nonce(body["params"][0]))
                            entry = owner.wires[nonce]
                            entry["responses"].append(response)
                            accepted_hash = owner.accepted_hash(body, response)
                            if accepted_hash is not None:
                                previous = owner.accepted.get(nonce)
                                assert previous is None or previous == accepted_hash, "One signed wire returned different transaction hashes"
                                owner.accepted[nonce] = accepted_hash
                            durable(owner.directory / "signed-wires.json", owner.wires)
                            drop_response = hide and accepted_hash is not None
                            if drop_response:
                                owner.dropped += 1
                        if drop_response:
                            self.close_connection = True
                            self.connection.shutdown(socket.SHUT_RDWR)
                            return
                    encoded = json.dumps(response).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                except Exception as error:
                    with owner.lock:
                        owner.errors.append(type(error).__name__ + ": " + str(error))
                    self.close_connection = True

        self.server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        port = self.server.server_address[1]
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @staticmethod
    def accepted_hash(body, response):
        if not isinstance(response, dict) or "error" in response:
            return None
        if response.get("jsonrpc") != "2.0" or response.get("id") != body.get("id"):
            return None
        result = response.get("result")
        if isinstance(result, str) and re.fullmatch(r"0x[0-9a-fA-F]{64}", result):
            return result.lower()
        return None

    @staticmethod
    def fields(wire):
        raw = bytes.fromhex(wire.removeprefix("0x"))
        assert len(raw) < 1024 and raw[0] == 2, "Only EIP-1559 transfers allowed"
        fields = rlp(raw[1:])
        assert isinstance(fields, list) and len(fields) == 12
        return fields

    @classmethod
    def nonce(cls, wire):
        return int.from_bytes(cls.fields(wire)[1], "big")

    def validate(self, wire):
        fields = self.fields(wire)
        chain, nonce, priority, fee, gas = [int.from_bytes(v, "big") for v in fields[:5]]
        assert chain == self.m["chain_id"]
        assert self.m["initial_nonce"] <= nonce < self.m["initial_nonce"] + self.m["transactions"]
        assert priority <= fee <= self.m["max_fee_per_gas"]
        assert 21000 <= gas <= self.m["gas_limit"]
        assert fields[5].hex() == self.m["recipient"][2:].lower()
        assert int.from_bytes(fields[6], "big") == 1 and fields[7] == b"" and fields[8] == []
        with self.lock:
            old = self.wires.get(str(nonce))
            assert old is None or old["wire"] == wire, "Replacement wire refused by test guard"
            if old is None:
                self.wires[str(nonce)] = {"wire": wire, "responses": [], "attempts": 0, "first_seen_unix": time.time()}
            entry = self.wires[str(nonce)]
            assert entry["attempts"] < 20, "Per-intent broadcast cap"
            entry["attempts"] += 1
            # Reserve before dispatch, including failures with unknown outcomes.
            durable(self.directory / "signed-wires.json", self.wires)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def main():
    if not __debug__:
        raise RuntimeError("Run without Python optimization; guard assertions are required")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain", type=int, choices=CHAINS, required=True)
    parser.add_argument("--transactions", type=int, default=10)
    parser.add_argument("--rate", type=float, default=1)
    parser.add_argument("--crash", action="store_true")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    assert 1 <= args.transactions <= 40 and 0 < args.rate <= 20
    assert not args.report.exists(), "Do not overwrite evidence"
    # Separate chain locks prevent concurrent processes from sharing nonce space.
    lockpath = STATE / f"public-evm-{args.chain}.lock"
    lockfd = os.open(str(lockpath), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.write(lockfd, str(os.getpid()).encode())
    os.close(lockfd)
    run_id = uuid.uuid4().hex
    directory = STATE / "public-runs" / run_id
    directory.mkdir(mode=0o700, parents=True)
    redis_dir = directory / "redis"
    redis_dir.mkdir(mode=0o700)
    children, streams, guard = [], [], None
    rpc_url = GATEWAY + "/" + str(args.chain)
    report = {"scenario": "public EOA response loss and Engine/Redis crash recovery" if args.crash else "public EOA transfers and duplicate admission",
              "chain_id": args.chain, "network": CHAINS[args.chain], "transactions": args.transactions, "offered_rate_tps": args.rate,
              "run_id": run_id, "private_recovery_directory": str(directory), "outcome": "incomplete",
              "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "binary_sha256": hashlib.sha256((ROOT / "target/debug/thirdweb-engine").read_bytes()).hexdigest()}
    started = time.monotonic()
    def spawn(command, name, cwd=None, env=None):
        stream = (directory / name).open("w")
        streams.append(stream)
        proc = subprocess.Popen(command, cwd=cwd, env=env, stdout=stream, stderr=subprocess.STDOUT)
        children.append(proc)
        return proc
    try:
        before = request(GATEWAY + "/metrics")[1]
        assert before["budget"]["remainingCalls"] >= 25000
        assert int(rpc(rpc_url, "eth_chainId", []), 16) == args.chain
        sender = (STATE / "test-evm-address").read_text().strip()
        recipient = "0x" + uuid.uuid4().hex + uuid.uuid4().hex[:8]
        initial_balance = int(rpc(rpc_url, "eth_getBalance", [sender, "latest"]), 16)
        initial_nonce = int(rpc(rpc_url, "eth_getTransactionCount", [sender, "latest"]), 16)
        assert int(rpc(rpc_url, "eth_getTransactionCount", [sender, "pending"]), 16) == initial_nonce, "Existing pending transactions; reconcile first"
        assert rpc(rpc_url, "eth_getCode", [recipient, "latest"]) == "0x"
        assert int(rpc(rpc_url, "eth_getBalance", [recipient, "latest"]), 16) == 0
        block = rpc(rpc_url, "eth_getBlockByNumber", ["latest", False])
        priority = max(1000000, int(rpc(rpc_url, "eth_maxPriorityFeePerGas", []), 16))
        max_fee = 3 * int(block["baseFeePerGas"], 16) + priority
        assert max_fee <= 10_000_000_000, "Fee exceeds test ceiling"
        estimate = int(rpc(rpc_url, "eth_estimateGas", [{"from": sender, "to": recipient, "value": "0x1", "data": "0x"}]), 16)
        gas = estimate * 125 // 100
        assert 21000 <= gas <= 2_000_000
        max_cost = args.transactions * (gas * max_fee + 1)
        # OP Stack separately debits L1 posting/operator fees. This reserve is an
        # experiment allocation, not a protocol-enforced cap on those components.
        extra_fee_reserve = args.transactions * 10**12 if args.chain in (11155420, 84532) else 0
        debit_allocation = max_cost + extra_fee_reserve
        assert debit_allocation < min(initial_balance // 3, 2 * 10**15), "Run exceeds fee allocation"
        manifest = {**report, "sender": sender, "recipient": recipient, "initial_nonce": initial_nonce,
                    "initial_sender_balance_wei": initial_balance, "gas_limit": gas, "max_fee_per_gas": max_fee,
                    "max_priority_fee_per_gas": priority, "max_execution_cost_wei": max_cost,
                    "additional_chain_fee_reserve_wei": extra_fee_reserve, "total_debit_allocation_wei": debit_allocation,
                    "started_at_unix": time.time()}
        payloads = [{"executionOptions": {"chainId": args.chain, "type": "EOA", "from": sender, "idempotencyKey": f"{run_id}-{i}"},
                     "params": [{"to": recipient, "value": "0x1", "data": "0x", "gasLimit": gas,
                                 "maxFeePerGas": max_fee, "maxPriorityFeePerGas": priority}]} for i in range(args.transactions)]
        manifest["payloads"] = payloads
        durable(directory / "manifest.json", manifest)
        redis_port, server_port, proxy_port = ports(3)
        token, diagnostic = uuid.uuid4().hex + uuid.uuid4().hex, uuid.uuid4().hex
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("APP__", "ENGINE_"))}
        env.update({"APP_ENVIRONMENT": "production", "RUST_LOG": "warn", "ENGINE_PRIVATE_KEY": (STATE / "test-evm-key").read_text().strip(),
            "ENGINE_SIGNING_TOKEN": token, "APP__REDIS__URL": f"redis://127.0.0.1:{redis_port}/", "APP__SERVER__HOST": "127.0.0.1",
            "APP__SERVER__PORT": str(server_port), "APP__SERVER__DIAGNOSTIC_ACCESS_PASSWORD": diagnostic,
            f"APP__EVM_RPC__ENDPOINTS__{args.chain}__URL": f"http://127.0.0.1:{proxy_port}",
            "APP__QUEUE__EXECUTION_NAMESPACE": run_id, "APP__QUEUE__LOCAL_CONCURRENCY": "4",
            "APP__QUEUE__POLLING_INTERVAL_MS": "20", "APP__QUEUE__LEASE_DURATION_SECONDS": "5"})
        for name in ["WEBHOOK_WORKERS", "EXTERNAL_BUNDLER_SEND_WORKERS", "USEROP_CONFIRM_WORKERS", "EOA_EXECUTOR_WORKERS", "SOLANA_EXECUTOR_WORKERS"]:
            env[f"APP__QUEUE__{name}"] = "1"
        # This private file contains authentication and process recovery details.
        durable(directory / "restart-config.json", {k: v for k, v in env.items() if k.startswith(("APP__", "ENGINE_"))})
        base = f"http://127.0.0.1:{server_port}"
        def redis(*command):
            raw = subprocess.check_output([os.environ.get("REDIS_CLI_BIN", "redis-cli"), "-p", str(redis_port), "--json", *map(str, command)], text=True, timeout=5)
            return json.loads(raw)
        def start_redis(name):
            proc = spawn([os.environ.get("REDIS_SERVER_BIN", "redis-server"), "--bind", "127.0.0.1", "--port", str(redis_port),
                "--dir", str(redis_dir), "--save", "", "--appendonly", "yes", "--appendfsync", "always"], name)
            for _ in range(100):
                try:
                    if redis("PING") == "PONG":
                        return proc
                except (subprocess.CalledProcessError, ValueError):
                    pass
                time.sleep(.1)
            raise RuntimeError("Redis startup failed")
        def engine(name):
            proc = spawn([str(ROOT / "target/debug/thirdweb-engine")], name, ROOT / "server", env)
            for _ in range(200):
                try:
                    if request(base + "/health")[0] == 200:
                        return proc
                except OSError:
                    pass
                assert proc.poll() is None, "Engine exited"
                time.sleep(.1)
            raise RuntimeError("Engine startup failed")
        redis_process = start_redis("redis-before.log")
        guard = Guard(manifest, directory, proxy_port, args.crash)
        process = engine("engine-before.log")
        submitted, receipts, latencies = {}, {}, {}
        def send(payload, record=False):
            txid = payload["executionOptions"]["idempotencyKey"]
            start = time.monotonic()
            if record:
                submitted[txid] = start
            code, body = request(base + "/v1/write/transaction", payload, {"x-engine-signing-token": token})
            assert code == 202, body
            return time.monotonic() - start
        def collect():
            assert not guard.errors, guard.errors
            for payload in payloads:
                txid = payload["executionOptions"]["idempotencyKey"]
                if txid in receipts or txid not in submitted:
                    continue
                _, body = request(base + f"/admin/executors/eoa/{sender}:{args.chain}/transaction/{txid}", headers={"x-diagnostic-access-password": diagnostic})
                data = body.get("result", {}).get("transactionData")
                if data and data.get("receipt"):
                    receipt = data["receipt"]
                    assert receipt["status"] == "0x1", receipt
                    receipts[txid] = receipt
                    latencies[txid] = time.monotonic() - submitted[txid]
            return len(receipts) == args.transactions
        admission_latencies = []
        admission_offsets = []
        send_start = time.monotonic()
        for i, payload in enumerate(payloads):
            delay = send_start + i / args.rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            admission_offsets.append(time.monotonic() - send_start)
            admission_latencies.append(send(payload, record=True))
            if not args.crash:
                collect()
        admission_seconds = time.monotonic() - send_start
        if args.crash:
            def all_sends_accepted():
                with guard.lock:
                    assert not guard.errors, guard.errors
                    return len(guard.accepted) == args.transactions
            wait(all_sends_accepted, timeout=90)
            stop(process, crash=True)
            stop(redis_process, crash=True)
            with guard.lock:
                guard.hide = False
            redis_process = start_redis("redis-after.log")
            process = engine("engine-after.log")
        wait(collect)
        completion_seconds = time.monotonic() - send_start
        hashes = {r["transactionHash"] for r in receipts.values()}
        assert len(hashes) == args.transactions
        if args.crash:
            assert {value.lower() for value in hashes} == set(guard.accepted.values()), "Recovered receipts differ from the sends accepted before the crash"
        verified, execution_fees, l1_fees = [], 0, 0
        nonces = set()
        for txid, receipt in receipts.items():
            txhash = receipt["transactionHash"]
            independent = rpc(rpc_url, "eth_getTransactionReceipt", [txhash])
            tx = rpc(rpc_url, "eth_getTransactionByHash", [txhash])
            assert independent["status"] == "0x1" and independent["blockHash"] == receipt["blockHash"]
            assert tx["hash"] == txhash and tx["from"].lower() == sender.lower() and tx["to"].lower() == recipient.lower()
            assert int(tx["chainId"], 16) == args.chain and int(tx["value"], 16) == 1 and tx["input"] == "0x"
            nonces.add(int(tx["nonce"], 16))
            execution_fees += int(independent["gasUsed"], 16) * int(independent["effectiveGasPrice"], 16)
            l1_fees += int(independent.get("l1Fee", "0x0"), 16)
            verified.append({"request_id": txid, "transaction": tx, "receipt": independent, "engine_receipt_observed_seconds": round(latencies[txid], 3)})
        assert nonces == set(range(initial_nonce, initial_nonce + args.transactions))
        assert int(rpc(rpc_url, "eth_getBalance", [recipient, "latest"]), 16) == args.transactions
        final_balance = int(rpc(rpc_url, "eth_getBalance", [sender, "latest"]), 16)
        actual_fees = initial_balance - final_balance - args.transactions
        assert execution_fees + l1_fees <= actual_fees <= debit_allocation, "Fee reconciliation outside test allocation"
        def empty():
            _, body = request(base + f"/admin/executors/eoa/{sender}:{args.chain}/state", headers={"x-diagnostic-access-password": diagnostic})
            return all(body["result"][key] == 0 for key in ["pendingCount", "submittedCount", "borrowedCount", "recycledNoncesCount"])
        wait(empty, timeout=30)
        with guard.lock:
            sends_before = guard.sends
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(send, payloads))
        time.sleep(5)
        assert empty() and not guard.errors, guard.errors
        assert guard.sends == sends_before, "Duplicate requests triggered new broadcasts"
        assert int(rpc(rpc_url, "eth_getTransactionCount", [sender, "pending"]), 16) == initial_nonce + args.transactions
        assert int(rpc(rpc_url, "eth_getBalance", [recipient, "latest"]), 16) == args.transactions
        assert int(rpc(rpc_url, "eth_getBalance", [sender, "latest"]), 16) == final_balance
        # Two agreeing receipts can both describe an orphaned block. Recheck the
        # canonical hash by block number after duplicate admission has drained.
        receipt_blocks = {(item["receipt"]["blockNumber"], item["receipt"]["blockHash"])
                          for item in verified}
        for block_number, block_hash in receipt_blocks:
            block = rpc(rpc_url, "eth_getBlockByNumber", [block_number, False])
            assert block is not None and block["hash"] == block_hash, "Receipt block is no longer canonical"
        report.update({"outcome": "pass", "sender": sender, "recipient": recipient, "unique_chain_effects": args.transactions,
            "duplicate_chain_effects": 0, "duplicate_requests": args.transactions, "duplicate_rpc_sends": 0,
            "initial_nonce": initial_nonce, "final_nonce": initial_nonce + args.transactions,
            "initial_sender_balance_wei": initial_balance, "final_sender_balance_wei": final_balance,
            "actual_fees_wei": actual_fees, "receipt_execution_fees_wei": execution_fees, "receipt_l1_fees_wei": l1_fees,
            "other_fee_components_wei": actual_fees - execution_fees - l1_fees,
            "additional_chain_fee_reserve_wei": extra_fee_reserve, "total_debit_allocation_wei": debit_allocation,
            "gas_limit": gas, "max_fee_per_gas": max_fee, "admission_seconds": round(admission_seconds, 3),
            "completion_seconds": round(completion_seconds, 3), "batch_completion_tps": round(args.transactions / completion_seconds, 3),
            "admission_latency_ms": [round(t * 1000, 3) for t in admission_latencies],
            "admission_start_offsets_seconds": [round(t, 4) for t in admission_offsets],
            "maximum_schedule_lag_ms": round(max(t - i / args.rate for i, t in enumerate(admission_offsets)) * 1000, 3),
            "confirmed_transactions": verified, "discarded_send_responses": guard.dropped,
            "unique_upstream_accepted_sends": len(guard.accepted),
            "actual_rpc_sends": guard.sends, "unique_signed_wires": len(guard.wires),
            "redis_persistence": "appendonly=yes, appendfsync=always; process crash tests do not prove failover/power-loss safety",
            "confirmation_scope": "successful receipt and matching canonical block observed; not long-term finality or reorg qualification",
            "workload": "1 wei EIP-1559 transfers; gas and fees precomputed, guard blocks replacements; one signer; debug Engine"})
    except Exception as error:
        report.update({"outcome": "fail", "error": type(error).__name__ + ": " + str(error)})
        raise
    finally:
        for proc in reversed(children):
            stop(proc)
        if guard:
            guard.close()
            report["guard_errors"] = guard.errors
        for stream in streams:
            stream.close()
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        try:
            after = request(GATEWAY + "/metrics")[1]
            report["budget_after"] = after["budget"]
            report["rpc_methods"] = {key: {field: value[field] - before.get("methods", {}).get(key, {}).get(field, 0)
                for field in ["count", "httpErrors", "rpcErrors", "totalMs"]} for key, value in after["methods"].items() if key.startswith(str(args.chain) + ":")}
            report["rpc_calls"] = sum(m["count"] for m in report["rpc_methods"].values())
            report["estimated_rpc_cost_usd"] = report["rpc_calls"] * .000006
        except Exception:
            report["metrics_unavailable"] = True
        durable(directory / "report.json", report)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        durable(args.report, report)
        if report["outcome"] == "pass" or guard is None or not guard.wires:
            lockpath.unlink()
        else:
            # A dead process does not mean an absent on-chain effect. Block a new
            # run until these exact nonces/wires have been reconciled deliberately.
            report["reconciliation_lock_retained"] = str(lockpath)
            durable(directory / "report.json", report)
            durable(args.report, report)
        print(json.dumps({k: v for k, v in report.items() if k not in ["confirmed_transactions", "rpc_methods", "admission_latency_ms"]}, indent=2))


if __name__ == "__main__":
    main()
