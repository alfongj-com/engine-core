#!/usr/bin/env python3
"""Actual Engine -> Redis -> Ed25519 signer -> isolated Solana validator recovery.

The proxy discards accepted send responses and hides statuses. Engine is killed
and restarted against the same Redis; recovery must retransmit identical bytes,
then complete once historical statuses become visible. Only loopback RPCs are
used. Requires a built Engine, redis-server/redis-cli, and Solana CLI/validator.
Fresh keys and ledger live in a private temporary directory outside the repo.
"""
import argparse
import base64
import concurrent.futures
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
RESERVED_PORTS = set()


def port(pair=False):
    while True:
        with socket.socket() as first:
            first.bind(("127.0.0.1", 0))
            number = first.getsockname()[1]
            if number in RESERVED_PORTS or (pair and number + 1 in RESERVED_PORTS):
                continue
            if not pair:
                RESERVED_PORTS.add(number)
                return number
            try:
                with socket.socket() as second:
                    second.bind(("127.0.0.1", number + 1))
                RESERVED_PORTS.update([number, number + 1])
                return number
            except OSError:
                pass


def request(url, data=None, headers=None, timeout=10):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.status, json.load(response)


def until(check, timeout=90, label="condition"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except (OSError, urllib.error.URLError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass
        time.sleep(0.1)
    raise AssertionError(f"Timed out waiting for {label}; last result={last}")


def stop(proc, crash=False):
    if proc.poll() is None:
        proc.send_signal(signal.SIGKILL if crash else signal.SIGINT)
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


class FaultProxy:
    def __init__(self, upstream, rpc_port):
        self.upstream = upstream
        self.lock = threading.Lock()
        self.hide_statuses = True
        self.drop_responses = True
        self.messages = {}  # keyed by exact base64 wire bytes
        self.dropped = 0
        self.errors = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    assert 0 < length <= 65536
                    body = json.loads(self.rfile.read(length))
                    method = body["method"]
                    if method == "getSignatureStatuses":
                        assert body["params"][1]["searchTransactionHistory"] is True
                        with owner.lock:
                            hide = owner.hide_statuses
                        if hide:
                            response = {"jsonrpc": "2.0", "id": body["id"], "result": {
                                "context": {"slot": 0}, "value": [None] * len(body["params"][0])}}
                        else:
                            _, response = request(owner.upstream, body)
                    else:
                        _, response = request(owner.upstream, body)
                    if method == "sendTransaction":
                        wire = body["params"][0]
                        assert body["params"][1]["encoding"] == "base64"
                        assert body["params"][1]["maxRetries"] == 0
                        base64.b64decode(wire, validate=True)
                        with owner.lock:
                            entry = owner.messages.setdefault(wire, {"calls": 0, "signature": None})
                            entry["calls"] += 1
                            if isinstance(response.get("result"), str):
                                entry["signature"] = response["result"]
                            drop = owner.drop_responses
                            if drop:
                                owner.dropped += 1
                        if drop:
                            # Forwarding finished: the validator may execute the transaction,
                            # but Engine cannot distinguish acceptance from a failed send.
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
                        owner.errors.append(str(error))
                    self.close_connection = True

        self.server = ThreadingHTTPServer(("127.0.0.1", rpc_port), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transactions", type=int, default=12)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--rust-log", default="info", help="Engine log filter; use debug to verify credential redaction")
    parser.add_argument("--solana-bin-dir", type=Path, default=os.environ.get("SOLANA_BIN_DIR"))
    args = parser.parse_args()
    assert 1 <= args.transactions <= 40
    bindir = args.solana_bin_dir
    def solana_bin(name):
        return str(bindir / name) if bindir else name
    redis_bin = os.environ.get("REDIS_SERVER_BIN", "redis-server")
    redis_cli = os.environ.get("REDIS_CLI_BIN", "redis-cli")
    run_id = uuid.uuid4().hex
    logs = Path(tempfile.mkdtemp(prefix="engine-solana-recovery-"))
    os.chmod(logs, 0o700)
    keydir = Path(tempfile.mkdtemp(prefix="engine-solana-keys-"))
    os.chmod(keydir, 0o700)
    children, streams, proxy = [], [], None
    report = {"scenario": "Solana accepted-send response loss, Engine crash/restart, identical-byte retransmission, duplicate admission",
              "transactions": args.transactions, "log_directory": str(logs)}
    started = time.monotonic()
    rpc_port, redis_port, server_port, proxy_port, faucet_port = port(True), port(), port(), port(), port()
    gossip_port = port()
    upstream = f"http://127.0.0.1:{rpc_port}"
    base = f"http://127.0.0.1:{server_port}"

    def spawn(command, name, cwd=None, child_env=None):
        stream = (logs / name).open("w")
        streams.append(stream)
        proc = subprocess.Popen(command, cwd=cwd, env=child_env, stdout=stream, stderr=subprocess.STDOUT)
        children.append(proc)
        return proc

    def rpc(method, params):
        _, response = request(upstream, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        if "error" in response:
            raise AssertionError(f"Local validator {method}: {response['error']}")
        return response["result"]

    def redis(*command):
        output = subprocess.check_output([redis_cli, "-p", str(redis_port), "--json", *map(str, command)], text=True, timeout=5, stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return json.loads(output)

    try:
        identities = {}
        for name in ["payer", "recipient"]:
            path = keydir / f"{name}.json"
            subprocess.run([solana_bin("solana-keygen"), "new", "--no-bip39-passphrase", "--silent", "--outfile", str(path)],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            os.chmod(path, 0o600)
            identities[name] = subprocess.check_output([solana_bin("solana-keygen"), "pubkey", str(path)], text=True).strip()
        payer, recipient = identities["payer"], identities["recipient"]
        validator = spawn([solana_bin("solana-test-validator"), "--quiet", "--reset", "--ledger", str(logs / "ledger"),
            "--rpc-port", str(rpc_port), "--faucet-port", str(faucet_port), "--gossip-port", str(gossip_port),
            "--dynamic-port-range", "25000-25200", "--bind-address", "127.0.0.1"], "validator.log")
        until(lambda: rpc("getHealth", []) == "ok", timeout=120, label="isolated validator startup")
        report["validator_version"] = rpc("getVersion", [])
        spawn([redis_bin, "--bind", "127.0.0.1", "--port", str(redis_port), "--save", "", "--appendonly", "no"], "redis.log")
        until(lambda: redis("PING") == "PONG", label="Redis startup")
        for address, amount in [(payer, 10_000_000_000), (recipient, 1_000_000_000)]:
            signature = rpc("requestAirdrop", [address, amount, {"commitment": "confirmed"}])
            until(lambda: (rpc("getSignatureStatuses", [[signature], {"searchTransactionHistory": True}])["value"][0] or {}).get("confirmationStatus") == "finalized",
                  label="local faucet finalization")
        initial_payer = rpc("getBalance", [payer, {"commitment": "finalized"}])["value"]
        initial_recipient = rpc("getBalance", [recipient, {"commitment": "finalized"}])["value"]
        proxy = FaultProxy(upstream, proxy_port)
        token = uuid.uuid4().hex + uuid.uuid4().hex
        sentinel = "LOCAL_RPC_SECRET_" + uuid.uuid4().hex
        env = os.environ.copy()
        env.pop("ENGINE_PRIVATE_KEY", None)
        env.update({"APP_ENVIRONMENT": "production", "RUST_LOG": args.rust_log, "ENGINE_SOLANA_KEYPAIR_FILE": str(keydir / "payer.json"),
            "ENGINE_SIGNING_TOKEN": token, "APP__REDIS__URL": f"redis://127.0.0.1:{redis_port}/",
            "APP__SERVER__HOST": "127.0.0.1", "APP__SERVER__PORT": str(server_port),
            "APP__SERVER__DIAGNOSTIC_ACCESS_PASSWORD": uuid.uuid4().hex,
            "APP__QUEUE__EXECUTION_NAMESPACE": run_id, "APP__QUEUE__LOCAL_CONCURRENCY": "4",
            "APP__QUEUE__POLLING_INTERVAL_MS": "20", "APP__QUEUE__LEASE_DURATION_SECONDS": "5"})
        for network in ["LOCAL", "DEVNET", "MAINNET"]:
            env[f"APP__SOLANA__{network}__HTTP_URL"] = f"http://127.0.0.1:{proxy_port}/{sentinel}?apiKey={sentinel}"
            env[f"APP__SOLANA__{network}__WS_URL"] = f"ws://127.0.0.1:{rpc_port+1}"
        for name in ["WEBHOOK_WORKERS", "EXTERNAL_BUNDLER_SEND_WORKERS", "USEROP_CONFIRM_WORKERS", "EOA_EXECUTOR_WORKERS", "SOLANA_EXECUTOR_WORKERS"]:
            env[f"APP__QUEUE__{name}"] = "4" if name == "SOLANA_EXECUTOR_WORKERS" else "1"
        def engine(name):
            proc = spawn([str(ROOT / "target/debug/thirdweb-engine")], name, ROOT / "server", env)
            until(lambda: request(base + "/health")[0] == 200, label="Engine startup")
            return proc
        process = engine("engine-before.log")
        lamports = 1000
        payloads = [{"idempotencyKey": f"{run_id}-{i}", "instructions": [{"programId": "11111111111111111111111111111111",
            "accounts": [{"pubkey": payer, "isSigner": True, "isWritable": True}, {"pubkey": recipient, "isSigner": False, "isWritable": True}],
            "data": base64.b64encode(struct.pack("<IQ", 2, lamports)).decode(), "encoding": "base64"}],
            "executionOptions": {"signerAddress": payer, "chainId": "solana:local", "commitment": "confirmed"}}
            for i in range(args.transactions)]
        def send(payload):
            status, body = request(base + "/v1/solana/transaction", payload, {"x-engine-signing-token": token})
            assert status == 202, body
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(send, payloads))
        def accepted_all():
            with proxy.lock:
                return len(proxy.messages) == args.transactions and all(item["signature"] for item in proxy.messages.values())
        until(accepted_all, label="all sends accepted with responses discarded")
        # Kill between attempts: no stale storage-lock wait is needed to replay.
        until(lambda: not redis("KEYS", f"{run_id}:solana_tx_lock:*"), label="transaction locks released after unknown sends")
        stop(process, crash=True)
        attempts = {}
        for payload in payloads:
            txid = payload["idempotencyKey"]
            key = f"{run_id}:solana_tx_attempt:{txid}"
            attempt = json.loads(redis("GET", key))
            assert redis("TTL", key) == -1, "recovery evidence must not expire"
            admission = f"{run_id}:solana_admission:{txid}"
            assert redis("HGET", admission, "state") == "active"
            assert redis("TTL", admission) == -1, "unknown intent must retain admission identity"
            assert attempt["signed_transaction"] in proxy.messages
            assert attempt["signature"] == proxy.messages[attempt["signed_transaction"]]["signature"]
            attempts[txid] = attempt
        assert len({a["signature"] for a in attempts.values()}) == args.transactions
        with proxy.lock:
            before_counts = {wire: item["calls"] for wire, item in proxy.messages.items()}
            proxy.drop_responses = False
        process = engine("engine-after.log")
        def replayed_all():
            with proxy.lock:
                assert set(proxy.messages) == set(before_counts), "recovery changed a signed message"
                return all(proxy.messages[wire]["calls"] > count for wire, count in before_counts.items())
        until(replayed_all, label="identical-byte retransmission after restart")
        with proxy.lock:
            proxy.hide_statuses = False
        queue = f"twmq:{run_id}_solana_executor"
        until(lambda: redis("LLEN", queue + ":success") == args.transactions, label="all queue terminal commits")
        assert redis("LLEN", queue + ":failed") == 0
        assert not redis("KEYS", f"{run_id}:solana_tx_attempt:*"), "terminal commit did not clean attempts"
        signatures = [attempt["signature"] for attempt in attempts.values()]
        until(lambda: all((s or {}).get("confirmationStatus") == "finalized" for s in rpc("getSignatureStatuses", [signatures, {"searchTransactionHistory": True}])["value"]),
              label="finalized on-chain effects")
        fees = 0
        for txid, attempt in attempts.items():
            admission = f"{run_id}:solana_admission:{txid}"
            assert redis("HGET", admission, "state") == "completed"
            assert 0 < redis("TTL", admission) <= 86_400
            result = json.loads(redis("HGET", queue + ":jobs:result", txid))
            assert result["signature"] == attempt["signature"]
            receipt = rpc("getTransaction", [attempt["signature"], {"encoding": "json", "commitment": "finalized", "maxSupportedTransactionVersion": 0}])
            assert receipt["meta"]["err"] is None
            index = receipt["transaction"]["message"]["accountKeys"].index(recipient)
            assert receipt["meta"]["postBalances"][index] - receipt["meta"]["preBalances"][index] == lamports
            fees += receipt["meta"]["fee"]
        # Duplicate HTTP admission must preserve terminal deduplication and wire identity.
        with proxy.lock:
            sends_before_duplicate = sum(item["calls"] for item in proxy.messages.values())
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(send, payloads))
        # Remove exactly the bounded queue history that normal completion pruning
        # removes. Admission tombstones must still prevent a fresh signature.
        for txid in attempts:
            redis("SREM", queue + ":dedup", txid)
            redis("HDEL", queue + ":jobs:data", txid)
            redis("HDEL", queue + ":jobs:result", txid)
            redis("DEL", queue + f":job:{txid}:meta", queue + f":job:{txid}:errors")
            redis("LREM", queue + ":success", 0, txid)
        assert redis("LLEN", queue + ":success") == 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(send, payloads))
        time.sleep(2)
        assert redis("LLEN", queue + ":success") == 0, "pruned duplicate was re-enqueued"
        assert not redis("KEYS", f"{run_id}:solana_tx_attempt:*")
        assert rpc("getBalance", [recipient, {"commitment": "finalized"}])["value"] == initial_recipient + args.transactions * lamports
        assert rpc("getBalance", [payer, {"commitment": "finalized"}])["value"] == initial_payer - args.transactions * lamports - fees
        with proxy.lock:
            assert not proxy.errors, proxy.errors
            assert sum(item["calls"] for item in proxy.messages.values()) == sends_before_duplicate
            replay_count = sum(item["calls"] - 1 for item in proxy.messages.values())
        stop(process)
        for log in ["engine-before.log", "engine-after.log"]:
            assert sentinel not in (logs / log).read_text(), "RPC path/query credentials leaked into Engine logs"
        report.update({"outcome": "pass", "unique_chain_effects": args.transactions, "duplicate_chain_effects": 0,
            "same_wire_retransmissions": replay_count, "discarded_send_responses": proxy.dropped,
            "terminal_attempt_records_remaining": 0, "completed_admission_tombstones": args.transactions,
            "duplicates_after_queue_pruning": "no new queue jobs or RPC sends",
            "rpc_url_secret_logged": False, "rust_log": args.rust_log,
            "payer": payer, "recipient": recipient, "lamports_per_transfer": lamports, "total_transaction_fees_lamports": fees,
            "wire_sha256": sorted(hashlib.sha256(base64.b64decode(wire)).hexdigest() for wire in proxy.messages),
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "redis_persistence": "disabled; Engine process-restart test, not Redis power-loss durability"})
    except Exception as error:
        report.update({"outcome": "fail", "error": str(error)})
        raise
    finally:
        for child in reversed(children):
            stop(child)
        if proxy:
            proxy.close()
        for stream in streams:
            stream.close()
        # Delete only generated key files; retain logs/ledger for reproducing failures.
        for keyfile in keydir.iterdir():
            keyfile.unlink()
        keydir.rmdir()
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
