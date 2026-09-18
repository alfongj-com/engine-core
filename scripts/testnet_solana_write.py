#!/usr/bin/env python3
"""Bounded Engine transfers on Solana Devnet; dry-run unless --execute is supplied.

Every Engine RPC crosses a local policy proxy and the existing spending gateway.
Private manifests, recipient keys, logs and Redis AOF survive failure. Resume uses
the original intents and signed bytes; it never creates replacement identities.
"""
import argparse
import base64
import collections
import fcntl
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
STATE = Path.home() / ".config/engine-core"
PAYER = "BymPLiErFxJBV47sc31B7VnMoePFnACshrMx3VxkjC69"
KEYPAIR = STATE / "test-solana-keypair.json"
GATEWAY = "http://127.0.0.1:8788"
RPC = GATEWAY + "/solana-devnet"
DEVNET_GENESIS = "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG"
SYSTEM = "11111111111111111111111111111111"
MEMO = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
MAX_TRANSACTIONS = 20
MAX_RENT = 1_000_000
MAX_FEE = 10_000
MAX_RPC_CALLS = 3000
MAX_BROADCASTS = 20
MAX_BODY = 16 * 1024 * 1024
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def b58encode(value):
    number = int.from_bytes(value, "big")
    encoded = ""
    while number:
        number, digit = divmod(number, 58)
        encoded = ALPHABET[digit] + encoded
    return "1" * (len(value) - len(value.lstrip(b"\0"))) + encoded


def b58decode(value):
    number = 0
    for char in value:
        require(char in ALPHABET, "Invalid base58 character")
        number = number * 58 + ALPHABET.index(char)
    encoded = number.to_bytes((number.bit_length() + 7) // 8, "big")
    return b"\0" * (len(value) - len(value.lstrip("1"))) + encoded


def inspect_wire(encoded, plan):
    """Accept only the exact v0 transfer + unique Engine memo in the manifest.

    Signature validity is checked by Solana preflight and the finalized receipt;
    this parser independently bounds what the Engine is allowed to broadcast.
    """
    wire = base64.b64decode(encoded, validate=True)
    require(len(wire) <= 1232, "Transaction exceeds Solana packet size")
    offset = 0

    def take(size):
        nonlocal offset
        require(size >= 0 and offset + size <= len(wire), "Truncated wire transaction")
        result = wire[offset:offset + size]
        offset += size
        return result

    def shortvec():
        value = 0
        for shift in (0, 7, 14):
            byte = take(1)[0]
            value |= (byte & 127) << shift
            if not byte & 128:
                require(shift == 0 or byte != 0, "Noncanonical shortvec")
                return value
        raise RuntimeError("Oversized shortvec")

    require(shortvec() == 1, "Only the single configured payer may sign")
    signature = b58encode(take(64))
    message_start = offset
    require(take(1) == b"\x80", "Only v0 Engine messages are allowed")
    require(take(3) == bytes([1, 0, 2]), "Unexpected account permissions")
    require(shortvec() == 4, "Unexpected transaction accounts")
    accounts = [b58encode(take(32)) for _ in range(4)]
    require(accounts[0] == plan["payer"] and accounts[1] == plan["recipient"], "Wrong payer or recipient")
    require(set(accounts[2:]) == {SYSTEM, MEMO}, "Unexpected programs")
    blockhash = b58encode(take(32))
    require(shortvec() == 2, "Only transfer and Engine memo instructions are allowed")
    instructions = []
    for _ in range(2):
        program = take(1)[0]
        indices = list(take(shortvec()))
        data = take(shortvec())
        require(program < 4 and all(index < 4 for index in indices), "Bad account index")
        instructions.append((accounts[program], indices, data))
    require(shortvec() == 0 and offset == len(wire), "Lookup tables or trailing bytes are not allowed")
    transfer, memo = instructions
    require(transfer[0] == SYSTEM and transfer[1] == [0, 1], "Unexpected transfer instruction")
    require(len(transfer[2]) == 12 and transfer[2][:4] == struct.pack("<I", 2), "Not a System transfer")
    require(memo[0] == MEMO and memo[1] == [], "Unexpected memo instruction")
    text = memo[2].decode("utf-8")
    require(text.startswith("thirdweb-engine:"), "Missing Engine identity")
    intent = text.removeprefix("thirdweb-engine:")
    expected = {item["idempotencyKey"]: item for item in plan["payloads"]}
    require(intent in expected, "Unknown intent cannot be broadcast")
    expected_data = base64.b64decode(expected[intent]["instructions"][0]["data"])
    require(transfer[2] == expected_data, "Transfer amount differs from manifest")
    return {"intent": intent, "signature": signature, "blockhash": blockhash,
            "wire": encoded, "wire_sha256": hashlib.sha256(wire).hexdigest(),
            "message": base64.b64encode(wire[message_start:]).decode()}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise RuntimeError("Redirect refused")


HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


class RpcError(RuntimeError):
    def __init__(self, method, error):
        super().__init__(f"RPC {method} returned an error (provider details withheld)")
        self.code = error.get("code", -32603)


def request(url, data=None, headers=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", **(headers or {})})
    with HTTP.open(req, timeout=20) as response:
        encoded = response.read(MAX_BODY + 1)
        require(len(encoded) <= MAX_BODY, "Response too large")
        return response.status, {key.lower(): value for key, value in response.headers.items()}, json.loads(encoded)


def durable_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as stream:
        os.chmod(temporary, 0o600)
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def free_port(used):
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in used:
            used.add(port)
            return port


class Run:
    def __init__(self, directory, manifest, timeout):
        self.directory, self.manifest = directory, manifest
        self.lock = threading.RLock()
        self.deadline = time.monotonic() + timeout
        self.processes, self.logs = [], []
        self.proxy = None
        self.redis_ready = False
        self.engine = None
        self.used_ports = set()
        self.redis_port = free_port(self.used_ports)
        self.engine_port = free_port(self.used_ports)
        self.proxy_port = free_port(self.used_ports)
        self.redis_cli = os.environ.get("REDIS_CLI_BIN", "redis-cli")
        self.save()

    def save(self):
        with self.lock:
            durable_json(self.directory / "manifest.json", self.manifest)

    def check_deadline(self):
        require(time.monotonic() < self.deadline, "Run deadline exhausted; preserve state and reconcile")

    def rpc(self, method, params):
        self.check_deadline()
        with self.lock:
            used = self.manifest.get("rpc_reserved", 0)
            require(used < MAX_RPC_CALLS, "Local RPC budget exhausted")
            self.manifest["rpc_reserved"] = used + 1
            self.manifest.setdefault("rpc_methods", {})[method] = self.manifest.get("rpc_methods", {}).get(method, 0) + 1
            self.save()
        status, headers, response = request(RPC, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        require(status == 200 and headers.get("x-engine-rpc-gateway") == "1", "Expected budget gateway")
        require(response.get("jsonrpc") == "2.0" and response.get("id") == 1 and ("result" in response) != ("error" in response), "Invalid gateway RPC envelope")
        if "error" in response:
            raise RpcError(method, response["error"])
        return response["result"]

    def observe_completions(self):
        if not self.redis_ready:
            return
        identities = [item["idempotencyKey"] for item in self.manifest["payloads"]]
        results = self.redis("HMGET", f"twmq:{self.manifest['run_id']}_solana_executor:jobs:result", *identities)
        now = time.time()
        with self.lock:
            observations = self.manifest.setdefault("finalized_observed_at", {})
            changed = False
            for identity, result in zip(identities, results):
                if result is not None and identity not in observations:
                    observations[identity] = now
                    changed = True
            if changed:
                self.save()

    def wait(self, check, label, interval=0.5):
        while True:
            self.check_deadline()
            if self.manifest.get("proxy_failure"):
                raise RuntimeError(self.manifest["proxy_failure"])
            self.observe_completions()
            if check():
                return
            time.sleep(interval)

    def spawn(self, args, name, env=None, cwd=None):
        log = (self.directory / name).open("a")
        os.chmod(self.directory / name, 0o600)
        self.logs.append(log)
        process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, env=env, cwd=cwd)
        self.processes.append(process)
        self.manifest.setdefault("processes", []).append({"pid": process.pid, "log": name})
        self.save()
        return process

    def redis(self, *args):
        result = subprocess.check_output([self.redis_cli, "--json", "-p", str(self.redis_port), *map(str, args)],
                                         timeout=5, text=True, stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return json.loads(result)

    def start_redis(self):
        data = self.directory / "redis"
        data.mkdir(mode=0o700, exist_ok=True)
        self.spawn([os.environ.get("REDIS_SERVER_BIN", "redis-server"), "--bind", "127.0.0.1", "--port", str(self.redis_port),
                    "--dir", str(data), "--save", "", "--appendonly", "yes", "--appendfsync", "always"], "redis.log")
        def ready():
            try:
                return self.redis("PING") == "PONG"
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                return False
        self.wait(ready, "Redis startup")
        self.redis_ready = True
        require(self.redis("CONFIG", "GET", "appendonly")["appendonly"] == "yes", "Redis AOF required")
        require(self.redis("CONFIG", "GET", "appendfsync")["appendfsync"] == "always", "Redis synchronous AOF required")

    def start_engine(self, log="engine.log"):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("APP__", "ENGINE_"))}
        env.update({"APP_ENVIRONMENT": "production", "RUST_LOG": "info", "ENGINE_SOLANA_KEYPAIR_FILE": str(KEYPAIR),
                    "ENGINE_SIGNING_TOKEN": self.manifest["token"], "APP__SERVER__HOST": "127.0.0.1",
                    "APP__SERVER__PORT": str(self.engine_port), "APP__REDIS__URL": f"redis://127.0.0.1:{self.redis_port}/",
                    "APP__SERVER__DIAGNOSTIC_ACCESS_PASSWORD": uuid.uuid4().hex,
                    "APP__QUEUE__EXECUTION_NAMESPACE": self.manifest["run_id"],
                    "APP__QUEUE__LOCAL_CONCURRENCY": str(len(self.manifest["payloads"])),
                    "APP__QUEUE__POLLING_INTERVAL_MS": "100", "APP__QUEUE__LEASE_DURATION_SECONDS": "5"})
        for network in ("LOCAL", "DEVNET", "MAINNET"):
            env[f"APP__SOLANA__{network}__HTTP_URL"] = f"http://127.0.0.1:{self.proxy_port}/"
            env[f"APP__SOLANA__{network}__WS_URL"] = "ws://127.0.0.1:1"
        for worker in ("WEBHOOK_WORKERS", "EXTERNAL_BUNDLER_SEND_WORKERS", "USEROP_CONFIRM_WORKERS", "EOA_EXECUTOR_WORKERS", "SOLANA_EXECUTOR_WORKERS"):
            env[f"APP__QUEUE__{worker}"] = "1"
        self.engine = self.spawn([str(ROOT / "target/debug/thirdweb-engine")], log, env, ROOT / "server")
        def ready():
            require(self.engine.poll() is None, "Engine exited during startup")
            try:
                return request(f"http://127.0.0.1:{self.engine_port}/health")[0] == 200
            except (OSError, urllib.error.URLError):
                return False
        self.wait(ready, "Engine startup")

    def submit(self, payload):
        self.check_deadline()
        # Persist before HTTP admission, including when the HTTP response is lost.
        with self.lock:
            self.manifest.setdefault("admissions_attempted", [])
            if payload["idempotencyKey"] not in self.manifest["admissions_attempted"]:
                self.manifest["admissions_attempted"].append(payload["idempotencyKey"])
                self.manifest.setdefault("admitted_at", {})[payload["idempotencyKey"]] = time.time()
            self.save()
        status, _, _ = request(f"http://127.0.0.1:{self.engine_port}/v1/solana/transaction", payload,
                               {"x-engine-signing-token": self.manifest["token"]})
        require(status == 202, "Admission did not return202; inspect retained state")

    def stop(self):
        # Stop spending before closing the proxy. Keep AOF, attempts, keys and logs.
        for process in reversed(self.processes):
            stop_process(process)
        if self.proxy:
            self.proxy.close()
        for log in self.logs:
            log.close()
        self.save()


def stop_process(process, crash=False):
    if process and process.poll() is None:
        process.send_signal(signal.SIGKILL if crash else signal.SIGINT)
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


class PolicyProxy:
    def __init__(self, run):
        self.run = run
        self.hide = run.manifest["crash"] and not run.manifest.get("crash_completed", False)
        self.drop = self.hide
        self.inflight = 0
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                if (self.client_address[0] != "127.0.0.1" or self.path != "/"
                        or self.headers.get("Host") != f"127.0.0.1:{run.proxy_port}"
                        or self.headers.get("Origin") is not None
                        or self.headers.get("Sec-Fetch-Site") is not None):
                    self.send_error(403)
                    return
                try:
                    run.check_deadline()
                    length = int(self.headers.get("Content-Length", "0"))
                    require(0 < length <= 16384, "Invalid request size")
                    body = json.loads(self.rfile.read(length))
                    require(isinstance(body, dict) and body.get("jsonrpc") == "2.0", "Single RPC required")
                    method, params = body["method"], body["params"]
                    require(method in {"getLatestBlockhash", "getSignatureStatuses", "getBlockHeight", "getTransaction", "sendTransaction", "isBlockhashValid", "getVersion"}, "Engine method outside harness policy")
                    if method == "getSignatureStatuses" and owner.hide:
                        result = {"context": {"slot": 0}, "value": [None] * len(params[0])}
                    elif method == "sendTransaction":
                        result, drop = owner.broadcast(params)
                        if drop:
                            self.close_connection = True
                            self.connection.shutdown(socket.SHUT_RDWR)
                            return
                    else:
                        result = run.rpc(method, params)
                    encoded = json.dumps({"jsonrpc": "2.0", "id": body["id"], "result": result}).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)
                except RpcError as error:
                    encoded = json.dumps({"jsonrpc": "2.0", "id": body["id"], "error": {"code": error.code, "message": "Upstream RPC error (details withheld)"}}).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                except Exception as error:
                    with run.lock:
                        # Stop the experiment; a failed call may already have executed.
                        run.manifest["proxy_failure"] = f"Policy/RPC failure: {type(error).__name__}: {error}"
                        run.save()
                    self.close_connection = True

        self.server = ThreadingHTTPServer(("127.0.0.1", run.proxy_port), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def broadcast(self, params):
        run = self.run
        require(len(params) == 2 and params[1].get("encoding") == "base64" and params[1].get("maxRetries") == 0 and params[1].get("skipPreflight") is False, "Preflight and explicit node retry bound required")
        info = inspect_wire(params[0], run.manifest)
        intent = info["intent"]
        with run.lock:
            previous = run.manifest["wires"].get(intent)
            if previous:
                require(previous["wire"] == info["wire"], "Fresh signature for existing intent refused")
                require(previous["broadcasts"] < MAX_BROADCASTS, "Broadcast limit reached")
            attempt = run.redis("GET", f"{run.manifest['run_id']}:solana_tx_attempt:{intent}")
            require(attempt is not None, "Engine must persist signed bytes before sending")
            attempt = json.loads(attempt)
            require(attempt["signed_transaction"] == info["wire"] and attempt["signature"] == info["signature"], "Persisted attempt mismatch")
            if not previous:
                quote = run.rpc("getFeeForMessage", [info["message"], {"commitment": "confirmed"}])
                require(isinstance(quote.get("value"), int) and 0 < quote["value"] <= MAX_FEE, "Unexpected or unavailable transaction fee")
                previous = {key: value for key, value in info.items() if key != "message"}
                previous.update({"quoted_fee_lamports": quote["value"], "broadcasts": 0, "accepted": False})
                run.manifest["wires"][intent] = previous
            previous["broadcasts"] += 1
            self.inflight += 1
            run.save()  # durable evidence BEFORE forwarding, including response loss
        try:
            result = run.rpc("sendTransaction", params)
            require(result == info["signature"], "RPC returned another signature")
            with run.lock:
                previous["accepted"] = True
                if self.drop:
                    run.manifest["discarded_send_responses"] = run.manifest.get("discarded_send_responses", 0) + 1
                run.save()
            return result, self.drop
        finally:
            with run.lock:
                self.inflight -= 1

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def payload(intent, recipient, amount):
    return {"idempotencyKey": intent, "instructions": [{"programId": SYSTEM, "accounts": [
        {"pubkey": PAYER, "isSigner": True, "isWritable": True},
        {"pubkey": recipient, "isSigner": False, "isWritable": True}],
        "data": base64.b64encode(struct.pack("<IQ", 2, amount)).decode(), "encoding": "base64"}],
        "executionOptions": {"signerAddress": PAYER, "chainId": "solana:devnet", "commitment": "finalized", "maxBlockhashRetries": 0}}


def validate_manifest(manifest):
    require(manifest["version"] == 1 and manifest["payer"] == PAYER and manifest["gateway"] == RPC,
            "Unexpected manifest identity")
    require(len(b58decode(manifest["recipient"])) == 32 and manifest["recipient"] != PAYER,
            "Invalid recipient")
    require(manifest["run_id"].startswith("public-solana-") and len(manifest["run_id"]) == 46,
            "Invalid run namespace")
    count = len(manifest["payloads"])
    require(1 <= count <= MAX_TRANSACTIONS, "Transaction count exceeds bounds")
    require(type(manifest.get("rate", 1)) in (int, float) and 0 < manifest.get("rate", 1) <= 5,
            "Admission rate exceeds bound")
    rent = manifest["rent_exempt_minimum_lamports"]
    require(type(rent) is int and 0 < rent <= MAX_RENT, "Rent exceeds bound")
    amounts = [rent] + [1] * (count - 1)
    expected = [payload(f"{manifest['run_id']}-{i}", manifest["recipient"], amount)
                for i, amount in enumerate(amounts)]
    require(manifest["payloads"] == expected and manifest["total_transfer_lamports"] == sum(amounts),
            "Saved payloads differ from the bounded plan")
    require(type(manifest.get("rpc_reserved", 0)) is int and 0 <= manifest.get("rpc_reserved", 0) <= MAX_RPC_CALLS,
            "Invalid persisted RPC counter")
    require(set(manifest["wires"]).issubset({item["idempotencyKey"] for item in expected}),
            "Unexpected saved broadcast identity")
    for identity, saved in manifest["wires"].items():
        decoded = inspect_wire(saved["wire"], manifest)
        require(decoded["intent"] == identity and decoded["signature"] == saved["signature"]
                and type(saved["broadcasts"]) is int and 1 <= saved["broadcasts"] <= MAX_BROADCASTS,
                "Invalid persisted broadcast evidence")


def provenance():
    binary = ROOT / "target/debug/thirdweb-engine"
    digest = hashlib.sha256()
    with binary.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return {"source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "engine_binary_sha256": digest.hexdigest(),
            "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}


def gateway_counters():
    status, headers, value = request(GATEWAY + "/metrics")
    require(status == 200 and headers.get("x-engine-rpc-gateway") == "1" and value.get("accepting"), "Live budget gateway required")
    return {"methods": {key: item["count"] for key, item in value["methods"].items() if key.startswith("solana-devnet:")},
            "campaign_observed": value["budget"]["observed"]}


def verify_effects(run):
    manifest = run.manifest
    queue = f"twmq:{manifest['run_id']}_solana_executor"
    count = len(manifest["payloads"])
    def completed():
        require(run.redis("LLEN", queue + ":failed") == 0, "Engine recorded failed work; reconcile retained state")
        return run.redis("LLEN", queue + ":success") == count
    run.wait(completed, "finalized Engine queue results")
    run.observe_completions()
    require(len(manifest["wires"]) == count, "Missing broadcast identities")
    signatures = [item["signature"] for item in manifest["wires"].values()]
    require(len(set(signatures)) == count, "Two intents shared a signature")
    fees = 0
    receipts = []
    for item in manifest["payloads"]:
        identity = item["idempotencyKey"]
        wire = manifest["wires"][identity]
        result = json.loads(run.redis("HGET", queue + ":jobs:result", identity))
        require(result["signature"] == wire["signature"], "Queue/result identity differs")
        receipt = run.rpc("getTransaction", [wire["signature"], {"encoding": "json", "commitment": "finalized", "maxSupportedTransactionVersion": 0}])
        require(receipt is not None and receipt["meta"]["err"] is None, "Missing or failed finalized receipt")
        require(receipt["transaction"]["signatures"][0] == wire["signature"], "Wrong receipt signature")
        keys = receipt["transaction"]["message"]["accountKeys"]
        payer_index, recipient_index = keys.index(PAYER), keys.index(manifest["recipient"])
        amount = struct.unpack("<IQ", base64.b64decode(item["instructions"][0]["data"]))[1]
        meta = receipt["meta"]
        require(meta["postBalances"][recipient_index] - meta["preBalances"][recipient_index] == amount, "Wrong recipient transfer effect")
        require(0 < meta["fee"] <= MAX_FEE, "Fee exceeded permitted bound")
        require(meta["preBalances"][payer_index] - meta["postBalances"][payer_index] == amount + meta["fee"], "Wrong payer debit")
        fees += meta["fee"]
        receipts.append({"intent": identity, "signature": wire["signature"], "slot": receipt["slot"], "amount_lamports": amount,
                         "fee_lamports": meta["fee"], "admission_to_finalized_observed_seconds":
                         round(manifest["finalized_observed_at"][identity] - manifest["admitted_at"][identity], 3)})
    broadcasts = sum(item["broadcasts"] for item in manifest["wires"].values())
    for item in manifest["payloads"]:
        run.submit(item)
        time.sleep(1)
    time.sleep(2)
    require(sum(item["broadcasts"] for item in manifest["wires"].values()) == broadcasts, "Duplicate IDs triggered more broadcasts")
    require(not run.redis("KEYS", f"{manifest['run_id']}:solana_tx_attempt:*"), "Terminal attempts remain")
    payer_balance = run.rpc("getBalance", [PAYER, {"commitment": "finalized"}])["value"]
    recipient_balance = run.rpc("getBalance", [manifest["recipient"], {"commitment": "finalized"}])["value"]
    require(payer_balance == manifest["initial_payer_lamports"] - manifest["total_transfer_lamports"] - fees, "Payer balance accounting differs; external activity or duplicate effect")
    require(recipient_balance == manifest["initial_recipient_lamports"] + manifest["total_transfer_lamports"], "Recipient balance accounting differs")
    history = run.rpc("getSignaturesForAddress", [manifest["recipient"], {"limit": 100, "commitment": "finalized"}])
    require({item["signature"] for item in history} == set(signatures) and len(history) == count, "Recipient signature history has missing or extra effects")
    paced_times = [manifest["admitted_at"][item["idempotencyKey"]] for item in manifest["payloads"][1:]]
    paced_seconds = paced_times[-1] - paced_times[0] if len(paced_times) > 1 else None
    return {"outcome": "pass", "network": "solana-devnet", "payer": PAYER, "recipient": manifest["recipient"], "transactions": count,
            "engine_local_concurrency": count,
            "unique_finalized_effects": count, "duplicate_effects": 0, "duplicate_replay_extra_broadcasts": 0,
            "transferred_lamports": manifest["total_transfer_lamports"], "fees_lamports": fees,
            "payer_final_lamports": payer_balance, "recipient_final_lamports": recipient_balance,
            "receipts": receipts, "crash_mode": manifest["crash"], "discarded_send_responses": manifest.get("discarded_send_responses", 0),
            "wire_retransmissions": sum(item["broadcasts"] - 1 for item in manifest["wires"].values()),
            "crash_recovered_intents": count - 1 if manifest.get("crash_completed") else 0,
            "crash_scope": "Engine SIGKILL after response loss; rent bootstrap already finalized; Redis remains running",
            "rpc_reserved": manifest["rpc_reserved"], "rpc_methods": manifest["rpc_methods"],
            "provenance": manifest["provenance"], "elapsed_seconds": round(time.time() - manifest["created_at_unix"], 3),
            "requested_post_bootstrap_admissions_per_second": manifest.get("rate", 1),
            "paced_admissions": len(paced_times), "paced_admission_elapsed_seconds": round(paced_seconds, 3) if paced_seconds else None,
            "observed_admission_spacing_per_second": round((len(paced_times) - 1) / paced_seconds, 3) if paced_seconds else None,
            "bootstrap_admission_to_finalized_observed_seconds": receipts[0]["admission_to_finalized_observed_seconds"],
            "admission_profile": "First rent-bearing transfer finalized before remaining paced transfers; duplicate replays follow completion",
            "latency_scope": "HTTP admission start to local observation of Engine finalized result; polling and recovery time included",
            "redis_persistence": "appendonly yes; appendfsync always; Redis not deliberately crashed", "run_directory": str(run.directory)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--transactions", type=int, default=10)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--rate", type=float, default=1, help="Post-bootstrap admissions per second (maximum5)")
    parser.add_argument("--crash", action="store_true")
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--resume", type=Path)
    args = parser.parse_args()
    require(1 <= args.transactions <= MAX_TRANSACTIONS and 30 <= args.timeout <= 600, "Transaction/timeout limit out of range")
    require(0 < args.rate <= 5, "Rate must be positive and at most5 per second")
    require(not (args.run_dir and args.resume), "Choose a new run directory or resume")
    if not args.execute:
        print(json.dumps({"dry_run": True, "network": "solana-devnet", "transactions": args.transactions, "rate_per_second": args.rate,
                          "maximum_rent_lamports": MAX_RENT, "maximum_fee_per_transaction_lamports": MAX_FEE,
                          "maximum_rpc_calls": MAX_RPC_CALLS, "maximum_broadcasts_per_intent": MAX_BROADCASTS,
                          "payer": PAYER, "gateway": RPC, "crash": args.crash}, indent=2))
        return
    STATE.mkdir(mode=0o700, exist_ok=True)
    wallet_lock = (STATE / "solana-write.lock").open("a")
    os.chmod(STATE / "solana-write.lock", 0o600)
    fcntl.flock(wallet_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    active_path = STATE / "solana-write-active.json"
    if active_path.exists():
        unresolved = json.loads(active_path.read_text())["run_directory"]
        require(args.resume is not None and str(args.resume.expanduser().resolve()) == unresolved,
                f"Unresolved run blocks new spending; inspect and resume {unresolved}")
    binary_dir = Path(os.environ.get("SOLANA_BIN_DIR", "/tmp/engine-core-solana/solana-release/bin"))
    actual = subprocess.check_output([str(binary_dir / "solana-keygen"), "pubkey", str(KEYPAIR)], text=True).strip()
    require(actual == PAYER and KEYPAIR.stat().st_mode & 0o077 == 0, "Unexpected signer identity or unsafe key permissions")
    if args.resume:
        directory = args.resume.expanduser().resolve()
        manifest = json.loads((directory / "manifest.json").read_text())
        require(manifest["payer"] == PAYER and manifest["gateway"] == RPC and manifest["version"] == 1, "Unexpected saved run identity")
        require(manifest.get("outcome") != "pass", "This run already passed; do not repeat funded experiments")
        for prior in manifest.get("processes", []):
            try:
                os.kill(prior["pid"], 0)
            except ProcessLookupError:
                continue
            raise RuntimeError("Prior child PID is alive; inspect and stop it before resume")
        require((directory / "redis/appendonlydir").exists(), "Missing AOF; recovery cannot safely recreate queue state")
        manifest.pop("proxy_failure", None)
    else:
        directory = (args.run_dir or (STATE / "runs" / ("solana-devnet-" + uuid.uuid4().hex))).expanduser().resolve()
        require(not directory.exists(), "Refusing to overwrite a run directory")
        directory.mkdir(parents=True, mode=0o700)
        recipient_key = directory / "recipient-keypair.json"
        subprocess.run([str(binary_dir / "solana-keygen"), "new", "--no-bip39-passphrase", "--silent", "--outfile", str(recipient_key)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.chmod(recipient_key, 0o600)
        recipient = subprocess.check_output([str(binary_dir / "solana-keygen"), "pubkey", str(recipient_key)], text=True).strip()
        # The gateway does not yet allow this method. This single read is explicitly
        # permitted against the official free RPC; all Engine RPC uses the gateway.
        status, _, rent_result = request("https://api.devnet.solana.com", {"jsonrpc": "2.0", "id": 1, "method": "getMinimumBalanceForRentExemption", "params": [0, {"commitment": "finalized"}]})
        rent = rent_result.get("result")
        require(status == 200 and isinstance(rent, int) and 0 < rent <= MAX_RENT, "Unexpected minimum rent exemption")
        run_id = "public-solana-" + uuid.uuid4().hex
        amounts = [rent] + [1] * (args.transactions - 1)
        manifest = {"version": 1, "run_id": run_id, "created_at_unix": time.time(), "payer": PAYER, "recipient": recipient,
                    "gateway": RPC, "crash": args.crash, "rate": args.rate, "token": uuid.uuid4().hex + uuid.uuid4().hex,
                    "payloads": [payload(f"{run_id}-{i}", recipient, amount) for i, amount in enumerate(amounts)],
                    "rent_exempt_minimum_lamports": rent, "rent_source": "https://api.devnet.solana.com", "free_rpc_calls": 1,
                    "total_transfer_lamports": sum(amounts), "wires": {}, "rpc_reserved": 0}
    validate_manifest(manifest)
    current_provenance = provenance()
    if "provenance" in manifest:
        require(manifest["provenance"]["engine_binary_sha256"] == current_provenance["engine_binary_sha256"],
                "Engine binary changed; inspect recovery compatibility before resuming")
    else:
        manifest["provenance"] = current_provenance
    os.chmod(directory, 0o700)
    snapshot = directory / ("harness-" + current_provenance["harness_sha256"][:12] + ".py")
    snapshot.write_bytes(Path(__file__).read_bytes())
    os.chmod(snapshot, 0o600)
    durable_json(active_path, {"run_directory": str(directory)})
    run = Run(directory, manifest, args.timeout)
    before = None
    report = {"outcome": "unknown", "run_directory": str(directory)}
    try:
        before = gateway_counters()
        require(run.rpc("getGenesisHash", []) == DEVNET_GENESIS, "Only Solana Devnet is allowed")
        require(1 <= len(manifest["payloads"]) <= MAX_TRANSACTIONS, "Saved transaction count exceeds bounds")
        if "initial_payer_lamports" not in manifest:
            manifest["initial_payer_lamports"] = run.rpc("getBalance", [PAYER, {"commitment": "finalized"}])["value"]
            manifest["initial_recipient_lamports"] = run.rpc("getBalance", [manifest["recipient"], {"commitment": "finalized"}])["value"]
            require(manifest["initial_recipient_lamports"] == 0, "Expected fresh recipient")
            require(manifest["initial_payer_lamports"] >= manifest["total_transfer_lamports"] + len(manifest["payloads"]) * MAX_FEE + MAX_RENT, "Insufficient balance within reserved spending bound")
            run.save()
        run.start_redis()
        run.proxy = PolicyProxy(run)
        run.start_engine()
        queue = f"twmq:{manifest['run_id']}_solana_executor"
        # Confirm the first rent-bearing transfer before admitting one-lamport
        # transfers: concurrent Solana execution does not guarantee their order.
        first = manifest["payloads"][0]
        run.proxy.hide = False
        run.proxy.drop = False
        run.submit(first)
        run.wait(lambda: run.redis("HGET", queue + ":jobs:result", first["idempotencyKey"]) is not None, "rent-bearing transfer finalized")
        if manifest["crash"] and len(manifest["payloads"]) > 1 and not manifest.get("crash_completed"):
            run.proxy.hide = True
            run.proxy.drop = True
        paced_start = time.monotonic()
        for index, item in enumerate(manifest["payloads"][1:], 1):
            time.sleep(max(0, paced_start + index / manifest.get("rate", 1) - time.monotonic()))
            run.observe_completions()
            run.submit(item)
        if run.proxy.drop:
            run.wait(lambda: len(manifest["wires"]) == len(manifest["payloads"]) and all(item["accepted"] for item in manifest["wires"].values()), "accepted sends before crash")
            run.wait(lambda: not run.redis("KEYS", f"{manifest['run_id']}:solana_tx_lock:*") and run.proxy.inflight == 0, "storage locks released")
            before_counts = {identity: item["broadcasts"] for identity, item in manifest["wires"].items() if identity != first["idempotencyKey"]}
            manifest["before_restart_broadcast_counts"] = before_counts
            run.save()
            stop_process(run.engine, crash=True)
            run.proxy.drop = False
            run.start_engine("engine-after-crash.log")
            run.wait(lambda: all(manifest["wires"][identity]["broadcasts"] > count for identity, count in before_counts.items()), "same-wire retransmissions")
            manifest["after_restart_broadcast_counts"] = {identity: manifest["wires"][identity]["broadcasts"] for identity in before_counts}
            run.proxy.hide = False
            manifest["crash_completed"] = True
            run.save()
        report = verify_effects(run)
        after = gateway_counters()
        report["gateway_solana_method_deltas"] = {key: value - before["methods"].get(key, 0) for key, value in after["methods"].items()}
        report["gateway_delta_scope"] = "Solana network only; includes any concurrent Solana users of gateway"
        report["free_official_rpc_calls"] = manifest["free_rpc_calls"]
        manifest["outcome"] = "pass"
    except Exception as error:
        report.update({"outcome": "unknown", "reason": f"{type(error).__name__}: {error}", "recovery": "Stop and inspect retained manifest/AOF. Resume only this run; never recreate identities."})
        manifest["outcome"] = "unknown"
        raise
    finally:
        run.stop()
        durable_json(directory / "report.json", report)
        if report["outcome"] == "pass":
            active_path.unlink()
        print(json.dumps(report, indent=2))
        wallet_lock.close()


if __name__ == "__main__":
    main()
