#!/usr/bin/env python3
"""Reconcile a completed public EVM run whose harness failed after inclusion.

Restores its existing Redis AOF and original Engine configuration, then repeats
only its saved idempotent requests. Every outbound broadcast is rejected. A
failure retains the chain lock and all recovery evidence; no new intent is made.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import threading
import time
from urllib.parse import urlparse

import testnet_evm_write as evm
from local_eoa_recovery import stop


READ_METHODS = {
    "eth_chainId", "eth_getBalance", "eth_getTransactionCount", "eth_getCode",
    "eth_getBlockByNumber", "eth_getTransactionReceipt", "eth_getTransactionByHash",
    "eth_feeHistory", "eth_gasPrice", "eth_maxPriorityFeePerGas", "eth_estimateGas",
}


class ReadBudget:
    """Count/reserve each paid read before dispatch, including ambiguous failures."""
    def __init__(self, chain, limit, request):
        self.chain, self.limit, self.original = chain, limit, request
        self.count = 0
        self.inflight = 0
        self.lock = threading.Condition()

    def request(self, url, data=None, headers=None):
        paid = url.startswith(evm.GATEWAY + "/") and url != evm.GATEWAY + "/metrics"
        if paid:
            if url != evm.GATEWAY + "/" + str(self.chain) or not isinstance(data, dict) or data.get("method") not in READ_METHODS:
                raise RuntimeError("Reconciliation permits only read methods on its original chain")
            with self.lock:
                if self.count >= self.limit:
                    raise RuntimeError("Reconciliation RPC read budget exhausted")
                self.count += 1
                self.inflight += 1
        try:
            return self.original(url, data, headers)
        finally:
            if paid:
                with self.lock:
                    self.inflight -= 1
                    self.lock.notify_all()

    def drain(self):
        with self.lock:
            return self.lock.wait_for(lambda: self.inflight == 0, timeout=25)


class NoBroadcastGuard(evm.Guard):
    def __init__(self, *args, **kwargs):
        self.refused_broadcasts = 0
        super().__init__(*args, **kwargs)

    def validate(self, _wire):
        with self.lock:
            self.refused_broadcasts += 1
        raise RuntimeError("Reconciliation refuses every broadcast; original signed evidence retained")


def loopback_port(url, scheme):
    parsed = urlparse(url)
    if parsed.scheme != scheme or parsed.hostname != "127.0.0.1" or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise RuntimeError("Saved service URL is not an uncredentialed loopback endpoint")
    if parsed.path not in ("", "/") or not parsed.port:
        raise RuntimeError("Unexpected saved service path or port")
    return parsed.port


def assert_ports_free(ports):
    held = []
    try:
        for port in ports:
            sock = socket.socket()
            held.append(sock)
            sock.bind(("127.0.0.1", port))
    finally:
        for sock in held:
            sock.close()


def file_hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise RuntimeError("Run without Python optimization; verification assertions are required")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state-dir", type=Path, default=evm.STATE)
    parser.add_argument("--max-rpc-calls", type=int, default=200)
    parser.add_argument("--redis-server", default=os.environ.get("REDIS_SERVER_BIN", "redis-server"))
    parser.add_argument("--redis-cli", default=os.environ.get("REDIS_CLI_BIN", "redis-cli"))
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    assert re.fullmatch(r"[0-9a-f]{32}", args.run_id)
    assert 50 <= args.max_rpc_calls <= 1000 and not args.report.exists()
    directory = args.state_dir / "public-runs" / args.run_id
    manifest = json.loads((directory / "manifest.json").read_text())
    original = json.loads((directory / "report.json").read_text())
    wires = json.loads((directory / "signed-wires.json").read_text())
    saved_env = json.loads((directory / "restart-config.json").read_text())
    chain, count = manifest["chain_id"], manifest["transactions"]
    assert chain in evm.CHAINS and 1 <= count <= 40
    assert manifest["run_id"] == args.run_id and original["outcome"] == "fail"
    assert saved_env["APP__QUEUE__EXECUTION_NAMESPACE"] == args.run_id
    assert saved_env["APP__SERVER__HOST"] == "127.0.0.1"
    redis_port = loopback_port(saved_env["APP__REDIS__URL"], "redis")
    proxy_port = loopback_port(saved_env[f"APP__EVM_RPC__ENDPOINTS__{chain}__URL"], "http")
    server_port = int(saved_env["APP__SERVER__PORT"])
    assert 1 <= server_port <= 65535 and len({redis_port, proxy_port, server_port}) == 3
    assert_ports_free([redis_port, proxy_port, server_port])
    binary = evm.ROOT / "target/debug/thirdweb-engine"
    assert file_hash(binary) == manifest["binary_sha256"], "Use the original qualified binary"
    chain_lock = args.state_dir / f"public-evm-{chain}.lock"
    original_lock = chain_lock.read_bytes()
    # An active previous harness must finish or be reconciled by its owner first.
    previous_pid = int(original_lock)
    try:
        os.kill(previous_pid, 0)
    except ProcessLookupError:
        pass
    else:
        raise RuntimeError("Original harness PID is still active; refusing overlap")
    reconcile_lock = directory / "reconciliation.lock"
    fd = os.open(str(reconcile_lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.write(fd, str(os.getpid()).encode())
    os.close(fd)
    work = directory / ("reconciliation-" + str(time.time_ns()))
    work.mkdir(mode=0o700)
    original_hashes = {name: file_hash(directory / name) for name in ["manifest.json", "report.json", "signed-wires.json"]}
    children, streams, guard = [], [], None
    budget = ReadBudget(chain, args.max_rpc_calls, evm.request)
    # The existing guard and helpers resolve this function at call time. All their
    # gateway traffic therefore shares this process's single pre-dispatch budget.
    evm.request = budget.request
    report = {"scenario": "restore existing EVM AOF and reconcile original IDs without broadcasts",
              "run_id": args.run_id, "chain_id": chain, "network": evm.CHAINS[chain],
              "transactions": count, "outcome": "incomplete", "original_outcome": original["outcome"],
              "original_error": original.get("error"), "original_evidence_sha256": original_hashes,
              "source_commit": manifest["source_commit"], "binary_sha256": manifest["binary_sha256"],
              "confirmation_scope": "successful receipts rechecked against current canonical blocks; not long-term finality",
              "rpc_read_limit": args.max_rpc_calls}
    started = time.monotonic()

    def spawn(command, name, cwd=None, env=None):
        stream = (work / name).open("w")
        streams.append(stream)
        child = subprocess.Popen(command, cwd=cwd, env=env, stdout=stream, stderr=subprocess.STDOUT)
        children.append(child)
        return child

    def redis(*command):
        result = subprocess.check_output([args.redis_cli, "-p", str(redis_port), "--json", *command], text=True, timeout=5, stderr=subprocess.DEVNULL)
        return json.loads(result)

    try:
        assert evm.request(evm.GATEWAY + "/metrics")[1]["budget"]["remainingCalls"] >= args.max_rpc_calls
        url = evm.GATEWAY + "/" + str(chain)
        assert int(evm.rpc(url, "eth_chainId", []), 16) == chain
        sender, recipient = manifest["sender"], manifest["recipient"]
        initial_nonce = manifest["initial_nonce"]
        expected_nonces = set(range(initial_nonce, initial_nonce + count))
        assert {int(n) for n in wires} == expected_nonces
        assert len(manifest["payloads"]) == count
        verified, hashes, execution_fees, l1_fees = [], set(), 0, 0
        for nonce, wire in sorted(wires.items(), key=lambda pair: int(pair[0])):
            accepted = {response["result"].lower() for response in wire["responses"]
                        if isinstance(response, dict) and "error" not in response
                        and isinstance(response.get("result"), str)
                        and re.fullmatch(r"0x[0-9a-fA-F]{64}", response["result"])}
            assert len(accepted) == 1, "Original accepted hash is missing or inconsistent"
            tx_hash = accepted.pop()
            assert evm.Guard.nonce(wire["wire"]) == int(nonce)
            receipt = evm.rpc(url, "eth_getTransactionReceipt", [tx_hash])
            tx = evm.rpc(url, "eth_getTransactionByHash", [tx_hash])
            assert receipt and tx and receipt["status"] == "0x1"
            assert receipt["transactionHash"].lower() == tx_hash == tx["hash"].lower()
            assert tx["blockHash"] == receipt["blockHash"] and tx["blockNumber"] == receipt["blockNumber"]
            assert tx["from"].lower() == sender.lower() and tx["to"].lower() == recipient.lower()
            assert int(tx["chainId"], 16) == chain and int(tx["nonce"], 16) == int(nonce)
            assert int(tx["value"], 16) == 1 and tx["input"] == "0x"
            assert int(tx["gas"], 16) <= manifest["gas_limit"] and int(tx["maxFeePerGas"], 16) <= manifest["max_fee_per_gas"]
            execution_fees += int(receipt["gasUsed"], 16) * int(receipt["effectiveGasPrice"], 16)
            l1_fees += int(receipt.get("l1Fee", "0x0"), 16)
            verified.append({"nonce": int(nonce), "transaction": tx, "receipt": receipt})
            hashes.add(tx_hash)
        assert len(hashes) == count

        def balances():
            return {"sender": int(evm.rpc(url, "eth_getBalance", [sender, "latest"]), 16),
                    "recipient": int(evm.rpc(url, "eth_getBalance", [recipient, "latest"]), 16),
                    "latest_nonce": int(evm.rpc(url, "eth_getTransactionCount", [sender, "latest"]), 16),
                    "pending_nonce": int(evm.rpc(url, "eth_getTransactionCount", [sender, "pending"]), 16)}

        before = balances()
        assert before["recipient"] == count and before["latest_nonce"] == before["pending_nonce"] == initial_nonce + count
        actual_fees = manifest["initial_sender_balance_wei"] - before["sender"] - count
        assert actual_fees == execution_fees + l1_fees, "Additional fee components require explicit reconciliation"
        # This is a reconciliation check against the originally intended reserve,
        # not a retroactive assertion that execution maxFee capped L1 fees.
        assert actual_fees < min(manifest["initial_sender_balance_wei"] // 3, 2 * 10**15)
        report.update({"sender": sender, "recipient": recipient, "initial_nonce": initial_nonce,
                       "actual_fees_wei": actual_fees, "receipt_execution_fees_wei": execution_fees,
                       "receipt_l1_fees_wei": l1_fees, "other_fee_components_wei": 0,
                       "original_execution_ceiling_wei": manifest["max_execution_cost_wei"],
                       "fee_failure_cause": "The harness compared total debit including L1 fees to an execution-only gas-times-fee bound.",
                       "chain_before_duplicate": before, "confirmed_transactions": verified})
        evm.durable(work / "chain-before-restore.json", report)
        shutil.copytree(directory / "redis", work / "redis-before-restore")
        redis_process = spawn([args.redis_server, "--bind", "127.0.0.1", "--port", str(redis_port),
                               "--dir", str(directory / "redis"), "--save", "", "--appendonly", "yes", "--appendfsync", "always"], "redis.log")
        for _ in range(100):
            try:
                if redis("PING") == "PONG":
                    break
            except (subprocess.CalledProcessError, ValueError):
                pass
            assert redis_process.poll() is None
            time.sleep(.1)
        else:
            raise RuntimeError("Redis restore did not become ready")
        guard = NoBroadcastGuard(manifest, work, proxy_port)
        env = os.environ.copy()
        env.update(saved_env)
        env["APP_ENVIRONMENT"] = "production"
        process = spawn([str(binary)], "engine.log", evm.ROOT / "server", env)
        base = f"http://127.0.0.1:{server_port}"
        for _ in range(200):
            try:
                if evm.request(base + "/health")[0] == 200:
                    break
            except OSError:
                pass
            assert process.poll() is None
            time.sleep(.1)
        else:
            raise RuntimeError("Engine restore did not become ready")
        diagnostic = {"x-diagnostic-access-password": saved_env["APP__SERVER__DIAGNOSTIC_ACCESS_PASSWORD"]}

        def empty():
            assert not guard.errors and guard.refused_broadcasts == 0, "Restore tried to broadcast; reconcile manually"
            _, body = evm.request(base + f"/admin/executors/eoa/{sender}:{chain}/state", headers=diagnostic)
            return all(body["result"][key] == 0 for key in ["pendingCount", "submittedCount", "borrowedCount", "recycledNoncesCount"])

        evm.wait(empty, timeout=30)
        restored = {}
        for payload in manifest["payloads"]:
            txid = payload["executionOptions"]["idempotencyKey"]
            assert txid.startswith(args.run_id + "-")
            assert payload["executionOptions"]["chainId"] == chain and payload["executionOptions"]["from"] == sender
            assert payload["params"][0]["to"] == recipient and payload["params"][0]["value"] == "0x1"
            _, body = evm.request(base + f"/admin/executors/eoa/{sender}:{chain}/transaction/{txid}", headers=diagnostic)
            data = body["result"]["transactionData"]
            assert data and data["transaction_id"] == txid and data["receipt"]["status"] == "0x1"
            restored[txid] = data["receipt"]["transactionHash"].lower()
        assert set(restored.values()) == hashes and len(restored) == count

        def replay(payload):
            code, body = evm.request(base + "/v1/write/transaction", payload, {"x-engine-signing-token": saved_env["ENGINE_SIGNING_TOKEN"]})
            assert code == 202 and body["result"]["transactions"][0]["id"] == payload["executionOptions"]["idempotencyKey"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(replay, manifest["payloads"]))
        time.sleep(5)
        assert empty() and guard.sends == 0 and guard.refused_broadcasts == 0
        after = balances()
        assert after == before, "Duplicate admission changed a chain balance or nonce"
        for number, block_hash in {(row["receipt"]["blockNumber"], row["receipt"]["blockHash"]) for row in verified}:
            block = evm.rpc(url, "eth_getBlockByNumber", [number, False])
            assert block and block["hash"] == block_hash, "Receipt is no longer canonical"
        assert all(file_hash(directory / name) == digest for name, digest in original_hashes.items())
        report.update({"outcome": "pass", "unique_chain_effects": count, "duplicate_requests": count,
                       "duplicate_chain_effects": 0, "rpc_sends": 0, "refused_broadcasts": 0,
                       "restored_receipts": restored, "chain_after_duplicate": after,
                       "original_failure_evidence_unchanged": True,
                       "redis_persistence": "existing appendonly AOF restored with appendfsync=always",
                       "reconciliation_lock_cleared": True})
    except Exception as error:
        report.update({"outcome": "fail", "error": type(error).__name__ + ": " + str(error), "reconciliation_lock_cleared": False})
        raise
    finally:
        for child in reversed(children):
            stop(child)
        if guard:
            guard.close()
            report["guard_errors"] = guard.errors
            report["refused_broadcasts"] = guard.refused_broadcasts
            if guard.errors or guard.refused_broadcasts:
                report.update({"outcome": "fail", "error": "Guard rejected a request; retain chain lock", "reconciliation_lock_cleared": False})
        if not budget.drain():
            report.update({"outcome": "fail", "error": "RPC requests did not drain; retain chain lock", "reconciliation_lock_cleared": False})
        for stream in streams:
            stream.close()
        if report["outcome"] == "pass" and chain_lock.read_bytes() != original_lock:
            report.update({"outcome": "fail", "error": "Chain lock changed during reconciliation", "reconciliation_lock_cleared": False})
        report.update({"rpc_calls": budget.count, "estimated_rpc_cost_usd": budget.count * .000006,
                       "elapsed_seconds": round(time.monotonic() - started, 3)})
        evm.durable(work / "report.json", report)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        evm.durable(args.report, report)
        if report["outcome"] == "pass":
            chain_lock.unlink()
        reconcile_lock.unlink()
        print(json.dumps({key: value for key, value in report.items() if key not in ["confirmed_transactions", "restored_receipts"]}, indent=2))


if __name__ == "__main__":
    main()
