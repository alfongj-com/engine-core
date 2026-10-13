#!/usr/bin/env python3
"""Set up one fixed revert-only testnet fixture using a separate local signer.

This setup is not Engine contract-creation qualification. Engine executes the
subsequent --revert-to test. No configurable bytecode, destination or value.
Retain the manifest and exact signed wire on every outcome. Failure keeps the same
per-chain lock used by testnet_evm_write.py; reconcile it before another run.
Only the private report contains the deployed address for a later --revert-to.
"""
import argparse
import json
import os
from pathlib import Path
import pty
import re
import select
import shutil
import subprocess
import sys
import termios
import time
import uuid

import testnet_evm_write as evm
from testnet_engine import file_hash

INITCODE = "0x6005600c60003960056000f360006000fd"
RUNTIME = "0x60006000fd"
# Sepolia's Oct 2026 state-creation repricing exceeds the earlier 200k cap.
# A measured 218,384 estimate needs 272,980 with our 25% buffer; the independent
# 0.001 ETH debit ceiling remains unchanged. https://eips.ethereum.org/EIPS/eip-8037
MAX_GAS = 300_000
MAX_FEE = 10_000_000_000
MAX_DEBIT = 10**15
SECP256K1_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141


def fixture_gas_limit(estimate):
    gas = (estimate * 125 + 99) // 100
    assert 53_000 <= gas <= MAX_GAS, "Creation gas estimate outside fixture allocation"
    return gas


def encode_rlp(value):
    """Canonical re-encoding rejects alternate length/integer encodings."""
    is_list = isinstance(value, list)
    raw = b"".join(encode_rlp(item) for item in value) if is_list else value
    if not is_list and len(raw) == 1 and raw[0] < 128:
        return raw
    base = 192 if is_list else 128
    if len(raw) < 56:
        return bytes([base + len(raw)]) + raw
    size = len(raw).to_bytes((len(raw).bit_length() + 7) // 8, "big")
    return bytes([base + 55 + len(size)]) + size + raw


class FixtureGuard(evm.Guard):
    """One exact creation nonce and wire; reservations precede public dispatch."""
    def validate_request(self, body):
        assert body.get("method") in {
            "eth_chainId", "eth_getBalance", "eth_getTransactionCount", "eth_getCode",
            "eth_getBlockByNumber", "eth_getTransactionReceipt", "eth_getTransactionByHash",
            "eth_feeHistory", "eth_gasPrice", "eth_maxPriorityFeePerGas", "eth_estimateGas",
            "eth_sendRawTransaction", "eth_blockNumber",
        }, "Fixture guard refuses this RPC method"
        assert isinstance(body.get("params"), list), "Invalid RPC parameters"
        if body["method"] == "eth_sendRawTransaction":
            assert len(body["params"]) == 1, "Only one exact signed wire is allowed"

    def validate(self, wire):
        assert isinstance(wire, str) and re.fullmatch(r"0x02[0-9a-fA-F]+", wire)
        fields = self.fields(wire)
        assert b"\x02" + encode_rlp(fields) == bytes.fromhex(wire[2:]), "Noncanonical RLP"
        numbers = {}
        for index in (0, 1, 2, 3, 4, 6, 9, 10, 11):
            field = fields[index]
            assert isinstance(field, bytes) and len(field) <= 32 and not field.startswith(b"\x00"), "Noncanonical integer"
            numbers[index] = int.from_bytes(field, "big")
        assert numbers[0] == self.m["chain_id"] in evm.CHAINS
        assert numbers[1] == self.m["initial_nonce"], "Only the reserved creation nonce is allowed"
        assert numbers[2] <= self.m["max_priority_fee_per_gas"]
        assert numbers[2] <= numbers[3] <= self.m["max_fee_per_gas"] <= MAX_FEE
        assert 53_000 <= numbers[4] <= self.m["gas_limit"] <= MAX_GAS
        assert fields[5] == b"" and numbers[6] == 0, "Only zero-value creation is allowed"
        assert fields[7] == bytes.fromhex(INITCODE[2:]) and fields[8] == [], "Only the fixed fixture is allowed"
        assert numbers[9] in (0, 1) and 0 < numbers[10] < SECP256K1_N and 0 < numbers[11] <= SECP256K1_N // 2
        with self.lock:
            entry = self.wires.get(str(numbers[1]))
            assert entry is None or entry["wire"] == wire, "Replacement wire refused"
            if entry is None:
                entry = {"wire": wire, "responses": [], "attempts": 0, "first_seen_unix": time.time()}
                self.wires[str(numbers[1])] = entry
            assert entry["attempts"] < 20, "Fixture broadcast cap"
            entry["attempts"] += 1
            evm.durable(self.directory / "signed-wires.json", self.wires)


def cast_private(cast, arguments, key, directory):
    """Feed Cast's hidden prompt through a bounded private TTY, never argv/env."""
    assert re.fullmatch(r"(?:0x)?[0-9a-fA-F]{64}", key)
    wrapper = "import fcntl,os,sys,termios; fcntl.ioctl(0,termios.TIOCSCTTY,0); os.execvpe(sys.argv[1],sys.argv[1:],os.environ)"
    arguments = list(arguments)
    arguments.insert(arguments.index("--create") if "--create" in arguments else len(arguments), "--interactive")
    command = [sys.executable, "-c", wrapper, str(cast), *arguments]
    env = {"PATH": os.defpath, "LANG": "C", "TERM": "dumb"}
    assert key not in " ".join(command) and key not in json.dumps(env)
    master, slave = pty.openpty()
    attributes = termios.tcgetattr(slave)
    attributes[3] &= ~termios.ECHO
    termios.tcsetattr(slave, termios.TCSANOW, attributes)
    child = None
    try:
        child = subprocess.Popen(command, stdin=slave, stdout=subprocess.PIPE, stderr=slave,
                                 start_new_session=True, cwd=directory, env=env)
        os.close(slave)
        slave = None
        captured, sent, deadline = b"", False, time.monotonic() + 20
        while child.poll() is None:
            if time.monotonic() >= deadline:
                raise RuntimeError("Local fixture signer timed out")
            if select.select([master], [], [], .1)[0]:
                try:
                    chunk = os.read(master, 4096)
                except OSError:
                    break
                captured += chunk
                assert len(captured) <= 16384, "Unexpected signer output"
                if not sent and b"private key" in captured.lower():
                    assert not termios.tcgetattr(master)[3] & termios.ECHO, "Signer TTY echo enabled"
                    os.write(master, (key + "\n").encode())
                    sent = True
        output, _ = child.communicate(timeout=max(.1, deadline - time.monotonic()))
        assert sent and child.returncode == 0, "Local fixture signer failed"
        assert len(output) <= 8192 and key.encode() not in output + captured, "Signer output refused"
        return output.decode().strip()
    finally:
        if child is not None and child.poll() is None:
            child.kill()
            child.communicate(timeout=5)
        os.close(master)
        if slave is not None:
            os.close(slave)


def sign_fixture(cast, key, manifest, directory):
    address = cast_private(cast, ["wallet", "address"], key, directory)
    assert address.lower() == manifest["sender"].lower(), "Fixture signer does not match funded test wallet"
    # Every transaction field is supplied. Port 1 is deliberately unreachable:
    # a local signer that unexpectedly needs a provider must fail before dispatch.
    arguments = ["mktx", "--chain", str(manifest["chain_id"]), "--nonce", str(manifest["initial_nonce"]),
                 "--gas-limit", str(manifest["gas_limit"]), "--gas-price", str(manifest["max_fee_per_gas"]),
                 "--priority-gas-price", str(manifest["max_priority_fee_per_gas"]), "--value", "0",
                 "--rpc-url", "http://127.0.0.1:1", "--no-proxy", "--create", INITCODE]
    signed = cast_private(cast, arguments, key, directory)
    assert re.fullmatch(r"0x02[0-9a-fA-F]+", signed), "Unexpected signed fixture format"
    hashed = subprocess.run([str(cast), "keccak"], input=signed, capture_output=True, text=True, timeout=10,
                            cwd=directory, env={"PATH": os.defpath, "LANG": "C"})
    txhash = hashed.stdout.strip()
    assert hashed.returncode == 0 and re.fullmatch(r"0x[0-9a-fA-F]{64}", txhash), "Cannot hash signed fixture"
    evm.durable(directory / "signed-fixture.json", {"wire": signed, "transaction_hash": txhash, "nonce": manifest["initial_nonce"]})
    return signed, txhash


def send_original(guard_url, directory):
    """Ambiguous replies consume reservations; retries keep the exact same wire."""
    retained = json.loads((directory / "signed-fixture.json").read_text())
    signed, txhash = retained["wire"], retained["transaction_hash"]
    for attempt in range(3):
        try:
            _, response = evm.request(guard_url, {"jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction", "params": [signed]})
            if "result" in response:
                assert response["result"].lower() == txhash.lower(), "Provider returned another transaction identity"
            # An RPC error can be an already-known transaction after response loss.
            # Reconcile only the locally hashed wire, never make another identity.
            return
        except OSError:
            if attempt == 2:
                return
            time.sleep(5)


def verify_deployment(url, receipt, manifest, *, expected_hash):
    """Provider-based finality and exact deployed code; not a consensus proof."""
    assert receipt["transactionHash"].lower() == expected_hash.lower(), "Receipt does not identify the retained signed fixture"
    txhash = receipt["transactionHash"]
    independent = evm.rpc(url, "eth_getTransactionReceipt", [txhash])
    tx = evm.rpc(url, "eth_getTransactionByHash", [txhash])
    assert independent and tx and independent["transactionHash"].lower() == txhash.lower()
    assert independent["status"] == receipt["status"] == "0x1"
    assert independent["blockHash"] == receipt["blockHash"] and independent["blockNumber"] == receipt["blockNumber"]
    assert tx["hash"].lower() == txhash.lower() and tx["blockHash"] == independent["blockHash"]
    assert tx["from"].lower() == independent["from"].lower() == manifest["sender"].lower()
    assert tx["to"] is None and independent["to"] is None
    assert int(tx["chainId"], 16) == manifest["chain_id"] and int(tx["nonce"], 16) == manifest["initial_nonce"]
    assert int(tx["type"], 16) == 2 and int(tx["value"], 16) == 0 and tx["input"].lower() == INITCODE
    assert int(tx["gas"], 16) <= manifest["gas_limit"]
    assert int(tx["maxPriorityFeePerGas"], 16) <= manifest["max_priority_fee_per_gas"]
    assert int(tx["maxFeePerGas"], 16) <= manifest["max_fee_per_gas"] and tx.get("accessList") == []
    address = independent.get("contractAddress")
    assert isinstance(address, str) and re.fullmatch(r"0x[0-9a-fA-F]{40}", address) and int(address, 16) != 0
    assert receipt.get("contractAddress", address) == address
    evm.verify_finalized_receipt(url, independent["blockNumber"], independent["blockHash"])
    assert evm.rpc(url, "eth_getCode", [address, independent["blockNumber"]]).lower() == RUNTIME, "Unexpected fixture runtime"
    return address, independent, tx


def finalized_covers(url, receipt):
    checkpoint = evm.rpc(url, "eth_getBlockByNumber", ["finalized", False])
    assert checkpoint is not None, "Finalized checkpoint unavailable"
    return int(checkpoint["number"], 16) >= int(receipt["blockNumber"], 16)


def main():
    if not __debug__:
        raise RuntimeError("Run without Python optimization; guard assertions are required")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain", type=int, choices=evm.CHAINS, required=True)
    parser.add_argument("--cast-bin", default=os.environ.get("CAST_BIN", "cast"))
    parser.add_argument("--timeout", type=int, default=1800, help="Finalized completion timeout; maximum 7200 seconds")
    parser.add_argument("--report", type=Path, required=True, help="Private report, including the fixture address")
    args = parser.parse_args()
    assert 60 <= args.timeout <= 7200 and not args.report.exists()
    found = shutil.which(args.cast_bin)
    assert found, "Cast is required for local fixture signing"
    cast = Path(found).resolve(strict=True)
    signer = {"kind": "separate local Cast fixture setup; Engine not used", "binary_sha256": file_hash(cast),
              "harness_sha256": file_hash(__file__), "guard_module_sha256": file_hash(evm.__file__)}
    evm.STATE.mkdir(mode=0o700, exist_ok=True)
    lockpath = evm.STATE / f"public-evm-{args.chain}.lock"
    lockfd = os.open(str(lockpath), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(lockfd, "w") as lock:
        lock.write(str(os.getpid()))
        lock.flush()
        os.fsync(lock.fileno())
    run_id = uuid.uuid4().hex
    directory = evm.STATE / "public-runs" / run_id
    directory.mkdir(mode=0o700, parents=True)
    report = {"scenario": "fixed revert-only fixture setup outside Engine", "chain_id": args.chain, "network": evm.CHAINS[args.chain],
              "transactions": 1, "run_id": run_id, "private_recovery_directory": str(directory), "signer": signer,
              "outcome": "incomplete", "finality_timeout_seconds": args.timeout}
    guard = None
    started = time.monotonic()
    try:
        url = evm.GATEWAY + "/" + str(args.chain)
        before = evm.request(evm.GATEWAY + "/metrics")[1]
        assert before["budget"]["remainingCalls"] >= 5000, "Insufficient gateway call allocation"
        assert int(evm.rpc(url, "eth_chainId", []), 16) == args.chain
        sender = (evm.STATE / "test-evm-address").read_text().strip()
        assert re.fullmatch(r"0x[0-9a-fA-F]{40}", sender)
        balance = int(evm.rpc(url, "eth_getBalance", [sender, "latest"]), 16)
        nonce = int(evm.rpc(url, "eth_getTransactionCount", [sender, "latest"]), 16)
        assert int(evm.rpc(url, "eth_getTransactionCount", [sender, "pending"]), 16) == nonce, "Reconcile pending transactions first"
        assert evm.rpc(url, "eth_getBlockByNumber", ["finalized", False]) is not None
        latest = evm.rpc(url, "eth_getBlockByNumber", ["latest", False])
        priority = max(1_000_000, int(evm.rpc(url, "eth_maxPriorityFeePerGas", []), 16))
        fee = 3 * int(latest["baseFeePerGas"], 16) + priority
        assert priority <= fee <= MAX_FEE, "Fee ceiling exceeded"
        estimate = int(evm.rpc(url, "eth_estimateGas", [{"from": sender, "value": "0x0", "data": INITCODE}]), 16)
        report["estimated_gas"] = estimate
        gas = fixture_gas_limit(estimate)
        report["gas_limit"] = gas
        reserve = 10**12 if args.chain in (11155420, 84532) else 0
        allocation = gas * fee + reserve
        assert allocation < min(balance // 3, MAX_DEBIT), "Fixture exceeds fee allocation"
        manifest = {**report, "sender": sender, "initial_nonce": nonce, "initial_sender_balance_wei": balance,
                    "gas_limit": gas, "max_fee_per_gas": fee, "max_priority_fee_per_gas": priority,
                    "value_wei": 0, "initcode": INITCODE, "runtime": RUNTIME,
                    "additional_chain_fee_reserve_wei": reserve, "total_debit_allocation_wei": allocation}
        evm.durable(directory / "manifest.json", manifest)
        key = (evm.STATE / "test-evm-key").read_text().strip()
        signed, txhash = sign_fixture(cast, key, manifest, directory)
        del key
        guard = FixtureGuard(manifest, directory, 0)
        guard_url = f"http://127.0.0.1:{guard.server.server_address[1]}/"
        completion_deadline = time.monotonic() + args.timeout
        send_original(guard_url, directory)
        receipt = None
        def completed():
            nonlocal receipt
            assert not guard.errors, "Fixture forwarding guard failed"
            receipt = evm.rpc(url, "eth_getTransactionReceipt", [txhash])
            assert receipt is None or receipt["transactionHash"].lower() == txhash.lower(), "Receipt does not identify the retained signed fixture"
            return receipt is not None
        evm.wait(completed, timeout=max(0, completion_deadline - time.monotonic()), interval=5)
        evm.wait(lambda: finalized_covers(url, receipt), timeout=max(0, completion_deadline - time.monotonic()), interval=5)
        address, independent, tx = verify_deployment(url, receipt, manifest, expected_hash=txhash)
        assert len(guard.wires) == 1 and not guard.errors
        assert not guard.accepted or set(guard.accepted.values()) == {txhash.lower()}
        final_nonce = int(evm.rpc(url, "eth_getTransactionCount", [sender, "pending"]), 16)
        final_balance = int(evm.rpc(url, "eth_getBalance", [sender, "latest"]), 16)
        execution_fees = int(independent["gasUsed"], 16) * int(independent["effectiveGasPrice"], 16)
        actual_fees = balance - final_balance
        assert final_nonce == nonce + 1 and execution_fees + int(independent.get("l1Fee", "0x0"), 16) <= actual_fees <= allocation
        report.update({"outcome": "pass", "contract_address": address, "receipt": independent, "transaction": tx,
                       "unique_chain_effects": 1, "actual_rpc_sends": guard.sends, "actual_fees_wei": actual_fees,
                       "initial_nonce": nonce, "final_nonce": final_nonce, "total_debit_allocation_wei": allocation,
                       "runtime_verified": True, "confirmation_scope": "Configured provider's canonical receipt and covering finalized checkpoint"})
    except Exception as error:
        report.update({"outcome": "fail", "error": type(error).__name__ + ": " + str(error)})
    finally:
        if guard:
            guard.close()
            report["guard_errors"] = guard.errors
            if guard.errors:
                report["outcome"] = "fail"
        try:
            after = evm.request(evm.GATEWAY + "/metrics")[1]
            report["budget_after"] = after["budget"]
            report["rpc_calls"] = sum(value["count"] - before.get("methods", {}).get(key, {}).get("count", 0)
                                      for key, value in after["methods"].items() if key.startswith(str(args.chain) + ":"))
        except Exception:
            report["metrics_unavailable"] = True
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        if report["outcome"] != "pass":
            report["reconciliation_lock_retained"] = str(lockpath)
        evm.durable(directory / "report.json", report)
        args.report.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        evm.durable(args.report, report)
        if report["outcome"] == "pass":
            lockpath.unlink()
        print(json.dumps({key: report[key] for key in ("outcome", "network", "transactions", "unique_chain_effects", "actual_rpc_sends", "actual_fees_wei", "elapsed_seconds") if key in report}))
    return 0 if report["outcome"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
