#!/usr/bin/env python3
"""Real Engine + Anvil: provisional inclusion, reorg, restart, same-nonce recovery.

Uses only disposable loopback processes and public test key 1. Engine must
restore the orphaned intent with its retained signed wire; the harness does not
edit queue state or submit transactions to rescue recovery. An explicit legacy
comparison option permits a same-nonce fee replacement with equivalent proof.
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

from local_eoa_recovery import ROOT, FROM, TO, ports, request, rpc, until, stop
from capacity_faults import RpcFaultProxy, evm_node_wire, verify_evm_node_wire


def verify_recovered_attempt(attempts, original_attempt, original_transaction, before, after,
                             transaction, receipt, block, reverted=False, allow_replacement=False):
    """Require an Engine-only post-fault acceptance and matching canonical wire.

    `before` and `after` are accepted-wire snapshots from the proxy used only by
    Engine. One persisted attempt can therefore prove two actual broadcasts.
    """
    original_hash = original_attempt["transactionHash"].lower()
    by_hash = {attempt["transactionHash"].lower(): attempt for attempt in attempts}
    assert len(by_hash) == len(attempts) and attempts, "Duplicate or absent durable attempt"
    assert by_hash.get(original_hash) == original_attempt, "Original durable wire changed"
    verify_evm_node_wire(original_attempt["signedTransaction"], original_transaction, original_hash)
    assert {attempt["nonce"] for attempt in attempts} == {0}, "Recovery changed nonce"
    assert all(attempt["chainId"] == 31337 and attempt["sender"].lower() == FROM
               for attempt in attempts), "Recovery changed chain or signer"
    winner = transaction["hash"].lower()
    assert winner in by_hash, "Canonical winner is not a durable attempt"
    if not allow_replacement:
        assert len(attempts) == 1 and winner == original_hash, "Expected original-wire replay without a new attempt"
    else:
        # A fee replacement may change fees and signature, not gas limit,
        # access/authorization lists, transaction type or any execution field.
        expected = dict(original_transaction)
        for key in ("gasPrice", "maxFeePerGas", "maxPriorityFeePerGas", "v", "yParity", "r", "s"):
            if key in transaction:
                expected[key] = transaction[key]
        assert evm_node_wire(expected) == evm_node_wire(transaction), "Fee replacement changed signed execution fields"
    decoded = verify_evm_node_wire(by_hash[winner]["signedTransaction"], transaction, winner)
    assert decoded["wire_replay_key"] == f"evm:31337:{FROM}:0", "Canonical wire changed replay identity"
    assert transaction["from"].lower() == FROM and transaction["to"].lower() == TO
    assert int(transaction["chainId"], 16) == 31337 and int(transaction["nonce"], 16) == 0
    assert int(transaction["value"], 16) == 1 and transaction.get("input", "0x") == "0x"
    digest = decoded["wire_digest"]
    accepted = after.get(digest)
    old_count = before.get(digest, {}).get("accepted_responses", 0)
    assert accepted and accepted["identity"].lower() == winner, "No accepted winning wire"
    assert accepted["accepted_responses"] > old_count, "No actual Engine resend after the fault"
    assert receipt["transactionHash"].lower() == winner and receipt["status"] == ("0x0" if reverted else "0x1")
    assert receipt["blockHash"] == block["hash"] and receipt["blockNumber"] == block["number"]
    assert block["transactions"] == [winner], "Canonical block does not contain exactly the intended execution"
    return {"recovery_mode": "exact_wire" if winner == original_hash else "same_nonce_replacement",
            "post_restart_accepted_winning_sends": accepted["accepted_responses"] - old_count,
            "winning_wire_digest": digest}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revert", action="store_true")
    parser.add_argument("--allow-fee-replacement", action="store_true",
                        help="Legacy comparison only: permit an independently verified same-nonce replacement")
    parser.add_argument("--engine-bin", type=Path, default=ROOT / "target/debug/thirdweb-engine")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    redis_port, engine_port, anvil_port = ports(3)
    logs = Path(tempfile.mkdtemp(prefix="engine-eoa-reorg-"))
    journal = logs / "recovery.sqlite"
    namespace = "reorg-" + uuid.uuid4().hex
    txid = namespace + "-intent"
    token = uuid.uuid4().hex + uuid.uuid4().hex
    base, rpc_url = f"http://127.0.0.1:{engine_port}", f"http://127.0.0.1:{anvil_port}"
    headers = {"x-engine-signing-token": token}
    executable = str(args.engine_bin.resolve())
    env = os.environ.copy()
    env.update({"APP_ENVIRONMENT": "production", "RUST_LOG": "warn",
        "ENGINE_PRIVATE_KEY": f"{1:064x}", "ENGINE_SIGNING_TOKEN": token,
        "APP__REDIS__URL": f"redis://127.0.0.1:{redis_port}/",
        "APP__RECOVERY__JOURNAL_PATH": str(journal),
        "APP__SERVER__HOST": "127.0.0.1", "APP__SERVER__PORT": str(engine_port),
        "APP__SERVER__DIAGNOSTIC_ACCESS_PASSWORD": uuid.uuid4().hex,
        "APP__EVM_RPC__ENDPOINTS__31337__URL": rpc_url,
        "APP__EVM_RPC__ENDPOINTS__31337__FINALITY__MODE": "depth",
        "APP__EVM_RPC__ENDPOINTS__31337__FINALITY__CONFIRMATIONS": "2",
        "APP__QUEUE__EXECUTION_NAMESPACE": namespace,
        "APP__QUEUE__LOCAL_CONCURRENCY": "1", "APP__QUEUE__POLLING_INTERVAL_MS": "20",
        "APP__QUEUE__LEASE_DURATION_SECONDS": "2"})
    for kind in ["WEBHOOK", "EXTERNAL_BUNDLER_SEND", "USEROP_CONFIRM", "EOA_EXECUTOR", "SOLANA_EXECUTOR"]:
        env[f"APP__QUEUE__{kind}_WORKERS"] = "1"
    children, streams = [], []
    proxy = None

    def spawn(command, name, cwd=None, child_env=None):
        stream = (logs / name).open("w")
        streams.append(stream)
        child = subprocess.Popen(command, cwd=cwd, env=child_env, stdout=stream, stderr=subprocess.STDOUT)
        children.append(child)
        return child

    def redis(*command):
        return subprocess.check_output([os.environ.get("REDIS_CLI_BIN", "redis-cli"), "-p", str(redis_port), "--raw", *map(str, command)], text=True).strip()

    def engine(name):
        child = spawn([executable], name, ROOT / "server", env)
        until(lambda: request(base + "/health")[0] == 200)
        return child

    def ledger(sql):
        with sqlite3.connect(f"file:{journal}?mode=ro", uri=True) as db:
            return db.execute(sql, (txid,)).fetchall()

    def nonterminal():
        assert redis("HGET", f"{namespace}:eoa_executor:tx_data:{txid}", "status") not in ("confirmed", "failed")
        assert ledger("SELECT state FROM admissions WHERE id=?") == [("admitted",)]

    report = {"scenario": "pre-finality reorg and same-nonce recovery", "reverted_execution": args.revert,
        "policy": {"mode": "depth", "confirmations": 2}, "log_directory": str(logs),
        "manual_signed_wire_replay": False, "allow_fee_replacement": args.allow_fee_replacement}
    started = time.monotonic()
    try:
        spawn([os.environ.get("REDIS_SERVER_BIN", "redis-server"), "--bind", "127.0.0.1", "--port", str(redis_port),
            "--save", "", "--appendonly", "no"], "redis.log")
        until(lambda: subprocess.run([os.environ.get("REDIS_CLI_BIN", "redis-cli"), "-p", str(redis_port), "PING"],
            capture_output=True, text=True).stdout.strip() == "PONG")
        spawn([os.environ.get("ANVIL_BIN", "anvil"), "--host", "127.0.0.1", "--port", str(anvil_port), "--chain-id", "31337", "--no-mining", "--silent"], "anvil.log")
        until(lambda: rpc(rpc_url, "eth_chainId", []) == "0x7a69")
        proxy = RpcFaultProxy(rpc_url, audit_path=logs / "engine-rpc.jsonl")
        env["APP__EVM_RPC__ENDPOINTS__31337__URL"] = proxy.url
        rpc(rpc_url, "anvil_setBalance", [FROM, hex(10**21)])
        if args.revert:
            rpc(rpc_url, "anvil_setCode", [TO, "0x60006000fd"])
        snapshot = rpc(rpc_url, "evm_snapshot", [])
        result = subprocess.run([executable, "--initialize-recovery"], cwd=ROOT / "server", env=env, capture_output=True, timeout=20)
        (logs / "initialize.log").write_bytes(result.stdout + result.stderr)
        assert result.returncode == 0, "Recovery initialization failed; inspect private logs"
        process = engine("engine-before.log")
        payload = {"executionOptions": {"chainId":31337, "type":"EOA", "from":FROM, "idempotencyKey":txid},
            "params":[{"to":TO, "value":"0x1", "data":"0x", "gasLimit":40000 if args.revert else 21000}]}
        assert request(base + "/v1/write/transaction", payload, headers)[0] == 202
        until(lambda: int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "pending"]), 16) == 1)
        rpc(rpc_url, "evm_mine", [])
        first_block = rpc(rpc_url, "eth_getBlockByNumber", ["latest", False])
        first_hash = first_block["transactions"][0]
        original_transaction = rpc(rpc_url, "eth_getTransactionByHash", [first_hash])
        initial_attempts = [json.loads(row[0]) for row in ledger("SELECT payload FROM attempts WHERE id=?")]
        assert len(initial_attempts) == 1 and initial_attempts[0]["transactionHash"] == first_hash
        original_attempt = initial_attempts[0]
        original_digest = hashlib.sha256(bytes.fromhex(original_attempt["signedTransaction"][2:])).hexdigest()
        initial_accepted = proxy.snapshot(include_wires=True)["accepted_wires"]
        assert initial_accepted[original_digest]["identity"] == first_hash
        receipt = rpc(rpc_url, "eth_getTransactionReceipt", [first_hash])
        assert receipt["status"] == ("0x0" if args.revert else "0x1")
        # More than one full receipt polling interval, still below required depth.
        deadline = time.monotonic() + 7
        while time.monotonic() < deadline:
            nonterminal()
            time.sleep(0.2)
        # The recovery trigger must observe a genuine rollback of Engine's own
        # cached count, not merely wait until unrelated 60-second fee handling.
        nonce_keys = subprocess.check_output([os.environ.get("REDIS_CLI_BIN", "redis-cli"),
            "-p", str(redis_port), "--scan", "--pattern",
            f"{namespace}:eoa_executor:last_tx_nonce:31337:*"], text=True).splitlines()
        assert len(nonce_keys) == 1 and nonce_keys[0].rsplit(":", 1)[1].lower() == FROM
        until(lambda: redis("GET", nonce_keys[0]) == "1")
        stop(process, crash=True)
        until(lambda: proxy.snapshot()["active"] == 0)
        assert rpc(rpc_url, "evm_revert", [snapshot]) is True
        rpc(rpc_url, "anvil_dropAllTransactions", [])
        assert rpc(rpc_url, "eth_getTransactionReceipt", [first_hash]) is None
        assert int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "latest"]), 16) == 0
        assert int(rpc(rpc_url, "eth_getBalance", [TO, "latest"]), 16) == 0
        rpc(rpc_url, "evm_setNextBlockTimestamp", [int(first_block["timestamp"], 16) + 2])
        rpc(rpc_url, "evm_mine", [])  # A different canonical branch at the old height.
        before_restart = proxy.snapshot(include_wires=True)["accepted_wires"]
        process = engine("engine-after.log")
        nonterminal()
        until(lambda: int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "pending"]), 16) == 1, timeout=100)
        nonterminal()
        attempts = [json.loads(row[0]) for row in ledger("SELECT payload FROM attempts WHERE id=?")]
        assert {attempt["nonce"] for attempt in attempts} == {0}, attempts
        assert ledger("SELECT replay_key FROM admissions WHERE id=?") == [(f"evm:31337:{FROM}:0",)]
        rpc(rpc_url, "evm_mine", [])
        winning_block = rpc(rpc_url, "eth_getBlockByNumber", ["latest", False])
        assert winning_block["hash"] != first_block["hash"]
        winning_hash = winning_block["transactions"][0]
        recovered = verify_recovered_attempt(attempts, original_attempt, original_transaction, before_restart,
            proxy.snapshot(include_wires=True)["accepted_wires"],
            rpc(rpc_url, "eth_getTransactionByHash", [winning_hash]),
            rpc(rpc_url, "eth_getTransactionReceipt", [winning_hash]), winning_block,
            reverted=args.revert, allow_replacement=args.allow_fee_replacement)
        nonterminal()
        rpc(rpc_url, "evm_mine", [])  # Exactly one subsequent block is insufficient.
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            nonterminal()
            time.sleep(0.2)
        rpc(rpc_url, "evm_mine", [])
        terminal = "failed" if args.revert else "confirmed"
        until(lambda: redis("HGET", f"{namespace}:eoa_executor:tx_data:{txid}", "status") == terminal)
        assert ledger("SELECT state FROM admissions WHERE id=?") == [("terminal",)]
        terminal_proofs = ledger("SELECT evidence FROM terminal_evidence WHERE id=?")
        assert len(terminal_proofs) == 1, "Conflicting or duplicate terminal evidence"
        proof = json.loads(terminal_proofs[0][0])
        assert proof["transactionHash"] == winning_hash
        assert proof["outcome"] == ("reverted" if args.revert else "success")
        assert proof["finality"]["blockHash"] == winning_block["hash"]
        assert proof["finality"]["checkpointNumber"] >= int(winning_block["number"], 16)
        assert proof["finality"]["policy"] == report["policy"]
        sends_before_duplicate = proxy.snapshot()["forwarded_sends"]
        assert request(base + "/v1/write/transaction", payload, headers)[0] == 202
        rpc(rpc_url, "evm_mine", [])
        time.sleep(2)  # Allow duplicate admission to reach any incorrectly queued worker.
        assert int(rpc(rpc_url, "eth_getTransactionCount", [FROM, "pending"]), 16) == 1
        assert int(rpc(rpc_url, "eth_getBalance", [TO, "latest"]), 16) == (0 if args.revert else 1)
        assert proxy.snapshot()["forwarded_sends"] == sends_before_duplicate, "Terminal retry broadcast again"
        assert len(ledger("SELECT payload FROM attempts WHERE id=?")) == len(attempts)
        report.update(outcome="pass", provisional_result_withheld=True, orphaned_receipt_disappeared=True,
            engine_recovered_original_nonce=True, attempts=len(attempts), unique_chain_effects=1,
            duplicate_effects=0, terminal_status=terminal, original_transaction_hash=first_hash,
            final_transaction_hash=winning_hash, **recovered,
            elapsed_seconds=round(time.monotonic()-started,3))
    except Exception as error:
        report.update(outcome="fail", error=str(error))
        raise
    finally:
        for child in reversed(children): stop(child)
        if proxy is not None:
            report["engine_rpc"] = proxy.snapshot(include_wires=True)
            proxy.close()
        for stream in streams: stream.close()
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
