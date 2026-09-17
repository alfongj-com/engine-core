#!/usr/bin/env python3
"""Actual Engine HTTP checks against five testnets through the budget gateway.

Read-only except local signing. Starts disposable Redis and Engine processes;
never broadcasts a transaction. Requires the gateway, built Engine, and a fresh
test-only Solana key file. No provider key is loaded by this script or Engine.
"""
import argparse
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile
import time
import urllib.error
import uuid

from local_eoa_recovery import ROOT, ports, request, stop, until

CHAINS = {11155111: "Ethereum Sepolia", 421614: "Arbitrum Sepolia",
          11155420: "OP Sepolia", 84532: "Base Sepolia"}
MULTICALL = "0xcA11bde05977b3631167028862bE2a173976CA11"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path.home()/".config/engine-core")
    parser.add_argument("--gateway-port", type=int, default=8788)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    gateway = f"http://127.0.0.1:{args.gateway_port}"
    before = request(gateway+"/metrics")[1]
    redis_port, server_port = ports(2)
    token = uuid.uuid4().hex + uuid.uuid4().hex
    logs = Path(tempfile.mkdtemp(prefix="engine-testnet-read-"))
    public_key = (args.state_dir/"test-solana-address").read_text().strip()
    env = os.environ.copy()
    env.update({"APP_ENVIRONMENT": "production", "RUST_LOG": "warn",
        "ENGINE_SIGNING_TOKEN": token,
        "ENGINE_SOLANA_KEYPAIR_FILE": str(args.state_dir/"test-solana-keypair.json"),
        "APP__REDIS__URL": f"redis://127.0.0.1:{redis_port}/",
        "APP__SERVER__HOST": "127.0.0.1", "APP__SERVER__PORT": str(server_port),
        "APP__QUEUE__EXECUTION_NAMESPACE": uuid.uuid4().hex,
        "APP__QUEUE__LOCAL_CONCURRENCY": "1",
        "APP__SOLANA__DEVNET__HTTP_URL": gateway+"/solana-devnet"})
    for chain in CHAINS:
        env[f"APP__EVM_RPC__ENDPOINTS__{chain}__URL"] = gateway+f"/{chain}"
    for name in ["WEBHOOK_WORKERS", "EXTERNAL_BUNDLER_SEND_WORKERS", "USEROP_CONFIRM_WORKERS", "EOA_EXECUTOR_WORKERS", "SOLANA_EXECUTOR_WORKERS"]:
        env[f"APP__QUEUE__{name}"] = "1"
    children, streams = [], []
    def spawn(command, name, cwd=None, child_env=None):
        stream = (logs/name).open("w")
        streams.append(stream)
        child = subprocess.Popen(command, cwd=cwd, env=child_env, stdout=stream, stderr=subprocess.STDOUT)
        children.append(child)
        return child
    base = f"http://127.0.0.1:{server_port}"
    headers = {"x-engine-signing-token": token}
    report = {"started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "scope": "actual Engine HTTP reads and local Solana signing; no broadcasts or throughput claim",
              "provider": "dRPC via bounded loopback gateway", "evm": [],
              "log_directory": str(logs)}
    try:
        spawn([os.environ.get("REDIS_SERVER_BIN", "redis-server"), "--bind", "127.0.0.1", "--port", str(redis_port), "--save", "", "--appendonly", "no"], "redis.log")
        spawn([str(ROOT/"target/debug/thirdweb-engine")], "engine.log", ROOT/"server", env)
        until(lambda: request(base+"/health")[0] == 200)
        for chain, name in CHAINS.items():
            payload = {"readOptions": {"chainId": chain, "multicall": False}, "params": [{
                "contractAddress": MULTICALL, "method": "function getChainId() view returns (uint256)", "params": []}]}
            start = time.monotonic()
            status, body = request(base+"/v1/read/contract", payload, headers)
            assert status == 200, body
            item = body["result"][0]
            # The public HTTP result wraps successful ABI return values.
            assert "result" in item and "error" not in item, item
            observed = int(item["result"])
            assert observed == chain, f"Wrong chain: expected {chain}, observed {observed}"
            report["evm"].append({"network": name, "chain_id": chain, "observed_chain_id": observed,
                                  "http_status": status, "elapsed_ms": round((time.monotonic()-start)*1000, 2)})
        denied_before = request(gateway+"/metrics")[1]["forwarded"]
        for bad_headers in [{}, {"x-engine-signing-token": "invalid"}, {"x-thirdweb-secret-key": "not-a-credential"}]:
            try:
                request(base+"/v1/read/contract", payload, bad_headers)
                raise AssertionError("Unauthenticated paid RPC use was allowed")
            except urllib.error.HTTPError as error:
                assert error.code == 400, error.code
        assert request(gateway+"/metrics")[1]["forwarded"] == denied_before, "Denied reads reached paid RPC"
        report["unauthorized_reads_rejected_without_rpc"] = 3
        solana_payload = {"instructions": [{"programId": "11111111111111111111111111111111",
            "accounts": [{"pubkey": public_key, "isSigner": True, "isWritable": True},
                         {"pubkey": public_key, "isSigner": False, "isWritable": True}],
            "data": struct.pack("<IQ", 2, 1).hex(), "encoding": "hex"}],
            "executionOptions": {"chainId": "solana:devnet", "signerAddress": public_key, "commitment": "confirmed"}}
        status, body = request(base+"/v1/solana/sign/transaction", solana_payload, headers)
        assert status == 200, body
        signed = body["result"]
        _, simulation = request(gateway+"/solana-devnet", {"jsonrpc": "2.0", "id": 1,
            "method": "simulateTransaction", "params": [signed["signedTransaction"],
            {"encoding": "base64", "sigVerify": True, "commitment": "confirmed"}]})
        assert "result" in simulation, simulation
        value = simulation["result"]["value"]
        assert value["err"] in (None, "AccountNotFound"), value
        report["solana"] = {"network": "Solana Devnet", "public_key": public_key,
            "local_signing_http_status": status, "simulation_error": value["err"],
            "simulation_slot": simulation["result"]["context"]["slot"],
            "execution_proven": value["err"] is None,
            "note": "AccountNotFound means faucet funding is still required; signed transactions were not broadcast."}
        report["outcome"] = "pass"
    except Exception as error:
        report.update({"outcome": "fail", "error": str(error)})
        raise
    finally:
        for child in reversed(children):
            stop(child)
        for stream in streams:
            stream.close()
        after = request(gateway+"/metrics")[1]
        report["gateway_calls"] = after["forwarded"] - before["forwarded"]
        report["estimated_usage_usd"] = report["gateway_calls"] * 0.000006
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2)+"\n")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
