#!/usr/bin/env python3
"""Real Engine + Anvil: Redis loss/rollback, halt, quarantine, fresh intake.

Only disposable loopback processes and the public test key 1 are used. The
irreversible effect is mined while Engine is stopped so its journal remains
uncertain. Recovery must not repeat that effect, even after losing all Redis
queue evidence. A fresh intent and terminal duplicate exercise resumed intake.
"""
import argparse
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import uuid

from local_eoa_recovery import ROOT, FROM, TO, ports, request, rpc, until, stop


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--loss", choices=["flush", "rollback"], required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    redis_port, engine_port, anvil_port = ports(3)
    logs = Path(tempfile.mkdtemp(prefix="engine-redis-disaster-"))
    redis_dir = logs / "redis"
    redis_dir.mkdir(mode=0o700)
    journal = logs / "recovery.sqlite"
    namespace = "disaster-" + uuid.uuid4().hex
    new_namespace = namespace + "-recovered"
    token = uuid.uuid4().hex + uuid.uuid4().hex
    admin = uuid.uuid4().hex
    base, rpc_url = f"http://127.0.0.1:{engine_port}", f"http://127.0.0.1:{anvil_port}"
    headers = {"x-engine-signing-token": token}
    executable = str(ROOT / "target/debug/thirdweb-engine")
    env = os.environ.copy()
    env.update({"APP_ENVIRONMENT": "production", "RUST_LOG": "warn",
        "ENGINE_PRIVATE_KEY": f"{1:064x}", "ENGINE_SIGNING_TOKEN": token,
        "APP__REDIS__URL": f"redis://127.0.0.1:{redis_port}/",
        "APP__RECOVERY__JOURNAL_PATH": str(journal),
        "APP__SERVER__HOST": "127.0.0.1", "APP__SERVER__PORT": str(engine_port),
        "APP__SERVER__DIAGNOSTIC_ACCESS_PASSWORD": admin,
        "APP__EVM_RPC__ENDPOINTS__31337__URL": rpc_url,
        "APP__EVM_RPC__ENDPOINTS__31337__FINALITY__MODE": "depth",
        "APP__EVM_RPC__ENDPOINTS__31337__FINALITY__CONFIRMATIONS": "0",
        "APP__QUEUE__EXECUTION_NAMESPACE": namespace,
        "APP__QUEUE__LOCAL_CONCURRENCY": "1", "APP__QUEUE__POLLING_INTERVAL_MS": "20",
        "APP__QUEUE__LEASE_DURATION_SECONDS": "2"})
    for kind in ["WEBHOOK", "EXTERNAL_BUNDLER_SEND", "USEROP_CONFIRM", "EOA_EXECUTOR", "SOLANA_EXECUTOR"]:
        env[f"APP__QUEUE__{kind}_WORKERS"] = "1"
    children, streams = [], []

    def spawn(command, name, cwd=None, child_env=None):
        stream = (logs / name).open("w")
        streams.append(stream)
        child = subprocess.Popen(command, cwd=cwd, env=child_env, stdout=stream, stderr=subprocess.STDOUT)
        children.append(child)
        return child

    def redis(*command):
        return subprocess.check_output([os.environ.get("REDIS_CLI_BIN", "redis-cli"), "-p", str(redis_port), "--raw", *map(str, command)], text=True).strip()

    def start_redis(name):
        child = spawn([os.environ.get("REDIS_SERVER_BIN", "redis-server"), "--bind", "127.0.0.1", "--port", str(redis_port),
            "--dir", str(redis_dir), "--dbfilename", "snapshot.rdb", "--save", "", "--appendonly", "no"], name)
        until(lambda: subprocess.run([os.environ.get("REDIS_CLI_BIN", "redis-cli"), "-p", str(redis_port), "PING"],
            capture_output=True, text=True).stdout.strip() == "PONG")
        return child

    def cli(operation, success=True):
        result = subprocess.run([executable, operation], cwd=ROOT / "server", env=env, capture_output=True, timeout=20)
        (logs / (operation.removeprefix("--") + ".log")).write_bytes(result.stdout + result.stderr)
        assert (result.returncode == 0) == success, f"Unexpected result for {operation}; inspect {logs}"

    def engine(name):
        child = spawn([executable], name, ROOT / "server", env)
        until(lambda: request(base + "/health")[0] == 200)
        return child

    def payload(id):
        return {"executionOptions": {"chainId":31337, "type":"EOA", "from":FROM, "idempotencyKey":id},
            "params":[{"to":TO, "value":"0x1", "data":"0x", "gasLimit":21000}]}

    def rejected(body, expected):
        try:
            request(base + "/v1/write/transaction", body, headers)
        except urllib.error.HTTPError as error:
            assert error.code == expected, (error.code, error.read())
            return
        raise AssertionError("Unsafe replay was accepted")

    started = time.monotonic()
    report = {"scenario":"Redis disaster quarantine and fresh intake", "loss":args.loss, "log_directory":str(logs)}
    try:
        redis_process = start_redis("redis-before.log")
        spawn([os.environ.get("ANVIL_BIN", "anvil"), "--host", "127.0.0.1", "--port", str(anvil_port), "--chain-id", "31337", "--no-mining", "--silent"], "anvil.log")
        until(lambda: rpc(rpc_url,"eth_chainId",[]) == "0x7a69")
        rpc(rpc_url,"anvil_setBalance",[FROM,hex(10**21)])
        cli("--initialize-recovery")
        assert redis("SAVE") == "OK"  # Deliberately stale checkpoint, before either intent.
        first = payload(namespace + "-uncertain")
        fresh = payload(namespace + "-fresh")
        process = engine("engine-before.log")
        assert request(base+"/v1/write/transaction",first,headers)[0] == 202
        until(lambda:int(rpc(rpc_url,"eth_getTransactionCount",[FROM,"pending"]),16) == 1)
        stop(process,crash=True)
        rpc(rpc_url,"evm_mine",[])
        assert int(rpc(rpc_url,"eth_getBalance",[TO,"latest"]),16) == 1
        if args.loss == "rollback":
            stop(redis_process,crash=True)
            redis_process = start_redis("redis-rollback.log")
        else:
            assert redis("FLUSHDB") == "OK"  # Own disposable database only.
        cli("--reattach-recovery",success=False)
        refused = subprocess.run([executable],cwd=ROOT/"server",env=env,capture_output=True,timeout=20)
        (logs/"refused-start.log").write_bytes(refused.stdout+refused.stderr)
        assert refused.returncode != 0, "Redis loss must block ordinary startup"
        env["APP__QUEUE__EXECUTION_NAMESPACE"] = new_namespace
        cli("--recover-redis")
        process = engine("engine-recovered.log")
        rejected(first,503)
        with sqlite3.connect(f"file:{journal}?mode=ro",uri=True) as database:
            state = database.execute("SELECT state FROM admissions WHERE id=?",(first["executionOptions"]["idempotencyKey"],)).fetchone()
            assert state == ("quarantined",), state
        assert int(rpc(rpc_url,"eth_getTransactionCount",[FROM,"pending"]),16) == 1
        # A fresh user intent can run; the quarantined ID and nonce binding stay.
        assert request(base+"/v1/write/transaction",fresh,headers)[0] == 202
        until(lambda:int(rpc(rpc_url,"eth_getTransactionCount",[FROM,"pending"]),16) == 2)
        rpc(rpc_url,"evm_mine",[])
        txid = fresh["executionOptions"]["idempotencyKey"]
        until(lambda:redis("HGET",f"{new_namespace}:eoa_executor:tx_data:{txid}","status") == "confirmed")
        assert int(rpc(rpc_url,"eth_getBalance",[TO,"latest"]),16) == 2
        assert request(base+"/v1/write/transaction",fresh,headers)[0] == 202
        rejected(first,503)
        rpc(rpc_url,"evm_mine",[])
        assert int(rpc(rpc_url,"eth_getTransactionCount",[FROM,"latest"]),16) == 2
        assert int(rpc(rpc_url,"eth_getBalance",[TO,"latest"]),16) == 2
        # Losing the live projection must also close HTTP admission/signing.
        assert redis("FLUSHDB") == "OK"
        def halted_live():
            try:
                request(base + "/health")
            except urllib.error.HTTPError as error:
                return error.code == 503
            return False
        until(halted_live)
        rejected(payload(namespace + "-must-not-send"),503)
        assert int(rpc(rpc_url,"eth_getTransactionCount",[FROM,"pending"]),16) == 2
        report.update(outcome="pass", prior_uncertain_effects=1, new_intent_effects=1,
            live_projection_loss_halts_writes=True,
            duplicate_effects=0, quarantined_ids=1, unsafe_reattach_rejected=True,
            ordinary_startup_rejected=True, new_namespace_healthy=True,
            terminal_duplicate_created_no_work=True, elapsed_seconds=round(time.monotonic()-started,3))
    except Exception as error:
        report.update(outcome="fail",error=str(error))
        raise
    finally:
        for child in reversed(children): stop(child)
        for stream in streams: stream.close()
        if args.report:
            args.report.parent.mkdir(parents=True,exist_ok=True)
            args.report.write_text(json.dumps(report,indent=2)+"\n")
        print(json.dumps(report,indent=2))


if __name__ == "__main__":
    main()
