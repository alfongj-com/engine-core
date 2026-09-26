"""Offline adversarial oracle tests and real loopback HTTP fault controls."""
import base64
import copy
import hashlib
import http.client
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import shutil
from pathlib import Path
import struct
import tempfile
import threading
import time
import unittest
import urllib.request

from capacity_faults import (Audit, DRAIN_FIELDS, FaultError, FaultPlan, RpcFaultProxy,
    SYSTEM_PROGRAM, MEMO_PROGRAM, b58encode, build_evm_fixture, build_solana_fixture,
    decode_solana_intent, decode_solana_wire, digest, evaluate_campaign,
    evm_contract_setup, evm_intent_fields, loopback_url, run_triggered_fault,
    decode_evm_wire, evm_node_wire, verify_evm_node_wire, KeepAliveHttp)


def post(url, method, params, id=1, headers=None):
    body = json.dumps({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=3) as response:
        return json.load(response)


class Upstream:
    def __init__(self, result=None, truncated=False):
        self.calls, self.client_ports, self.lock = [], [], threading.Lock()
        self.result = result or (lambda call: "0x" + hashlib.sha256(call["params"][0].encode()).hexdigest()
                                 if call["method"] == "eth_sendRawTransaction" else "0x1")
        owner = self
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def setup(self):
                super().setup()
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            def log_message(self, *_args):
                pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                with owner.lock:
                    owner.calls.append(body)
                    owner.client_ports.append(self.client_address[1])
                answer = owner.result(body)
                result = {"jsonrpc": "2.0", "id": body["id"], **(answer if isinstance(answer, dict) else {"result": answer})}
                encoded = json.dumps(result).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(encoded) + (10 if truncated else 0)))
                self.end_headers()
                self.wfile.write(encoded)
                if truncated:
                    self.close_connection = True
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/"
    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)


def shortvec(number):
    value = bytearray()
    while number > 127:
        value.append((number & 127) | 128)
        number >>= 7
    return bytes(value) + bytes([number])


def b58decode(value):
    number = 0
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    for char in value:
        number = number * 58 + alphabet.index(char)
    return b"\0" * (len(value) - len(value.lstrip("1"))) + number.to_bytes((number.bit_length() + 7) // 8, "big")


def solana_wire(txid="sol-1", invalid=False, multi=False, legacy=False, extra=False):
    payer, recipient = b58encode(b"p" * 32), b58encode(b"r" * 32)
    instructions = [(2, [0, 1], struct.pack("<I", 0xFFFFFFFF) if invalid else struct.pack("<IQ", 2, 1000))]
    if multi:
        instructions.append((2, [0, 1], struct.pack("<IQ", 2, 1001)))
    if extra:
        instructions.append((2, [0, 1], b"bogus"))
    instructions.append((3, [], ("thirdweb-engine:" + txid).encode()))
    message = (b"" if legacy else b"\x80") + b"\x01\x00\x02\x04"
    message += b"p" * 32 + b"r" * 32 + b58decode(SYSTEM_PROGRAM) + b58decode(MEMO_PROGRAM) + b"b" * 32
    message += shortvec(len(instructions))
    for program, accounts, data in instructions:
        message += bytes([program]) + shortvec(len(accounts)) + bytes(accounts) + shortvec(len(data)) + data
    if not legacy:
        message += b"\0"
    wire = base64.b64encode(b"\x01" + b"s" * 64 + message).decode()
    return wire, payer, recipient


class ProxyTests(unittest.TestCase):
    def setUp(self):
        self.upstream = Upstream()
        self.proxies = []
    def tearDown(self):
        for proxy in self.proxies:
            proxy.close()
        self.upstream.close()
    def proxy(self, plan=None, **kwargs):
        proxy = RpcFaultProxy(self.upstream.url, plan, **kwargs)
        self.proxies.append(proxy)
        return proxy

    def test_lost_response_requires_actual_acceptance_and_retries_same_wire(self):
        proxy = self.proxy(FaultPlan(drop_accepted_sends=1))
        with self.assertRaises(Exception):
            post(proxy.url, "eth_sendRawTransaction", ["0x0102"])
        self.assertEqual(len(self.upstream.calls), 1)
        self.assertEqual(proxy.wait_for_accepted(1, .1), 1)
        result = post(proxy.url, "eth_sendRawTransaction", ["0x0102"])
        snapshot = proxy.snapshot(include_wires=True)
        self.assertEqual(snapshot["accepted_unique_wires"], 1)
        self.assertEqual(snapshot["dropped_responses"], 1)
        self.assertEqual(next(iter(snapshot["accepted_wires"].values())), {"identity": result["result"], "accepted_responses": 2})
        self.assertEqual([r["params"] for r in self.upstream.calls], [["0x0102"], ["0x0102"]])
        events = snapshot["audit"]["tail"]
        self.assertTrue(any(e["event"] == "send_accepted" and e["response_dropped"] for e in events))
        self.assertNotIn("accepted_wires", proxy.snapshot())
        self.assertNotIn("tail", proxy.snapshot()["audit"])

    def test_response_loss_trigger_waits_for_threshold_and_audits_batch_collateral(self):
        proxy = self.proxy(FaultPlan(drop_accepted_sends=1, after_accepted=1))
        post(proxy.url, "eth_sendRawTransaction", ["0x01"])
        body = [{"jsonrpc": "2.0", "id": i, "method": "eth_sendRawTransaction", "params": [f"0x0{i}"]} for i in [2, 3]]
        req = urllib.request.Request(proxy.url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
        with self.assertRaises(Exception):
            urllib.request.urlopen(req, timeout=3)
        snapshot = proxy.snapshot(include_wires=True)
        self.assertEqual(snapshot["accepted_unique_wires"], 3)
        self.assertEqual(snapshot["dropped_responses"], 1)
        self.assertEqual(snapshot["lost_http_responses"], 1)
        self.assertEqual(snapshot["accepted_responses_lost"], 2)
        self.assertEqual(snapshot["http_requests"], 2)
        self.assertEqual(snapshot["incoming_batches"], 1)
        self.assertEqual(snapshot["upstream_http_requests"], 3)
        lost = next(e for e in snapshot["audit"]["tail"] if e["event"] == "http_response_lost")
        self.assertTrue(lost["batch"])
        self.assertEqual(len(lost["accepted_identities"]), 2)

    def test_rejection_does_not_trigger_crash_or_consume_loss_budget(self):
        self.upstream.result = lambda _call: {"error": {"code": -32000, "message": "rejected"}}
        proxy = self.proxy(FaultPlan(drop_accepted_sends=1))
        self.assertIn("error", post(proxy.url, "eth_sendRawTransaction", ["0x01"]))
        called = []
        with self.assertRaisesRegex(FaultError, "trigger did not reach"):
            run_triggered_fault(proxy, 1, "crash", [("kill", lambda: called.append("kill"))], timeout=.01)
        self.assertEqual(called, [])
        self.assertEqual(proxy.snapshot()["dropped_responses"], 0)

    def test_finite_error_latency_and_release_are_observable(self):
        proxy = self.proxy(FaultPlan(rpc_errors={"eth_blockNumber": 1}, latency_ms={"eth_blockNumber": 25}))
        started = time.monotonic()
        self.assertIn("error", post(proxy.url, "eth_blockNumber", []))
        self.assertGreaterEqual(time.monotonic() - started, .02)
        self.assertEqual(self.upstream.calls, [])
        self.assertEqual(post(proxy.url, "eth_blockNumber", [])["result"], "0x1")
        proxy.release()
        self.assertEqual(post(proxy.url, "eth_blockNumber", [])["result"], "0x1")
        stats = proxy.snapshot()["methods"]["eth_blockNumber"]
        self.assertEqual((stats["count"], stats["errors"]), (3, 1))
        self.assertGreaterEqual(stats["total_ms"], 40)
        self.assertEqual(len(self.upstream.calls), 2)

    def test_lifecycle_steps_follow_acceptance_and_failed_step_stops_sequence(self):
        proxy = self.proxy()
        post(proxy.url, "eth_sendRawTransaction", ["0x03"])
        called = []
        def fail():
            called.append("failed")
            raise RuntimeError("injected restart error")
        with self.assertRaises(RuntimeError):
            run_triggered_fault(proxy, 1, "redis-restart", [("stop", lambda: called.append("stop")), ("reattach", fail), ("restart", lambda: called.append("restart"))])
        self.assertEqual(called, ["stop", "failed"])
        events = proxy.snapshot(include_wires=True)["audit"]["tail"]
        accepted = next(e for e in events if e["event"] == "send_accepted")
        triggered = next(e for e in events if e["event"] == "fault_triggered")
        self.assertLess(accepted["sequence"], triggered["sequence"])
        self.assertFalse(any(e["event"] == "fault_completed" for e in events))

    def test_local_only_and_browser_boundary(self):
        for url in ["https://127.0.0.1:1", "http://example.com:1", "http://169.254.169.254:80", "http://localhost:1", "http://user:pass@127.0.0.1:1"]:
            with self.subTest(url=url), self.assertRaises((FaultError, ValueError)):
                loopback_url(url)
        proxy = self.proxy()
        for path, headers in [("", {"Origin": "https://evil.example"}), ("", {"Host": "evil.example"}), ("bad", {})]:
            with self.subTest(path=path, headers=headers), self.assertRaises(Exception):
                post(proxy.url + path, "eth_blockNumber", [], headers=headers)
        self.assertEqual(self.upstream.calls, [])

    def test_designated_invalid_solana_wire_only_preflight_override(self):
        wire, _payer, _recipient = solana_wire(invalid=True)
        self.upstream.result = lambda call: decode_solana_wire(call["params"][0])["signature"]
        proxy = self.proxy(preflight_bypass_ids={"sol-1"})
        params = [wire, {"encoding": "base64", "maxRetries": 0, "skipPreflight": False}]
        post(proxy.url, "sendTransaction", params)
        self.assertTrue(self.upstream.calls[-1]["params"][1]["skipPreflight"])
        other, _, _ = solana_wire(txid="other", invalid=True)
        post(proxy.url, "sendTransaction", [other, params[1]])
        self.assertFalse(self.upstream.calls[-1]["params"][1]["skipPreflight"])
        valid, _, _ = solana_wire()
        with self.assertRaises(Exception):
            post(proxy.url, "sendTransaction", [valid, params[1]])
        self.assertEqual(len(self.upstream.calls), 2, "valid instruction cannot use malformed-fixture bypass")
        snapshot = proxy.snapshot(include_wires=True)
        self.assertEqual(snapshot["injected_preflight_bypass"], 1)
        self.assertTrue(any(e["event"] == "injected_preflight_bypass" for e in snapshot["audit"]["tail"]))

    def test_proxy_capacity_rejects_instead_of_hiding_unbounded_work(self):
        entered, release = threading.Event(), threading.Event()
        self.upstream.result = lambda _call: (entered.set(), release.wait(3), "0x1")[-1]
        proxy = self.proxy(FaultPlan(max_inflight=1))
        thread = threading.Thread(target=lambda: post(proxy.url, "eth_blockNumber", []))
        thread.start()
        self.assertTrue(entered.wait(1))
        try:
            with self.assertRaises(urllib.error.HTTPError) as error:
                post(proxy.url, "eth_blockNumber", [])
            self.assertEqual(error.exception.code, 503)
            self.assertEqual(proxy.snapshot()["proxy_overloads"], 1)
        finally:
            release.set()
            thread.join(timeout=3)


class OracleTests(unittest.TestCase):
    def setUp(self):
        self.expected = [{"id": "one", "chain": 1, "family": "evm", "outcome": "success", "effects": {"recipient": 1}, "intent_digest": "payload", "max_fee": 100}]
        self.observations = {"one": {"admitted": True,
            "attempts": [{"identity": "hash", "wire_digest": "wire", "replay_key": "evm:1:sender:0", "wire_replay_key": "evm:1:sender:0", "intent_digest": "payload"}],
            "executions": [{"identity": "hash", "outcome": "success", "effects": {"recipient": 1}, "fee": 42, "canonical": True, "finalized": True}],
            "terminal": {"identity": "hash", "outcome": "success"}}}
        self.drain = dict.fromkeys(DRAIN_FIELDS, 0)
    def evaluate(self):
        return evaluate_campaign(self.expected, self.observations, self.drain)
    def test_full_evidence_passes(self):
        self.assertEqual(self.evaluate()["outcome"], "pass")
    def test_duplicate_execution_cannot_hide_behind_matching_terminal(self):
        self.observations["one"]["executions"].append(copy.deepcopy(self.observations["one"]["executions"][0]))
        self.assertFalse(self.evaluate()["safety_pass"])
    def test_one_chain_effect_cannot_be_attributed_to_two_offered_ids(self):
        self.expected.append({**self.expected[0], "id": "two"})
        self.observations["two"] = copy.deepcopy(self.observations["one"])
        result = self.evaluate()
        self.assertFalse(result["safety_pass"])
        self.assertTrue(any("already attributed" in message for message in result["safety_failures"]))

    def test_wrong_terminal_or_early_finality_is_unsafe(self):
        for mutation in [lambda row: row["terminal"].update(outcome="revert"),
                         lambda row: row["terminal"].update(identity="other"),
                         lambda row: row["executions"][0].update(finalized=False),
                         lambda row: row["executions"][0].update(canonical=False)]:
            before = copy.deepcopy(self.observations)
            mutation(self.observations["one"])
            self.assertFalse(self.evaluate()["safety_pass"])
            self.observations = before
    def test_changed_attempt_payload_or_nonce_is_unsafe(self):
        for change in [{"intent_digest": "changed"}, {"replay_key": "evm:1:sender:1"}, {"wire_digest": "different"}]:
            extra = {**self.observations["one"]["attempts"][0], **change}
            self.observations["one"]["attempts"].append(extra)
            self.assertFalse(self.evaluate()["safety_pass"])
            self.observations["one"]["attempts"].pop()
    def test_fee_only_replacement_same_nonce_can_settle_once(self):
        self.observations["one"]["attempts"].append({"identity": "fee-bump", "wire_digest": "wire2", "replay_key": "evm:1:sender:0", "wire_replay_key": "evm:1:sender:0", "intent_digest": "payload"})
        self.observations["one"]["executions"][0]["identity"] = "fee-bump"
        self.observations["one"]["terminal"]["identity"] = "fee-bump"
        self.assertTrue(self.evaluate()["safety_pass"])
        self.expected[0]["family"] = "solana"
        self.assertFalse(self.evaluate()["safety_pass"], "Solana must retain original signature")
    def test_hidden_queue_load_and_dropped_offers_fail_liveness(self):
        self.drain["client_pending"] = 1
        self.assertFalse(self.evaluate()["liveness_pass"])
        self.assertTrue(self.evaluate()["safety_pass"])
        self.drain["client_pending"] = 0
        del self.drain["redis_delayed"]
        self.assertFalse(self.evaluate()["liveness_pass"], "missing observation is not zero backlog")
        self.drain["redis_delayed"] = 0
        self.expected.append({**self.expected[0], "id": "offered-but-dropped"})
        self.assertFalse(self.evaluate()["liveness_pass"])
    def test_extra_intent_fee_and_effect_mutations_fail(self):
        self.observations["unplanned"] = copy.deepcopy(self.observations["one"])
        self.assertFalse(self.evaluate()["safety_pass"])
        del self.observations["unplanned"]
        self.observations["one"]["executions"][0]["fee"] = 101
        self.assertFalse(self.evaluate()["safety_pass"])
        self.observations["one"]["executions"][0]["fee"] = 42
        self.observations["one"]["executions"][0]["effects"] = {"recipient": 2}
        self.assertFalse(self.evaluate()["safety_pass"])
    def test_expected_parked_requires_persisted_attempt_never_capacity_pass(self):
        self.expected[0].update(expected_to_park=True, outcome="revert", effects={})
        self.observations["one"].update(executions=[], terminal=None,
            retained={"signed_attempt": True, "journal_state": "admitted", "queue_state": "redis_delayed"})
        self.drain.update(redis_delayed=1, journal_unresolved=1)
        result = self.evaluate()
        self.assertEqual(result["outcome"], "expected_parked")
        self.assertFalse(result["eligible_for_rate_assessment"])
        self.assertTrue(result["safety_pass"] and result["liveness_pass"])
        self.drain["redis_delayed"] = 2
        self.assertFalse(self.evaluate()["liveness_pass"], "one parked ID cannot conceal extra work")


class FixtureTests(unittest.TestCase):
    def test_mixed_solana_wire_decodes_without_copying_expectations(self):
        for multi in [False, True]:
            for legacy in [False, True]:
                wire, payer, recipient = solana_wire(multi=multi, legacy=legacy)
                expected = build_solana_fixture(payer, recipient, "sol-1", int(multi), mixed=True)["expected"]
                actual = decode_solana_intent(wire, "sol-1")
                self.assertEqual(digest(actual), expected["intent_digest"])
                with self.assertRaises(FaultError):
                    decode_solana_intent(wire, "wrong-id")
        for wire in [solana_wire(extra=True)[0], solana_wire(invalid=True, extra=True)[0]]:
            with self.assertRaises(FaultError):
                decode_solana_wire(wire)
    def test_evm_mixed_fixture_uses_unique_slots_and_normalized_chain_tx(self):
        sender, recipient = "0x" + "1" * 40, "0x" + "4" * 40
        kinds = []
        for index in range(3):
            fixture = build_evm_fixture(31337, sender, recipient, f"id-{index}", index, mixed=True)
            expected, payload = fixture["expected"], fixture["payload"]
            kinds.append(expected["fixture"])
            param = payload["params"][0]
            tx = {"chainId": hex(31337), "from": sender, "to": param["to"], "value": param["value"], "input": param["data"]}
            self.assertEqual(digest(evm_intent_fields(tx)), expected["intent_digest"])
            if index:
                self.assertEqual(int(param["data"], 16), index + 1)
        self.assertEqual(kinds, ["transfer", "storage", "revert"])
        self.assertEqual([item[0] for item in evm_contract_setup()], ["anvil_setCode", "anvil_setCode"])
    def test_audit_is_full_on_disk_bounded_in_memory_and_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            audit = Audit(path)
            for index in range(300):
                audit.record("sample", index=index)
            summary = audit.snapshot()
            self.assertTrue(summary["tail_truncated"])
            self.assertEqual(len(summary["tail"]), 256)
            audit.close()
            records = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([r["sequence"] for r in records], list(range(1, 301)))
            self.assertTrue(all("utc" in row and "elapsed_seconds" in row for row in records))
            with self.assertRaises(FileExistsError):
                Audit(path)



# Captured from the six-intent local Engine/Anvil smoke; public test key 1 only.
SIGNED_EVM = "0x02f864827a6b80012b8252089411111111111111111111111111111111111111110180c080a0550ce7c6089abbb5acf70ce5a3cafc42795652dcb98e0bee17a29b3e7fbeab6ca0138bec0e7924b597ae870da7d660eb1ec1b7843542959c06d4ed8ee7d766264d"
EVM_TX = {"type": "0x2", "chainId": "0x7a6b", "nonce": "0x0", "gas": "0x5208", "maxFeePerGas": "0x2b", "maxPriorityFeePerGas": "0x1",
          "to": "0x1111111111111111111111111111111111111111", "value": "0x1", "accessList": [], "input": "0x",
          "r": "0x550ce7c6089abbb5acf70ce5a3cafc42795652dcb98e0bee17a29b3e7fbeab6c",
          "s": "0x138bec0e7924b597ae870da7d660eb1ec1b7843542959c06d4ed8ee7d766264d", "yParity": "0x0", "v": "0x0",
          "hash": "0xfef62e4633369a378eebe2a8364c45970de07ba14032d67ed855b52c9dd72952", "from": "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"}
CAST = os.environ.get("CAST_BIN") or shutil.which("cast")


class EvmDecoderTests(unittest.TestCase):
    def test_real_captured_wire_rejects_changed_rpc_signature_nonce_or_fees(self):
        decoded = verify_evm_node_wire(SIGNED_EVM, EVM_TX, EVM_TX["hash"])
        self.assertEqual(decoded["wire_replay_key"], "evm:31339:0x7e5f4552091a69125d5dfcb7b8c2659029395bdf:0")
        for key, value in [("nonce", "0x1"), ("gas", "0x5209"), ("value", "0x2"), ("r", "0x1"), ("maxFeePerGas", "0x2c"), ("input", "0x00")]:
            with self.subTest(field=key), self.assertRaises(FaultError):
                verify_evm_node_wire(SIGNED_EVM, {**EVM_TX, key: value})
        with self.assertRaises(FaultError):
            verify_evm_node_wire(SIGNED_EVM, EVM_TX, "0x" + "00" * 32)

    @unittest.skipUnless(CAST, "set CAST_BIN to pinned Foundry Cast for independent offline signature recovery")
    def test_cast_recovers_real_wire_hash_and_sender_without_node_history(self):
        decoded = decode_evm_wire(SIGNED_EVM, CAST)
        self.assertEqual(decoded["identity"], EVM_TX["hash"])
        self.assertEqual(decoded["intent_fields"]["sender"], EVM_TX["from"])
        self.assertEqual(decoded["intent_fields"]["chain"], 31339)
        self.assertEqual(decoded["intent_fields"]["value"], 1)

    @unittest.skipUnless(CAST, "set CAST_BIN to pinned Foundry Cast for independent envelope decoding")
    def test_legacy_access_list_and_type4_layout_agree_with_independent_cast(self):
        for kind in (0, 1, 4):
            tx = {**EVM_TX, "type": hex(kind), "nonce": "0x5", "r": "0x1", "s": "0x1", "gasPrice": "0x3", "v": hex(31339 * 2 + 35) if kind == 0 else "0x0"}
            if kind == 4:
                tx["authorizationList"] = [{"chainId": "0x7a6b", "address": "0x2222222222222222222222222222222222222222", "nonce": "0x3", "yParity": "0x0", "r": "0x2", "s": "0x2"}]
            with self.subTest(kind=kind):
                decoded = decode_evm_wire("0x" + evm_node_wire(tx).hex(), CAST)
                self.assertEqual(decoded["intent_fields"]["chain"], 31339)
                self.assertEqual(decoded["intent_fields"]["to"], EVM_TX["to"])
                self.assertTrue(decoded["wire_replay_key"].endswith(":5"))


class KeepAliveTests(unittest.TestCase):
    def test_valid_json_with_truncated_http_framing_is_not_accepted_or_retried(self):
        upstream = Upstream(truncated=True)
        client = KeepAliveHttp()
        body = b'{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
        try:
            with self.assertRaisesRegex(FaultError, "truncated HTTP response"):
                client.request(upstream.url, body)
            self.assertEqual(len(upstream.calls), 1)
            self.assertEqual(client.snapshot()["owned_connections"], 0)
            self.assertEqual(client.snapshot().get("responses", 0), 0)
        finally:
            client.close()
            upstream.close()

    def test_actual_socket_reuse_on_both_proxy_hops(self):
        upstream = Upstream()
        proxy = RpcFaultProxy(upstream.url)
        client = KeepAliveHttp()
        try:
            for index in range(5):
                body = json.dumps({"jsonrpc": "2.0", "id": index, "method": "eth_blockNumber", "params": []}).encode()
                status, raw = client.request(proxy.url, body)
                self.assertEqual((status, json.loads(raw)["result"]), (200, "0x1"))
            self.assertEqual(len(set(upstream.client_ports)), 1, "upstream TCP socket was not reused")
            self.assertEqual(client.snapshot()["connections_opened"], 1)
            self.assertEqual(client.snapshot()["connection_reuses"], 4)
            self.assertEqual(proxy.snapshot()["http_transport"]["connections_opened"], 1)
            self.assertEqual(proxy.snapshot()["http_transport"]["connection_reuses"], 4)
            self.assertEqual(proxy.snapshot()["http_requests"], 5)
        finally:
            client.close()
            proxy.close()
            upstream.close()

    def test_lost_accepted_post_never_retries_inside_transport(self):
        upstream = Upstream()
        proxy = RpcFaultProxy(upstream.url, FaultPlan(drop_accepted_sends=1))
        client = KeepAliveHttp()
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction", "params": ["0x1234"]}).encode()
        try:
            with self.assertRaises((http.client.HTTPException, OSError)):
                client.request(proxy.url, body)
            self.assertEqual(len(upstream.calls), 1, "lost accepted send was silently replayed")
            self.assertEqual(client.snapshot()["requests"], 1)
            self.assertEqual(client.snapshot()["failures"][0]["type"], "RemoteDisconnected")
            status, raw = client.request(proxy.url, body)
            self.assertEqual(status, 200)
            self.assertIn("result", json.loads(raw))
            self.assertEqual(len(upstream.calls), 2, "only the explicit caller retry may send again")
            self.assertEqual(client.snapshot()["connections_opened"], 2)
        finally:
            client.close()
            proxy.close()
            upstream.close()

    def test_response_cap_discards_socket_and_diagnostics_do_not_print_url(self):
        upstream = Upstream(result=lambda _call: "x" * 2048)
        client = KeepAliveHttp()
        body = b'{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
        try:
            with self.assertRaisesRegex(FaultError, "exceeds cap"):
                client.request(upstream.url, body, max_response_bytes=100)
            stats = client.snapshot()
            self.assertEqual(stats["owned_connections"], 0)
            self.assertNotIn(upstream.url, json.dumps(stats))
            self.assertEqual(stats["failures"][0]["type"], "FaultError")
        finally:
            client.close()
            upstream.close()


if __name__ == "__main__":
    unittest.main()
