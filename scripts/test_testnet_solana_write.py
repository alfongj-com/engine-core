"""Offline adversarial checks for the public Devnet harness; no provider calls."""
import base64
import copy
from http.client import RemoteDisconnected
import json
import struct
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from testnet_solana_write import (DEVNET_GENESIS, MEMO, PAYER, RPC, SYSTEM, PolicyProxy, Run,
                                 b58decode, b58encode, finish_run, inspect_wire, payload,
                                 request, validate_manifest, verify_effects)


def shortvec(number):
    encoded = bytearray()
    while number > 127:
        encoded.append((number & 127) | 128)
        number >>= 7
    return bytes(encoded) + bytes([number])


def fixture(amount=890880, recipient=None, identity=None, signature=b"s" * 64,
            extra_instruction=False, lookup=b"\0"):
    recipient = recipient or b58encode(bytes(range(32)))
    identity = identity or "public-solana-" + "a" * 32 + "-0"
    transfer = struct.pack("<IQ", 2, amount)
    memo = ("thirdweb-engine:" + identity).encode()
    message = (b"\x80\x01\x00\x02\x04" + b58decode(PAYER) + b58decode(recipient)
               + b58decode(SYSTEM) + b58decode(MEMO) + b"b" * 32
               + bytes([3 if extra_instruction else 2])
               + b"\x02\x02\x00\x01" + shortvec(len(transfer)) + transfer
               + b"\x03\x00" + shortvec(len(memo)) + memo)
    if extra_instruction:
        message += b"\x02\x00\x00"
    return base64.b64encode(b"\x01" + signature + message + lookup).decode()


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.recipient = b58encode(bytes(range(32)))
        self.run_id = "public-solana-" + "a" * 32
        self.identity = self.run_id + "-0"
        self.manifest = {"version": 1, "run_id": self.run_id, "payer": PAYER,
                         "recipient": self.recipient, "gateway": RPC, "crash": False,
                         "payloads": [payload(self.identity, self.recipient, 890880)],
                         "rent_exempt_minimum_lamports": 890880, "total_transfer_lamports": 890880,
                         "wires": {}, "rpc_reserved": 0}
        self.run = Run(Path(self.temp.name), self.manifest, 30)
        self.proxy = PolicyProxy(self.run)

    def tearDown(self):
        self.proxy.close()
        self.temp.cleanup()

    def test_wire_business_effect_guards(self):
        self.assertEqual(inspect_wire(fixture(), self.manifest)["intent"], self.identity)
        for wire in [fixture(amount=890881), fixture(recipient=b58encode(b"z" * 32)),
                     fixture(identity="unknown"), fixture(extra_instruction=True), fixture(lookup=b"\x01"),
                     base64.b64encode(base64.b64decode(fixture())[:-1]).decode(),
                     base64.b64encode(base64.b64decode(fixture()) + b"extra").decode()]:
            with self.subTest(wire=wire[:20]), self.assertRaises(RuntimeError):
                inspect_wire(wire, self.manifest)

    def test_manifest_cannot_expand_spending_on_resume(self):
        validate_manifest(self.manifest)
        for mutate in [lambda plan: plan.update(total_transfer_lamports=1),
                       lambda plan: plan["payloads"][0]["executionOptions"].update(chainId="solana:mainnet"),
                       lambda plan: plan["payloads"][0]["executionOptions"].update(priorityFee={"type": "auto"}),
                       lambda plan: plan.update(recipient=PAYER)]:
            plan = copy.deepcopy(self.manifest)
            mutate(plan)
            with self.assertRaises(RuntimeError):
                validate_manifest(plan)

    def test_broadcast_is_recorded_before_forward_and_fresh_signature_refused(self):
        wire = fixture()
        info = inspect_wire(wire, self.manifest)
        params = [wire, {"encoding": "base64", "maxRetries": 0, "skipPreflight": False}]
        def rpc(method, _params):
            if method == "getFeeForMessage":
                return {"value": 5000}
            persisted = json.loads((Path(self.temp.name) / "manifest.json").read_text())
            self.assertEqual(persisted["wires"][self.identity]["broadcasts"], 1)
            self.assertEqual(persisted["wires"][self.identity]["wire"], wire)
            return info["signature"]
        attempt = json.dumps({"signed_transaction": wire, "signature": info["signature"]})
        with patch.object(self.run, "redis", return_value=attempt), patch.object(self.run, "rpc", side_effect=rpc) as calls:
            self.proxy.broadcast(params)
            with self.assertRaisesRegex(RuntimeError, "Fresh signature"):
                self.proxy.broadcast([fixture(signature=b"t" * 64), params[1]])
            self.assertEqual(calls.call_count, 2)

    def test_unpersisted_or_overpriced_transaction_never_sent(self):
        wire = fixture()
        info = inspect_wire(wire, self.manifest)
        params = [wire, {"encoding": "base64", "maxRetries": 0, "skipPreflight": False}]
        with patch.object(self.run, "redis", return_value=None), patch.object(self.run, "rpc") as calls:
            with self.assertRaisesRegex(RuntimeError, "persist"):
                self.proxy.broadcast(params)
            calls.assert_not_called()
        attempt = json.dumps({"signed_transaction": wire, "signature": info["signature"]})
        with patch.object(self.run, "redis", return_value=attempt), patch.object(self.run, "rpc", return_value={"value": 10001}) as calls:
            with self.assertRaisesRegex(RuntimeError, "fee"):
                self.proxy.broadcast(params)
            self.assertEqual([call.args[0] for call in calls.call_args_list], ["getFeeForMessage"])
        self.assertEqual(self.manifest["wires"], {})

    def test_browser_origin_wrong_host_or_path_never_forward(self):
        body = {"jsonrpc": "2.0", "id": 1, "method": "getVersion", "params": []}
        with patch.object(self.run, "rpc") as calls:
            for path, headers in [("/", {"Origin": "https://hostile.example"}),
                                  ("/", {"Host": "hostile.example"}),
                                  ("/bad", {}), ("/", {"Sec-Fetch-Site": "same-origin"})]:
                with self.subTest(path=path, headers=headers), self.assertRaises(Exception):
                    request(f"http://127.0.0.1:{self.run.proxy_port}{path}", body, headers)
            calls.assert_not_called()

    def test_engine_genesis_check_is_budgeted_before_forwarding(self):
        forwarded = []

        def upstream(url, body):
            forwarded.append(body)
            self.assertEqual(url, RPC)
            self.assertEqual(body, {"jsonrpc": "2.0", "id": 1,
                                    "method": "getGenesisHash", "params": []})
            persisted = json.loads((Path(self.temp.name) / "manifest.json").read_text())
            self.assertEqual(persisted["rpc_reserved"], len(forwarded))
            self.assertEqual(persisted["rpc_methods"]["getGenesisHash"], len(forwarded))
            return 200, {"x-engine-rpc-gateway": "1"}, {
                "jsonrpc": "2.0", "id": 1, "result": DEVNET_GENESIS}

        # The local client remains real HTTP. Only Run.rpc's upstream transport
        # is replaced, so admission, durable accounting and response mapping run.
        with patch("testnet_solana_write.request", side_effect=upstream):
            for rpc_id, params in [(7, None), (8, [])]:
                with self.subTest(params=params):
                    status, _, response = request(f"http://127.0.0.1:{self.run.proxy_port}/", {
                        "jsonrpc": "2.0", "id": rpc_id, "method": "getGenesisHash", "params": params})
                    self.assertEqual(status, 200)
                    self.assertEqual(response, {"jsonrpc": "2.0", "id": rpc_id,
                                                "result": DEVNET_GENESIS})
        self.assertEqual(len(forwarded), 2)
        self.assertNotIn("proxy_failure", self.manifest)

    def test_wrong_genesis_stops_run_without_returning_a_network_identity(self):
        unexpected = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
        with patch("testnet_solana_write.request", return_value=(
                200, {"x-engine-rpc-gateway": "1"},
                {"jsonrpc": "2.0", "id": 1, "result": unexpected})) as upstream:
            with self.assertRaises(RemoteDisconnected):
                request(f"http://127.0.0.1:{self.run.proxy_port}/", {
                    "jsonrpc": "2.0", "id": 9, "method": "getGenesisHash", "params": None})
        upstream.assert_called_once_with(RPC, {
            "jsonrpc": "2.0", "id": 1, "method": "getGenesisHash", "params": []})
        persisted = json.loads((Path(self.temp.name) / "manifest.json").read_text())
        self.assertEqual(persisted["rpc_reserved"], 1)
        self.assertEqual(persisted["rpc_methods"]["getGenesisHash"], 1)
        self.assertEqual(persisted["proxy_failure"],
                         "Policy/RPC failure: RuntimeError: Only Solana Devnet is allowed")
        self.assertEqual(persisted["wires"], {})

    def test_shutdown_drains_late_proxy_failure_before_marking_run_passed(self):
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        self.run.proxy = self.proxy
        report = {"outcome": "pass"}

        def upstream(*_args):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("Test failed to release upstream")
            return 200, {"x-engine-rpc-gateway": "1"}, {
                "jsonrpc": "2.0", "id": 1, "result": "wrong-genesis"}

        def client():
            try:
                request(f"http://127.0.0.1:{self.run.proxy_port}/", {
                    "jsonrpc": "2.0", "id": 1, "method": "getGenesisHash", "params": None})
            except (RemoteDisconnected, ConnectionResetError):
                pass

        def finish():
            finish_run(self.run, report)
            finished.set()

        with patch("testnet_solana_write.request", side_effect=upstream):
            caller = threading.Thread(target=client)
            caller.start()
            finalizer = None
            try:
                self.assertTrue(entered.wait(2))
                self.proxy.server.shutdown()
                finalizer = threading.Thread(target=finish)
                finalizer.start()
                self.assertFalse(finished.wait(.1))
            finally:
                release.set()
                caller.join(5)
                if finalizer:
                    finalizer.join(5)
        self.assertTrue(finished.is_set())
        self.assertEqual(report["outcome"], "unknown")
        persisted = json.loads((Path(self.temp.name) / "manifest.json").read_text())
        self.assertEqual(persisted["outcome"], "unknown")
        self.assertEqual(persisted["rpc_reserved"], 1)
        self.assertEqual(report["proxy_failure"], persisted["proxy_failure"])

    def test_queue_success_still_requires_independent_finalized_effect(self):
        info = inspect_wire(fixture(), self.manifest)
        signature = info["signature"]
        self.manifest.update(wires={self.identity: {**info, "broadcasts": 1}},
                             initial_payer_lamports=1_000_000, initial_recipient_lamports=0,
                             admitted_at={self.identity: 1}, finalized_observed_at={self.identity: 2},
                             created_at_unix=1, provenance={}, rpc_methods={})
        valid = {"slot": 2, "transaction": {"signatures": [signature], "message": {
            "accountKeys": [PAYER, self.recipient]}}, "meta": {"err": None, "fee": 5000,
            "preBalances": [1_000_000, 0], "postBalances": [104120, 890880]}}

        def redis(command, *_args):
            if command == "LLEN":
                return 0 if _args[0].endswith(":failed") else 1
            if command == "HGET":
                return json.dumps({"signature": signature})
            if command == "KEYS":
                return []
            self.fail("Unexpected Redis operation")

        def provider(receipt):
            def rpc(method, params):
                if method == "getTransaction":
                    self.assertEqual(params[1]["commitment"], "finalized")
                    return receipt
                if method == "getBalance":
                    self.assertEqual(params[1]["commitment"], "finalized")
                    return {"value": 104120 if params[0] == PAYER else 890880}
                if method == "getSignaturesForAddress":
                    return [{"signature": signature}]
                self.fail("Unexpected provider operation")
            return rpc

        bad_receipts = [None, {**valid, "meta": {**valid["meta"], "err": "execution failed"}},
                        {**valid, "meta": {**valid["meta"], "postBalances": [104119, 890880]}}]
        for receipt in bad_receipts:
            with self.subTest(receipt=receipt is None), patch.object(self.run, "redis", side_effect=redis), \
                    patch.object(self.run, "rpc", side_effect=provider(receipt)), patch.object(self.run, "submit") as submit:
                with self.assertRaises(RuntimeError):
                    verify_effects(self.run)
                submit.assert_not_called()
        with patch.object(self.run, "redis", side_effect=redis), patch.object(self.run, "rpc", side_effect=provider(valid)), \
                patch.object(self.run, "submit"), patch("testnet_solana_write.time.sleep"):
            self.assertEqual(verify_effects(self.run)["unique_finalized_effects"], 1)
        # A forbidden duplicate path may be stopped by the guard before any
        # additional wire reaches the chain. That still invalidates the run.
        def duplicate_policy_failure(_payload):
            self.manifest["proxy_failure"] = "Unexpected duplicate RPC method"
        with patch.object(self.run, "redis", side_effect=redis), patch.object(self.run, "rpc", side_effect=provider(valid)), \
                patch.object(self.run, "submit", side_effect=duplicate_policy_failure), patch("testnet_solana_write.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "Policy proxy failed"):
                verify_effects(self.run)


if __name__ == "__main__":
    unittest.main()
