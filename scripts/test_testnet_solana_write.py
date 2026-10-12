"""Offline adversarial checks for the public Devnet harness; no provider calls."""
import base64
import copy
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from testnet_solana_write import (MEMO, PAYER, RPC, SYSTEM, PolicyProxy, Run,
                                 b58decode, b58encode, inspect_wire, payload,
                                 request, validate_manifest)


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


if __name__ == "__main__":
    unittest.main()
