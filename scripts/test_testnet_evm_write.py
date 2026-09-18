"""Offline checks for the public harness's spending guard; no provider calls."""
import concurrent.futures
import http.client
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from testnet_evm_write import Guard, rlp


def encode(value):
    if isinstance(value, list):
        raw = b"".join(encode(v) for v in value)
        prefix = 192
    else:
        raw = value if isinstance(value, bytes) else (value.to_bytes((value.bit_length()+7)//8, "big"))
        if len(raw) == 1 and raw[0] < 128:
            return raw
        prefix = 128
    if len(raw) < 56:
        return bytes([prefix + len(raw)]) + raw
    length = len(raw).to_bytes((len(raw).bit_length()+7)//8, "big")
    return bytes([prefix + 55 + len(length)]) + length + raw


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.guard = Guard({"chain_id": 11155111, "initial_nonce": 7, "transactions": 2,
                            "recipient": "0x" + "11" * 20, "max_fee_per_gas": 100, "gas_limit": 30000}, Path(self.temp.name), 0)
        self.fields = [11155111, 7, 10, 100, 21000, bytes.fromhex("11" * 20), 1, b"", [], 0, 1, 2]

    def tearDown(self):
        self.guard.close()
        self.temp.cleanup()

    def wire(self, fields=None):
        return "0x02" + encode(fields or self.fields).hex()

    def test_concurrent_attempts_reserve_before_any_response(self):
        def attempt(_):
            try:
                self.guard.validate(self.wire())
                return True
            except AssertionError:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            outcomes = list(pool.map(attempt, range(60)))
        self.assertEqual(sum(outcomes), 20)
        self.assertEqual(self.guard.wires["7"]["attempts"], 20)
        self.assertEqual(self.guard.wires["7"]["responses"], [])

    def test_wrong_intent_or_spending_limits_reject_before_reservation(self):
        for index, value in [(0, 1), (1, 6), (1, 9), (2, 101), (3, 101), (4, 30001),
                             (5, bytes.fromhex("22" * 20)), (6, 2), (7, b"code"), (8, [b"access"])]:
            with self.subTest(index=index, value=value):
                fields = self.fields.copy()
                fields[index] = value
                with self.assertRaises(AssertionError):
                    self.guard.validate(self.wire(fields))
                self.assertEqual(self.guard.wires, {})

    def test_same_nonce_different_signature_rejects(self):
        self.guard.validate(self.wire())
        replacement = self.fields.copy()
        replacement[-1] = 3
        with self.assertRaises(AssertionError):
            self.guard.validate(self.wire(replacement))
        self.assertEqual(self.guard.wires["7"]["attempts"], 1)

    def test_truncated_or_trailing_rlp_rejects(self):
        raw = encode(self.fields)
        for bad in [raw[:-1], raw+b"\x00", b"\xff", b"\xc1\x82\x01"]:
            with self.assertRaises(ValueError):
                rlp(bad)

    def test_browser_origin_host_and_path_never_reach_gateway(self):
        port = self.guard.server.server_address[1]
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []})
        with patch("testnet_evm_write.request") as forward:
            for path, headers in [("/", {"Origin": "https://example.com"}),
                                  ("/", {"Host": "attacker.example"}), ("/unexpected", {})]:
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
                try:
                    connection.request("POST", path, body, headers)
                    with self.assertRaises(http.client.RemoteDisconnected):
                        connection.getresponse()
                finally:
                    connection.close()
            forward.assert_not_called()
        self.assertEqual(self.guard.calls, 0)


if __name__ == "__main__":
    unittest.main()
