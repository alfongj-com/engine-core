"""Offline checks for the public harness's spending guard; no provider calls."""
import concurrent.futures
import copy
import http.client
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from testnet_evm_write import Guard, finalize_guard, rlp, validate_revert_target, verify_finalized_receipt, verify_retained_terminal


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

    def test_reverting_call_cannot_transfer_value_or_change_target(self):
        self.guard.m["value_wei"] = 0
        zero_value = self.fields.copy()
        zero_value[6] = 0
        self.guard.validate(self.wire(zero_value))
        with self.assertRaises(AssertionError):
            self.guard.validate(self.wire())
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
                    # The guard closes before reading the rejected request body.
                    # Depending on TCP timing, the peer observes EOF or a reset.
                    with self.assertRaises((http.client.RemoteDisconnected, ConnectionResetError)):
                        connection.getresponse()
                finally:
                    connection.close()
            forward.assert_not_called()
        self.assertEqual(self.guard.calls, 0)
        self.assertEqual(self.guard.errors, [
            "AssertionError: Browser requests refused",
            "AssertionError: Non-loopback Host refused",
            "AssertionError: Unexpected guard path",
        ])

    def test_fixture_request_policy_rejects_before_forwarding(self):
        port = self.guard.server.server_address[1]
        body = {"jsonrpc": "2.0", "id": 1, "method": "eth_sendTransaction", "params": [{}]}
        with patch.object(self.guard, "validate_request", side_effect=RuntimeError("Method refused")) as policy, \
                patch("testnet_evm_write.request") as forward:
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            try:
                connection.request("POST", "/", json.dumps(body))
                with self.assertRaises((http.client.RemoteDisconnected, ConnectionResetError)):
                    connection.getresponse()
            finally:
                connection.close()
            policy.assert_called_once_with(body)
            forward.assert_not_called()
        self.assertEqual(self.guard.calls, 0)
        self.assertEqual(self.guard.wires, {})

    def test_shutdown_waits_for_inflight_failure_before_publishing_success(self):
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        report = {"outcome": "pass"}

        def upstream(*_args):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("Test failed to release upstream")
            raise RuntimeError("Late gateway failure")

        def client():
            connection = http.client.HTTPConnection("127.0.0.1", self.guard.server.server_address[1], timeout=5)
            try:
                connection.request("POST", "/", json.dumps({
                    "jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}))
                connection.getresponse()
            except (http.client.RemoteDisconnected, ConnectionResetError):
                pass
            finally:
                connection.close()

        def finish():
            finalize_guard(self.guard, report)
            finished.set()

        with patch("testnet_evm_write.request", side_effect=upstream):
            caller = threading.Thread(target=client)
            caller.start()
            finalizer = None
            try:
                self.assertTrue(entered.wait(2))
                # Stop accepting first; the remaining close must wait for this
                # handler, even after the accept loop has already stopped.
                self.guard.server.shutdown()
                finalizer = threading.Thread(target=finish)
                finalizer.start()
                self.assertFalse(finished.wait(.1))
            finally:
                release.set()
                caller.join(5)
                if finalizer:
                    finalizer.join(5)
        self.assertTrue(finished.is_set())
        self.assertEqual(report["outcome"], "fail")
        self.assertEqual(report["guard_errors"], ["RuntimeError: Late gateway failure"])


class FinalityTests(unittest.TestCase):
    def test_retained_terminal_requires_outcome_identity_and_finalized_policy(self):
        receipt = {"transactionHash": "0x" + "11" * 32, "blockNumber": "0x5",
                   "blockHash": "0x" + "22" * 32, "status": "0x1"}
        finality = {"blockNumber": 5, "blockHash": receipt["blockHash"], "checkpointNumber": 6,
                    "checkpointHash": "0x" + "33" * 32, "policy": {"mode": "finalized"}}
        with patch("testnet_evm_write.rpc") as public_rpc:
            for status, outcome in [("confirmed", "0x1"), ("failed", "0x0")]:
                matching_receipt = {**receipt, "status": outcome}
                read = unittest.mock.Mock(return_value=[status, json.dumps(matching_receipt), json.dumps(finality)])
                self.assertEqual(verify_retained_terminal(read, "namespace", "intent", matching_receipt, outcome),
                                 {"status": status, "finality": finality})
                read.assert_called_once_with("HMGET", "namespace:eoa_executor:tx_data:intent", "status", "receipt", "finality")
            public_rpc.assert_not_called()
        invalid = [
            ("pending", receipt, finality), ("failed", receipt, finality),
            ("confirmed", {**receipt, "transactionHash": "0x" + "44" * 32}, finality),
            ("confirmed", receipt, {**finality, "policy": {"mode": "depth", "confirmations": 1000}}),
            ("confirmed", receipt, {**finality, "checkpointNumber": 4}),
            ("confirmed", receipt, {**finality, "blockNumber": 6}),
            ("confirmed", receipt, {**finality, "blockHash": "0x" + "44" * 32}),
            ("confirmed", receipt, {**finality, "checkpointNumber": 5}),
        ]
        for status, stored_receipt, stored_finality in invalid:
            with self.subTest(status=status, finality=stored_finality), self.assertRaises(AssertionError):
                verify_retained_terminal(lambda *_: [status, json.dumps(stored_receipt), json.dumps(stored_finality)],
                                         "namespace", "intent", receipt, "0x1")
        for missing in (["confirmed", None, json.dumps(finality)], ["confirmed", json.dumps(receipt), None]):
            with self.subTest(missing=missing), self.assertRaises(AssertionError):
                verify_retained_terminal(lambda *_: missing, "namespace", "intent", receipt, "0x1")

    def test_canonical_receipt_alone_is_not_finalized(self):
        for response in [None, {"number": "0x4", "hash": "checkpoint"}]:
            with self.subTest(response=response), patch("testnet_evm_write.rpc", return_value=response) as read:
                with self.assertRaises(AssertionError):
                    verify_finalized_receipt("local", "0x5", "receipt")
                self.assertEqual(read.call_args.args[2], ["finalized", False])
                self.assertEqual(read.call_count, 1)

    def test_covering_checkpoint_and_both_canonical_identities_are_required(self):
        valid = [{"number": "0x6", "hash": "checkpoint"}, {"number": "0x5", "hash": "receipt"},
                 {"number": "0x6", "hash": "checkpoint"}]
        with patch("testnet_evm_write.rpc", side_effect=valid):
            verify_finalized_receipt("local", "0x5", "receipt")
        for index in (1, 2):
            responses = [dict(value) for value in valid]
            responses[index]["hash"] = "orphan"
            with self.subTest(index=index), patch("testnet_evm_write.rpc", side_effect=responses):
                with self.assertRaises(AssertionError):
                    verify_finalized_receipt("local", "0x5", "receipt")

    def test_revert_preflight_rejects_transport_errors_and_success(self):
        failures = [{"jsonrpc": "2.0", "id": 1, "result": "0x"},
                    {"jsonrpc": "2.0", "id": 1, "error": {"code": -32005, "message": "rate limited"}},
                    {"jsonrpc": "2.0", "id": 2, "error": {"code": 3}}]
        for response in failures:
            with self.subTest(response=response), patch("testnet_evm_write.request", return_value=(200, response)):
                with self.assertRaises(AssertionError):
                    validate_revert_target("local", "sender", "recipient", "0x60006000fd")
        with patch("testnet_evm_write.request", return_value=(200, {"jsonrpc": "2.0", "id": 1, "error": {"code": 3}})):
            validate_revert_target("local", "sender", "recipient", "0x60006000fd")

    def test_retained_checkpoint_must_also_remain_canonical(self):
        finalized = {"number": "0x8", "hash": "current"}
        valid = [finalized, {"number": "0x5", "hash": "receipt"}, finalized,
                 {"number": "0x6", "hash": "retained"}]
        evidence = {"checkpointNumber": 6, "checkpointHash": "retained"}
        with patch("testnet_evm_write.rpc", side_effect=valid):
            verify_finalized_receipt("local", "0x5", "receipt", evidence)
        invalid = copy.deepcopy(valid)
        invalid[-1]["hash"] = "orphan"
        with patch("testnet_evm_write.rpc", side_effect=invalid), self.assertRaises(AssertionError):
            verify_finalized_receipt("local", "0x5", "receipt", evidence)


if __name__ == "__main__":
    unittest.main()
