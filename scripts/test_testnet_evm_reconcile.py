"""Read-only recovery safety gates; no paid RPCs or Engine processes."""
import copy
import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import testnet_evm_write as evm
from testnet_evm_reconcile import NoBroadcastGuard, ReadBudget, reconcile_fees, retained_transaction_hash, verified_runtime
from test_testnet_evm_write import encode


class ReconciliationTests(unittest.TestCase):
    def test_original_binary_and_original_harness_are_verified_separately(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "engine"
            binary.write_text("#!/bin/sh\nexit 0\n")
            os.chmod(binary, 0o700)
            saved = evm.provenance(binary, "a" * 40, evm.__file__)
            # Bind to committed original bytes, even when the current reconciler
            # imports a newer forwarding helper. Neither version is relabeled.
            original_source = subprocess.check_output(["git", "show", "HEAD:scripts/testnet_evm_write.py"], cwd=evm.ROOT)
            saved["harness_sha256"] = hashlib.sha256(original_source).hexdigest()
            manifest = {"provenance": saved, "binary_sha256": saved["engine_binary_sha256"]}
            original = copy.deepcopy(manifest)
            verified, current = verified_runtime(manifest, original)
            self.assertEqual(verified, saved)
            self.assertEqual(current["engine_binary_sha256"], saved["engine_binary_sha256"])
            without_source = copy.deepcopy(manifest)
            without_source["provenance"]["declared_binary_source_commit"] = None
            old, now = verified_runtime(without_source, copy.deepcopy(without_source))
            self.assertIsNone(old["declared_binary_source_commit"])
            self.assertIsNone(now["declared_binary_source_commit"])
            for field in ("engine_binary_sha256", "harness_sha256", "process_helper_sha256"):
                changed = copy.deepcopy(manifest)
                changed["provenance"][field] = "0" * 64
                other = copy.deepcopy(changed)
                if field == "engine_binary_sha256":
                    changed["binary_sha256"] = other["binary_sha256"] = "0" * 64
                with self.subTest(field=field), self.assertRaises(RuntimeError):
                    verified_runtime(changed, other)
            with self.assertRaisesRegex(RuntimeError, "disagree"):
                verified_runtime(manifest, {})
            with self.assertRaisesRegex(RuntimeError, "Incomplete"):
                verified_runtime({}, {})
            binary.write_text("changed")
            with self.assertRaisesRegex(RuntimeError, "binary changed"):
                verified_runtime(manifest, original)

    def test_wire_hash_handles_lost_replies_and_refuses_contradictory_hashes(self):
        fields = [11155111, 7, 10, 100, 21000, bytes.fromhex("11" * 20), 1, b"", [], 0, 1, 2]
        wire = "0x02" + encode(fields).hex()
        expected = "0x" + "33" * 32
        entry = {"wire": wire, "responses": [{"error": {"code": -32000}}, {"error": {"code": -32000}}]}
        with patch("testnet_evm_reconcile.subprocess.check_output", return_value=expected + "\n") as keccak:
            self.assertEqual(retained_transaction_hash(entry, "/offline/cast"), expected)
            keccak.assert_called_once_with(["/offline/cast", "keccak", wire], text=True, stderr=subprocess.PIPE, timeout=5)
            entry["responses"].append({"result": expected})
            self.assertEqual(retained_transaction_hash(entry, "/offline/cast"), expected)
            entry["responses"].append({"result": "0x" + "44" * 32})
            with self.assertRaisesRegex(RuntimeError, "disagrees"):
                retained_transaction_hash(entry, "/offline/cast")
        with patch("testnet_evm_reconcile.subprocess.check_output", return_value="SHA3 is not Keccak"):
            with self.assertRaisesRegex(RuntimeError, "Invalid local"):
                retained_transaction_hash({"wire": wire, "responses": []}, "/offline/cast")

    @unittest.skipUnless(os.environ.get("CAST_BIN"), "Set CAST_BIN for native offline Keccak")
    def test_native_keccak_checks_the_exact_retained_bytes_without_rpc(self):
        wire = "0x02" + encode([11155111, 7, 10, 100, 21000, bytes.fromhex("11" * 20), 1, b"", [], 0, 1, 2]).hex()
        with patch("testnet_evm_write.request") as rpc:
            actual = retained_transaction_hash({"wire": wire, "responses": []}, os.environ["CAST_BIN"])
            self.assertRegex(actual, r"^0x[0-9a-f]{64}$")
            different = retained_transaction_hash({"wire": wire[:-2] + "03", "responses": []}, os.environ["CAST_BIN"])
            self.assertNotEqual(actual, different)
            rpc.assert_not_called()

    def test_fee_reconciliation_preserves_original_allocation_including_operator_fees(self):
        manifest = {"initial_sender_balance_wei": 1000, "transactions": 4, "total_debit_allocation_wei": 100}
        self.assertEqual(reconcile_fees(manifest, 916, 60, 10), (80, 10))
        for balance in (930, 895):
            with self.assertRaises(RuntimeError):
                reconcile_fees(manifest, balance, 60, 10)

    def test_read_budget_counts_failed_reads_and_refuses_writes_before_forwarding(self):
        with patch("testnet_evm_write.request", side_effect=OSError("unavailable")) as forward:
            budget = ReadBudget(11155111, 1, forward)
            url = evm.GATEWAY + "/11155111"
            read = {"method": "eth_getTransactionReceipt"}
            with self.assertRaises(OSError):
                budget.request(url, read)
            self.assertEqual((budget.count, budget.inflight), (1, 0))
            with self.assertRaisesRegex(RuntimeError, "budget exhausted"):
                budget.request(url, read)
            with self.assertRaisesRegex(RuntimeError, "only read"):
                budget.request(url, {"method": "eth_sendRawTransaction"})
            with self.assertRaisesRegex(RuntimeError, "only read"):
                budget.request(evm.GATEWAY + "/84532", read)
            forward.assert_called_once()
            self.assertTrue(budget.drain())

    def test_no_broadcast_guard_refuses_even_a_previously_accepted_wire(self):
        with tempfile.TemporaryDirectory() as directory:
            guard = NoBroadcastGuard({"chain_id": 11155111}, Path(directory), 0)
            try:
                with patch("testnet_evm_write.request") as upstream:
                    connection = http.client.HTTPConnection("127.0.0.1", guard.server.server_address[1], timeout=2)
                    try:
                        connection.request("POST", "/", json.dumps({"jsonrpc": "2.0", "id": 1,
                            "method": "eth_sendRawTransaction", "params": ["retained-wire"]}))
                        with self.assertRaises(http.client.RemoteDisconnected):
                            connection.getresponse()
                    finally:
                        connection.close()
                    upstream.assert_not_called()
                self.assertEqual(guard.refused_broadcasts, 1)
                self.assertEqual(guard.wires, {})
                self.assertEqual(len(guard.errors), 1)
            finally:
                guard.close()


if __name__ == "__main__":
    unittest.main()
