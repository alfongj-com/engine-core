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
from testnet_evm_reconcile import NoBroadcastGuard, ReadBudget, reconcile_fees, verified_runtime


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
