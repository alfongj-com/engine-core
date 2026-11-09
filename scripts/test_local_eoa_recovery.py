"""Offline adversarial checks for the cross-binary EOA identity oracle."""
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from local_eoa_recovery import capture_intent_hashes, verify_original_receipt


class OriginalIntentBindingTests(unittest.TestCase):
    def snapshot(self, rows, ids, pending):
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "journal.sqlite"
            with sqlite3.connect(journal) as db:
                db.execute("CREATE TABLE attempts(sequence INTEGER PRIMARY KEY,id TEXT,payload TEXT)")
                db.executemany("INSERT INTO attempts(id,payload) VALUES(?,?)", [
                    (transaction_id, json.dumps({"transactionHash": transaction_hash}))
                    for transaction_id, transaction_hash in rows])
            return capture_intent_hashes(journal, ids, pending)

    def test_each_receipt_keeps_its_original_intent_binding(self):
        hashes = self.snapshot([("invoice-a", "0xAA"), ("invoice-b", "0xBB")],
                               ["invoice-a", "invoice-b"], {"0xaa", "0xbb"})
        verify_original_receipt("invoice-a", {"transactionHash": "0xAA"}, hashes)
        verify_original_receipt("invoice-b", {"transactionHash": "0xBB"}, hashes)
        for transaction_id, transaction_hash in [("invoice-a", "0xbb"), ("invoice-b", "0xaa"), ("missing", "0xaa")]:
            with self.subTest(transaction_id=transaction_id):
                with self.assertRaisesRegex(AssertionError, "intent-to-transaction binding"):
                    verify_original_receipt(transaction_id, {"transactionHash": transaction_hash}, hashes)

    def test_missing_original_attempt_is_not_upgrade_evidence(self):
        with self.assertRaisesRegex(AssertionError, "no durable signed attempt"):
            self.snapshot([("invoice-a", "0xaa")], ["invoice-a", "invoice-b"], {"0xaa"})

    def test_shared_hash_cannot_cover_two_intents(self):
        with self.assertRaisesRegex(AssertionError, "multiple intent IDs"):
            self.snapshot([("invoice-a", "0xaa"), ("invoice-b", "0xaa")], ["invoice-a", "invoice-b"], {"0xaa"})

    def test_chain_and_original_journal_must_agree_before_upgrade(self):
        with self.assertRaisesRegex(AssertionError, "differ from Anvil"):
            self.snapshot([("invoice-a", "0xaa")], ["invoice-a"], {"0xbb"})


if __name__ == "__main__":
    unittest.main()
