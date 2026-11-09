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
                    (transaction_id, json.dumps({"transactionHash": transaction_hash, "nonce": nonce}))
                    for transaction_id, transaction_hash, nonce in rows])
            return capture_intent_hashes(journal, ids, pending)

    def pending(self, *hashes_and_nonces):
        return [{"hash": transaction_hash, "nonce": hex(nonce)} for transaction_hash, nonce in hashes_and_nonces]

    def test_each_receipt_keeps_its_original_intent_binding(self):
        hashes = self.snapshot([("invoice-a", "0xAA", 0), ("invoice-b", "0xBB", 1)],
                               ["invoice-a", "invoice-b"], self.pending(("0xaa", 0), ("0xbb", 1)))
        verify_original_receipt("invoice-a", {"transactionHash": "0xAA"}, hashes)
        verify_original_receipt("invoice-b", {"transactionHash": "0xBB"}, hashes)
        for transaction_id, transaction_hash in [("invoice-a", "0xbb"), ("invoice-b", "0xaa"), ("missing", "0xaa")]:
            with self.subTest(transaction_id=transaction_id):
                with self.assertRaisesRegex(AssertionError, "intent-to-transaction binding"):
                    verify_original_receipt(transaction_id, {"transactionHash": transaction_hash}, hashes)

    def test_missing_original_attempt_is_not_upgrade_evidence(self):
        with self.assertRaisesRegex(AssertionError, "no durable signed attempt"):
            self.snapshot([("invoice-a", "0xaa", 0)], ["invoice-a", "invoice-b"], self.pending(("0xaa", 0)))

    def test_shared_hash_cannot_cover_two_intents(self):
        with self.assertRaisesRegex(AssertionError, "multiple intent IDs"):
            self.snapshot([("invoice-a", "0xaa", 0), ("invoice-b", "0xaa", 0)],
                          ["invoice-a", "invoice-b"], self.pending(("0xaa", 0)))

    def test_chain_and_original_journal_must_agree_before_upgrade(self):
        with self.assertRaisesRegex(AssertionError, "differ from Anvil"):
            self.snapshot([("invoice-a", "0xaa", 0)], ["invoice-a"], self.pending(("0xbb", 0)))

    def test_current_replacements_cover_each_intent_and_preserve_its_full_history(self):
        hashes = self.snapshot([("invoice-a", "0xaa", 0), ("invoice-a", "0xab", 0),
                                ("invoice-b", "0xba", 1), ("invoice-b", "0xbb", 1)],
                               ["invoice-a", "invoice-b"], self.pending(("0xab", 0), ("0xbb", 1)))
        self.assertEqual(hashes, {"invoice-a": {"0xaa", "0xab"}, "invoice-b": {"0xba", "0xbb"}})
        for transaction_hash in hashes["invoice-a"]:
            verify_original_receipt("invoice-a", {"transactionHash": transaction_hash}, hashes)
        with self.assertRaisesRegex(AssertionError, "intent-to-transaction binding"):
            verify_original_receipt("invoice-a", {"transactionHash": "0xbb"}, hashes)

    def test_other_intents_pending_hashes_cannot_cover_an_intent_without_a_witness(self):
        with self.assertRaisesRegex(AssertionError, "no pending signed transaction"):
            self.snapshot([("invoice-a", "0xaa", 0), ("invoice-a", "0xab", 0), ("invoice-b", "0xbb", 1)],
                          ["invoice-a", "invoice-b"], self.pending(("0xaa", 0), ("0xab", 0)))

    def test_pending_replacement_must_keep_the_original_nonce(self):
        with self.assertRaisesRegex(AssertionError, "nonce differs"):
            self.snapshot([("invoice-a", "0xaa", 0), ("invoice-a", "0xab", 0)],
                          ["invoice-a"], self.pending(("0xab", 1)))
        with self.assertRaisesRegex(AssertionError, "changed its reserved nonce"):
            self.snapshot([("invoice-a", "0xaa", 0), ("invoice-a", "0xab", 1)],
                          ["invoice-a"], self.pending(("0xab", 1)))


if __name__ == "__main__":
    unittest.main()
