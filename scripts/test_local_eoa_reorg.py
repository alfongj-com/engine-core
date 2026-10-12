"""Offline adversarial checks for the reorg recovery evidence oracle."""
import copy
import hashlib
import unittest

from capacity_faults import FaultError, evm_node_wire
from local_eoa_reorg import FROM, TO, verify_recovered_attempt


def fixture():
    # Synthetic signed-field structure: this tests evidence association, not
    # cryptography. The process scenario obtains this object from real Anvil.
    tx = {"hash": "0x" + "11" * 32, "type": "0x2", "chainId": "0x7a69",
          "nonce": "0x0", "from": FROM, "to": TO, "value": "0x1", "input": "0x",
          "gas": "0x5208", "maxPriorityFeePerGas": "0x1", "maxFeePerGas": "0xa",
          "accessList": [], "yParity": "0x0", "r": "0x1", "s": "0x2"}
    wire = evm_node_wire(tx)
    attempt = {"chainId": 31337, "sender": FROM, "nonce": 0,
               "transactionHash": tx["hash"], "signedTransaction": "0x" + wire.hex()}
    digest = hashlib.sha256(wire).hexdigest()
    before = {digest: {"identity": tx["hash"], "accepted_responses": 1}}
    after = {digest: {"identity": tx["hash"], "accepted_responses": 2}}
    block = {"hash": "0x" + "aa" * 32, "number": "0x2", "transactions": [tx["hash"]]}
    receipt = {"transactionHash": tx["hash"], "status": "0x1",
               "blockHash": block["hash"], "blockNumber": block["number"]}
    return {"attempts": [attempt], "original_attempt": copy.deepcopy(attempt),
            "original_transaction": copy.deepcopy(tx),
            "before": before, "after": after, "transaction": tx,
            "receipt": receipt, "block": block}


class RecoveryOracleTests(unittest.TestCase):
    def test_one_durable_attempt_proves_two_actual_sends(self):
        result = verify_recovered_attempt(**fixture())
        self.assertEqual(result["recovery_mode"], "exact_wire")
        self.assertEqual(result["post_restart_accepted_winning_sends"], 1)

    def test_attempt_metadata_without_post_fault_send_is_rejected(self):
        data = fixture()
        data["after"] = copy.deepcopy(data["before"])
        with self.assertRaisesRegex(AssertionError, "actual Engine resend"):
            verify_recovered_attempt(**data)

    def test_same_nonce_fee_replacement_requires_explicit_legacy_mode(self):
        data = fixture()
        tx = data["transaction"]
        tx.update(hash="0x" + "22" * 32, maxFeePerGas="0xc", r="0x3")
        wire = evm_node_wire(tx)
        replacement = {**data["attempts"][0], "transactionHash": tx["hash"],
                       "signedTransaction": "0x" + wire.hex()}
        data["attempts"].append(replacement)
        data["after"][hashlib.sha256(wire).hexdigest()] = {
            "identity": tx["hash"], "accepted_responses": 1}
        data["receipt"]["transactionHash"] = tx["hash"]
        data["block"]["transactions"] = [tx["hash"]]
        with self.assertRaisesRegex(AssertionError, "original-wire replay"):
            verify_recovered_attempt(**data)
        result = verify_recovered_attempt(**data, allow_replacement=True)
        self.assertEqual(result["recovery_mode"], "same_nonce_replacement")
        tx["gas"] = "0x6000"
        data["attempts"][1]["signedTransaction"] = "0x" + evm_node_wire(tx).hex()
        with self.assertRaisesRegex(AssertionError, "signed execution fields"):
            verify_recovered_attempt(**data, allow_replacement=True)

    def test_wrong_nonce_signer_and_winner_are_rejected(self):
        for field, value in [("nonce", "0x1"), ("from", TO), ("hash", "0x" + "bb" * 32)]:
            with self.subTest(field=field):
                data = fixture()
                data["transaction"][field] = value
                with self.assertRaises((AssertionError, FaultError)):
                    verify_recovered_attempt(**data)

    def test_receipt_must_match_canonical_block_and_outcome(self):
        for field, value in [("blockHash", "0x" + "bb" * 32), ("blockNumber", "0x3"),
                             ("transactionHash", "0x" + "cc" * 32), ("status", "0x0")]:
            with self.subTest(field=field):
                data = fixture()
                data["receipt"][field] = value
                with self.assertRaises(AssertionError):
                    verify_recovered_attempt(**data)

    def test_reverted_execution_requires_a_real_reverted_receipt(self):
        data = fixture()
        with self.assertRaises(AssertionError):
            verify_recovered_attempt(**data, reverted=True)
        data["receipt"]["status"] = "0x0"
        self.assertEqual(verify_recovered_attempt(**data, reverted=True)["recovery_mode"], "exact_wire")

    def test_original_wire_cannot_change_or_gain_a_duplicate_attempt(self):
        data = fixture()
        data["attempts"][0]["signedTransaction"] += "00"
        with self.assertRaisesRegex(AssertionError, "Original durable wire changed"):
            verify_recovered_attempt(**data)
        data = fixture()
        data["attempts"].append(copy.deepcopy(data["attempts"][0]))
        with self.assertRaisesRegex(AssertionError, "Duplicate"):
            verify_recovered_attempt(**data)


if __name__ == "__main__":
    unittest.main()
