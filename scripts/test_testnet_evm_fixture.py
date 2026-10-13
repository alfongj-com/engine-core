"""Offline fixed-fixture spending, identity, finality and failure-retention checks."""
import concurrent.futures
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import testnet_evm_fixture as fixture


def wire(fields):
    encoded = [item.to_bytes((item.bit_length() + 7) // 8, "big") if isinstance(item, int) else item for item in fields]
    return "0x02" + fixture.encode_rlp(encoded).hex()


class FixtureGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.manifest = {"chain_id": 11155111, "initial_nonce": 7, "max_fee_per_gas": 100,
                         "max_priority_fee_per_gas": 10, "gas_limit": 70_000}
        self.guard = fixture.FixtureGuard(self.manifest, Path(self.temp.name), 0)
        self.fields = [11155111, 7, 10, 100, 60_000, b"", 0, bytes.fromhex(fixture.INITCODE[2:]), [], 0, 1, 2]

    def tearDown(self):
        self.guard.close()
        self.temp.cleanup()

    def test_creation_is_fixed_zero_value_one_nonce_with_bounded_all_fields(self):
        wrong = [(0, 1), (1, 6), (1, 8), (2, 11), (2, 101), (3, 101), (4, 52_999), (4, 70_001),
                 (5, b"\x11" * 20), (6, 1), (7, b""), (7, bytes.fromhex(fixture.RUNTIME[2:])),
                 (8, [b"access"]), (9, 2), (10, 0), (10, fixture.SECP256K1_N),
                 (11, 0), (11, fixture.SECP256K1_N // 2 + 1)]
        for index, value in wrong:
            with self.subTest(index=index, value=value):
                fields = self.fields.copy()
                fields[index] = value
                with self.assertRaises(AssertionError):
                    self.guard.validate(wire(fields))
                self.assertEqual(self.guard.wires, {})
        self.guard.validate(wire(self.fields))
        self.assertEqual(self.guard.wires["7"]["attempts"], 1)

    def test_noncanonical_integer_and_rlp_or_wrong_type_are_refused(self):
        zero = self.fields.copy()
        zero[6] = b"\x00"
        encoded = wire(self.fields)
        raw = bytes.fromhex(encoded[4:])
        # The canonical list fits a short prefix; long form must not be accepted.
        self.assertTrue(192 <= raw[0] <= 247)
        long_form = "0x02f8" + bytes([raw[0] - 192]).hex() + raw[1:].hex()
        for bad in [wire(zero), long_form, encoded + "00", "0x01" + encoded[4:], "0x02"]:
            with self.subTest(wire=bad), self.assertRaises((AssertionError, ValueError)):
                self.guard.validate(bad)
            self.assertEqual(self.guard.wires, {})

    def test_manifest_cannot_relax_hardcoded_fee_gas_or_chain_limits(self):
        for name, value in [("chain_id", 1), ("gas_limit", fixture.MAX_GAS + 1), ("max_fee_per_gas", fixture.MAX_FEE + 1)]:
            with self.subTest(name=name):
                self.guard.m = {**self.manifest, name: value}
                fields = self.fields.copy()
                if name == "chain_id":
                    fields[0] = value
                with self.assertRaises(AssertionError):
                    self.guard.validate(wire(fields))
                self.assertEqual(self.guard.wires, {})

    def test_observed_repriced_creation_has_bounded_buffer(self):
        self.assertEqual(fixture.fixture_gas_limit(218_384), 272_980)
        self.assertEqual(fixture.fixture_gas_limit(240_000), fixture.MAX_GAS)
        for estimate in (21_000, 240_001, 2**64):
            with self.subTest(estimate=estimate), self.assertRaises(AssertionError):
                fixture.fixture_gas_limit(estimate)

    def test_ambiguous_sends_consume_cap_and_cannot_change_signed_wire(self):
        signed = wire(self.fields)
        def reserve(_):
            try:
                self.guard.validate(signed)
                return True
            except AssertionError:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            self.assertEqual(sum(pool.map(reserve, range(45))), 20)
        retained = json.loads((Path(self.temp.name) / "signed-wires.json").read_text())
        self.assertEqual(retained["7"]["attempts"], 20)
        self.assertEqual(retained["7"]["responses"], [])
        changed = self.fields.copy()
        changed[-1] = 3
        with self.assertRaisesRegex(AssertionError, "Replacement wire"):
            self.guard.validate(wire(changed))
        self.assertEqual(self.guard.wires["7"]["attempts"], 20)

    def test_unchecked_send_methods_and_extra_wire_parameters_are_refused(self):
        for method, params in [("eth_sendTransaction", [{}]), ("eth_signTransaction", [{}]),
                               ("personal_sendTransaction", [{}]), ("eth_sendRawTransaction", ["wire", "extra"]),
                               ("eth_sendRawTransaction", []), ("eth_chainId", {})]:
            with self.subTest(method=method), self.assertRaises(AssertionError):
                self.guard.validate_request({"method": method, "params": params})
        self.guard.validate_request({"method": "eth_getTransactionReceipt", "params": ["hash"]})


class SignerAndRetentionTests(unittest.TestCase):
    def test_lost_responses_retry_only_the_durable_wire_and_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            retained = {"wire": "0x02dead", "transaction_hash": "0x" + "ab" * 32, "nonce": 7}
            fixture.evm.durable(path / "signed-fixture.json", retained)
            with patch.object(fixture.evm, "request", side_effect=[OSError("lost reply"), (200, {"result": retained["transaction_hash"]})]) as send, patch.object(fixture.time, "sleep"):
                fixture.send_original("local", path)
            self.assertEqual([call.args[1]["params"] for call in send.call_args_list], [[retained["wire"]]] * 2)
            self.assertEqual(json.loads((path / "signed-fixture.json").read_text()), retained)
            with patch.object(fixture.evm, "request", side_effect=OSError) as send, patch.object(fixture.time, "sleep"):
                fixture.send_original("local", path)
                self.assertEqual(send.call_count, 3)

    def test_missing_durable_wire_or_changed_provider_identity_never_creates_a_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            with patch.object(fixture.evm, "request") as send, self.assertRaises(FileNotFoundError):
                fixture.send_original("local", path)
            send.assert_not_called()
            fixture.evm.durable(path / "signed-fixture.json", {"wire": "original", "transaction_hash": "expected"})
            with patch.object(fixture.evm, "request", return_value=(200, {"result": "another"})) as send:
                with self.assertRaisesRegex(AssertionError, "another transaction"):
                    fixture.send_original("local", path)
            self.assertEqual(send.call_count, 1)

    def test_private_tty_has_no_echo_or_key_in_argv_environment_or_output(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            child = path / "fake-cast"
            child.write_text("#!" + fixture.sys.executable + "\n" +
                             "import os,sys,termios\n" +
                             "with open('/dev/tty') as tty, open('/dev/tty','w') as prompt:\n" +
                             " assert not termios.tcgetattr(tty)[3] & termios.ECHO\n" +
                             " prompt.write('Enter private key: '); prompt.flush()\n" +
                             " key=tty.readline().strip()\n" +
                             " assert key not in str(sys.argv) and key not in str(dict(os.environ))\n" +
                             "print('safe-output')\n")
            child.chmod(0o700)
            self.assertEqual(fixture.cast_private(child, [], 'a' * 64, path), 'safe-output')
            child.write_text(child.read_text().replace("print('safe-output')", "print(key)"))
            with self.assertRaisesRegex(AssertionError, "Signer output refused"):
                fixture.cast_private(child, [], 'a' * 64, path)

    def test_failure_retains_shared_chain_lock_and_private_report_without_stdout_details(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state"
            report = Path(directory) / "report.json"
            arguments = ["fixture", "--chain", "11155111", "--cast-bin", fixture.sys.executable, "--report", str(report)]
            output = io.StringIO()
            with patch("sys.argv", arguments), patch.object(fixture.evm, "STATE", state), \
                    patch.object(fixture.evm, "request", side_effect=RuntimeError("private endpoint and credential")), \
                    patch("sys.stdout", output):
                self.assertEqual(fixture.main(), 1)
            self.assertTrue((state / "public-evm-11155111.lock").exists())
            self.assertEqual(json.loads(report.read_text())["outcome"], "fail")
            self.assertNotIn(directory, output.getvalue())
            self.assertNotIn("private endpoint", output.getvalue())
            self.assertEqual(report.stat().st_mode & 0o777, 0o600)


class DeploymentVerificationTests(unittest.TestCase):
    def setUp(self):
        self.sender, self.contract = "0x" + "11" * 20, "0x" + "22" * 20
        self.receipt = {"transactionHash": "0x" + "33" * 32, "status": "0x1", "blockHash": "block", "blockNumber": "0x5",
                        "from": self.sender, "to": None, "contractAddress": self.contract}
        self.tx = {"hash": self.receipt["transactionHash"], "blockHash": "block", "from": self.sender, "to": None,
                   "chainId": hex(11155111), "nonce": "0x7", "type": "0x2", "value": "0x0", "input": fixture.INITCODE,
                   "gas": hex(60_000), "maxFeePerGas": hex(100), "maxPriorityFeePerGas": hex(10), "accessList": []}
        self.manifest = {"sender": self.sender, "chain_id": 11155111, "initial_nonce": 7,
                         "gas_limit": 70_000, "max_fee_per_gas": 100, "max_priority_fee_per_gas": 10}

    def test_only_finalized_canonical_creation_with_exact_runtime_passes(self):
        with patch.object(fixture.evm, "rpc", side_effect=[self.receipt, self.tx, fixture.RUNTIME]) as read, \
                patch.object(fixture.evm, "verify_finalized_receipt") as finalized:
            address, _, _ = fixture.verify_deployment("local", self.receipt, self.manifest, expected_hash=self.receipt["transactionHash"])
        self.assertEqual(address, self.contract)
        finalized.assert_called_once_with("local", "0x5", "block")
        self.assertEqual(read.call_args.args[1:], ("eth_getCode", [self.contract, "0x5"]))

    def test_another_receipt_cannot_replace_the_locally_hashed_signed_identity(self):
        with patch.object(fixture.evm, "rpc") as read:
            with self.assertRaisesRegex(AssertionError, "retained signed fixture"):
                fixture.verify_deployment("local", self.receipt, self.manifest, expected_hash="0x" + "44" * 32)
            read.assert_not_called()

    def test_wrong_execution_identity_runtime_or_receipt_is_rejected(self):
        for name, value in [("to", self.contract), ("from", self.contract), ("chainId", "0x1"), ("nonce", "0x8"),
                            ("type", "0x0"), ("value", "0x1"), ("input", "0x"), ("gas", hex(70_001)),
                            ("maxFeePerGas", hex(101)), ("maxPriorityFeePerGas", hex(11)), ("accessList", [{}])]:
            with self.subTest(field=name), patch.object(fixture.evm, "rpc", side_effect=[self.receipt, {**self.tx, name: value}, fixture.RUNTIME]), \
                    patch.object(fixture.evm, "verify_finalized_receipt"):
                with self.assertRaises(AssertionError):
                    fixture.verify_deployment("local", self.receipt, self.manifest, expected_hash=self.receipt["transactionHash"])
        for runtime in ("0x", "0x60006000f3"):
            with self.subTest(runtime=runtime), patch.object(fixture.evm, "rpc", side_effect=[self.receipt, self.tx, runtime]), \
                    patch.object(fixture.evm, "verify_finalized_receipt"):
                with self.assertRaises(AssertionError):
                    fixture.verify_deployment("local", self.receipt, self.manifest, expected_hash=self.receipt["transactionHash"])
        for field, value in [("status", "0x0"), ("blockHash", "orphan"), ("contractAddress", "0x" + "00" * 20)]:
            with self.subTest(field=field), patch.object(fixture.evm, "rpc", side_effect=[{**self.receipt, field: value}, self.tx]):
                with self.assertRaises(AssertionError):
                    fixture.verify_deployment("local", self.receipt, self.manifest, expected_hash=self.receipt["transactionHash"])

    def test_finality_poll_never_substitutes_latest_or_confirms_insufficient_checkpoint(self):
        for number, expected in [("0x4", False), ("0x5", True), ("0x6", True)]:
            with patch.object(fixture.evm, "rpc", return_value={"number": number}) as read:
                self.assertEqual(fixture.finalized_covers("local", self.receipt), expected)
                self.assertEqual(read.call_args.args[2], ["finalized", False])
        with patch.object(fixture.evm, "rpc", return_value=None), self.assertRaises(AssertionError):
            fixture.finalized_covers("local", self.receipt)



if __name__ == "__main__":
    unittest.main()
