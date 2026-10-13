"""Offline checks for frozen binaries and retained journal startup authority."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from testnet_engine import pinned_binary, prepare_recovery, provenance


class EngineSetupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.binary = self.directory / "engine"
        self.binary.write_text("#!/bin/sh\nexit 0\n")
        os.chmod(self.binary, 0o700)
        self.evidence = provenance(self.binary, "a" * 40, __file__)
        self.env = {"APP__RECOVERY__JOURNAL_PATH": str(self.directory / "recovery.sqlite")}

    def tearDown(self):
        self.temp.cleanup()

    def test_changed_binary_is_rejected_before_process_or_recovery_command(self):
        self.binary.write_text("#!/bin/sh\nexit 1\n")
        with self.assertRaisesRegex(RuntimeError, "binary changed"):
            pinned_binary(self.evidence)
        with patch("testnet_engine.subprocess.run") as spawn:
            with self.assertRaisesRegex(RuntimeError, "binary changed"):
                prepare_recovery(self.evidence, self.directory, self.env)
            spawn.assert_not_called()

    def test_new_and_resumed_authority_cannot_be_interchanged(self):
        with patch("testnet_engine.subprocess.run") as spawn:
            spawn.return_value.returncode = 0
            with self.assertRaisesRegex(RuntimeError, "retained journal"):
                prepare_recovery(self.evidence, self.directory, self.env, resume=True)
            spawn.assert_not_called()
            prepare_recovery(self.evidence, self.directory, self.env)
            self.assertEqual(spawn.call_args.args[0], [str(self.binary.resolve()), "--initialize-recovery"])
            (self.directory / "recovery.sqlite").touch()
            with self.assertRaisesRegex(RuntimeError, "empty journal"):
                prepare_recovery(self.evidence, self.directory, self.env)
            prepare_recovery(self.evidence, self.directory, self.env, resume=True)
            self.assertEqual(spawn.call_args.args[0], [str(self.binary.resolve()), "--reattach-recovery"])
            self.assertEqual(spawn.call_count, 2)

    def test_refused_reattach_never_falls_back_to_initialize(self):
        (self.directory / "recovery.sqlite").touch()
        with patch("testnet_engine.subprocess.run") as spawn:
            spawn.return_value.returncode = 1
            spawn.return_value.stdout = b"private recovery detail"
            spawn.return_value.stderr = b"private error"
            with self.assertRaisesRegex(RuntimeError, "setup refused"):
                prepare_recovery(self.evidence, self.directory, self.env, resume=True)
            self.assertEqual(spawn.call_count, 1)
            self.assertEqual(spawn.call_args.args[0][-1], "--reattach-recovery")
        self.assertEqual((self.directory / "recovery-setup.log").stat().st_mode & 0o777, 0o600)


if __name__ == "__main__":
    unittest.main()
