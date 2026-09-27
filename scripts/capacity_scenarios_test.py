#!/usr/bin/env python3
"""Offline scenarios: inventory conservation and deliberate fault prerequisites."""
import copy
import tempfile
import unittest

import capacity_campaign as campaign


class RecoveryInventoryTests(unittest.TestCase):
    def snapshots(self):
        admissions = {
            "complete": {"kind": "eoa", "fingerprint": "f1", "payload_sha256": "p1", "state": "terminal", "replay_key": "evm:1:signer:0"},
            "unknown": {"kind": "eoa", "fingerprint": "f2", "payload_sha256": "p2", "state": "admitted", "replay_key": "evm:1:signer:1"},
            "unsigned": {"kind": "erc4337", "fingerprint": "f3", "payload_sha256": "random-uid-preserved", "state": "admitted", "replay_key": None},
        }
        before = {"control": {"deployment": "d", "epoch": 1, "namespace": "old", "checkpoint": 80, "halted": "mismatch"},
                  "admissions": admissions,
                  "attempts": {1: {"id": "complete", "digest": "a1"}, 2: {"id": "unknown", "digest": "a2"}},
                  "table_sha256": {"terminal_evidence": "proof", "chain_checkpoints": "checkpoint", "chain_halts": "no-halts"}}
        after = copy.deepcopy(before)
        after["control"].update(epoch=2, namespace="new", checkpoint=0, halted=None)
        after["admissions"]["unknown"]["state"] = "quarantined"
        return before, after

    def test_exact_restore_preserves_original_random_uid_and_unknown_binding(self):
        before, after = self.snapshots()
        self.assertEqual(campaign.validate_recovery_inventory(before, after, "new"),
                         {"terminal": 1, "quarantined": 1, "unsent": 1})
        self.assertEqual(before["admissions"]["unsigned"], after["admissions"]["unsigned"])

    def test_unknown_attempt_may_not_be_reactivated_or_reported_terminal(self):
        for state in ("admitted", "terminal"):
            with self.subTest(state=state):
                before, after = self.snapshots()
                after["admissions"]["unknown"]["state"] = state
                with self.assertRaises(RuntimeError): campaign.validate_recovery_inventory(before, after, "new")

    def test_payload_replay_and_evidence_tampering_are_independently_rejected(self):
        mutations = (
            lambda a: a["admissions"]["unknown"].update(replay_key="evm:1:signer:2"),
            lambda a: a["admissions"]["unsigned"].update(payload_sha256="regenerated-uid"),
            lambda a: a["attempts"][2].update(digest="different-wire"),
            lambda a: a["table_sha256"].update(terminal_evidence="replaced-proof"),
            lambda a: a["admissions"].pop("unknown"),
        )
        for mutation in mutations:
            before, after = self.snapshots()
            mutation(after)
            with self.assertRaises(RuntimeError): campaign.validate_recovery_inventory(before, after, "new")

    def test_recovery_must_advance_epoch_without_changing_deployment(self):
        for changes in ({"epoch": 1}, {"deployment": "copy"}, {"namespace": "old"}, {"checkpoint": 80}, {"halted": "mismatch"}):
            with self.subTest(changes=changes):
                before, after = self.snapshots()
                after["control"].update(changes)
                with self.assertRaises(RuntimeError): campaign.validate_recovery_inventory(before, after, "new")

    def test_bounded_guard_probes_cover_ends_without_duplicate_ids(self):
        probes = campaign.evenly_spaced(list(range(100_000)), 20)
        self.assertEqual(len(probes), len(set(probes)))
        self.assertEqual(len(probes), 20)
        self.assertEqual((probes[0], probes[-1]), (0, 99_999))


class ScenarioConfigurationTests(unittest.TestCase):
    def parse(self, *flags):
        # These tests validate configuration and never execute Engine. Supply an
        # existing placeholder so a clean checkout does not need a release build.
        with tempfile.NamedTemporaryFile() as engine:
            return campaign.arguments(["--engine-bin", engine.name, "--chain", "evm2=5",
                                       "--report", "/tmp/not-created.json", *flags])

    def test_reorg_rejects_uncontrolled_or_unrepresentable_finality(self):
        for flags in (("--depth", "evm2=1"), ("--block-seconds", "evm2=.25")):
            with self.assertRaises(ValueError): self.parse("--chaos", "reorg", *flags)
        args, profiles = self.parse("--chaos", "reorg", "--depth", "evm2=2")
        self.assertEqual(profiles["evm2"]["depth"], 2)

    def test_unstable_solana_balance_cut_is_not_qualified_as_redis_recovery(self):
        with self.assertRaises(ValueError):
            self.parse("--chain", "solana=5", "--solana-bin-dir", "/tmp/bin", "--chaos", "redis-recover")

    def test_poll_experiment_is_bounded_and_explicit(self):
        args, _ = self.parse("--solana-confirmation-poll-seconds", "5")
        self.assertEqual(args.solana_confirmation_poll_seconds, 5)
        for interval in ("0", "6"):
            with self.assertRaises(ValueError): self.parse("--solana-confirmation-poll-seconds", interval)


if __name__ == "__main__":
    unittest.main()
