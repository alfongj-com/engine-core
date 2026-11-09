import copy
from collections import Counter
from contextlib import redirect_stderr
import io
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from capacity_sweep import NoQualifiedRate, PROFILES, Sweep, arguments, closed_trial, eligible, selected_rates


class QualificationTests(unittest.TestCase):
    def test_predispatch_miss_can_only_continue_after_all_admitted_work_closes(self):
        report = {"outcome": "incomplete", "owned_children_stopped": True,
                  "oracle": {"safety_pass": True, "liveness_pass": False,
                             "observed_intents": 9, "offered_intents": 10,
                             "liveness_failures": ["missing: offered intent not admitted"]},
                  "requested_fault_validated": True,
                  "timing_observations": [{"phase": "load", "http_status": 200, "values": {}}] * 2,
                  "predispatch_drops": [{"id": "missing", "chain": "one", "reason": "schedule_lag"}],
                  "drain": dict.fromkeys(("client_pending", "http_inflight", "journal_unresolved",
                      "node_pending", "redis_active", "redis_borrowed", "redis_delayed", "redis_pending", "redis_submitted"), 0),
                  "per_chain": {"one": {"offered": 10, "client": {"dispatched": 9, "dropped_schedule_lag": 1},
                                        "responses": {"202": 9}}}}
        self.assertTrue(closed_trial(report))
        self.assertFalse(eligible(report), "a closed failed screen is still disqualified")
        mutations = [lambda r: r["drain"].update(journal_unresolved=1),
                     lambda r: r["drain"].pop("journal_unresolved"),
                     lambda r: r["oracle"].update(safety_pass=None),
                     lambda r: r["oracle"].update(observed_intents=8),
                     lambda r: r["oracle"].update(offered_intents=11),
                     lambda r: r.update(owned_children_stopped=False),
                     lambda r: r.update(infrastructure_stop={"reason": "disk"}),
                     lambda r: r.update(journal_halted=True),
                     lambda r: r.update(errors=["unobserved"]),
                     lambda r: r["per_chain"]["one"].update(responses={"202": 8, "503": 1}),
                     lambda r: r["per_chain"]["one"]["client"].update(dropped_campaign_aborted=1),
                     lambda r: r.update(events=[{"event": "observer_error"}]),
                     lambda r: r.update(timing_observations=[]),
                     lambda r: r["timing_observations"][0].update(unavailable="TimeoutError"),
                     lambda r: r["timing_observations"][0].update(http_status=503),
                     lambda r: r["timing_observations"][0].pop("values"),
                     lambda r: r.update(requested_fault_validated=False),
                     lambda r: r["oracle"]["liveness_failures"].append("requested chaos did not fully trigger and recover"),
                     lambda r: r["oracle"].update(liveness_failures=["other: offered intent not admitted"]),
                     lambda r: r["oracle"].update(liveness_failures=["missing: not completely settled"]),
                     lambda r: r.update(predispatch_drops=[]),
                     lambda r: r["predispatch_drops"][0].update(reason="client_capacity"),
                     lambda r: r["predispatch_drops"][0].update(chain="other")]
        for mutation in mutations:
            value = copy.deepcopy(report)
            mutation(value)
            self.assertFalse(closed_trial(value))

    def test_drained_safety_is_separate_from_sustainable_capacity(self):
        report = {"outcome": "pass", "owned_children_stopped": True,
                  "oracle": {"safety_pass": True, "liveness_pass": True},
                  "all_chain_capacity_candidate": True,
                  "per_chain": {"solana": {"late_window": {"capacity_candidate": True}}}}
        self.assertTrue(eligible(report))
        mutations = [lambda r: r.update(outcome="unsafe"),
                     lambda r: r.update(owned_children_stopped=False),
                     lambda r: r["oracle"].update(liveness_pass=False),
                     lambda r: r["oracle"].update(safety_pass=False),
                     lambda r: r.update(all_chain_capacity_candidate=False),
                     lambda r: r["per_chain"]["solana"]["late_window"].update(capacity_candidate=False),
                     lambda r: r.update(infrastructure_stop={"reason": "disk"}),
                     lambda r: r.update(errors=["lost observation"]),
                     lambda r: r.update(per_chain={})]
        for mutation in mutations:
            value = copy.deepcopy(report)
            mutation(value)
            self.assertFalse(eligible(value))
        self.assertFalse(eligible({}))

    def test_shared_rates_require_complete_distinct_confirmations_and_same_binary(self):
        summaries = [{"profile": profile, "qualified_tps": 50,
                      "engine_binary_sha256": "fixed", "confirmation_reports": ["a", "b", "c"]}
                     for profile in PROFILES]
        self.assertEqual(selected_rates(summaries, "fixed"), dict.fromkeys(PROFILES, 50))
        for field, value in [("profile", "evm12"), ("qualified_tps", None), ("qualified_tps", True),
                             ("qualified_tps", 0), ("engine_binary_sha256", "other"),
                             ("confirmation_reports", ["a", "a", "a"]),
                             ("confirmation_reports", ["a", "b"])]:
            changed = copy.deepcopy(summaries)
            changed[-1][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                selected_rates(changed, "fixed")
        with self.assertRaises(ValueError):
            selected_rates(summaries[:-1], "fixed")

    def test_drop_witness_records_the_actual_offer_and_reason_under_client_accounting(self):
        from capacity_campaign import Campaign
        campaign = Campaign.__new__(Campaign)
        campaign.lock = threading.Lock()
        campaign.namespace, campaign.report = "run", {}
        campaign.stats = {"solana": Counter()}
        campaign.dropped(SimpleNamespace(chain="solana", index=17), "client_capacity")
        self.assertEqual(campaign.stats["solana"], {"dropped_client_capacity": 1})
        self.assertEqual(campaign.report["predispatch_drops"], [
            {"id": "run-solana-17", "chain": "solana", "reason": "client_capacity"}])


class PlannedSweep(Sweep):
    """Exercise the real bounded search with explicit trial outcomes; no nodes."""
    def __init__(self, directory, candidates, seconds=300, late_window_seconds=180):
        self.output = Path(directory)
        self.args = SimpleNamespace(profile="solana", individuals=self.output / "individuals", max_screen_rate=800,
                                    seconds=seconds, late_window_seconds=late_window_seconds,
                                    engine_bin=Path("/unused/engine"), solana_bin_dir=Path("/unused/solana"))
        self.binary_sha = "fixed"
        self.summary = {"trials": [], "seconds_per_trial": seconds,
                        "late_window_seconds": late_window_seconds, "warmup_seconds": 60}
        self.candidates = iter(candidates)
        self.calls = []

    def trial(self, rates, chaos="none"):
        row = {"report": str(len(self.calls)) + ".json", "capacity_candidate": next(self.candidates)}
        self.calls.append((rates, chaos))
        self.summary["trials"].append(row)
        return row

    def seed_individuals(self, rate):
        for profile in PROFILES:
            directory = self.args.individuals / profile
            directory.mkdir(parents=True)
            reports = [f"{index}.json" for index in range(3)]
            (directory / "summary.json").write_text(json.dumps({
                "profile": profile, "qualified_tps": rate, "engine_binary_sha256": "fixed",
                "confirmation_reports": reports, "seconds_per_trial": self.args.seconds,
                "late_window_seconds": self.args.late_window_seconds, "warmup_seconds": 60}))
            for name in reports:
                (directory / name).write_text(json.dumps({
                    "engine_binary_sha256": "fixed", "duration_seconds": self.args.seconds, "outcome": "pass",
                    "minimum_late_window_seconds": self.args.late_window_seconds, "warmup_seconds": 60,
                    "owned_children_stopped": True, "all_chain_capacity_candidate": True,
                    "oracle": {"safety_pass": True, "liveness_pass": True},
                    "per_chain": {profile: {"offered_tps": rate, "late_window": {"capacity_candidate": True}}}}))


class SearchTests(unittest.TestCase):
    def test_duration_arguments_preserve_defaults_and_refuse_short_or_unbounded_windows(self):
        common = ["individual", "--profile", "solana", "--engine-bin", "/unused/engine",
                  "--solana-bin-dir", "/unused/solana", "--output", "/unused/output"]
        defaults = arguments(common)
        self.assertEqual((defaults.seconds, defaults.late_window_seconds), (300, 180))
        for seconds, window in ((300, 240), (600, 480), (900, 840)):
            args = arguments(common + ["--seconds", str(seconds), "--late-window-seconds", str(window)])
            self.assertEqual((args.seconds, args.late_window_seconds), (seconds, window))
        for seconds, window in ((0, 180), (299, 180), (901, 180), (600, 179), (300, 241), (900, 841)):
            with self.subTest(seconds=seconds, window=window), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                arguments(common + ["--seconds", str(seconds), "--late-window-seconds", str(window)])
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(common)
            args.output = Path(directory) / "must-not-be-created"
            for seconds, window in ((True, 180), (600, True), (600, 541)):
                args.seconds, args.late_window_seconds = seconds, window
                with self.subTest(seconds=seconds, window=window), self.assertRaises(ValueError):
                    Sweep(args)
                self.assertFalse(args.output.exists())

    def test_trial_passes_and_verifies_duration_and_window_without_changing_thresholds(self):
        for mismatch in (None, "duration_seconds", "minimum_late_window_seconds", "warmup_seconds"):
            with self.subTest(mismatch=mismatch), tempfile.TemporaryDirectory() as directory:
                sweep = PlannedSweep(directory, [], seconds=600, late_window_seconds=480)
                report = {"engine_binary_sha256": "fixed", "duration_seconds": 600,
                          "minimum_late_window_seconds": 480, "warmup_seconds": 60,
                          "outcome": "pass", "owned_children_stopped": True,
                          "oracle": {"safety_pass": True, "liveness_pass": True},
                          "drain": dict.fromkeys(("client_pending", "http_inflight", "journal_unresolved",
                              "node_pending", "redis_active", "redis_borrowed", "redis_delayed", "redis_pending", "redis_submitted"), 0),
                          "all_chain_capacity_candidate": True,
                          "per_chain": {"solana": {"late_window": {"capacity_candidate": True}}}}
                if mismatch:
                    report[mismatch] += 1
                (Path(directory) / "00-none.json").write_text(json.dumps(report))
                child = Mock()
                child.wait.return_value = 0
                with patch("capacity_sweep.subprocess.Popen", return_value=child) as start:
                    if mismatch:
                        with self.assertRaisesRegex(RuntimeError, "different duration"):
                            Sweep.trial(sweep, {"solana": 100})
                    else:
                        self.assertTrue(Sweep.trial(sweep, {"solana": 100})["capacity_candidate"])
                    command = start.call_args.args[0]
                    self.assertEqual(command[command.index("--seconds") + 1], "600")
                    self.assertEqual(command[command.index("--late-window-seconds") + 1], "480")
                    self.assertEqual(command[command.index("--warmup-seconds") + 1], "60")
                    self.assertNotIn("--max-admission-p99-ms", command)
                    child.wait.assert_called_once_with(timeout=1500)

    def test_long_individual_trial_records_inventory_limit_and_confirms_lower_rate(self):
        with tempfile.TemporaryDirectory() as directory:
            sweep = PlannedSweep(directory, [True] * 7, seconds=900, late_window_seconds=840)
            sweep.individual()
            self.assertEqual([rates["solana"] for rates, _ in sweep.calls], [25, 50, 100, 200, 400, 400, 400])
            self.assertEqual(sweep.summary["qualified_tps"], 400)
            self.assertFalse(sweep.summary["search_ceiling_reached"])
            self.assertEqual(sweep.summary["inventory_limited_candidates"][0]["offered_intents"], 720000)

    def test_shared_inventory_uses_configured_duration_for_every_factor(self):
        with tempfile.TemporaryDirectory() as directory:
            sweep = PlannedSweep(directory, [True] * 8, seconds=600, late_window_seconds=480)
            sweep.seed_individuals(800)
            sweep.shared()
            self.assertEqual(sweep.summary["qualified_shared_rates"], dict.fromkeys(PROFILES, 200))
            limited = sweep.summary["inventory_limited_candidates"]
            self.assertEqual([(row["factor"], row["offered_intents"]) for row in limited], [(1, 1920000), (.5, 960000)])
            self.assertTrue(all(sum(rates.values()) * 600 <= 500000 for rates, _ in sweep.calls))

    def test_shared_refuses_mixed_summary_or_confirmation_observation_settings(self):
        fields = {"summary": ("seconds_per_trial", "late_window_seconds", "warmup_seconds"),
                  "confirmation": ("duration_seconds", "minimum_late_window_seconds", "warmup_seconds")}
        for kind, names in fields.items():
            for field in names:
                with self.subTest(kind=kind, field=field), tempfile.TemporaryDirectory() as directory:
                    sweep = PlannedSweep(directory, [], seconds=600, late_window_seconds=480)
                    sweep.seed_individuals(100)
                    path = sweep.args.individuals / PROFILES[0] / ("summary.json" if kind == "summary" else "0.json")
                    value = json.loads(path.read_text())
                    value[field] += 1
                    path.write_text(json.dumps(value))
                    with self.assertRaises(ValueError):
                        sweep.shared()
                    self.assertEqual(sweep.calls, [])

    def test_restricted_screen_confirms_ceiling_without_offering_above_it(self):
        with tempfile.TemporaryDirectory() as directory:
            sweep = PlannedSweep(directory, [True] * 5)
            sweep.args.max_screen_rate = 100
            sweep.individual()
            self.assertEqual([rates["solana"] for rates, _ in sweep.calls], [25, 50, 100, 100, 100])
            saved = json.loads((Path(directory) / "summary.json").read_text())
            self.assertEqual(saved["qualified_tps"], 100)
            self.assertEqual(saved["configured_search_ceiling_tps"], 100)
            self.assertTrue(saved["search_ceiling_reached"])
            self.assertEqual(len(saved["confirmation_reports"]), 3)

    def test_default_search_still_screens_every_existing_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            sweep = PlannedSweep(directory, [True] * 8)
            sweep.individual()
            self.assertEqual([rates["solana"] for rates, _ in sweep.calls], [25, 50, 100, 200, 400, 800, 800, 800])
            self.assertEqual(sweep.summary["configured_search_ceiling_tps"], 800)
            self.assertTrue(sweep.summary["search_ceiling_reached"])

    def test_screening_argument_rejects_invalid_candidates_before_any_fixture(self):
        common = ["individual", "--profile", "solana", "--engine-bin", "/unused/engine",
                  "--solana-bin-dir", "/unused/solana", "--output", "/unused/output"]
        self.assertEqual(arguments(common).max_screen_rate, 800)
        self.assertEqual(arguments(common + ["--max-screen-rate", "100"]).max_screen_rate, 100)
        for value in ("0", "-1", "12", "101", "1600"):
            with self.subTest(value=value), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                arguments(common + ["--max-screen-rate", value])
            self.assertEqual(raised.exception.code, 2)
        with tempfile.TemporaryDirectory() as directory:
            args = arguments(common)
            args.output = Path(directory) / "must-not-be-created"
            for value in (0, -1, 12, 101, True):
                args.max_screen_rate = value
                with self.subTest(value=value), self.assertRaises(ValueError):
                    Sweep(args)
                self.assertFalse(args.output.exists())

    def test_failed_twelve_confirmation_still_tests_and_confirms_six(self):
        with tempfile.TemporaryDirectory() as directory:
            sweep = PlannedSweep(directory, [False, True, False, True, True, True])
            sweep.individual()
            self.assertEqual([rates["solana"] for rates, _ in sweep.calls], [25, 12, 12, 6, 6, 6])
            self.assertEqual(sweep.summary["qualified_tps"], 6)
            self.assertEqual(sweep.summary["confirmation_reports"], ["3.json", "4.json", "5.json"])

    def test_failed_high_rate_confirmations_can_fall_back_below_screening_range(self):
        with tempfile.TemporaryDirectory() as directory:
            sweep = PlannedSweep(directory, [True, False, False, True, True, True])
            sweep.individual()
            self.assertEqual([rates["solana"] for rates, _ in sweep.calls], [25, 50, 25, 12, 12, 12])
            self.assertEqual(sweep.summary["qualified_tps"], 12)

    def test_no_individual_qualification_is_failure_with_summary_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            sweep = PlannedSweep(directory, [False, False, False])
            with self.assertRaises(NoQualifiedRate):
                sweep.individual()
            saved = json.loads((Path(directory) / "summary.json").read_text())
            self.assertIsNone(saved["qualified_tps"])
            self.assertEqual(len(saved["trials"]), 3)

    def test_inventory_limited_shared_maximum_continues_to_measurable_reduction(self):
        with tempfile.TemporaryDirectory() as directory:
            sweep = PlannedSweep(directory, [True] * 3 + [False] * 5)
            sweep.seed_individuals(800)
            sweep.shared()
            self.assertEqual(sweep.summary["qualified_shared_rates"], dict.fromkeys(PROFILES, 400))
            self.assertEqual(len(sweep.summary["confirmation_reports"]), 3)
            limited = sweep.summary["inventory_limited_candidates"]
            self.assertEqual([(row["factor"], row["offered_intents"]) for row in limited], [(1, 960000)])
            self.assertEqual([chaos for _, chaos in sweep.calls],
                ["none"] * 3 + ["engine-crash", "lost-send", "rpc-errors", "redis-restart", "reorg"])
            self.assertTrue(all(sum(rates.values()) * 300 <= 500000 for rates, _ in sweep.calls))

    def test_no_shared_qualification_is_failure_and_never_claims_fault_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            sweep = PlannedSweep(directory, [False] * 3)
            sweep.seed_individuals(50)
            with self.assertRaises(NoQualifiedRate):
                sweep.shared()
            saved = json.loads((Path(directory) / "summary.json").read_text())
            self.assertIsNone(saved["qualified_shared_rates"])
            self.assertEqual(saved["confirmation_reports"], [])
            self.assertEqual([chaos for _, chaos in sweep.calls], ["none"] * 3)


if __name__ == "__main__":
    unittest.main()
