import copy
from collections import Counter
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest

from capacity_sweep import NoQualifiedRate, PROFILES, Sweep, closed_trial, eligible, selected_rates


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
    def __init__(self, directory, candidates):
        self.output = Path(directory)
        self.args = SimpleNamespace(profile="solana", individuals=self.output / "individuals")
        self.binary_sha = "fixed"
        self.summary = {"trials": []}
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
                "confirmation_reports": reports}))
            for name in reports:
                (directory / name).write_text(json.dumps({
                    "engine_binary_sha256": "fixed", "duration_seconds": 300, "outcome": "pass",
                    "owned_children_stopped": True, "all_chain_capacity_candidate": True,
                    "oracle": {"safety_pass": True, "liveness_pass": True},
                    "per_chain": {profile: {"offered_tps": rate, "late_window": {"capacity_candidate": True}}}}))


class SearchTests(unittest.TestCase):
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
