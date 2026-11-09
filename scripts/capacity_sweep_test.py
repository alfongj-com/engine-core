import copy
import unittest

from capacity_sweep import PROFILES, closed_trial, eligible, selected_rates


class QualificationTests(unittest.TestCase):
    def test_predispatch_miss_can_only_continue_after_all_admitted_work_closes(self):
        report = {"outcome": "incomplete", "owned_children_stopped": True,
                  "oracle": {"safety_pass": True, "liveness_pass": False,
                             "observed_intents": 9, "offered_intents": 10},
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
                     lambda r: r["per_chain"]["one"]["client"].update(dropped_campaign_aborted=1)]
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


if __name__ == "__main__":
    unittest.main()
