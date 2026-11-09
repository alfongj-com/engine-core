import copy
import unittest

from capacity_sweep import PROFILES, eligible, selected_rates


class QualificationTests(unittest.TestCase):
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
