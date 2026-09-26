"""Guard against a broken checker being mistaken for a successful proof."""
import unittest

from check import classify_output


class ResultClassification(unittest.TestCase):
    def test_positive_requires_exhaustion_and_clean_exit(self):
        success = "Model checking completed. No error has been found.\n42 states generated, 20 distinct states found, 0 states left on queue."
        self.assertTrue(classify_output(success, 0, "pass"))
        self.assertFalse(classify_output(success, -9, "pass"))
        self.assertFalse(classify_output(success.replace("0 states left", "1 states left"), 0, "pass"))
        self.assertFalse(classify_output(success + "\nError: invariant broken", 0, "pass"))

    def test_negative_requires_named_counterexample(self):
        trace = "Error: Invariant Fence is violated.\nError: The behavior up to this point is:\nState 1:"
        self.assertTrue(classify_output(trace, 12, "invariant:Fence"))
        self.assertFalse(classify_output(trace, 12, "invariant:Other"))
        self.assertFalse(classify_output(trace, 1, "invariant:Fence"))
        self.assertFalse(classify_output("Parse error", 12, "invariant:Fence"))
        self.assertFalse(classify_output("Error: Invariant Fence is violated.", 12, "invariant:Fence"))

    def test_liveness_requires_temporal_counterexample(self):
        trace = "Error: Temporal properties were violated.\nState 1:"
        self.assertTrue(classify_output(trace, 13, "liveness"))
        self.assertFalse(classify_output(trace, 12, "liveness"))
        self.assertFalse(classify_output("Out of memory", 13, "liveness"))


if __name__ == "__main__":
    unittest.main()
