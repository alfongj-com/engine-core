import copy
import unittest

from capacity_metrics import parse_metrics, summarize_timing_window


class TimingParsingTests(unittest.TestCase):
    def test_histogram_buckets_and_counts_keep_exact_labels(self):
        result = parse_metrics(b'# TYPE tw_engine_journal_duration_seconds histogram\n'
            b'tw_engine_journal_duration_seconds_bucket{phase="serial_wait",operation="admission",le="+Inf"} 7\n'
            b'tw_engine_journal_duration_seconds_count{operation="admission",phase="serial_wait"} 7\n'
            b'tw_engine_journal_duration_seconds_sum{operation="admission",phase="serial_wait"} 0.125\n')
        self.assertEqual(len(result), 3)
        self.assertEqual(result['tw_engine_journal_duration_seconds_sum{"operation":"admission","phase":"serial_wait"}'], .125)

    def test_sensitive_label_families_are_omitted(self):
        self.assertEqual(parse_metrics(b'tw_engine_wallet_duration{eoa_address="private"} 1\n'
                                      b'tw_engine_rpc_duration{endpoint="private"} 1\n'), {})

    def test_duplicate_or_invalid_series_cannot_silently_change_totals(self):
        bad = [b'twmq_count 1\ntwmq_count 2\n', b'twmq_count -1\n', b'twmq_count NaN\n',
               b'twmq_count{phase="one",phase="two"} 1\n', b'twmq_count{broken} 1\n']
        for raw in bad:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_metrics(raw)


class TimingWindowTests(unittest.TestCase):
    sum_key = 'tw_engine_journal_duration_seconds_sum{"phase":"serial_wait"}'
    count_key = 'tw_engine_journal_duration_seconds_count{"phase":"serial_wait"}'

    def report(self):
        def sample(seconds, count, total, phase="load"):
            return {"phase": phase, "seconds": seconds, "engine_pid": 123,
                    "http_status": 200, "scrape_seconds": .01,
                    "values": {self.count_key: count, self.sum_key: total}}
        return {"warmup_seconds": 10, "timing_observations": [
            sample(0, 1, 10), sample(10, 10, 100), sample(20, 15, 150),
            sample(30, 20, 180), sample(40, 200, 2000, "drain")]}

    def test_window_excludes_warmup_and_drain_and_keeps_overlapping_seconds(self):
        result = summarize_timing_window(self.report())
        self.assertEqual((result["from_seconds"], result["to_seconds"]), (10, 30))
        self.assertEqual(result["metrics"], [{
            "metric": "tw_engine_journal_duration_seconds", "labels": {"phase": "serial_wait"},
            "observations": 10, "sum": 80, "mean": 8}])

    def test_new_series_starts_at_zero(self):
        report = self.report()
        report["timing_observations"][1]["values"] = {}
        result = summarize_timing_window(report)["metrics"][0]
        self.assertEqual((result["observations"], result["sum"], result["mean"]), (20, 180, 9))

    def test_unreliable_windows_fail_instead_of_reporting_plausible_means(self):
        mutations = [
            lambda rows: rows[2].update(engine_pid=456),
            lambda rows: rows[2].update(http_status=503),
            lambda rows: rows[2].pop("values"),
            lambda rows: rows[2]["values"].update({self.count_key: 9}),
            lambda rows: rows[2]["values"].update({self.sum_key: 99}),
            lambda rows: rows[2]["values"].update({self.sum_key: float("nan")}),
            lambda rows: rows[2].update(seconds=30),
            lambda rows: rows[2].update(scrape_seconds=float("inf")),
            lambda rows: rows[3]["values"].pop(self.count_key),
            lambda rows: rows[3]["values"].pop(self.sum_key),
        ]
        for mutate in mutations:
            report = copy.deepcopy(self.report())
            mutate(report["timing_observations"])
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                summarize_timing_window(report)

    def test_too_few_samples_cannot_supply_a_window(self):
        report = self.report()
        report["timing_observations"] = report["timing_observations"][1:2]
        with self.assertRaises(ValueError):
            summarize_timing_window(report)


if __name__ == "__main__":
    unittest.main()
