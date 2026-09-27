import unittest

from capacity_metrics import parse_metrics


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


if __name__ == "__main__":
    unittest.main()
