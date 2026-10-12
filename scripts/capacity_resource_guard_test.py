import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import capacity_resource_guard as guard


def measurement(stamp=0, host=10000, guest=9000, inodes=8000, identity='guest'):
    return {'monotonic': stamp, 'duration_ms': 1, 'resources': {
        'host_bytes': guard.Capacity('host', 20000, host),
        'guest_bytes': guard.Capacity(identity, 20000, guest),
        'guest_inodes': guard.Capacity(identity, 20000, inodes)}}


def policy(**kwargs):
    return guard.ResourcePolicy({key: guard.Budget(100, 1, 0) for key in
                                 ('host_bytes', 'guest_bytes', 'guest_inodes')},
                                horizon_seconds=600, factor=2, **kwargs)


class Resources(unittest.TestCase):
    def test_fixed_setup_is_reserved_without_extrapolating_then_load_spikes_count(self):
        budget = {'host_bytes': guard.Budget(8 * guard.GIB, 2 * guard.MIB, .02, 2 * guard.GIB)}
        instance = guard.ResourcePolicy(budget, 2880, setup_phase=True)
        def sample(stamp, free):
            return {'monotonic': stamp, 'resources': {'host_bytes': guard.Capacity('host', 460 * guard.GIB, int(free * guard.GIB))}}
        initial = instance.assess(sample(0, 30))
        self.assertTrue(initial['allowed'])
        self.assertGreater(initial['resources']['host_bytes']['required_available'], 22 * guard.GIB)
        setup = instance.assess(sample(60, 28.5))
        self.assertTrue(setup['allowed'])
        self.assertEqual(setup['resources']['host_bytes']['peak_observed_consumption_per_second'], 0)
        baseline = instance.assess(sample(90, 28.4), finish_setup=True)
        self.assertTrue(baseline['allowed'])
        self.assertEqual(baseline['resource_phase'], 'steady')
        steady = instance.assess(sample(150, 28.3))
        self.assertTrue(steady['allowed'])
        spike = instance.assess(sample(210, 26.8))
        self.assertFalse(spike['allowed'])
        self.assertGreater(spike['resources']['host_bytes']['peak_observed_consumption_per_second'], 20 * guard.MIB)
        # Setup phase is not permission to consume the floor or future-run reserve.
        other = guard.ResourcePolicy(budget, 2880, setup_phase=True)
        other.assess(sample(0, 30))
        self.assertFalse(other.assess(sample(60, 10))['allowed'])

    def test_realistic_host_margin_and_host_only_probe_cover_non_native_jobs(self):
        budget = {'host_bytes': guard.Budget(8 * guard.GIB, 2 * guard.MIB, .02)}
        policy = guard.ResourcePolicy(budget, 2880)
        result = policy.assess({'monotonic': 0, 'resources': {'host_bytes': guard.Capacity('host', 460 * guard.GIB, 30 * guard.GIB)}})
        self.assertTrue(result['allowed'])
        self.assertLess(result['resources']['host_bytes']['required_available'], 22 * guard.GIB)
        result = policy.assess({'monotonic': 60, 'resources': {'host_bytes': guard.Capacity('host', 460 * guard.GIB, 8 * guard.GIB)}})
        self.assertFalse(result['allowed'])
        run = mock.Mock()
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(set(guard.resource_probe(directory, run=run)['resources']), {'host_bytes'})
        run.assert_not_called()

    def test_df_full_disk_and_inode_exhaustion_are_not_parse_failures(self):
        text = 'Filesystem 1-blocks Used Available Capacity Mounted\n/dev/vda1 1000 1000 0 100% /\nFilesystem Inodes IUsed IFree IUse% Mounted\n/dev/vda1 500 500 0 100% /\n'
        value = guard.parse_guest_df(text)
        self.assertEqual(value['guest_bytes'].available, 0)
        self.assertEqual(value['guest_inodes'].available, 0)
        for broken in ('garbage', text.replace('/dev/vda1', 'overlay', 1), text.replace('1000 1000 0', '1000 1000 -1')):
            with self.assertRaises(guard.ProbeFailure): guard.parse_guest_df(broken)

    def test_preflight_reserves_the_entire_horizon_and_shutdown_not_only_floor(self):
        assessment = policy().assess(measurement(host=1500))
        self.assertFalse(assessment['allowed'])
        self.assertEqual(assessment['resources']['host_bytes']['required_available'], 1660)
        self.assertIn('host_bytes:insufficient_projected_reserve', assessment['stop']['details'])

    def test_observed_growth_replaces_lower_declared_budget_and_latches(self):
        instance = policy()
        self.assertTrue(instance.assess(measurement())['allowed'])
        assessment = instance.assess(measurement(60, guest=7000))
        self.assertFalse(assessment['allowed'])
        self.assertAlmostEqual(assessment['resources']['guest_bytes']['peak_observed_consumption_per_second'], 2000 / 60)
        self.assertFalse(instance.assess(measurement(120))['allowed'])

    def test_freed_space_does_not_reset_peak_and_other_resources_are_independent(self):
        instance = policy()
        instance.assess(measurement())
        second = instance.assess(measurement(60, guest=8940))
        self.assertTrue(second['allowed'])
        third = instance.assess(measurement(120, guest=9000))
        self.assertEqual(third['resources']['guest_bytes']['peak_observed_consumption_per_second'], 1)
        self.assertEqual(third['resources']['host_bytes']['peak_observed_consumption_per_second'], 0)
        self.assertFalse(instance.assess(measurement(180, inodes=50))['allowed'])

    def test_layout_change_missing_probe_and_horizon_are_fenced(self):
        instance = policy(); instance.assess(measurement())
        self.assertFalse(instance.assess(measurement(60, identity='replacement'))['allowed'])
        instance = policy(); instance.assess(measurement())
        self.assertEqual(instance.failed_probe('timeout')['stop']['reason'], 'resource_observation_unavailable')
        self.assertFalse(instance.assess(measurement(60))['allowed'])
        instance = policy(); instance.assess(measurement())
        self.assertFalse(instance.assess(measurement(600))['allowed'])

    def test_invalid_budgets_never_disable_guard_silently(self):
        for rate in (0, -1, float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                guard.Budget(100, rate).validate()
        with self.assertRaises(ValueError): policy(interval_seconds=1)

    def test_probe_is_only_bounded_read_commands_without_live_vm(self):
        run = mock.Mock(return_value=mock.Mock(returncode=0, stdout='Filesystem B Used Avail Cap Mount\n/dev/vda1 10000 1000 9000 10% /\nFilesystem I Used Avail Cap Mount\n/dev/vda1 5000 1000 4000 20% /\n'))
        with tempfile.TemporaryDirectory() as directory:
            result = guard.resource_probe(directory, vm='engine-nitro', run=run)
        cmd = run.call_args.args[0]
        self.assertEqual(cmd, [guard.LIMA, 'shell', 'engine-nitro', '--'] + guard.GUEST_COMMAND)
        self.assertEqual(run.call_args.kwargs['timeout'], 8)
        self.assertEqual(result['resources']['guest_bytes'].available, 9000)
        run.reset_mock()
        with self.assertRaises(ValueError): guard.resource_probe('/tmp', vm='a; rm', run=run)
        run.assert_not_called()

    def test_monitor_evidence_is_private_no_overwrite_and_stops_before_child(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory) / 'guard.jsonl'
            stops = []
            monitor = guard.GuardMonitor(policy(), lambda: measurement(host=1), evidence, stops.append)
            result = monitor.preflight()
            self.assertFalse(result['allowed'])
            with self.assertRaises(RuntimeError): monitor.start()
            monitor.close()
            self.assertEqual(len(stops), 1)
            self.assertEqual(evidence.stat().st_mode & 0o777, 0o600)
            self.assertFalse(json.loads(evidence.read_text())['allowed'])
            next_monitor = guard.GuardMonitor(policy(), lambda: measurement(), evidence, stops.append)
            with self.assertRaises(FileExistsError): next_monitor.preflight()

    def test_observer_programming_error_also_latches_instead_of_silently_stopping(self):
        with tempfile.TemporaryDirectory() as directory:
            stops = []
            monitor = guard.GuardMonitor(policy(), lambda: {'bad': 'sample'}, Path(directory) / 'guard.jsonl', stops.append)
            self.assertFalse(monitor.preflight()['allowed'])
            monitor.close()
            self.assertEqual(stops[0]['category'], 'KeyError')

    def test_different_host_volume_is_rejected_before_lima_probe(self):
        run = mock.Mock()
        with mock.patch('capacity_resource_guard.os.stat', side_effect=[mock.Mock(st_dev=1), mock.Mock(st_dev=2)]):
            with self.assertRaisesRegex(guard.ProbeFailure, 'multiple_host_filesystems'):
                guard.resource_probe('/tmp', run=run, expected_host_paths=['/other'])
        run.assert_not_called()

    def test_monitor_probe_failure_is_fixed_category_not_subprocess_output(self):
        with tempfile.TemporaryDirectory() as directory:
            def unavailable(): raise OSError('secret output')
            stops = []
            path = Path(directory) / 'guard.jsonl'
            monitor = guard.GuardMonitor(policy(), unavailable, path, stops.append)
            self.assertFalse(monitor.preflight()['allowed'])
            monitor.close()
            self.assertNotIn('secret', path.read_text())
            self.assertEqual(stops[0]['category'], 'OSError')


class Provider(unittest.TestCase):
    def test_outage_debounce_reset_then_latched_no_auto_recovery(self):
        fence = guard.ProviderAvailability()
        self.assertIsNone(fence.observe(0, False, 'ConnectionRefusedError'))
        self.assertIsNone(fence.observe(5, False, 'ConnectionRefusedError'))
        self.assertIsNone(fence.observe(10, True))
        self.assertIsNone(fence.observe(15, False))
        self.assertIsNone(fence.observe(20, False))
        outcome = fence.observe(25, False)
        self.assertEqual(outcome['reason'], 'provider_unavailable')
        self.assertEqual(outcome['failed_span_seconds'], 10)
        self.assertEqual(fence.observe(30, True), outcome)

    def test_compressed_failures_cannot_fake_elapsed_outage(self):
        fence = guard.ProviderAvailability()
        for now in (0, .1, .2): self.assertIsNone(fence.observe(now, False))
        self.assertIsNotNone(fence.observe(10, False))

    def test_read_probe_has_exact_one_request_no_retry_no_secret_error(self):
        connection = mock.Mock()
        connection.request.side_effect = ConnectionRefusedError('secret node URL')
        factory = mock.Mock(return_value=connection)
        result = guard.provider_probe('http://127.0.0.1:18547', 'evm', 412346, factory)
        self.assertEqual(result, (False, 'ConnectionRefusedError'))
        connection.request.assert_called_once()
        connection.close.assert_called_once()
        body = json.loads(connection.request.call_args.args[2])
        self.assertEqual(body, {'jsonrpc': '2.0', 'id': 1, 'method': 'eth_chainId', 'params': []})
        self.assertEqual(factory.call_args.kwargs['timeout'], 2)

    def test_success_identity_mismatch_rpc_error_and_oversize_are_distinguished(self):
        for body, status, expected in [
                ({'jsonrpc': '2.0', 'id': 1, 'result': hex(412346)}, 200, (True, None)),
                ({'jsonrpc': '2.0', 'id': 1, 'result': '0x1'}, 200, (False, 'provider_identity_or_health_mismatch')),
                ({'jsonrpc': '2.0', 'id': 1, 'error': {'message': 'secret'}}, 200, (False, 'provider_rpc_failure')),
                ('x' * 65537, 200, (False, 'provider_http_failure')),
                ({'result': 'not a number'}, 503, (False, 'provider_http_failure'))]:
            connection = mock.Mock()
            connection.getresponse.return_value = mock.Mock(status=status)
            connection.getresponse.return_value.read.return_value = json.dumps(body).encode()
            self.assertEqual(guard.provider_probe('http://127.0.0.1:18547', 'evm', 412346,
                                                 mock.Mock(return_value=connection)), expected)
            connection.request.assert_called_once()

    def test_no_public_credentialed_or_redirect_routing(self):
        factory = mock.Mock()
        for url in ('https://127.0.0.1', 'http://localhost', 'http://user:pass@127.0.0.1',
                    'http://127.0.0.1?key=secret', 'http://example.com'):
            with self.assertRaises(ValueError): guard.provider_probe(url, 'evm', 1, factory)
        factory.assert_not_called()


if __name__ == '__main__': unittest.main()
