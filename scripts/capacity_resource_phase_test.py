"""Phase budget regressions: fake resources/processes only, no service or RPC use."""
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

import capacity_resource_guard as guard
import capacity_campaign as campaign
import capacity_infrastructure_test as infrastructure


def host_sample(stamp, available):
    return {'monotonic': stamp, 'resources': {
        'host_bytes': guard.Capacity('host', 245107195904, available)}}


def instance():
    return guard.ResourcePolicy({'host_bytes': guard.Budget(8 * guard.GIB, 2 * guard.MIB, .02)},
                                horizon_seconds=2880, setup_phase=True)


def before_verification(policy):
    for sample, setup in [(host_sample(0, 34000000000), False),
                          (host_sample(34, 34000000000), True),
                          (host_sample(360.048347167, 33544347648), False)]:
        if not policy.assess(sample, finish_setup=setup)['allowed']:
            raise AssertionError('Invalid fixture reserve')


class PhasePolicy(unittest.TestCase):
    def test_exact_incident_depletion_keeps_peak_but_only_reserves_verification(self):
        sample = host_sample(420.059106959, 32865955840)
        old = instance(); before_verification(old)
        failed = old.assess(sample)
        self.assertFalse(failed['allowed'])  # Reproduces obsolete whole-horizon policy.
        self.assertAlmostEqual(failed['remaining_horizon_seconds'], 2459.940893041)
        self.assertEqual(failed['resources']['host_bytes']['required_available'], 68276373546)
        new = instance(); before_verification(new)
        new.begin_reconciliation(419.372808)
        result = new.assess(sample)
        self.assertTrue(result['allowed'])
        self.assertEqual(result['resource_phase'], 'reconciliation')
        self.assertAlmostEqual(result['remaining_horizon_seconds'], 599.313701041)
        for field in ('floor', 'peak_observed_consumption_per_second', 'projected_growth_per_second'):
            self.assertEqual(result['resources']['host_bytes'][field], failed['resources']['host_bytes'][field])
        self.assertLess(result['resources']['host_bytes']['required_available'], 27 * guard.GIB)

    def test_existing_load_stop_never_clears_at_phase_transition(self):
        p = instance(); before_verification(p)
        first = p.assess(host_sample(420.059106959, 32865955840))
        p.begin_reconciliation(421)
        second = p.assess(host_sample(422, 34000000000))
        self.assertFalse(second['allowed'])
        self.assertEqual(first['stop'], second['stop'])
        self.assertEqual(first['resources']['host_bytes']['peak_observed_consumption_per_second'],
                         second['resources']['host_bytes']['peak_observed_consumption_per_second'])

    def test_reconciliation_is_finite_nonrenewable_and_never_extends_original_deadline(self):
        p = instance(); before_verification(p)
        p.begin_reconciliation(400)
        self.assertEqual(p.reconciliation_deadline, 1000)
        with self.assertRaises(RuntimeError): p.begin_reconciliation(500)
        self.assertEqual(p.reconciliation_deadline, 1000)
        self.assertFalse(p.assess(host_sample(1000, 33544347648))['allowed'])
        self.assertIn('declared_reconciliation_horizon_exhausted', p.stop['details'])
        q = instance(); before_verification(q)
        q.begin_reconciliation(2700)
        self.assertEqual(q.reconciliation_deadline, 2880)
        self.assertFalse(q.assess(host_sample(2880, 33544347648))['allowed'])

    def test_no_setup_transition_and_verification_spike_still_fences(self):
        p = instance(); p.assess(host_sample(0, 34000000000))
        with self.assertRaises(ValueError): p.begin_reconciliation(1)
        before = instance(); before_verification(before)
        before.begin_reconciliation(400)
        self.assertFalse(before.assess(host_sample(420, 28000000000))['allowed'])
        self.assertGreater(before.peaks['host_bytes'], 90_000_000)
        below_floor = instance(); before_verification(below_floor)
        below_floor.begin_reconciliation(400)
        self.assertFalse(below_floor.assess(host_sample(420, 7 * guard.GIB))['allowed'])


class PhaseMonitor(unittest.TestCase):
    def test_failed_stop_keeps_original_budget_and_performs_no_new_probe(self):
        with tempfile.TemporaryDirectory() as directory:
            p = instance()
            probe = mock.Mock(side_effect=[host_sample(0, 34000000000), host_sample(34, 34000000000)])
            monitor = guard.GuardMonitor(p, probe, Path(directory) / 'resource.jsonl', mock.Mock())
            monitor.preflight(); monitor.begin_load()
            with self.assertRaisesRegex(RuntimeError, 'failed stop'):
                monitor.begin_reconciliation(mock.Mock(side_effect=RuntimeError('failed stop')))
            self.assertIsNone(p.reconciliation_deadline)
            self.assertEqual(probe.call_count, 2)
            monitor.close()

    def test_monitor_cannot_assess_obsolete_horizon_after_engine_stop(self):
        with tempfile.TemporaryDirectory() as directory:
            probe = mock.Mock(side_effect=[host_sample(0, 34000000000), host_sample(34, 34000000000),
                                         host_sample(420, 33544347648), host_sample(421, 33544347648)])
            monitor = guard.GuardMonitor(instance(), probe, Path(directory) / 'resource.jsonl', mock.Mock())
            monitor.preflight(); monitor.begin_load()
            stopping, release, tick_entered, tick_done = (threading.Event() for _ in range(4))
            errors = []
            def stopped():
                stopping.set()
                if not release.wait(2): raise AssertionError('Fixture stop not released')
            def transition():
                try: monitor.begin_reconciliation(stopped)
                except BaseException as error: errors.append(error)
            def tick():
                tick_entered.set()
                try: monitor.tick('monitor')
                except BaseException as error: errors.append(error)
                finally: tick_done.set()
            with mock.patch.object(guard.time, 'monotonic', return_value=419):
                first = threading.Thread(target=transition); first.start()
                self.assertTrue(stopping.wait(1))
                second = threading.Thread(target=tick); second.start()
                self.assertTrue(tick_entered.wait(1))
                self.assertFalse(tick_done.wait(.02))
                self.assertEqual(probe.call_count, 2)
                release.set(); first.join(2); second.join(2)
            self.assertFalse(first.is_alive() or second.is_alive())
            self.assertEqual(errors, [])
            monitor.close()
            rows = [json.loads(line) for line in monitor.path.read_text().splitlines()]
            self.assertEqual([row['phase'] for row in rows[-2:]], ['reconciliation_baseline', 'monitor'])
            self.assertEqual([row['resource_phase'] for row in rows[-2:]], ['reconciliation', 'reconciliation'])
            self.assertTrue(all(row['allowed'] for row in rows))


class CampaignBoundary(unittest.TestCase):
    setUp = infrastructure.Infrastructure.setUp

    def test_actual_reconcile_stops_engine_then_transitions_before_first_oracle_read(self):
        c = self.c
        c.report['resource_guard'] = {}
        c.profiles, c.nodes = {}, {}
        child = mock.Mock(); c.children = {'engine': child}
        p = instance(); before_verification(p)
        monitor = guard.GuardMonitor(p, lambda: host_sample(420.059106959, 32865955840),
            c.logs / 'phase.jsonl', c.stop_for_infrastructure)
        # Preflight/baseline already modeled above; attach private evidence stream.
        monitor.stream = monitor.path.open('x')
        self.addCleanup(monitor.close)
        c.resource_monitor = monitor
        class OracleReached(Exception): pass
        def read():
            self.assertAlmostEqual(p.reconciliation_deadline, 1019.372808)
            self.assertFalse(c.infrastructure_stop)
            raise OracleReached()
        c.read_journal = mock.Mock(side_effect=read)
        with mock.patch.object(campaign, 'stop') as stop, \
             mock.patch.object(guard.time, 'monotonic', return_value=419.372808):
            with self.assertRaises(OracleReached): c.reconcile()
        stop.assert_called_once_with(child)
        c.read_journal.assert_called_once()
        self.assertEqual(c.report['resource_guard']['reconciliation_budget_seconds'], 600)
        self.assertEqual(json.loads(monitor.path.read_text())['resource_phase'], 'reconciliation')

    def test_actual_reconcile_retains_prior_stop_and_captures_custody_without_rpc(self):
        c = self.c
        c.report['resource_guard'] = {}
        c.children = {'engine': mock.Mock()}
        c.read_journal, c.capture_interrupted_custody = mock.Mock(), mock.Mock()
        p = instance(); before_verification(p)
        p.failed_probe('earlier_load_failure')
        monitor = guard.GuardMonitor(p, lambda: host_sample(420, 33544347648),
            c.logs / 'phase.jsonl', c.stop_for_infrastructure)
        monitor.stream = monitor.path.open('x'); self.addCleanup(monitor.close)
        c.resource_monitor = monitor
        with mock.patch.object(campaign, 'stop'), mock.patch.object(guard.time, 'monotonic', return_value=419):
            c.reconcile()
        c.capture_interrupted_custody.assert_called_once()
        c.read_journal.assert_not_called()
        c.nodes['nitro'].call.assert_not_called()
        self.assertEqual(c.infrastructure_stop['category'], 'earlier_load_failure')


if __name__ == '__main__': unittest.main()
