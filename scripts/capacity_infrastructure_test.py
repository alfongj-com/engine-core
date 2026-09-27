"""Real campaign lifecycle with mocked providers/resources; no nodes or services."""
from collections import Counter
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

import capacity_campaign as campaign
from capacity_resource_guard import Capacity, GIB, ProviderAvailability


class Infrastructure(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        c = self.c = campaign.Campaign.__new__(campaign.Campaign)
        c.logs = Path(self.temp.name)
        c.journal = c.logs / 'recovery.sqlite'
        c.args = SimpleNamespace(report=c.logs / 'report.json', seconds=20, drain_seconds=30,
            sample_seconds=5, warmup_seconds=0, max_schedule_lag_ms=100, late_window_seconds=10,
            max_admission_p99_ms=1000, solana_park_every=0, chaos='none', http_timeout=1,
            retry_unknown=True, duplicate_count=100, native_resource_vm=None, reconcile_concurrency=1)
        c.created = c.started = 0
        c.phase = 'load'
        c.namespace = c.projection_namespace = 'fixture'
        c.redis_port = 1
        c.base, c.token = 'http://127.0.0.1:1', 'test-only'
        c.lock = threading.Lock()
        c.abort, c.stop_sampler, c.stop_guards = threading.Event(), threading.Event(), threading.Event()
        c.infrastructure_stop = None
        c.resource_monitor = None
        c.lifecycle_stop = threading.Event()
        c.chaos_thread = None
        c.guard_threads, c.provider_observations = [], []
        c.recovery_required = c.finality_halted = c.projection_recovered = False
        c.report, c.events, c.samples, c.errors = {}, [], [], []
        c.profiles = {'nitro': {'family': 'evm', 'rate': 2, 'chain_id': 412346,
                               'external_url': 'http://127.0.0.1:18547', 'drain_hook': ['never-start']}}
        c.nodes = {'nitro': SimpleNamespace(url='http://127.0.0.1:18547', call=mock.Mock(side_effect=AssertionError('No unavailable chain RPC')))}
        c.provider_fences = {'nitro': ProviderAvailability()}
        c.proxies = {'nitro': mock.Mock()}
        c.proxies['nitro'].snapshot.return_value = {'accepted_unique_wires': 2, 'calls': 3}
        c.children, c.streams = {}, []
        c.pool = SimpleNamespace(active=0, peak=1, failures=[], close=mock.Mock(), submit=lambda fn, offer: (fn(offer), True)[1])
        c.expected, c.id_chain, c.times, c.statuses = {}, {}, {}, {}
        c.stats, c.responses, c.transport_errors = {'nitro': Counter()}, {'nitro': Counter()}, {'nitro': Counter()}
        c.http_latency, c.scheduled_latency, c.http_start_lateness = {'nitro': []}, {'nitro': []}, {'nitro': []}
        c.fixture = mock.Mock(return_value=({}, {}))
        c.observer = SimpleNamespace(terminal=set(), admitted=set(), attempted=set(), latencies={'nitro': {}})
        c.capture_durable_fences = mock.Mock(return_value=False)

    def test_transient_fault_does_not_abort_and_outage_latches_distinct_reason(self):
        c = self.c
        with mock.patch.object(campaign, 'provider_probe', side_effect=[(False, 'ConnectionRefusedError'), (False, 'TimeoutError'), (True, None), (False, 'ConnectionRefusedError'), (False, 'ConnectionRefusedError'), (False, 'ConnectionRefusedError')]) as probe:
            for now in (0, 5, 10, 15, 20):
                c.provider_guard_tick('nitro', now)
                self.assertFalse(c.abort.is_set())
            c.provider_guard_tick('nitro', 25)
        self.assertTrue(c.abort.is_set())
        self.assertEqual(c.report['infrastructure_stop']['reason'], 'provider_unavailable')
        self.assertNotIn('journal_halted', c.report)
        self.assertFalse(c.recovery_required)
        self.assertEqual(probe.call_count, 6)
        self.assertEqual(probe.call_args.args[0], c.nodes['nitro'].url)
        c.nodes['nitro'].call.assert_not_called()

    def test_actual_load_stops_offers_skips_failed_end_sample_drain_hook_and_retries(self):
        c = self.c
        clock = SimpleNamespace(now=0)
        def sleep(seconds): clock.now += seconds
        def sample():
            row = {'seconds': clock.now, 'chains': {'nitro': {'admitted': 0, 'attempted': 0, 'terminal': 0, 'included': 0}}}
            c.samples.append(row); return row
        c.sample = mock.Mock(side_effect=sample)
        c.start_provider_guards = mock.Mock()  # ticks injected deterministically below
        c.wait_drain, c.retry_phase, c.spawn = mock.Mock(), mock.Mock(), mock.Mock()
        original = campaign.open_loop
        def controlled_loop(rates, duration, start, lag, dispatch, dropped, **kwargs):
            tick = 0
            def checked_dispatch(offer):
                nonlocal tick
                while clock.now >= tick:
                    c.provider_guard_tick('nitro', tick); tick += 5
                return dispatch(offer)
            return original(rates, duration, start, lag, checked_dispatch, dropped,
                            lambda: clock.now, sleep, **kwargs)
        fake_thread = mock.Mock(); fake_thread.is_alive.return_value = False
        with mock.patch.object(campaign, 'JournalObserver', return_value=c.observer), \
             mock.patch.object(campaign.threading, 'Thread', return_value=fake_thread), \
             mock.patch.object(campaign.time, 'monotonic', side_effect=lambda: clock.now), \
             mock.patch.object(campaign, 'open_loop', side_effect=controlled_loop), \
             mock.patch.object(campaign, 'provider_probe', return_value=(False, 'ConnectionRefusedError')), \
             mock.patch.object(campaign, 'request', return_value=(202, {})) as request:
            c.load()
        self.assertEqual(request.call_count, 20)  # t=0..9.5; outage latches at10
        self.assertEqual(c.stats['nitro']['dropped_infrastructure_stop'], 1)  # queued dispatch sees latch
        self.assertEqual(c.stats['nitro']['dropped_campaign_aborted'], 19)
        self.assertEqual(c.responses['nitro']['202'], 20)
        c.wait_drain.assert_not_called(); c.retry_phase.assert_not_called(); c.spawn.assert_not_called()
        self.assertEqual(c.sample.call_count, 1)
        self.assertTrue(c.report['operator_review_required'])
        self.assertLess(c.report['offered_phase_end_seconds'], 20)
        self.assertFalse(c.report['per_chain']['nitro']['late_window']['capacity_candidate'])

    def test_already_queued_dispatch_and_retry_do_not_post_after_latch(self):
        c = self.c
        c.stop_for_infrastructure({'reason': 'provider_unavailable'})
        with mock.patch.object(campaign, 'request') as request:
            c.submit(campaign.Offer('nitro', 1, 0))
            c.retry_phase()
        request.assert_not_called(); c.fixture.assert_not_called()
        self.assertEqual(c.stats['nitro']['dropped_infrastructure_stop'], 1)

    def make_journal(self):
        db = sqlite3.connect(self.c.journal)
        self.addCleanup(db.close)
        db.executescript('''PRAGMA journal_mode=WAL;
            CREATE TABLE admissions(id TEXT, state TEXT, payload TEXT);
            INSERT INTO admissions VALUES('a','terminal','private-fixture-secret');
            INSERT INTO admissions VALUES('b','admitted','private-fixture-secret');
            INSERT INTO admissions VALUES('c','admitted','private-fixture-secret');
            CREATE TABLE attempts(id TEXT, payload TEXT);
            INSERT INTO attempts VALUES('a','exact-original-wire');
            INSERT INTO attempts VALUES('b','exact-original-wire');
            CREATE TABLE terminal_evidence(id TEXT);
            INSERT INTO terminal_evidence VALUES('a');''')
        db.commit()
        self.assertTrue(Path(str(self.c.journal) + '-wal').is_file())

    def test_actual_run_preserves_wal_custody_when_oracle_is_unavailable(self):
        c = self.c
        self.make_journal()
        c.setup = mock.Mock()
        c.load = lambda: c.stop_for_infrastructure({'reason': 'provider_unavailable', 'chain': 'nitro'})
        c.reconcile = mock.Mock(side_effect=AssertionError('No doomed per-ID RPC sweep'))
        http = mock.Mock(); http.snapshot.return_value = {}
        with mock.patch.object(campaign, 'redis_command', return_value=0) as redis, \
             mock.patch.object(campaign, 'LOCAL_HTTP', http), mock.patch('builtins.print'):
            c.run()
        c.reconcile.assert_not_called()
        self.assertEqual(redis.call_count, 6)
        self.assertTrue(all(call.args[1] in ('LLEN', 'HLEN', 'ZCARD') for call in redis.call_args_list))
        summary = c.report['custody']['journal']
        self.assertTrue(summary['available'])
        self.assertEqual((summary['admitted'], summary['terminal'], summary['attempted_ids'], summary['unsigned']), (3, 1, 2, 1))
        backup = Path(summary['backup'])
        self.assertEqual(backup.stat().st_mode & 0o777, 0o600)
        with sqlite3.connect(backup) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM admissions').fetchone()[0], 3)
        report = json.loads(c.args.report.read_text())
        self.assertEqual(report['outcome'], 'infrastructure_stop')
        self.assertIsNone(report['oracle']['safety_pass'])
        self.assertFalse(report['oracle']['liveness_pass'])
        self.assertNotIn('drain', report)
        self.assertNotIn('private-fixture-secret', json.dumps(report))
        self.assertFalse(report['all_chain_capacity_candidate'])
        self.assertTrue(report['operator_review_required'])
        c.nodes['nitro'].call.assert_not_called()

    def test_resource_preflight_rejects_before_starting_any_service(self):
        c = self.c
        c.args.native_resource_vm = 'engine-nitro'
        c.args.resource_host_path = c.args.resource_vm_directory = c.logs
        c.args.host_growth_mib_second, c.args.guest_growth_mib_second, c.args.guest_growth_inodes_second = 2, 1, 4
        c.start_redis = mock.Mock()
        sample = {'monotonic': 0, 'resources': {'host_bytes': Capacity('host', 100 * GIB, 1),
                 'guest_bytes': Capacity('guest', 24 * GIB, 12 * GIB), 'guest_inodes': Capacity('guest', 1000000, 900000)}}
        with mock.patch.object(campaign, 'resource_probe', return_value=sample):
            with self.assertRaisesRegex(RuntimeError, 'preflight rejected'): c.setup()
        c.start_redis.assert_not_called()
        self.assertEqual(c.infrastructure_stop['reason'], 'resource_reserve_insufficient')
        c.close_guards()

    def test_native_cli_requires_resource_guard_and_explicit_bounded_budget(self):
        engine = self.c.logs / 'engine'; engine.write_text('fixture')
        args = ['--engine-bin', str(engine), '--report', str(self.c.args.report), '--chain', 'nitro=1',
                '--external-evm', 'nitro=http://127.0.0.1:18547', '--chain-id', 'nitro=412346']
        with self.assertRaisesRegex(ValueError, 'requires --native-resource-vm'): campaign.arguments(args)
        parsed, _ = campaign.arguments(args + ['--native-resource-vm', 'engine-nitro'])
        self.assertEqual(parsed.guest_growth_mib_second, 1)
        with self.assertRaises(ValueError): campaign.arguments(args + ['--native-resource-vm', 'engine-nitro', '--guest-growth-mib-second', 'nan'])


class CleanupGuards(unittest.TestCase):
    setUp = Infrastructure.setUp
    make_journal = Infrastructure.make_journal

    def test_stop_during_actual_reconcile_cannot_repromote_capacity(self):
        c = self.c
        c.setup, c.load = mock.Mock(), mock.Mock()
        c.profiles, c.id_chain, c.initial, c.nodes, c.proxies = {}, {}, {}, {}, {}
        child = mock.Mock(); child.poll.return_value = 0
        c.children = {'engine': child}
        c.report = {'per_chain': {}, 'journal_halted': False}
        c.read_journal, c.redis_drain = mock.Mock(return_value={}), mock.Mock(return_value={})
        def independent_oracle(*_):
            c.stop_for_infrastructure({'reason': 'resource_reserve_insufficient'})
            return {'safety_pass': True, 'liveness_pass': True, 'safety_failures': [],
                    'liveness_failures': [], 'outcome': 'pass'}
        http = mock.Mock(); http.snapshot.return_value = {}
        with mock.patch('capacity_faults.evaluate_campaign', side_effect=independent_oracle), \
             mock.patch.object(campaign, 'LOCAL_HTTP', http), mock.patch('builtins.print'):
            c.run()  # Uses the real reconciliation method and finally publication.
        self.assertTrue(c.report['oracle']['safety_pass'])  # Do not discard actual proof.
        self.assertEqual(c.report['outcome'], 'infrastructure_stop')
        self.assertFalse(c.report['all_chain_capacity_candidate'])
        self.assertTrue(c.report['operator_review_required'])

    def test_held_http_202_finishes_before_exception_custody_snapshot(self):
        c = self.c
        self.make_journal()
        c.setup = mock.Mock()
        c.pool = campaign.BoundedPool(1)
        started, release, captured = threading.Event(), threading.Event(), threading.Event()
        original_capture = c.capture_interrupted_custody
        def capture():
            self.assertEqual(c.statuses.get('held-original-id'), 202)
            self.assertEqual(c.pool.active, 0)
            captured.set(); original_capture()
        c.capture_interrupted_custody = capture
        def response(_):
            started.set()
            if not release.wait(2): raise AssertionError('fixture response never released')
            c.statuses['held-original-id'] = 202
            c.responses['nitro']['202'] += 1
        def load():
            c.pool.submit(response, None)
            if not started.wait(1): raise AssertionError('fixture request never started')
            raise ValueError('primary end-sample failure')
        c.load = load
        errors = []
        def run():
            try: c.run()
            except BaseException as error: errors.append(error)
        http = mock.Mock(); http.snapshot.return_value = {}
        with mock.patch.object(campaign, 'redis_command', return_value=0), \
             mock.patch.object(campaign, 'LOCAL_HTTP', http), mock.patch('builtins.print'):
            thread = threading.Thread(target=run); thread.start()
            self.assertTrue(started.wait(1))
            self.assertFalse(captured.wait(.03))  # actual pool drain is waiting on the held response
            release.set(); thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], ValueError)
        self.assertTrue(captured.is_set())
        response_file = Path(c.report['custody']['dispatched_id_response_evidence'])
        self.assertEqual(json.loads(response_file.read_text()), {'held-original-id': 202})
        self.assertEqual(c.report['custody']['http_responses']['nitro']['202'], 1)

    def test_chaos_cannot_mutate_after_stop_during_trigger_wait_or_before_restart(self):
        c = self.c
        c.args.chaos, c.args.chaos_chain, c.args.chaos_after = 'engine-crash', None, 1
        c.children = {'engine': mock.Mock()}
        c.start_engine = mock.Mock()
        def stopped_wait(*_, **__):
            c.stop_for_infrastructure({'reason': 'provider_unavailable'})
            return 1
        c.proxies['nitro'].wait_for_accepted.side_effect = stopped_wait
        with mock.patch.object(campaign, 'stop') as stop:
            c.chaos()
        stop.assert_not_called(); c.start_engine.assert_not_called()
        self.assertIn('chaos_stopped_at_lifecycle_fence', [row['event'] for row in c.events])
        # A second cut after the authorized crash but before restart must fence the latter.
        c.infrastructure_stop = None; c.lifecycle_stop.clear(); c.stop_guards.clear(); c.abort.clear()
        c.proxies['nitro'].wait_for_accepted.side_effect = None
        c.proxies['nitro'].wait_for_accepted.return_value = 1
        def after_crash(*_, **__): c.stop_for_infrastructure({'reason': 'resource_reserve_insufficient'})
        with mock.patch.object(campaign, 'stop', side_effect=after_crash) as stop:
            c.chaos()
        self.assertEqual(stop.call_count, 1)
        c.start_engine.assert_not_called()

    def test_exception_joins_lifecycle_worker_before_custody_and_cleanup(self):
        c = self.c
        self.make_journal()
        c.setup = mock.Mock()
        c.chaos_thread = mock.Mock(); c.chaos_thread.is_alive.return_value = False
        c.load = mock.Mock(side_effect=ValueError('primary'))
        original = c.capture_interrupted_custody
        def capture():
            self.assertTrue(c.lifecycle_stop.is_set())
            c.chaos_thread.join.assert_called_with(timeout=65)
            original()
        c.capture_interrupted_custody = capture
        http = mock.Mock(); http.snapshot.return_value = {}
        with mock.patch.object(campaign, 'redis_command', return_value=0), \
             mock.patch.object(campaign, 'LOCAL_HTTP', http), mock.patch('builtins.print'):
            with self.assertRaisesRegex(ValueError, 'primary'): c.run()
        self.assertTrue(c.report['operator_review_required'])

    def test_guard_cleanup_failure_does_not_skip_children_or_report(self):
        c = self.c
        self.make_journal()
        c.setup = mock.Mock()
        c.load = lambda: c.stop_for_infrastructure({'reason': 'provider_unavailable'})
        child = mock.Mock(); child.poll.return_value = None
        c.children = {'engine': child}
        c.close_guards = mock.Mock(side_effect=RuntimeError('guard stuck'))
        http = mock.Mock(); http.snapshot.return_value = {}
        def stop(process): process.poll.return_value = 0
        with mock.patch.object(campaign, 'stop', side_effect=stop), \
             mock.patch.object(campaign, 'redis_command', return_value=0), \
             mock.patch.object(campaign, 'LOCAL_HTTP', http), mock.patch('builtins.print'):
            c.run()
        self.assertTrue(c.report['owned_children_stopped'])
        self.assertTrue(c.args.report.is_file())
        self.assertIn('GuardCleanupFailed', [error['error_type'] for error in c.report['errors']])
        self.assertEqual(c.report['outcome'], 'infrastructure_stop')

    def test_custody_file_inventory_error_does_not_mask_original_exception(self):
        c = self.c
        self.make_journal()
        c.setup = mock.Mock()
        def fail():
            c.stop_for_infrastructure({'reason': 'provider_unavailable'})
            raise ValueError('original failure')
        c.load = fail
        http = mock.Mock(); http.snapshot.return_value = {}
        with mock.patch.object(Path, 'glob', side_effect=OSError('private storage diagnostic')), \
             mock.patch.object(campaign, 'redis_command', return_value=0), \
             mock.patch.object(campaign, 'LOCAL_HTTP', http), mock.patch('builtins.print'):
            with self.assertRaisesRegex(ValueError, 'original failure'): c.run()
        report = json.loads(c.args.report.read_text())
        self.assertEqual(report['error_type'], 'ValueError')
        self.assertEqual(report['custody']['capture_error'], 'OSError')
        self.assertNotIn('private storage diagnostic', json.dumps(report))
        self.assertIsNone(report['oracle']['safety_pass'])

    def test_planned_proxy_rpc_errors_do_not_count_as_direct_provider_outage(self):
        c = self.c
        c.proxies['nitro'].snapshot.return_value = {'methods': {'eth_sendRawTransaction': {'errors': 20}}}
        with mock.patch.object(campaign, 'provider_probe', return_value=(True, None)):
            for now in (0, 5, 10, 15): c.provider_guard_tick('nitro', now)
        self.assertIsNone(c.infrastructure_stop)
        self.assertFalse(c.abort.is_set())



class SupervisorIntegration(unittest.TestCase):
    setUp = Infrastructure.setUp

    def test_supervisor_preserves_outage_and_pause_failure_and_blocks_resume(self):
        spec = importlib.util.spec_from_file_location('guard_supervisor', str(Path(__file__).resolve().parents[1] / 'docs/baselines/capacity-2026-09-26/supervisor-v4/runner.py'))
        supervisor = importlib.util.module_from_spec(spec); spec.loader.exec_module(supervisor)
        root = supervisor.ROOT
        names = ('capacity_campaign.py', 'capacity_faults.py', 'capacity_resource_guard.py')
        frozen = {str(root / 'scripts' / name): name for name in names}
        frozen[str(supervisor.ENGINE)] = 'engine'
        state = {'id': 'fixture', 'frozen_sha256': frozen, 'runs': [], 'active': None, 'review_required': None,
                 'config': {'drain_seconds': 60, 'setup_seconds': 120, 'verification_seconds': 600,
                            'output_dir': str(self.c.logs)}}
        job = {'name': 'outage', 'seconds': 60, 'chains': ['nitro=1'], 'extra': []}
        state['jobs'] = [job]
        report = {'engine_binary_sha256': 'engine', 'harness_sha256': {n: n for n in names},
                  'outcome': 'infrastructure_stop', 'infrastructure_stop': {'reason': 'provider_unavailable'},
                  'operator_review_required': True, 'oracle': {'safety_pass': None}}
        def child(cmd, *_):
            report_path = Path(cmd[cmd.index('--report') + 1]); report_path.write_text(json.dumps(report))
            return 1
        def command(cmd, **_):
            if 'pause' in cmd: raise supervisor.subprocess.CalledProcessError(1, cmd)
            return mock.Mock(returncode=0)
        path = self.c.logs / 'state.json'
        with mock.patch.object(supervisor, 'verify_frozen'), mock.patch.object(supervisor, 'run_child', side_effect=child), \
             mock.patch.object(supervisor.subprocess, 'run', side_effect=command) as commands, \
             mock.patch.object(supervisor, 'event'):
            with self.assertRaisesRegex(RuntimeError, 'recovery/review'): supervisor.run_one(state, path, job)
        stored = json.loads(path.read_text())
        self.assertEqual(stored['active']['native_pause_error'], 'CalledProcessError')
        self.assertTrue(stored['review_required'])
        self.assertFalse(stored['runs'][0]['safely_settled'])
        with self.assertRaisesRegex(RuntimeError, 'Global stop'): supervisor.choose(stored)
        self.assertIn('capacity_resource_guard.py', ' '.join(commands.call_args_list[0].args[0]))
        self.assertEqual(json.loads(Path(stored['runs'][0]['report']).read_text())['infrastructure_stop']['reason'], 'provider_unavailable')
        host = supervisor.resource_preflight_command(state, {**job, 'chains': ['solana=1']}, self.c.logs / 'other.json')
        self.assertNotIn('--vm', host)
        self.assertIn('--host-growth-mib-second', host)


if __name__ == '__main__': unittest.main()
