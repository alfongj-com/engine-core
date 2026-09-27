#!/usr/bin/env python3
"""Offline fault-control tests: real SQLite fences, no Engine/node processes."""
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

import capacity_campaign as campaign


class DurableFenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.c = campaign.Campaign.__new__(campaign.Campaign)
        c = self.c
        c.logs = Path(self.directory.name)
        c.journal = c.logs / 'recovery.sqlite'
        with sqlite3.connect(c.journal) as db:
            db.executescript('''CREATE TABLE control(singleton INTEGER PRIMARY KEY, halted INTEGER, reason TEXT);
                INSERT INTO control VALUES(1,0,NULL);
                CREATE TABLE chain_halts(chain_id TEXT PRIMARY KEY, reason TEXT);''')
        c.args = SimpleNamespace(chaos='engine-crash', chaos_chain=None, chaos_after=1, seconds=3,
            drain_seconds=1800, sample_seconds=5, retry_unknown=True, duplicate_count=10, reconcile_concurrency=1,
            report=c.logs / 'report.json')
        c.created = c.started = time.monotonic()
        c.phase = 'load'
        c.lock = threading.Lock()
        c.abort, c.stop_sampler = threading.Event(), threading.Event()
        c.infrastructure_stop = None
        c.stop_guards = threading.Event()
        c.guard_threads, c.provider_observations = [], []
        c.resource_monitor = None
        c.recovery_required = c.finality_halted = c.projection_recovered = False
        c.report, c.events, c.samples, c.errors = {}, [], [], []
        c.profiles = {'evm2': {'family': 'evm', 'rate': 2}}
        c.proxies = {'evm2': mock.Mock()}
        c.proxies['evm2'].snapshot.return_value = {'accepted_unique_wires': 1}
        c.children = {'engine': mock.Mock(), 'node': mock.Mock()}
        c.children['engine'].poll.return_value = 1
        c.children['node'].poll.return_value = None
        c.streams = []
        c.pool = SimpleNamespace(active=0, peak=0, failures=[], close=mock.Mock())
        c.expected = {'accepted-unknown': {'id': 'accepted-unknown'}}
        c.statuses = {'accepted-unknown': None}
        c.observer = SimpleNamespace(terminal=set(), admitted={'accepted-unknown'}, attempted={'accepted-unknown'})
        c.sample = mock.Mock(side_effect=AssertionError('fenced drain must not sample RPC'))
        c.submit = mock.Mock(side_effect=AssertionError('fenced retry must not POST'))

    def halt(self, reason='Redis checkpoint mismatch', chain=None):
        with sqlite3.connect(self.c.journal) as db:
            if chain is None:
                db.execute('UPDATE control SET halted=1, reason=?', (reason,))
            else:
                db.execute('INSERT INTO chain_halts VALUES(?,?)', (chain, reason))

    def failed_restart(self):
        self.c.start_engine = mock.Mock(side_effect=RuntimeError('private URL must not enter report'))
        with mock.patch.object(campaign, 'stop'):
            self.c.chaos()

    def test_restart_fence_aborts_every_remaining_offer_without_claiming_recovery(self):
        self.halt()
        self.failed_restart()
        c = self.c
        self.assertTrue(c.recovery_required)
        self.assertTrue(c.abort.is_set())
        self.assertEqual(c.report['journal_halt_category'], 'redis_checkpoint_mismatch')
        self.assertEqual(c.report['durable_fence_stop']['context'], 'chaos_failure')
        self.assertEqual(c.errors, [{'phase': 'chaos', 'error_type': 'RuntimeError'}])
        self.assertIn('engine_sigkill', [e['event'] for e in c.events])
        self.assertNotIn('chaos_recovered', [e['event'] for e in c.events])
        dropped = []
        campaign.open_loop({'evm2': 2}, 3, c.started, .1,
            lambda offer: self.fail('aborted offer was dispatched'),
            lambda offer, reason: dropped.append((offer.index, reason)), cancelled=c.abort.is_set)
        self.assertEqual(dropped, [(i, 'campaign_aborted') for i in range(6)])
        # Existing accepted/unknown IDs remain in custody for reconciliation.
        self.assertEqual(c.expected, {'accepted-unknown': {'id': 'accepted-unknown'}})
        c.wait_drain()
        c.retry_phase()
        c.sample.assert_not_called()
        c.submit.assert_not_called()
        self.assertEqual(sum(e['event'] == 'durable_fence_stop' for e in c.events), 1)

    def test_arbitrary_restart_failure_never_fabricates_a_durable_fence(self):
        self.failed_restart()
        self.assertFalse(self.c.recovery_required)
        self.assertFalse(self.c.finality_halted)
        self.assertFalse(self.c.abort.is_set())
        self.assertNotIn('durable_fence_stop', self.c.report)
        self.assertEqual(campaign.campaign_outcome({}, self.c.report, True), 'error')
        self.assertEqual(self.c.errors[0]['error_type'], 'RuntimeError')

    def test_missing_invalid_or_unreadable_journal_is_not_a_fence(self):
        for state in ('missing', 'bad-schema', 'bad-halted'):
            with self.subTest(state=state):
                path = self.c.logs / (state + '.sqlite')
                self.c.journal = path
                if state != 'missing':
                    with sqlite3.connect(path) as db:
                        if state == 'bad-schema':
                            db.execute('CREATE TABLE unrelated(value TEXT)')
                        else:
                            db.executescript('CREATE TABLE control(singleton INTEGER, halted INTEGER, reason TEXT);'
                                'INSERT INTO control VALUES(1,2,NULL); CREATE TABLE chain_halts(chain_id TEXT);')
                self.assertFalse(self.c.stop_for_durable_fence('test'))
                self.assertFalse(self.c.abort.is_set())
                self.assertFalse(self.c.recovery_required)

    def test_drain_detects_chain_fence_without_rpc_or_retry_and_hides_reason(self):
        private = 'https://user:password@rpc.example/private-token'
        self.halt(private, chain='10')
        self.c.wait_drain()
        self.c.retry_phase()
        self.assertTrue(self.c.finality_halted)
        self.assertFalse(self.c.recovery_required)
        self.assertTrue(self.c.abort.is_set())
        self.assertEqual(self.c.report['durable_chain_halts'], ['10'])
        self.assertNotIn(private, json.dumps(self.c.report))
        self.c.sample.assert_not_called()
        self.c.submit.assert_not_called()

    def test_fence_appearing_during_drain_stops_at_next_existing_poll(self):
        c = self.c
        def sample():
            self.halt('Redis checkpoint mirror failed')
            row = {'seconds': 1}
            c.samples.append(row)
            return row
        c.sample = mock.Mock(side_effect=sample)
        with mock.patch.object(campaign.time, 'sleep') as sleep:
            self.assertEqual(c.wait_drain(), {'seconds': 1})
        self.assertEqual(c.sample.call_count, 1)
        self.assertEqual(sleep.call_count, 1)
        self.assertTrue(c.recovery_required)
        self.assertNotIn('drain_deadline_reached', [e['event'] for e in c.events])

    def test_unrecognized_private_reason_is_not_copied_or_interpreted(self):
        private = 'https://rpc.example/secret?key=credential'
        self.halt(private)
        self.assertTrue(self.c.stop_for_durable_fence('test'))
        self.assertEqual(self.c.report['journal_halt_category'], 'unrecognized_private_reason')
        self.assertNotIn(private, json.dumps(self.c.report))
        with sqlite3.connect(self.c.journal) as db:
            self.assertEqual(db.execute('SELECT reason FROM control').fetchone()[0], private)

    def test_run_still_reconciles_accepted_unknown_ids_before_owned_node_cleanup(self):
        self.halt()
        c = self.c
        c.setup = mock.Mock()
        c.load = self.failed_restart
        order = []
        def reconcile():
            self.assertTrue(c.recovery_required)
            self.assertEqual(set(c.expected), {'accepted-unknown'})
            self.assertIsNone(c.children['node'].poll())
            order.append('independent_reconciliation')
            # This test covers orchestration, not the separate per-ID wire oracle.
            c.report['oracle'] = {'safety_pass': True, 'liveness_pass': False, 'outcome': 'incomplete'}
        c.reconcile = reconcile
        def stop(child, **_):
            order.append('node_cleanup' if child is c.children['node'] else 'engine_cleanup')
            child.poll.return_value = 0
        http = mock.Mock()
        http.snapshot.return_value = {}
        with mock.patch.object(campaign, 'stop', side_effect=stop), mock.patch.object(campaign, 'LOCAL_HTTP', http), mock.patch('builtins.print'):
            c.run()
        self.assertLess(order.index('independent_reconciliation'), order.index('node_cleanup'))
        self.assertEqual(c.report['outcome'], 'fail_closed_recovery_required')
        self.assertFalse(c.report['oracle']['liveness_pass'])
        self.assertTrue(c.report['owned_children_stopped'])

    def test_actual_reconcile_never_calls_a_fenced_engine_restart_recovered(self):
        c = self.c
        c.profiles, c.id_chain, c.initial = {}, {}, {}
        c.read_journal = mock.Mock(return_value={})
        c.redis_drain = mock.Mock(return_value={})
        c.report = {'per_chain': {}, 'journal_halted': True}
        c.recovery_required = True
        c.events = [{'event': 'chaos_trigger', 'kind': 'engine-crash'}, {'event': 'engine_sigkill'},
                    {'event': 'durable_fence_stop'}, {'event': 'chaos_failed'}]
        c.proxies, c.nodes = {}, {}
        oracle = {'safety_pass': True, 'liveness_pass': True, 'safety_failures': [],
                  'liveness_failures': [], 'outcome': 'pass'}
        with mock.patch.object(campaign, 'stop'), mock.patch('capacity_faults.evaluate_campaign', return_value=oracle):
            c.reconcile()
        self.assertFalse(c.report['requested_fault_validated'])
        self.assertFalse(c.report['oracle']['liveness_pass'])
        self.assertEqual(c.report['outcome'], 'fail_closed_recovery_required')


if __name__ == '__main__':
    unittest.main()
