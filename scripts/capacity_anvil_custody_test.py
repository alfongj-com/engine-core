"""Failure-only custody tests; optional tiny pinned-Anvil restore, no Engine/load."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import capacity_campaign as campaign
import capacity_infrastructure_test as infrastructure_fixture


class SnapshotBounds(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def node(self, raw=b'{"historical_states":{},"transactions":[]}'):
        block = {"number": "0x1", "hash": "0x" + "ab" * 32}
        def call(method, params):
            if method == 'anvil_dumpState':
                self.assertEqual(params, [True])
                return '0x' + gzip.compress(raw).hex()
            if method == 'eth_getBlockByNumber': return dict(block)
            if method == 'txpool_content': return {'pending': {}, 'queued': {}}
            self.assertIn(method, ('evm_setIntervalMining', 'evm_setAutomine'))
            return True
        return SimpleNamespace(call=mock.Mock(side_effect=call))

    def test_private_synced_no_overwrite_and_pool_limit_is_explicit(self):
        target = self.root / 'snapshot'
        node = self.node()
        result = campaign.preserve_anvil_custody(node, target)
        self.assertEqual(node.call.call_args_list[:2], [mock.call('evm_setIntervalMining', [0]), mock.call('evm_setAutomine', [False])])
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)
        for item in ('state', 'pool_inventory'):
            path = Path(result[item]['path'])
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), result[item]['sha256'])
        self.assertFalse(result['pending_pool_restored_by_state'])
        self.assertEqual(result['status'], 'saved_mined_history_restore_unverified')
        old = (target / 'state.json').read_bytes()
        with self.assertRaises(FileExistsError): campaign.preserve_anvil_custody(node, target)
        self.assertEqual((target / 'state.json').read_bytes(), old)

    def test_oversized_gzip_or_expired_budget_never_publishes_state(self):
        target = self.root / 'oversized'
        with self.assertRaisesRegex(RuntimeError, 'exceeds bound'):
            campaign.preserve_anvil_custody(self.node(b'x' * 65), target, max_state_bytes=64)
        self.assertFalse((target / 'state.json').exists())
        self.assertFalse(list(target.glob('.anvil-custody-*')))
        node = self.node()
        with self.assertRaises(TimeoutError):
            campaign.preserve_anvil_custody(node, self.root / 'expired', seconds=-1)
        node.call.assert_not_called()

    def test_actual_floor_stops_snapshot_before_rpc(self):
        node = self.node()
        low = SimpleNamespace(f_blocks=100, f_frsize=campaign.GIB, f_bavail=1)
        with mock.patch.object(campaign.os, 'statvfs', return_value=low):
            with self.assertRaisesRegex(RuntimeError, 'disk reserve'):
                campaign.preserve_anvil_custody(node, self.root / 'low')
        node.call.assert_not_called()

    def test_interrupted_run_stops_engine_before_snapshot_and_keeps_failed_verdict(self):
        fixture = infrastructure_fixture.Infrastructure()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.make_journal()
        c = fixture.c
        c.profiles = {'owned': {'family': 'evm', 'chain_id': 31337}}
        c.nodes = {'owned': self.node()}
        c.responses = {'owned': {202: 3}}
        c.proxies = {}
        engine = mock.Mock(); engine.poll.return_value = None
        node_process = mock.Mock(); node_process.poll.return_value = None
        c.children = {'engine': engine, 'owned': node_process}
        c.setup = mock.Mock()
        c.load = lambda: c.stop_for_infrastructure({'reason': 'resource_reserve_insufficient'})
        c.reconcile = mock.Mock(side_effect=AssertionError('Do not promote stopped run'))
        order = []
        def stop(process, **_):
            order.append('engine_stop' if process is engine else 'node_stop')
            process.poll.return_value = 0
        def snapshot(*args):
            self.assertEqual(engine.poll(), 0)
            self.assertIsNone(node_process.poll())
            order.append('snapshot')
            return {'status': 'saved_mined_history_restore_unverified'}
        http = mock.Mock(); http.snapshot.return_value = {}
        with mock.patch.object(campaign, 'stop', side_effect=stop), \
             mock.patch.object(campaign, 'preserve_anvil_custody', side_effect=snapshot), \
             mock.patch.object(campaign, 'redis_command', return_value=0), \
             mock.patch.object(campaign, 'LOCAL_HTTP', http), mock.patch('builtins.print'):
            c.run()
        self.assertLess(order.index('engine_stop'), order.index('snapshot'))
        self.assertLess(order.index('snapshot'), order.index('node_stop'))
        self.assertEqual(c.report['outcome'], 'infrastructure_stop')
        self.assertIsNone(c.report['oracle']['safety_pass'])
        self.assertTrue(c.report['operator_review_required'])
        self.assertFalse(c.report['all_chain_capacity_candidate'])
        c.reconcile.assert_not_called()


class UnresolvedCleanup(unittest.TestCase):
    """Exercise ordinary Campaign.run cleanup; no providers or processes."""
    def setUp(self):
        fixture = infrastructure_fixture.Infrastructure()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.c = c = fixture.c
        c.profiles = {
            'owned': {'family': 'evm', 'chain_id': 31337},
            'external': {'family': 'evm', 'chain_id': 412346,
                         'external_url': 'http://127.0.0.1:18547'},
            'solana': {'family': 'solana'}}
        c.nodes = {name: mock.Mock() for name in c.profiles}
        c.proxies = {}
        self.engine, self.node = mock.Mock(), mock.Mock()
        self.engine.poll.return_value = self.node.poll.return_value = None
        c.children = {'engine': self.engine, 'owned': self.node}
        c.setup, c.load = mock.Mock(), mock.Mock()
        self.order = []
        self.oracle = {'outcome': 'incomplete', 'safety_pass': True, 'liveness_pass': False,
                       'safety_failures': [], 'liveness_failures': ['unresolved original intent']}
        self.drain = {'journal_unresolved': 2, 'node_pending': 1}
        self.snapshot_failure = False
        self.stop_failure = False
        self.snapshot_result = {'status': 'saved_mined_history_restore_unverified',
                                'pending_pool_restored_by_state': False, 'automatic_resume': False}

    def run_case(self, outcome='fail_closed_recovery_required'):
        c = self.c
        c.recovery_required = outcome == 'fail_closed_recovery_required'
        c.projection_recovered = outcome == 'recovered_with_quarantine'
        def reconcile():
            self.stop(self.engine)
            c.report.update({'oracle': self.oracle, 'drain': self.drain,
                             'offline_projection_recovered': c.projection_recovered,
                             'all_chain_capacity_candidate': False, 'requested_fault_validated': False})
            self.order.append('reconciled')
        c.reconcile = mock.Mock(side_effect=reconcile)
        http = mock.Mock(); http.snapshot.return_value = {}
        with mock.patch.object(campaign, 'stop', side_effect=self.stop), \
             mock.patch.object(campaign, 'preserve_anvil_custody', side_effect=self.snapshot) as capture, \
             mock.patch.object(campaign, 'LOCAL_HTTP', http), mock.patch('builtins.print'):
            c.run()
        self.assertTrue(c.report['owned_children_stopped'])
        self.assertEqual(c.report['errors'], [])
        self.assertEqual(c.report['outcome'], outcome)
        self.assertIs(c.report['oracle'], self.oracle)
        self.assertIs(c.report['drain'], self.drain)
        self.assertFalse(c.report['requested_fault_validated'])
        self.assertNotIn('operator_review_required', c.report)
        self.assertIsNone(c.infrastructure_stop)
        saved = json.loads(c.args.report.read_text())
        self.assertEqual(saved['oracle'], self.oracle)
        self.assertEqual(saved['drain'], self.drain)
        return capture, saved

    def stop(self, process, **_):
        label = 'engine_stop' if process is self.engine else 'node_stop'
        self.order.append(label)
        if self.stop_failure and label == 'engine_stop' and self.order.count('engine_stop') == 2:
            raise TimeoutError('fixture stop boundary failure')
        process.poll.return_value = 0

    def snapshot(self, node, directory):
        self.assertIs(node, self.c.nodes['owned'])
        self.assertEqual(self.engine.poll(), 0)
        self.assertIsNone(self.node.poll())
        self.assertEqual(directory.name, 'owned-unresolved-state')
        self.order.append('snapshot')
        if self.snapshot_failure:
            raise TimeoutError('fixture snapshot timeout')
        return self.snapshot_result

    def test_fail_closed_retains_history_after_reconciliation_before_node_cleanup(self):
        capture, saved = self.run_case()
        capture.assert_called_once()
        self.assertLess(self.order.index('reconciled'), self.order.index('snapshot'))
        self.assertLess(self.order.index('snapshot'), self.order.index('node_stop'))
        self.assertEqual(saved['unresolved_chain_custody']['journal_unresolved'], 2)
        self.assertEqual(saved['unresolved_chain_custody']['chains'], {'owned': self.snapshot_result})
        self.assertTrue(self.c.recovery_required)
        self.assertFalse(self.c.projection_recovered)
        for name in ('external', 'solana'):
            self.c.nodes[name].call.assert_not_called()

    def test_quarantine_preserves_proven_oracle_and_original_verdict(self):
        capture, saved = self.run_case('recovered_with_quarantine')
        capture.assert_called_once()
        self.assertTrue(saved['offline_projection_recovered'])
        self.assertFalse(saved['unresolved_chain_custody']['automatic_resume'])
        self.assertFalse(self.c.recovery_required)
        self.assertTrue(self.c.projection_recovered)

    def test_settled_normal_outcome_needs_no_snapshot_or_extra_rpc(self):
        self.drain['journal_unresolved'] = 0
        self.oracle.update(outcome='pass', liveness_pass=True, liveness_failures=[])
        capture, saved = self.run_case('pass')
        capture.assert_not_called()
        self.assertNotIn('unresolved_chain_custody', saved)
        for node in self.c.nodes.values(): node.call.assert_not_called()

    def test_ordinary_incomplete_oracle_also_retains_unresolved_history(self):
        capture, saved = self.run_case('incomplete')
        capture.assert_called_once()
        self.assertFalse(saved['operator_recovery_required'])
        self.assertFalse(self.c.projection_recovered)

    def test_missing_engine_process_records_unavailable_boundary_without_snapshot(self):
        del self.c.children['engine']
        capture, saved = self.run_case()
        capture.assert_not_called()
        self.assertEqual(saved['unresolved_chain_custody']['capture_error'], 'RuntimeError')
        self.assertEqual(saved['unresolved_chain_custody']['chains'], {})

    def test_existing_interrupted_custody_is_not_snapshotted_twice(self):
        prior = {'chain_custody': {'owned': self.snapshot_result}}
        self.c.report['custody'] = prior
        capture, saved = self.run_case()
        capture.assert_not_called()
        self.assertIs(self.c.report['custody'], prior)
        self.assertNotIn('unresolved_chain_custody', saved)

    def test_snapshot_failure_remains_visible_without_hiding_oracle_or_cleanup(self):
        self.snapshot_failure = True
        capture, saved = self.run_case('recovered_with_quarantine')
        capture.assert_called_once()
        entry = saved['unresolved_chain_custody']['chains']['owned']
        self.assertEqual(entry['status'], 'snapshot_failed')
        self.assertEqual(entry['error_type'], 'TimeoutError')
        self.assertFalse(entry['automatic_resume'])
        self.assertNotIn('fixture snapshot timeout', json.dumps(saved))

    def test_stop_boundary_failure_never_snapshots_and_cleanup_still_runs(self):
        self.stop_failure = True
        capture, saved = self.run_case()
        capture.assert_not_called()
        self.assertEqual(saved['unresolved_chain_custody']['capture_error'], 'TimeoutError')
        self.assertEqual(saved['unresolved_chain_custody']['chains'], {})
        self.assertEqual(self.order.count('engine_stop'), 3)


@unittest.skipUnless(os.environ.get('ANVIL_BIN'), 'Set ANVIL_BIN for pinned local restore qualification')
class ActualAnvilRestore(unittest.TestCase):
    def run_network(self, network):
        binary = Path(os.environ['ANVIL_BIN']).resolve()
        version = subprocess.check_output([str(binary), '--version'], text=True, timeout=5)
        self.assertIn('1.8.1', version)
        self.assertIn('982849d', version)
        with tempfile.TemporaryDirectory(prefix='anvil-custody-test-') as temporary:
            root = Path(temporary)
            children, streams = [], []
            def start(label, extra):
                port = campaign.port()
                stream = (root / (label + '.log')).open('wb'); streams.append(stream)
                child = subprocess.Popen([str(binary), '--host', '127.0.0.1', '--port', str(port),
                    '--chain-id', '31337', '--network', network, '--silent'] + extra,
                    stdout=stream, stderr=subprocess.STDOUT)
                children.append(child)
                node = campaign.Rpc('http://127.0.0.1:' + str(port))
                campaign.wait_until(lambda: node.call('eth_chainId', []) == '0x7a69', seconds=10)
                return child, node
            try:
                child, node = start('original', [])
                sender = node.call('eth_accounts', [])[0]
                recipient = '0x' + '11' * 20
                receipts, balances = [], []
                for index in range(3):
                    identity = node.call('eth_sendTransaction', [{'from': sender, 'to': recipient,
                        'value': hex(index + 1), 'gas': '0x5208', 'nonce': hex(index)}])
                    campaign.wait_until(lambda: node.call('eth_getTransactionReceipt', [identity]) is not None, seconds=10)
                    receipt = node.call('eth_getTransactionReceipt', [identity])
                    receipts.append(receipt)
                    balances.append(node.call('eth_getBalance', [recipient, receipt['blockNumber']]))
                    node.call('evm_mine', [])
                node.call('evm_setAutomine', [False])
                pending = [node.call('eth_sendTransaction', [{'from': sender, 'to': recipient,
                    'value': '0x1', 'gas': '0x5208', 'nonce': hex(nonce)}]) for nonce in (3, 5)]
                self.assertEqual(node.call('txpool_status', []), {'pending': '0x1', 'queued': '0x1'})
                snapshot = campaign.preserve_anvil_custody(node, root / 'custody')
                campaign.stop(child)
                _, restored = start('restored', ['--no-mining', '--load-state', snapshot['state']['path']])
                self.assertEqual(restored.call('eth_getBlockByNumber', ['0x0', False])['hash'], snapshot['genesis_hash'])
                self.assertEqual(restored.call('eth_getBlockByNumber', ['latest', False])['hash'], snapshot['head']['hash'])
                for before, balance in zip(receipts, balances):
                    after = restored.call('eth_getTransactionReceipt', [before['transactionHash']])
                    self.assertEqual(after, before)  # Full receipt, including hash/status/gas/fees/logs.
                    self.assertEqual(restored.call('eth_getBlockByHash', [before['blockHash'], False])['hash'], before['blockHash'])
                    self.assertEqual(restored.call('eth_getBalance', [recipient, before['blockNumber']]), balance)
                self.assertEqual(restored.call('eth_getBalance', [recipient, 'latest']), '0x6')
                self.assertEqual(restored.call('eth_getTransactionCount', [sender, 'latest']), '0x3')
                self.assertEqual(restored.call('txpool_status', []), {'pending': '0x0', 'queued': '0x0'})
                for identity in pending:
                    self.assertIsNone(restored.call('eth_getTransactionByHash', [identity]))
                pool = json.loads(Path(snapshot['pool_inventory']['path']).read_text())
                retained = [tx['hash'] for category in ('pending', 'queued')
                            for nonces in pool[category].values() for tx in nonces.values()]
                self.assertCountEqual(retained, pending)
                print(json.dumps({'network': network, 'anvil_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                    'restored_receipts': len(receipts), 'pending_not_in_snapshot': len(pending),
                    'head_hash_preserved': True, 'historical_balances_preserved': True}))
            finally:
                for child in reversed(children): campaign.stop(child)
                for stream in streams: stream.close()

    def test_ethereum_history_and_pending_exclusion(self): self.run_network('ethereum')
    def test_optimism_history_and_pending_exclusion(self): self.run_network('optimism')


if __name__ == '__main__': unittest.main()
