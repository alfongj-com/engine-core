import unittest
from unittest.mock import Mock
from capacity_campaign import observe_evm_pool, record_pool_observation, summarize_pool
from capacity_faults import DRAIN_FIELDS, evaluate_campaign

class PoolTests(unittest.TestCase):
    sender='0xabc'
    def tx(self, h, n):return {'hash':'0x'+h*64,'nonce':hex(n)}
    def test_inventory_distinguishes_orphaned_targets_from_later_queued_loss(self):
        pool={'pending':{self.sender:{'31':self.tx('1',31)}},'queued':{self.sender:{'32':self.tx('2',32),'70':self.tx('3',70)}}}
        before, hashes=summarize_pool(pool,self.sender,['0x'+'1'*64])
        after, remaining=summarize_pool({'pending':{},'queued':{}},self.sender,[])
        self.assertEqual(before['signer_transaction_count'],3)
        self.assertEqual(before['orphaned_target_hashes_present'],1)
        self.assertEqual(before['other_signer_hashes_present'],2)
        self.assertEqual((before['signer_nonce_min'],before['signer_nonce_max']),(31,70))
        self.assertEqual(len(hashes-remaining),3)
        self.assertEqual(after['signer_transaction_count'],0)
        self.assertIsNone(after['signer_nonce_min'])
    def test_hash_digest_detects_changed_identity_even_same_count_and_nonce(self):
        a,_=summarize_pool({'pending':{self.sender:{'31':self.tx('1',31)}},'queued':{}},self.sender,[])
        b,_=summarize_pool({'pending':{self.sender:{'31':self.tx('2',31)}},'queued':{}},self.sender,[])
        self.assertEqual(a['signer_nonce_sha256'],b['signer_nonce_sha256'])
        self.assertNotEqual(a['signer_hashes_sha256'],b['signer_hashes_sha256'])
    def test_other_wallet_and_malformed_pool_are_not_miscounted(self):
        a,_=summarize_pool({'pending':{'0xother':{'7':self.tx('1',7)}},'queued':{}},self.sender,[])
        self.assertEqual(a['all_transaction_count'],1)
        self.assertEqual(a['signer_transaction_count'],0)
        with self.assertRaises(ValueError):summarize_pool({'pending':[],'queued':{}},self.sender,[])

    def test_queued_transactions_behind_nonce_gap_fail_empty_drain(self):
        node = Mock()
        node.call.return_value = {'pending': '0x0', 'queued': '0x9a'}
        pool = observe_evm_pool(node, {}, pinned_nonce=32, pending_nonce=32)
        self.assertEqual(pool['nonce_pending_delta'], 0)
        self.assertEqual(pool['queued_count'], 154)
        self.assertEqual(pool['drain_count'], 154)
        self.assertFalse(pool['empty_pool_observed'])
        node.call.assert_called_once_with('txpool_status', [])
        expected = [{'id': 'one', 'chain': 1, 'family': 'evm', 'outcome': 'success',
                     'effects': {}, 'intent_digest': 'intent'}]
        observations = {'one': {'admitted': True,
            'attempts': [{'identity': 'hash', 'wire_digest': 'wire', 'replay_key': 'evm:1:sender:0',
                          'wire_replay_key': 'evm:1:sender:0', 'intent_digest': 'intent'}],
            'executions': [{'identity': 'hash', 'outcome': 'success', 'effects': {},
                            'fee': 1, 'canonical': True, 'finalized': True}],
            'terminal': {'identity': 'hash', 'outcome': 'success'}}}
        drain = dict.fromkeys(DRAIN_FIELDS, 0)
        self.assertTrue(evaluate_campaign(expected, observations, drain)['liveness_pass'])
        report = {'drain': drain}
        record_pool_observation(report, drain, 'local', pool)
        result = evaluate_campaign(expected, observations, drain)
        self.assertTrue(result['safety_pass'])
        self.assertFalse(result['liveness_pass'])
        self.assertIn('drain node_pending: 154', result['liveness_failures'])

    def test_clean_drain_remains_only_numeric_counters(self):
        node = Mock()
        node.call.return_value = {'pending': '0x0', 'queued': '0x0'}
        drain = dict.fromkeys(DRAIN_FIELDS, 0)
        report = {'drain': drain}
        pool = observe_evm_pool(node, {}, 32, 32)
        record_pool_observation(report, drain, 'local', pool)
        self.assertEqual(set(report['drain']), set(DRAIN_FIELDS))
        self.assertTrue(all(type(value) is int and value == 0 for value in report['drain'].values()))
        self.assertEqual(report['node_pool_observations']['local'], pool)

    def test_native_nonce_delta_never_claims_empty_pool_or_calls_txpool(self):
        node = Mock()
        for pending_nonce, delta in ((32, 0), (35, 3)):
            pool = observe_evm_pool(node, {'external_url': 'http://127.0.0.1:18547'}, 32, pending_nonce)
            self.assertEqual(pool['drain_count'], delta)
            self.assertFalse(pool['complete_pool_observation'])
            self.assertIsNone(pool['total_pool_count'])
            self.assertIsNone(pool['empty_pool_observed'])
            self.assertIn('zero does not prove', pool['limitation'])
        node.call.assert_not_called()

    def test_local_pool_count_requires_both_valid_counters(self):
        node = Mock()
        for malformed in ({'pending': '0x0'}, {'pending': 0, 'queued': True},
                          {'pending': 0, 'queued': -1}, {'pending': 0, 'queued': 'invalid'}):
            node.call.return_value = malformed
            with self.assertRaises(ValueError): observe_evm_pool(node, {}, 0, 0)
        node.call.return_value = {'pending': '0x2', 'queued': '0x3'}
        self.assertEqual(observe_evm_pool(node, {}, 0, 2)['drain_count'], 5)
        node.call.return_value = {'pending': '0x0', 'queued': '0x0'}
        self.assertTrue(observe_evm_pool(node, {}, 0, 0)['empty_pool_observed'])

if __name__=='__main__':unittest.main()
