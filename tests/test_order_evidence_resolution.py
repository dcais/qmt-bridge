import copy
import unittest

from order_bridge.common import new_order_document
from order_bridge.state import apply_observation, mark_reconciled, recompute_order


ACCOUNT = 'test-account'
NATIVE_REF = 123456789
NATIVE_ORDER_REF = '1234567890123456789'


def document():
    return new_order_document({'client_order_id': 'case', 'account_id': ACCOUNT,
                               'order_type': 'SINGLE', 'symbol': '511880.SH',
                               'side': 'BUY', 'quantity': 100,
                               'execution': {'type': 'DIRECT'}})


def order(oid='', status=50, filled=0, **changes):
    raw = {'m_strOrderSysID': oid, 'm_nOrderStatus': status,
           'm_nVolumeTotalOriginal': 100, 'm_nVolumeTraded': filled,
           'm_strInstrumentID': '511880', 'm_nOffsetFlag': 48,
           'm_strExchangeID': 'SH', 'm_strInsertDate': '20260926',
           'm_strAccountID': ACCOUNT, 'm_nTaskId': 1,
           'm_strRemark': 'test-remark', 'm_nRef': NATIVE_REF,
           'm_strOrderRef': NATIVE_ORDER_REF}
    raw.update(changes)
    return raw


def observe(doc, kind, raw):
    apply_observation(doc, kind, raw, 'callback', '2026-09-26T14:28:57+00:00')


def stopped_task(doc):
    observe(doc, 'task', {'m_nTaskId': 1, 'm_eStatus': 10,
                          'm_strAccountID': ACCOUNT, 'm_stockCode': '511880.SH',
                          'm_strRemark': 'test-remark', 'm_eOperationType': 18})


class EvidenceResolutionTests(unittest.TestCase):
    def test_real_callback_sequence_requires_completed_round(self):
        doc = document()
        observe(doc, 'order', order())
        self.assertEqual(len(doc['unassociated_evidence']), 1)
        observe(doc, 'order', order('xt-test', 57))
        stopped_task(doc)
        self.assertEqual(doc['unassociated_evidence'], [])
        self.assertFalse(doc['reconciliation_complete'])
        self.assertTrue(doc['reconcile_pending'])
        self.assertEqual(doc['resolved_evidence'][0]['raw']['m_nOrderStatus'], 50)
        self.assertEqual(doc['resolved_evidence'][0]['qmt_order_id'], 'xt-test')
        self.assertEqual(doc['resolved_evidence'][0]['reason'], 'MISSING_QMT_ID')
        mark_reconciled(doc, True)
        self.assertEqual(doc['execution_status'], 'REJECTED')
        self.assertEqual(doc['sync_status'], 'COMPLETE')
        self.assertFalse(doc['reconcile_pending'])

    def test_reverse_order_restart_and_duplicate_missing_report(self):
        doc = document()
        observe(doc, 'order', order('xt-test', 57))
        stopped_task(doc)
        mark_reconciled(doc, True)
        observe(doc, 'order', order())
        self.assertEqual(len(doc['resolved_evidence']), 1)
        self.assertEqual(doc['unassociated_evidence'], [])
        persisted = copy.deepcopy(doc)
        recompute_order(persisted)
        observe(persisted, 'order', order())
        self.assertEqual(len(persisted['resolved_evidence']), 1)
        self.assertEqual(persisted['unassociated_evidence'], [])
        mark_reconciled(persisted, True)
        self.assertEqual(persisted['execution_status'], 'REJECTED')

    def test_legacy_document_recovers_without_new_observation(self):
        doc = document()
        observe(doc, 'order', order('xt-test', 57))
        stopped_task(doc)
        doc['unassociated_evidence'] = [{'evidence_id': 'old', 'kind': 'order',
                                          'raw': order(), 'source': 'callback',
                                          'reason': 'MISSING_QMT_ID',
                                          'observed_at': '2026-09-26T14:28:57+00:00'}]
        mark_reconciled(doc, True)
        self.assertEqual(doc['unassociated_evidence'], [])
        self.assertEqual(doc['resolved_evidence'][0]['evidence_id'], 'old')
        self.assertEqual(doc['execution_status'], 'REJECTED')
        self.assertFalse(doc['reconcile_pending'])

    def test_scope_and_reference_conflicts_remain_pending(self):
        changes = ({'m_strAccountID': 'other'}, {'m_strInsertDate': '20260925'},
                   {'m_strExchangeID': 'SZ'}, {'m_nRef': 2},
                   {'m_strOrderRef': 'other'}, {'m_nRef': None, 'm_strOrderRef': ''},
                   {'m_strAccountID': ''}, {'m_strInsertDate': ''},
                   {'m_strInstrumentID': 'other'}, {'m_nOffsetFlag': 49},
                   {'m_strRemark': 'other'}, {'m_nTaskId': 2})
        for change in changes:
            with self.subTest(change=change):
                doc = document()
                observe(doc, 'order', order('xt-test', 57))
                observe(doc, 'order', order(**change))
                mark_reconciled(doc, True)
                self.assertEqual(len(doc['unassociated_evidence']), 1)
                self.assertEqual(doc['sync_status'], 'INCOMPLETE')

    def test_ambiguous_candidates_and_other_evidence_stay_active(self):
        doc = document()
        observe(doc, 'order', order('first', 57))
        observe(doc, 'order', order('second', 57))
        observe(doc, 'order', order())
        observe(doc, 'task', {'m_eStatus': 10})
        mark_reconciled(doc, True)
        self.assertEqual(len(doc['unassociated_evidence']), 2)
        self.assertEqual(doc.get('resolved_evidence', []), [])
        self.assertEqual(doc['sync_status'], 'INCOMPLETE')

    def test_earlier_cumulative_fill_cannot_disappear(self):
        doc = document()
        observe(doc, 'order', order(filled=40))
        observe(doc, 'order', order('xt-test', 57, filled=0))
        stopped_task(doc)
        mark_reconciled(doc, True)
        if doc['unassociated_evidence']:
            self.assertEqual(doc['sync_status'], 'INCOMPLETE')
        else:
            self.assertGreaterEqual(doc['qmt_orders'][0]['filled_quantity'], 40)
            self.assertEqual(doc['sync_status'], 'INCOMPLETE')
        self.assertTrue(doc['reconcile_pending'])

    def test_later_query_enriches_identified_order_with_native_references(self):
        doc = document()
        observe(doc, 'order', order())
        observe(doc, 'order', order('xt-test', 57, m_nRef=None, m_strOrderRef=''))
        self.assertEqual(len(doc['unassociated_evidence']), 1)
        # 状态和数量不变，新增的身份字段仍须进入规范事实。
        observe(doc, 'order', order('xt-test', 57))
        self.assertEqual(doc['unassociated_evidence'], [])
        self.assertEqual(doc['qmt_orders'][0]['native_order_ref'], NATIVE_ORDER_REF)
        self.assertEqual(doc['qmt_orders'][0]['raw']['m_strOrderRef'], NATIVE_ORDER_REF)

    def test_known_order_reference_conflict_blocks_completion(self):
        doc = document()
        observe(doc, 'order', order('xt-test', 57))
        observe(doc, 'order', order('xt-test', 57, m_strOrderRef='other'))
        stopped_task(doc)
        mark_reconciled(doc, True)
        self.assertEqual(doc['qmt_orders'][0]['native_order_ref'], NATIVE_ORDER_REF)
        self.assertEqual(doc['unassociated_evidence'][0]['reason'], 'CONFLICTING_ORDER_REFERENCE')
        self.assertEqual(doc['sync_status'], 'INCOMPLETE')

    def test_known_order_requires_explicit_scope(self):
        for key in ('m_strAccountID', 'm_strInsertDate', 'm_strExchangeID'):
            with self.subTest(key=key):
                doc = document()
                observe(doc, 'order', order('xt-test', 57, **{key: ''}))
                observe(doc, 'order', order())
                mark_reconciled(doc, True)
                self.assertIn('MISSING_QMT_ID', [row['reason'] for row in doc['unassociated_evidence']])
                self.assertEqual(doc.get('resolved_evidence', []), [])

    def test_resolved_identity_does_not_close_running_task_or_order(self):
        for known_status, task_status in ((57, 3), (50, 10)):
            with self.subTest(known_status=known_status, task_status=task_status):
                doc = document()
                observe(doc, 'order', order())
                observe(doc, 'order', order('xt-test', known_status))
                observe(doc, 'task', {'m_nTaskId': 1, 'm_eStatus': task_status})
                mark_reconciled(doc, True)
                self.assertEqual(doc['unassociated_evidence'], [])
                self.assertTrue(doc['reconcile_pending'])


if __name__ == '__main__':
    unittest.main()
