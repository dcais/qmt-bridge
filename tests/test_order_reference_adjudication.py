"""Offline regressions for reviewed duplicate references and quarantine of new conflicts."""
import copy
import importlib
import unittest

from test_order_state import document, order, deal


class ReferenceAdjudicationTests(unittest.TestCase):
    def states(self):
        for base in ('service.ipc_bridge.order', 'order_bridge'):
            yield importlib.import_module(base + '.state')

    def reviewed(self, state):
        doc = document()
        original = dict(order(status=56, filled=100), m_nRef=12,
                        m_strOrderRef='native-12', m_strAccountID=doc['account_id'],
                        m_strRemark=doc['remark'], m_nTaskId=1,
                        m_dTradeAmount=1000, m_dTradedPrice=10, m_nOrderStrategyType=1)
        state.apply_observation(doc, 'order', original, 'callback')
        state.apply_observation(doc, 'deal', deal(quantity=100, amount='1000'), 'callback')
        state.mark_reconciled(doc, now='2026-09-25T04:00:00Z')
        # Keep this fixture outside automatic market_sysid recognition: it tests manual adjudication.
        alias = dict(original, m_nRef=0, m_strOrderRef='reviewed-o1', m_nOrderStrategyType=2)
        state.apply_observation(doc, 'order', alias, 'query')
        evidence = copy.deepcopy(doc['unassociated_evidence'][0])
        evidence['resolution'] = dict(kind='MANUAL_NATIVE_REFERENCE_BINDING',
            order_id=doc['order_id'], decision_id='reviewed-test-decision', evidence_sha256='a' * 64)
        doc.setdefault('resolved_evidence', []).append(evidence)
        doc['unassociated_evidence'] = []
        state.mark_reconciled(doc, now='2026-09-25T04:00:00Z')
        self.assertTrue(state.state_execution_finished(doc))
        return doc, alias

    def test_reviewed_query_duplicate_is_idempotent_even_with_changed_ancillary_value(self):
        for state in self.states():
            closed, raw = self.reviewed(state)
            for repeat in (raw, dict(raw, m_nOrderStrategyType=3)):
                with self.subTest(module=state.__name__, value=repeat['m_nOrderStrategyType']):
                    doc = copy.deepcopy(closed)
                    self.assertFalse(state.apply_observation(doc, 'order', repeat, 'query'))
                    self.assertEqual(doc, closed)

    def test_unreviewed_wrong_order_or_callback_is_not_suppressed(self):
        for state in self.states():
            closed, raw = self.reviewed(state)
            for invalid in ('unreviewed', 'wrong_order', 'callback'):
                with self.subTest(module=state.__name__, invalid=invalid):
                    doc = copy.deepcopy(closed)
                    resolution = doc['resolved_evidence'][-1]['resolution']
                    if invalid == 'unreviewed':
                        resolution['kind'] = 'UNAPPROVED'
                    elif invalid == 'wrong_order':
                        resolution['order_id'] = 'other-order'
                    before_orders = copy.deepcopy(doc['qmt_orders'])
                    state.apply_observation(doc, 'order', dict(raw, m_nOrderStrategyType=3),
                                            'callback' if invalid == 'callback' else 'query')
                    self.assertTrue(doc['reconcile_pending'])
                    self.assertTrue(doc['unassociated_evidence'])
                    self.assertEqual(doc['qmt_orders'], before_orders)
                    self.assertEqual(doc['fills'], closed['fills'])

    def test_new_identity_economics_or_status_reopens_without_overwriting_existing_facts(self):
        for state in self.states():
            closed, raw = self.reviewed(state)
            mutations = ({'m_nVolumeTotalOriginal': 200, 'm_nVolumeTraded': 125},
                {'m_dTradeAmount': 2000}, {'m_dTradedPrice': 11}, {'m_nOrderStatus': 54},
                {'m_nRef': 13}, {'m_strOrderRef': 'OTHER_o1'}, {'m_nTaskId': 2},
                {'m_strAccountID': 'other-account'}, {'m_strRemark': 'other-remark'},
                {'m_strInstrumentID': '600000'}, {'m_nOffsetFlag': 49},
                {'m_strInsertDate': '20260926'}, {'m_strExchangeID': 'SZ'},
                {'m_strOrderSysID': 'o2'})
            for mutation in mutations:
                with self.subTest(module=state.__name__, mutation=mutation):
                    doc = copy.deepcopy(closed)
                    changed = dict(raw, m_nOrderStrategyType=3, **mutation)
                    self.assertTrue(state.apply_observation(doc, 'order', changed, 'query'))
                    self.assertEqual(doc['sync_status'], 'INCOMPLETE')
                    self.assertTrue(doc['reconcile_pending'])
                    self.assertEqual(doc['fills'], closed['fills'])
                    self.assertTrue(all(row in doc['qmt_orders'] for row in closed['qmt_orders']))

    def test_callback_derived_alias_with_float_traded_price_noise_is_equivalent(self):
        for state in self.states():
            doc = document()
            original = dict(order(status=56, filled=100), m_nRef=1090551164,
                            m_strOrderRef='9215126258110137097', m_strAccountID=doc['account_id'],
                            m_strRemark=doc['remark'], m_nTaskId=17,
                            m_strInsertTime='131052', m_dTradeAmount=20200.8,
                            m_dTradedPrice=101.00399999999999, m_nOrderStrategyType=555157360)
            state.apply_observation(doc, 'order', original, 'callback')
            state.apply_observation(doc, 'deal', dict(deal(quantity=100, amount='20200.8'),
                                                      m_nRef=1090551164, m_strOrderRef='9215126258110137097',
                                                      m_strAccountID=doc['account_id'], m_strRemark=doc['remark'],
                                                      m_nTaskId=17, m_dPrice=101.004,
                                                      m_nOrderStrategyType=555157360), 'callback')
            state.mark_reconciled(doc, now='2026-11-26T05:12:00Z')
            closed = copy.deepcopy(doc)
            callback_alias = dict(original, m_nRef=0, m_strOrderRef='SH_o1',
                                  m_strInsertTime='131118', m_dTradedPrice=101.004)
            query_alias = dict(callback_alias, m_nOrderStrategyType=1056947728)
            for source, payload in (('callback', callback_alias), ('query', query_alias)):
                with self.subTest(module=state.__name__, source=source):
                    candidate = copy.deepcopy(closed)
                    self.assertFalse(state.apply_observation(candidate, 'order', payload, source))
                    self.assertEqual(candidate, closed)
            # A real traded price change or non-alias reference still quarantines.
            for bad_change in ({'m_dTradedPrice': 101.01}, {'m_strOrderRef': 'SZ_o1'}):
                candidate = copy.deepcopy(closed)
                self.assertTrue(state.apply_observation(
                    candidate, 'order', dict(callback_alias, **bad_change), 'callback'))
                self.assertTrue(candidate['unassociated_evidence'])


if __name__ == '__main__':
    unittest.main()
