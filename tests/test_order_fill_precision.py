"""Offline fill precision regressions; no broker or database access."""
import copy
import importlib
import json
import unittest
from decimal import Decimal

from test_order_state import document, deal, order


class FillPrecisionTests(unittest.TestCase):
    def modules(self):
        return [importlib.import_module(name + '.state')
                for name in ('service.ipc_bridge.order', 'order_bridge')]

    def samples(self):
        doc = document()
        doc['items'][0]['quantity'] = 217600
        pairs = []
        for tid, qty, amount, rounded, fee, query_fee in (
                ('fill-a', 164548, '306552.924', '306552.92', 91.9658772, 91.96587599999998),
                ('fill-b', 53052, '98835.876', '98835.88', 29.6507628, 29.650763999999995)):
            raw = dict(deal(tid, qty, amount), m_dPrice=1.863, m_strAccountID=doc['account_id'],
                       m_strRemark=doc['remark'], m_nTaskId=1, m_dCommission=fee,
                       m_dComssion=fee, m_nOrderStrategyType=-2138807008)
            pairs.append((raw, dict(raw, m_dTradeAmount=rounded, m_dCommission=query_fee,
                                   m_dComssion=query_fee, m_nOrderStrategyType=-1621863200)))
        return doc, pairs

    def close(self, state, query_first=False):
        doc, pairs = self.samples()
        raw_order = dict(order(status=56, filled=217600, quantity=217600),
                         m_strAccountID=doc['account_id'], m_strRemark=doc['remark'])
        state.apply_observation(doc, 'order', raw_order, 'callback')
        for callback, query in pairs:
            state.apply_observation(doc, 'deal', query if query_first else callback,
                                    'query' if query_first else 'callback')
        state.mark_reconciled(doc, now='2026-09-25T07:00:00Z')
        self.assertTrue(state.state_execution_finished(doc))
        self.assertFalse(state.reconcile_pending(doc))
        return doc, pairs

    def test_real_pairs_in_both_arrival_orders_preserve_closed_document_274_rounds(self):
        for state in self.modules():
            for query_first in (False, True):
                doc, pairs = self.close(state, query_first)
                before = copy.deepcopy(doc)
                for index in range(274):
                    for callback, query in pairs:
                        raw = dict(callback if query_first else query, m_nOrderStrategyType=index)
                        self.assertFalse(state.apply_observation(
                            doc, 'deal', raw, 'callback' if query_first else 'query'))
                self.assertEqual(doc, before)
                self.assertEqual(len(doc['fills']), 2)
                self.assertEqual(sum(row['quantity'] for row in doc['fills']), 217600)
                self.assertEqual(sum(Decimal(row['amount']) for row in doc['fills']), Decimal('405388.800'))

    def test_commission_changes_missing_aliases_and_raw_values_never_gate_fill(self):
        for state in self.modules():
            for query_first in (False, True):
                closed, pairs = self.close(state, query_first)
                callback, query = pairs[0]
                base = callback if query_first else query
                variants = [dict(base, m_dCommission=999, m_dComssion=-10, m_dComission='unavailable')]
                variants.append({key: value for key, value in base.items()
                                 if key not in ('m_dCommission', 'm_dComssion')})
                variants.append(dict(variants[-1], m_dComission=None))
                variants.append(dict(base, m_dTradeAmount=closed['fills'][0]['amount'],
                                     m_dCommission=0, m_dComssion='changed'))
                for raw in variants:
                    doc = copy.deepcopy(closed)
                    self.assertFalse(state.apply_observation(
                        doc, 'deal', raw, 'callback' if query_first else 'query'))
                    self.assertEqual(doc, closed)
                    self.assertEqual(doc['fills'][0]['raw'], query if query_first else callback)

    def test_economics_identity_and_unknown_fields_stay_conflicts(self):
        mutations = ({'m_nVolume': 164547}, {'m_dPrice': 1.864}, {'m_dTradeAmount': '306552.93'},
                     {'m_strOrderSysID': 'other'}, {'m_nTaskId': 2}, {'m_strAccountID': 'other'},
                     {'m_strRemark': 'other'}, {'unknown_business_field': 1}, {'commission': 99})
        for state in self.modules():
            for change in mutations:
                with self.subTest(module=state.__name__, change=change):
                    doc, pairs = self.samples()
                    callback, query = pairs[0]
                    state.apply_observation(doc, 'deal', callback, 'callback')
                    before = copy.deepcopy(doc['fills'])
                    self.assertTrue(state.apply_observation(doc, 'deal', dict(query, **change), 'query'))
                    self.assertEqual(doc['fills'], before)
                    self.assertTrue(doc['unassociated_evidence'])
                    self.assertTrue(state.reconcile_pending(doc))

    def test_price_identity_and_unknown_fields_checked_without_amount_difference(self):
        for state in self.modules():
            for key in ('m_dPrice', 'm_strAccountID', 'm_strRemark', 'm_nTaskId'):
                doc, pairs = self.samples()
                callback, unused = pairs[0]
                state.apply_observation(doc, 'deal', callback, 'callback')
                raw = dict(callback)
                raw.pop(key)
                self.assertTrue(state.apply_observation(doc, 'deal', raw, 'query'))
                self.assertTrue(doc['unassociated_evidence'])
            for change in ({'m_dPrice': 1.864}, {'unknown_business_field': 1}, {'commission': 0}):
                doc, pairs = self.samples()
                callback, unused = pairs[0]
                state.apply_observation(doc, 'deal', callback, 'callback')
                self.assertTrue(state.apply_observation(doc, 'deal', dict(callback, **change), 'query'))

    def test_cent_rule_requires_complete_scope_stock_source_price_and_cent_query(self):
        for state in self.modules():
            for mode in ('same_source', 'missing_account', 'missing_remark', 'missing_price',
                         'wrong_product', 'missing_product', 'non_cent', 'wrong_product_value', 'wrong_market'):
                with self.subTest(module=state.__name__, mode=mode):
                    doc, pairs = self.samples()
                    callback, query = pairs[0]
                    if mode.startswith('missing_') and mode != 'missing_product':
                        key = {'missing_account': 'm_strAccountID', 'missing_remark': 'm_strRemark',
                               'missing_price': 'm_dPrice'}[mode]
                        callback.pop(key)
                        query.pop(key)
                    if mode == 'wrong_product':
                        doc['account_type'] = 'FUTURE'
                    if mode == 'missing_product':
                        doc.pop('account_type')
                    if mode == 'non_cent':
                        query['m_dTradeAmount'] = '306552.921'
                    if mode == 'wrong_product_value':
                        callback['m_dTradeAmount'] = '306552.923'
                    if mode == 'wrong_market':
                        callback['m_strExchangeID'] = query['m_strExchangeID'] = 'HK'
                        doc['items'][0]['symbol'] = '510300.HK'
                    state.apply_observation(doc, 'deal', callback, 'callback')
                    self.assertTrue(state.apply_observation(
                        doc, 'deal', query, 'callback' if mode == 'same_source' else 'query'))
                    self.assertTrue(doc['unassociated_evidence'])

    def test_cent_rule_accepts_sh_and_sz_half_up_ties_and_float_product_noise(self):
        for state in self.modules():
            for market in ('SH', 'SZ'):
                doc = document()
                doc['items'][0]['symbol'] = '510300.' + market
                callback = dict(deal(quantity=1, amount='1.2350000000000001'),
                                m_strExchangeID=market, m_dPrice=1.235,
                                m_strAccountID=doc['account_id'], m_strRemark=doc['remark'])
                state.apply_observation(doc, 'deal', callback, 'callback')
                before = copy.deepcopy(doc)
                self.assertFalse(state.apply_observation(
                    doc, 'deal', dict(callback, m_dTradeAmount='1.24'), 'query'))
                self.assertEqual(doc, before)

    def test_ambiguous_fill_identity_and_duplicate_rows_do_not_use_cent_exception(self):
        for state in self.modules():
            doc, pairs = self.samples()
            callback, query = pairs[0]
            state.apply_observation(doc, 'deal', callback, 'callback')
            doc['fills'].append(copy.deepcopy(doc['fills'][0]))
            self.assertTrue(state.apply_observation(doc, 'deal', query, 'query'))
            self.assertTrue(doc['unassociated_evidence'])

    def test_repeated_conflicts_preserve_evidence_id_raw_latch_and_entire_document(self):
        for state in self.modules():
            doc, pairs = self.samples()
            callback, query = pairs[0]
            state.apply_observation(doc, 'deal', callback, 'callback')
            query = dict(query, m_dTradeAmount='306552.93')
            self.assertTrue(state.apply_observation(doc, 'deal', query, 'query'))
            evidence = copy.deepcopy(doc['unassociated_evidence'])
            state.mark_reconciled(doc, now='2026-09-25T07:00:00Z')
            self.assertTrue(doc['reconciliation_complete'])
            before = copy.deepcopy(doc)
            for i in range(274):
                self.assertFalse(state.apply_observation(doc, 'deal', dict(query, m_nOrderStrategyType=i), 'query'))
            self.assertEqual(doc, before)
            self.assertEqual(doc['unassociated_evidence'], evidence)
            self.assertTrue(state.reconcile_pending(doc))
            self.assertEqual(doc['sync_status'], 'INCOMPLETE')

    def test_repeated_amount_conflict_with_commission_changes_preserves_evidence_and_latch(self):
        for state in self.modules():
            doc, pairs = self.samples()
            callback, query = pairs[0]
            state.apply_observation(doc, 'deal', callback, 'callback')
            query = dict(query, m_dTradeAmount='306552.93')
            self.assertTrue(state.apply_observation(doc, 'deal', query, 'query'))
            evidence = copy.deepcopy(doc['unassociated_evidence'])
            state.mark_reconciled(doc, now='2026-09-25T07:00:00Z')
            self.assertTrue(doc['reconciliation_complete'])
            before = copy.deepcopy(doc)
            for i in range(274):
                raw = dict(query, m_nOrderStrategyType=i, m_dCommission=i * 0.01,
                           m_dComssion=-i, m_dComission='value-' + str(i))
                if i % 2:
                    raw.pop('m_dCommission')
                if i % 3:
                    raw.pop('m_dComssion')
                self.assertFalse(state.apply_observation(doc, 'deal', raw, 'query'))
            self.assertEqual(doc, before)
            self.assertEqual(doc['unassociated_evidence'], evidence)
            self.assertTrue(doc['reconciliation_complete'])
            self.assertTrue(state.reconcile_pending(doc))
            self.assertEqual(doc['sync_status'], 'INCOMPLETE')

    def test_conflict_grouping_does_not_ignore_other_fields_source_or_reason(self):
        for state in self.modules():
            doc, pairs = self.samples()
            callback, query = pairs[0]
            state.apply_observation(doc, 'deal', callback, 'callback')
            query = dict(query, m_dTradeAmount='306552.93')
            self.assertTrue(state.apply_observation(doc, 'deal', query, 'query'))
            self.assertFalse(state.apply_observation(doc, 'deal', dict(query, m_dCommission=2), 'query'))
            self.assertEqual(len(doc['unassociated_evidence']), 1)
            variants = ((dict(query, m_nVolume=164547), 'query'),
                        (dict(query, m_dPrice=1.864), 'query'),
                        (dict(query, m_dTradeAmount='306552.94'), 'query'),
                        (dict(query, m_strOrderSysID='other'), 'query'),
                        (dict(query, unknown=1), 'query'),
                        (dict(query, commission=2), 'query'),
                        (dict(query, m_nOrderStrategyType=99), 'callback'))
            for raw, source in variants:
                self.assertTrue(state.apply_observation(doc, 'deal', raw, source))
            self.assertEqual(len(doc['unassociated_evidence']), 1 + len(variants))
            state.state_evidence(doc, 'deal', query, 'query', 'OTHER_REASON', '2026-09-25T07:00:00Z')
            self.assertEqual(len(doc['unassociated_evidence']), 2 + len(variants))

    def test_historical_quarantine_is_not_auto_resolved_even_after_json_restart(self):
        for state in self.modules():
            doc, pairs = self.samples()
            callback, query = pairs[0]
            state.apply_observation(doc, 'deal', callback, 'callback')
            state.state_evidence(doc, 'deal', query, 'query', 'CONFLICTING_FILL_IDENTITY', '2026-09-25T07:00:00Z')
            state.recompute_order(doc)
            for restart in (False, True):
                if restart:
                    doc = json.loads(json.dumps(doc))
                    state.restore_terminal_reconciliation(doc)
                    state.recompute_order(doc)
                before = copy.deepcopy(doc)
                self.assertFalse(state.apply_observation(doc, 'deal', query, 'query'))
                self.assertEqual(doc, before)
                self.assertTrue(state.reconcile_pending(doc))

    def test_resolved_evidence_is_not_semantically_grouped_or_changed(self):
        for state in self.modules():
            doc, pairs = self.samples()
            callback, query = pairs[0]
            state.apply_observation(doc, 'deal', callback, 'callback')
            query = dict(query, m_dTradeAmount='306552.93')
            state.apply_observation(doc, 'deal', query, 'query')
            doc['resolved_evidence'] = doc.pop('unassociated_evidence')
            resolved = copy.deepcopy(doc['resolved_evidence'])
            for change in ({'m_nOrderStrategyType': 42}, {'m_dCommission': 2},
                           {'m_dComssion': 3}, {'m_dComission': 4}):
                restored = copy.deepcopy(doc)
                self.assertTrue(state.apply_observation(restored, 'deal', dict(query, **change), 'query'))
                self.assertEqual(restored['resolved_evidence'], resolved)
                self.assertEqual(len(restored['unassociated_evidence']), 1)

    def test_closed_document_can_restore_unchanged_checkpoint_after_json_restart(self):
        for state in self.modules():
            doc, pairs = self.close(state)
            restored = json.loads(json.dumps(doc))
            self.assertTrue(state.restore_terminal_reconciliation(restored))
            before = copy.deepcopy(restored)
            for unused, query in pairs:
                self.assertFalse(state.apply_observation(restored, 'deal', query, 'query'))
            self.assertEqual(restored, before)
            self.assertFalse(state.reconcile_pending(restored))


if __name__ == '__main__':
    unittest.main()
