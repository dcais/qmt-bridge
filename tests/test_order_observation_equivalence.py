"""Offline observation equivalence regressions; Last modified: 2026-10-14."""
import copy
import importlib
import unittest

from test_order_state import document, order, deal


class ObservationEquivalenceTests(unittest.TestCase):
    def states(self):
        for base in ('service.ipc_bridge.order', 'order_bridge'):
            yield importlib.import_module(base + '.state')

    def snapshots(self):
        doc = document(algo=True)
        original = dict(order(oid='11127', status=56, filled=100),
                        m_nRef=1098925585, m_strOrderRef='9215126258110529789',
                        m_strAccountID=doc['account_id'], m_strRemark=doc['remark'], m_nTaskId=1,
                        m_dTradeAmount=847.0000000000001, m_dTradedPrice=8.47,
                        m_nOrderStrategyType=-2, m_strInsertTime='144629')
        alias = dict(original, m_nRef=0, m_strOrderRef='SH_11127',
                     m_nOrderStrategyType=-1538491512, m_strInsertTime='144640')
        fill = dict(deal(tid='50043060', quantity=100, amount='847.0000000000001', oid='11127'),
                    m_strAccountID=doc['account_id'], m_strRemark=doc['remark'], m_nTaskId=1)
        return doc, original, alias, fill

    def closed(self, state):
        doc, original, alias, fill = self.snapshots()
        state.apply_observation(doc, 'task', {'m_nTaskId': 1, 'm_eStatus': 7}, 'callback')
        state.apply_observation(doc, 'order', original, 'callback')
        state.apply_observation(doc, 'deal', fill, 'callback')
        state.mark_reconciled(doc, now='2026-09-25T07:00:00Z')
        self.assertTrue(state.state_execution_finished(doc))
        return doc, original, alias, fill

    def test_callback_then_equivalent_queries_preserve_entire_closed_document(self):
        for state in self.states():
            closed, original, alias, fill = self.closed(state)
            for native_ref in (None, '', 0, '0'):
                with self.subTest(module=state.__name__, native_ref=native_ref):
                    doc = copy.deepcopy(closed)
                    query = dict(alias, m_nRef=native_ref)
                    for unused in range(2):
                        self.assertFalse(state.apply_observation(doc, 'order', query, 'query'))
                        self.assertFalse(state.apply_observation(doc, 'deal', dict(fill, m_dTradeAmount=847.0), 'query'))
                    self.assertEqual(doc, closed)
                    self.assertEqual(doc['qmt_orders'][0]['raw'], original)
                    self.assertEqual(doc['fills'][0]['amount'], '847.0000000000001')
            doc = copy.deepcopy(closed)
            without_ref = dict(alias)
            without_ref.pop('m_nRef')
            self.assertFalse(state.apply_observation(doc, 'order', without_ref, 'query'))
            self.assertEqual(doc, closed)

    def test_query_then_callback_promotes_real_pair_without_counting_fills_twice(self):
        for state in self.states():
            doc, original, alias, fill = self.snapshots()
            state.apply_observation(doc, 'task', {'m_nTaskId': 1, 'm_eStatus': 7}, 'callback')
            state.apply_observation(doc, 'order', alias, 'query')
            row = doc['qmt_orders'][0]
            self.assertEqual(state.state_native_order_references(row), (None, None))
            self.assertNotIn('native_order_ref', row)
            state.apply_observation(doc, 'deal', dict(fill, m_dTradeAmount=847.0), 'query')
            state.mark_reconciled(doc)
            self.assertTrue(state.apply_observation(doc, 'order', original, 'callback'))
            self.assertFalse(doc['reconciliation_complete'])
            self.assertEqual(state.state_native_order_references(row), ('1098925585', '9215126258110529789'))
            self.assertEqual(row['raw'], original)
            self.assertFalse(state.apply_observation(doc, 'deal', fill, 'callback'))
            self.assertEqual(len(doc['fills']), 1)
            self.assertEqual(doc['fills'][0]['amount'], '847.0')
            self.assertFalse(doc.get('unassociated_evidence'))
            state.mark_reconciled(doc)
            closed = copy.deepcopy(doc)
            self.assertFalse(state.apply_observation(doc, 'order', alias, 'query'))
            self.assertEqual(doc, closed)

    def test_fill_amount_absolute_tolerance_includes_boundary_and_keeps_entry(self):
        cases = (('1000', '1000.001', True), ('1000', '999.999', True),
                 ('1000', '1000.0010000001', False), ('1000', '999.9989999999', False),
                 ('1000000000', '1000000000.01', False), ('0', '0.001', True),
                 ('0', '-0.001', False), ('1000', 'NaN', False), ('1000', 'Infinity', False))
        for state in self.states():
            for original_amount, query_amount, equivalent in cases:
                with self.subTest(module=state.__name__, original=original_amount, query=query_amount):
                    doc, unused, unused_alias, fill = self.snapshots()
                    fill['m_dTradeAmount'] = original_amount
                    state.apply_observation(doc, 'deal', fill, 'callback')
                    before = copy.deepcopy(doc)
                    changed = state.apply_observation(doc, 'deal', dict(fill, m_dTradeAmount=query_amount), 'query')
                    self.assertEqual(changed, not equivalent)
                    self.assertEqual(doc['fills'], before['fills'])
                    if equivalent:
                        self.assertEqual(doc, before)
                    else:
                        self.assertTrue(doc['unassociated_evidence'])

    def test_fill_identity_and_quantity_conflicts_are_not_amount_noise(self):
        mutations = ({'m_nVolume': 99}, {'m_strOrderSysID': 'different'}, {'m_nTaskId': 2},
                     {'m_strAccountID': 'other'}, {'m_strRemark': 'other'})
        for state in self.states():
            closed, unused, unused_alias, fill = self.closed(state)
            for mutation in mutations:
                with self.subTest(module=state.__name__, mutation=mutation):
                    doc = copy.deepcopy(closed)
                    self.assertTrue(state.apply_observation(doc, 'deal', dict(fill, m_dTradeAmount=847.0, **mutation), 'query'))
                    self.assertEqual(doc['unassociated_evidence'][0]['reason'], 'CONFLICTING_FILL_IDENTITY')
                    self.assertEqual(doc['fills'], closed['fills'])
            doc = document(basket=True)
            state.apply_observation(doc, 'deal', deal(quantity=100, amount='847'), 'callback')
            other_item = dict(deal(quantity=100, amount='847.001'), m_strInstrumentID='159001',
                              m_strExchangeID='SH', m_nOffsetFlag=49, symbol='159001.SZ')
            self.assertTrue(state.apply_observation(doc, 'deal', other_item, 'query'))
            self.assertEqual(doc['unassociated_evidence'][0]['reason'], 'CONFLICTING_FILL_IDENTITY')
            self.assertEqual(len(doc['fills']), 1)

    def test_order_amount_tolerance_boundary_does_not_overwrite_callback(self):
        for state in self.states():
            doc, original, alias, fill = self.snapshots()
            original['m_dTradeAmount'] = '847'
            alias['m_dTradeAmount'] = '847.001'
            state.apply_observation(doc, 'order', original, 'callback')
            before = copy.deepcopy(doc)
            self.assertFalse(state.apply_observation(doc, 'order', alias, 'query'))
            self.assertEqual(doc, before)
            self.assertTrue(state.apply_observation(doc, 'order', dict(alias, m_dTradeAmount='847.0010001'), 'query'))
            self.assertEqual(doc['qmt_orders'], before['qmt_orders'])
            self.assertEqual(doc['unassociated_evidence'][0]['reason'], 'CONFLICTING_ORDER_REFERENCE')

    def test_order_alias_requires_complete_scope_and_unchanged_business_facts(self):
        mutations = ({'m_nRef': -1}, {'m_nRef': '-1'}, {'m_nRef': 13}, {'m_strOrderRef': 'OTHER_11127'},
                     {'m_strAccountID': 'other'}, {'m_strInsertDate': '20260926'}, {'m_strExchangeID': 'SZ'},
                     {'m_strOrderSysID': 'other'}, {'m_strRemark': 'other'}, {'m_strInstrumentID': '600000'},
                     {'m_nOffsetFlag': 49}, {'m_nTaskId': 2}, {'m_nTaskId': None},
                     {'m_nVolumeTotalOriginal': 200}, {'m_nVolumeTraded': 99}, {'m_dTradedPrice': 8.48},
                     {'m_dTradeAmount': 848}, {'m_nOrderStatus': 54}, {'unknown_business_field': 1})
        missing = ('m_strAccountID', 'm_strInsertDate', 'm_strExchangeID', 'm_strOrderSysID', 'm_strRemark',
                   'm_strInstrumentID', 'm_nOffsetFlag', 'm_nVolumeTotalOriginal', 'm_nVolumeTraded',
                   'm_dTradedPrice', 'm_dTradeAmount', 'm_nOrderStatus')
        for state in self.states():
            closed, unused, alias, unused_fill = self.closed(state)
            examples = [dict(alias, **mutation) for mutation in mutations]
            for key in missing:
                raw = dict(alias)
                raw.pop(key)
                examples.append(raw)
            for raw in examples:
                with self.subTest(module=state.__name__, raw=raw):
                    doc = copy.deepcopy(closed)
                    self.assertTrue(state.apply_observation(doc, 'order', raw, 'query'))
                    self.assertEqual(doc['sync_status'], 'INCOMPLETE')
                    self.assertTrue(doc['reconcile_pending'])
                    self.assertIn(closed['qmt_orders'][0], doc['qmt_orders'])
            doc = copy.deepcopy(closed)
            self.assertFalse(state.apply_observation(doc, 'order', alias, 'callback'))
            self.assertEqual(doc, closed)
            for bad_callback in (
                dict(alias, m_strOrderRef='OTHER_11127'),
                dict(alias, m_dTradedPrice=8.48),
                dict(alias, m_nOrderStatus=54),
            ):
                bad_doc = copy.deepcopy(closed)
                self.assertTrue(state.apply_observation(bad_doc, 'order', bad_callback, 'callback'))
                self.assertEqual(bad_doc['unassociated_evidence'][0]['reason'], 'CONFLICTING_ORDER_REFERENCE')

    def test_ambiguous_child_or_item_cannot_use_alias_exception(self):
        for state in self.states():
            closed, unused, alias, unused_fill = self.closed(state)
            for field in ('qmt_orders', 'items'):
                doc = copy.deepcopy(closed)
                doc[field].append(copy.deepcopy(doc[field][0]))
                self.assertTrue(state.apply_observation(doc, 'order', alias, 'query'))
                self.assertTrue(doc['unassociated_evidence'])

    def test_alias_raw_never_supplies_missing_native_reference(self):
        for state in self.states():
            doc, original, alias, unused = self.snapshots()
            state.apply_observation(doc, 'order', alias, 'query')
            # Even a legacy partial native field cannot be paired with a raw query alias.
            doc['qmt_orders'][0]['native_ref'] = '1098925585'
            self.assertEqual(state.state_native_order_references(doc['qmt_orders'][0]), ('1098925585', None))
            missing = dict(original, m_strOrderSysID='', m_strOrderRef='SH_11127')
            state.apply_observation(doc, 'order', missing, 'callback')
            self.assertEqual(doc['unassociated_evidence'][0]['reason'], 'MISSING_QMT_ID')
            self.assertFalse(doc.get('resolved_evidence'))

    def test_duplicate_fill_preserves_existing_quarantine(self):
        for state in self.states():
            doc, unused, unused_alias, fill = self.closed(state)
            state.apply_observation(doc, 'deal', dict(fill, m_dTradeAmount=848), 'query')
            quarantined = copy.deepcopy(doc)
            self.assertFalse(state.apply_observation(doc, 'deal', dict(fill, m_dTradeAmount=847.0), 'query'))
            self.assertEqual(doc, quarantined)
            self.assertFalse(doc['reconciliation_complete'])

    def test_query_first_does_not_swallow_changed_callback_facts(self):
        for state in self.states():
            for mutation in ({'m_nVolumeTotalOriginal': 200}, {'m_nOrderStatus': 54}, {'m_nTaskId': 2}):
                doc, original, alias, unused = self.snapshots()
                state.apply_observation(doc, 'order', alias, 'query')
                self.assertTrue(state.apply_observation(doc, 'order', dict(original, **mutation), 'callback'))
                self.assertTrue(doc['unassociated_evidence'])
                self.assertFalse(doc['reconciliation_complete'])

    def test_stripped_terminal_order_query_with_op_sentinel_and_alias_are_equivalent(self):
        stripped_tags = {
            'm_nTaskId': -1,
            'm_strRemark': '',
            'm_strSource': '',
            'm_strOrderStrategyType': '',
            'm_strXTTrade': '其他终端',
        }
        for state in self.states():
            closed, unused, alias, unused_fill = self.closed(state)
            for src in (closed, alias):
                src['m_nTaskId'] = 17
                src['m_strRemark'] = closed['remark']
                src['m_strSource'] = 'IPC_ORDER'
                src['m_strOrderStrategyType'] = '函数下单'
                src['m_strXTTrade'] = '本终端'
                src['m_nOpType'] = 24
            base_raw = dict(closed['qmt_orders'][0]['raw'],
                            m_nTaskId=17,
                            m_strRemark=closed['remark'],
                            m_strSource='IPC_ORDER',
                            m_strOrderStrategyType='函数下单',
                            m_strXTTrade='本终端',
                            m_nOpType=24)
            closed['qmt_orders'][0]['qmt_task_id'] = '17'
            closed['qmt_orders'][0]['raw'] = dict(base_raw)
            closed['account_type'] = 'STOCK'

            for variant in (
                dict(base_raw, m_nOpType=2147483647, **stripped_tags),
                dict(base_raw, m_nRef=0, m_strOrderRef='SH_11127', m_nOpType=2147483647, **stripped_tags),
            ):
                doc = copy.deepcopy(closed)
                self.assertFalse(state.apply_observation(doc, 'order', variant, 'query'))
                self.assertEqual(doc, closed)

            bad = dict(base_raw, m_nOpType=23, **stripped_tags)
            bad_doc = copy.deepcopy(closed)
            self.assertFalse(state.state_equivalent_query_order(bad_doc, bad_doc['qmt_orders'][0], bad, 'query'))


if __name__ == '__main__':
    unittest.main()
