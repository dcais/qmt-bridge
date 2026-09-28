import copy
import unittest
from order_bridge.common import OrderError, new_order_document, public_order
from order_bridge.state import (apply_observation, apply_cancel_request, cancel_response,
                                pending_cancellations, mark_reconciled, recompute_order,
                                is_order_active, observation_identifiers, reconcile_pending)


def document(algo=False, basket=False):
    request = {'client_order_id': 'client', 'account_id': 'acct', 'order_type': 'SINGLE',
               'symbol': '510300.SH', 'side': 'BUY', 'quantity': 100,
               'execution': {'type': 'SMART' if algo else 'DIRECT'}}
    if basket:
        request.update(order_type='BASKET', items=[{'item_id': 'a', 'symbol': '510300.SH', 'side': 'BUY', 'quantity': 100},
                                                 {'item_id': 'b', 'symbol': '159001.SZ', 'side': 'SELL', 'quantity': 200}])
    return new_order_document(request)


def order(oid='o1', status=50, filled=0, quantity=100, symbol='510300', side=48, market='SH'):
    return {'m_strOrderSysID': oid, 'm_nOrderStatus': status, 'm_nVolumeTotalOriginal': quantity,
            'm_nVolumeTraded': filled, 'm_strInstrumentID': symbol, 'm_nOffsetFlag': side,
            'm_strExchangeID': market, 'm_strInsertDate': '20260925'}


def deal(tid='f1', quantity=40, amount='400', oid='o1'):
    return {'m_strTradeID': tid, 'm_nVolume': quantity, 'm_dTradeAmount': amount,
            'm_strOrderSysID': oid, 'm_strInstrumentID': '510300', 'm_nOffsetFlag': 48,
            'm_strExchangeID': 'SH', 'm_strTradeDate': '20260925'}


def observe(doc, kind, raw):
    return apply_observation(doc, kind, raw, 'callback', '2026-09-25T10:00:00+00:00')


class StateTests(unittest.TestCase):
    def test_reconcile_pending_enters_exits_and_reopens_on_late_fact(self):
        doc = document()
        self.assertFalse(reconcile_pending(doc))
        doc['submission_status'] = 'SUBMITTING'
        recompute_order(doc)
        self.assertTrue(doc['reconcile_pending'])
        observe(doc, 'order', order(status=56, filled=100))
        self.assertTrue(doc['reconcile_pending'])
        observe(doc, 'deal', deal(quantity=100, amount='1000'))
        mark_reconciled(doc, complete=True)
        self.assertEqual(doc['execution_status'], 'FILLED')
        self.assertFalse(doc['reconcile_pending'])
        observe(doc, 'order', order(oid='o2', status=50))
        self.assertTrue(doc['reconcile_pending'])
        self.assertTrue(doc['reconciliation_complete'] is False)
        observe(doc, 'order', order(oid='o2', status=50))
        self.assertTrue(doc['reconcile_pending'])

    def test_incomplete_reconcile_does_not_stamp_success(self):
        doc = document()
        doc['submission_status'] = 'UNKNOWN'
        mark_reconciled(doc, complete=False)
        self.assertIsNone(doc['last_reconciled_at'])
        self.assertTrue(doc['reconcile_pending'])

    def test_failed_checkpoints_preserve_confirmed_terminal_facts(self):
        cases = (('REJECTED', 57, 0), ('CANCELLED', 54, 0),
                 ('PARTIALLY_CANCELLED', 53, 40), ('FILLED', 56, 100))
        for expected, native_status, filled in cases:
            doc = document()
            if filled:
                observe(doc, 'deal', deal(quantity=filled, amount=str(filled * 10)))
            observe(doc, 'order', order(status=native_status, filled=filled))
            mark_reconciled(doc, now='2026-09-25T10:00:00+00:00')
            self.assertEqual(doc['execution_status'], expected)
            self.assertNotIn('terminal_facts_fingerprint', public_order(doc))
            for minute in ('01', '02'):
                mark_reconciled(doc, complete=False, now='2026-09-25T10:%s:00+00:00' % minute)
                self.assertEqual(doc['execution_status'], expected)
                self.assertEqual(doc['items'][0]['execution_status'], expected)
                self.assertEqual(doc['sync_status'], 'INCOMPLETE')
                self.assertFalse(doc['reconciliation_complete'])
                self.assertTrue(doc['reconcile_pending'])
                self.assertEqual(doc['last_reconciled_at'], '2026-09-25T10:00:00+00:00')
                self.assertFalse(recompute_order(doc))
                self.assertEqual(doc['execution_status'], expected)

    def test_failed_checkpoint_preserves_each_terminal_basket_item(self):
        doc = document(basket=True)
        observe(doc, 'order', order(status=54))
        observe(doc, 'order', order('o2', status=57, quantity=200,
                                    symbol='159001', side=49, market='SZ'))
        mark_reconciled(doc)
        self.assertEqual([item['execution_status'] for item in doc['items']], ['CANCELLED', 'REJECTED'])
        mark_reconciled(doc, complete=False)
        recompute_order(doc)
        self.assertEqual([item['execution_status'] for item in doc['items']], ['CANCELLED', 'REJECTED'])
        self.assertEqual(doc['execution_status'], 'CANCELLED')

    def test_failed_checkpoint_preserves_legacy_confirmed_terminal(self):
        doc = document()
        observe(doc, 'order', order(status=57))
        mark_reconciled(doc)
        doc.pop('terminal_facts_fingerprint')
        mark_reconciled(doc, complete=False)
        self.assertEqual(doc['execution_status'], 'REJECTED')
        self.assertEqual(doc['items'][0]['execution_status'], 'REJECTED')
        self.assertEqual(doc['sync_status'], 'INCOMPLETE')

    def test_failed_checkpoint_keeps_terminal_item_in_active_basket(self):
        doc = document(basket=True)
        observe(doc, 'order', order(status=54))
        observe(doc, 'order', order('o2', quantity=200,
                                    symbol='159001', side=49, market='SZ'))
        mark_reconciled(doc)
        self.assertEqual([item['execution_status'] for item in doc['items']], ['CANCELLED', 'WORKING'])
        doc.pop('terminal_facts_fingerprint')
        mark_reconciled(doc, complete=False)
        recompute_order(doc)
        self.assertEqual([item['execution_status'] for item in doc['items']], ['CANCELLED', 'WORKING'])
        self.assertEqual(doc['execution_status'], 'WORKING')
        self.assertTrue(doc['reconcile_pending'])

    def test_new_fact_after_failed_checkpoint_reopens_terminal_decision(self):
        for kind, raw, expected in (
                ('deal', deal(quantity=40), 'INCOMPLETE'),
                ('order', order('late'), 'WORKING'),
                ('error', {'message': 'late error'}, 'INCOMPLETE')):
            doc = document()
            observe(doc, 'order', order(status=54))
            mark_reconciled(doc)
            mark_reconciled(doc, complete=False)
            observe(doc, kind, raw)
            self.assertEqual(doc['execution_status'], expected)
            self.assertTrue(doc['reconcile_pending'])
            self.assertFalse(recompute_order(doc))
            self.assertEqual(doc['execution_status'], expected)
            if kind == 'deal':
                mark_reconciled(doc)
                self.assertEqual(doc['execution_status'], 'PARTIALLY_CANCELLED')

    def test_failed_checkpoint_does_not_confirm_unreconciled_terminal_child(self):
        doc = document()
        observe(doc, 'order', order(status=54))
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')
        mark_reconciled(doc, complete=False)
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')
        self.assertIsNone(doc['last_reconciled_at'])

    def test_late_error_before_failed_checkpoint_reopens_terminal(self):
        doc = document()
        observe(doc, 'order', order(status=54))
        mark_reconciled(doc)
        observe(doc, 'error', {'message': 'late error'})
        self.assertFalse(doc['reconciliation_complete'])
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')
        mark_reconciled(doc, complete=False)
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')

    def test_late_fill_identity_gap_overrides_preserved_terminal(self):
        doc = document()
        observe(doc, 'order', order(status=54))
        mark_reconciled(doc)
        mark_reconciled(doc, complete=False)
        raw = deal()
        del raw['m_strTradeID']
        observe(doc, 'deal', raw)
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')
        self.assertEqual(doc['sync_status'], 'INCOMPLETE')
        self.assertTrue(doc['unassociated_evidence'])
        self.assertTrue(doc['reconcile_pending'])

    def test_failed_checkpoint_does_not_finish_running_algorithm(self):
        doc = document(algo=True)
        observe(doc, 'task', {'m_nTaskId': 5, 'm_eStatus': 3})
        observe(doc, 'order', order(status=54))
        mark_reconciled(doc)
        self.assertNotIn(doc['execution_status'], ('CANCELLED', 'PARTIALLY_CANCELLED', 'FILLED'))
        mark_reconciled(doc, complete=False)
        self.assertNotIn(doc['execution_status'], ('CANCELLED', 'PARTIALLY_CANCELLED', 'FILLED'))
        self.assertTrue(doc['reconcile_pending'])

    def test_queued_cancel_is_local_and_never_dispatches(self):
        doc = document()
        record, status = apply_cancel_request(doc, {'cancel_request_id': 'c1'})
        self.assertEqual(status, 200)
        self.assertEqual(record['status'], 'CONFIRMED')
        self.assertEqual(doc['submission_status'], 'CANCELLED_LOCAL')
        self.assertEqual(doc['execution_status'], 'NOT_STARTED')
        self.assertFalse(is_order_active(doc))
        self.assertEqual(pending_cancellations(doc), [])
        self.assertEqual(doc['cancelled_quantity'], 0)

    def test_inflight_cancel_waits_for_identity_and_coalesces(self):
        doc = document()
        doc['submission_status'] = 'SUBMITTING'
        apply_cancel_request(doc, {'cancel_request_id': 'c1'})
        record, _ = apply_cancel_request(doc, {'cancel_request_id': 'c2'})
        self.assertEqual(record['canonical_cancel_request_id'], 'c1')
        self.assertEqual(doc['cancel_status'], 'WAITING_QMT_ID')
        self.assertEqual(pending_cancellations(doc), [])
        observe(doc, 'order', order())
        self.assertEqual(pending_cancellations(doc)[0]['target_id'], 'o1')
        self.assertEqual(pending_cancellations(doc)[0]['cancel_request_id'], 'c1')

    def test_cancel_identity_conflict_and_replay(self):
        doc = document()
        apply_cancel_request(doc, {'cancel_request_id': 'c1', 'reason': 'a'})
        _, code = apply_cancel_request(doc, {'cancel_request_id': 'c1', 'reason': 'a'})
        self.assertEqual(code, 200)
        with self.assertRaises(OrderError) as caught:
            apply_cancel_request(doc, {'cancel_request_id': 'c1', 'reason': 'b'})
        self.assertEqual(caught.exception.status, 409)
        self.assertTrue(cancel_response(doc, 'c1', True)['replayed'])
        result = cancel_response(doc, 'c1')
        self.assertEqual({key: result[key] for key in ('client_order_id', 'account_id', 'submission_status', 'execution_status')},
                         {'client_order_id': 'client', 'account_id': 'acct', 'submission_status': 'CANCELLED_LOCAL', 'execution_status': 'NOT_STARTED'})

    def test_unknown_cancel_is_never_reissued_even_with_alias(self):
        doc = document()
        observe(doc, 'order', order())
        apply_cancel_request(doc, {'cancel_request_id': 'c1'})
        doc['attempts'].append({'kind': 'CANCEL_ORDER', 'target_id': 'o1', 'cancel_request_id': 'c1', 'status': 'UNKNOWN'})
        recompute_order(doc)
        apply_cancel_request(doc, {'cancel_request_id': 'c2'})
        self.assertEqual(doc['cancel_status'], 'UNKNOWN')
        self.assertEqual(pending_cancellations(doc), [])

    def test_explicit_cancel_failure_allows_new_attempt_only_new_id(self):
        doc = document()
        observe(doc, 'order', order())
        apply_cancel_request(doc, {'cancel_request_id': 'c1'})
        doc['attempts'].append({'kind': 'CANCEL_ORDER', 'target_id': 'o1', 'cancel_request_id': 'c1', 'status': 'REJECTED'})
        recompute_order(doc)
        self.assertEqual(doc['cancel_status'], 'REJECTED')
        self.assertEqual(pending_cancellations(doc), [])
        apply_cancel_request(doc, {'cancel_request_id': 'c2'})
        self.assertEqual(pending_cancellations(doc)[0]['cancel_request_id'], 'c2')

    def test_task_first_then_children_and_late_child(self):
        doc = document(algo=True)
        observe(doc, 'task', {'m_nTaskId': 5, 'm_eStatus': 3})
        observe(doc, 'order', order())
        apply_cancel_request(doc, {'cancel_request_id': 'c1'})
        self.assertEqual([a['kind'] for a in pending_cancellations(doc)], ['CANCEL_TASK'])
        observe(doc, 'task', {'m_nTaskId': 5, 'm_eStatus': 8})
        self.assertEqual([a['kind'] for a in pending_cancellations(doc)], ['CANCEL_ORDER'])
        observe(doc, 'order', order(status=54))
        self.assertNotEqual(doc['cancel_status'], 'CONFIRMED')
        mark_reconciled(doc)
        self.assertEqual(doc['cancel_status'], 'CONFIRMED')
        observe(doc, 'order', order('o2'))
        self.assertNotEqual(doc['cancel_status'], 'CONFIRMED')
        self.assertEqual(pending_cancellations(doc)[0]['target_id'], 'o2')
        self.assertTrue(is_order_active(doc))

    def test_algorithm_with_missing_task_id_never_cancels_children_first(self):
        doc = document(algo=True)
        observe(doc, 'order', order())
        apply_cancel_request(doc, {'cancel_request_id': 'c1'})
        self.assertEqual(pending_cancellations(doc), [])

    def test_task_completed_is_not_parent_filled(self):
        doc = document(algo=True)
        observe(doc, 'task', {'m_nTaskId': 5, 'm_eStatus': 7, 'm_nBusinessNum': 100})
        mark_reconciled(doc)
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')
        self.assertEqual(doc['filled_quantity'], 0)
        self.assertEqual(doc['cancelled_quantity'], 0)

    def test_fills_deduplicate_and_late_fill_corrects_terminal(self):
        doc = document()
        observe(doc, 'order', order(status=54))
        mark_reconciled(doc)
        self.assertEqual(doc['execution_status'], 'CANCELLED')
        observe(doc, 'deal', deal(quantity=100, amount='1000'))
        self.assertEqual(doc['execution_status'], 'FILLED')
        self.assertFalse(observe(doc, 'deal', deal(quantity=100, amount='1000')))
        observe(doc, 'order', order(status=50))
        self.assertEqual(doc['filled_quantity'], 100)
        self.assertEqual(doc['execution_status'], 'FILLED')
        self.assertEqual(doc['qmt_orders'][0]['status'], 'FILLED')

    def test_out_of_order_partial_cancel_does_not_revive(self):
        doc = document()
        observe(doc, 'deal', deal())
        observe(doc, 'order', order(status=53, filled=40))
        observe(doc, 'order', order(status=50, filled=0))
        mark_reconciled(doc)
        self.assertEqual(doc['execution_status'], 'PARTIALLY_CANCELLED')
        self.assertEqual(doc['filled_quantity'], 40)
        self.assertEqual(doc['cancelled_quantity'], 60)
        self.assertEqual(doc['open_quantity'], 0)

    def test_same_price_quantity_different_trade_ids_are_distinct(self):
        doc = document()
        observe(doc, 'deal', deal('f1'))
        observe(doc, 'deal', deal('f2'))
        self.assertEqual(doc['filled_quantity'], 80)
        raw = deal('f1')
        raw['m_strTradeDate'] = '20260926'
        observe(doc, 'deal', raw)
        self.assertEqual(doc['filled_quantity'], 120)

    def test_missing_or_conflicting_fill_identity_retains_evidence(self):
        doc = document()
        raw = deal()
        del raw['m_strTradeID']
        observe(doc, 'deal', raw)
        observe(doc, 'deal', raw)
        self.assertEqual(len(doc['unassociated_evidence']), 1)
        self.assertEqual(doc['filled_quantity'], 0)
        observe(doc, 'deal', deal())
        observe(doc, 'deal', deal(amount='401'))
        self.assertEqual(doc['filled_amount'], '400')
        mark_reconciled(doc)
        self.assertEqual(doc['sync_status'], 'INCOMPLETE')
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')

    def test_order_cumulative_volume_is_not_synthetic_fill(self):
        doc = document()
        observe(doc, 'order', order(status=56, filled=100))
        mark_reconciled(doc)
        self.assertEqual(doc['filled_quantity'], 0)
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')
        self.assertEqual(doc['sync_status'], 'INCOMPLETE')
        self.assertTrue(is_order_active(doc))

    def test_basket_does_not_sum_quantities_across_symbols(self):
        doc = document(basket=True)
        observe(doc, 'deal', deal(quantity=100, amount='1000'))
        self.assertIsNone(doc['filled_quantity'])
        self.assertEqual([i['filled_quantity'] for i in doc['items']], [100, 0])
        self.assertNotEqual(doc['execution_status'], 'FILLED')

    def test_native_stock_side_uses_offset_not_direction(self):
        raw = order(side=49, market='SZ', symbol='159001')
        raw['m_nDirection'] = 48
        ids = observation_identifiers('order', raw)
        self.assertEqual(ids['side'], 'SELL')
        self.assertEqual(ids['symbol'], '159001.SZ')

    def test_recompute_and_identical_observation_are_idempotent(self):
        doc = document()
        observe(doc, 'order', order())
        self.assertFalse(observe(doc, 'order', order()))
        self.assertFalse(recompute_order(doc))
        snapshot = copy.deepcopy(doc)
        pending_cancellations(doc)
        self.assertEqual(doc, snapshot)

    def test_missing_order_day_blocks_complete_until_identity_arrives(self):
        doc = document()
        raw = order(status=54)
        del raw['m_strInsertDate']
        observe(doc, 'order', raw)
        mark_reconciled(doc)
        self.assertEqual(doc['sync_status'], 'INCOMPLETE')
        observe(doc, 'order', order(status=54))
        mark_reconciled(doc)
        self.assertEqual(doc['sync_status'], 'COMPLETE')

    def test_nonfinite_fill_amount_is_evidence_not_money(self):
        for amount in ('NaN', 'Infinity', 'invalid', '-1'):
            doc = document()
            observe(doc, 'deal', deal(amount=amount))
            self.assertEqual(doc['filled_quantity'], 0)
            self.assertEqual(doc['sync_status'], 'INCOMPLETE')

    def test_native_cancel_rejection_resolves_unknown_attempt(self):
        doc = document()
        observe(doc, 'order', order())
        apply_cancel_request(doc, {'cancel_request_id': 'c1'})
        doc['attempts'].append({'kind': 'CANCEL_ORDER', 'target_id': 'o1', 'cancel_request_id': 'c1', 'status': 'UNKNOWN'})
        raw = order()
        raw['m_nOrderSubmitStatus'] = 53
        observe(doc, 'order', raw)
        self.assertEqual(doc['cancel_status'], 'REJECTED')
        apply_cancel_request(doc, {'cancel_request_id': 'c2'})
        self.assertEqual(pending_cancellations(doc)[0]['cancel_request_id'], 'c2')

    def test_deadline_expires_only_unsubmitted_work(self):
        doc = document()
        doc['submit_before'] = '2026-09-25T10:00:00+00:00'
        recompute_order(doc, '2026-09-25T10:00:00+00:00')
        self.assertEqual(doc['submission_status'], 'EXPIRED')
        self.assertFalse(is_order_active(doc))
        _, status = apply_cancel_request(doc, {'cancel_request_id': 'expired-cancel'})
        self.assertEqual(status, 200)
        self.assertEqual(doc['cancel_status'], 'NOT_NEEDED')
        doc['submission_status'] = 'UNKNOWN'
        recompute_order(doc, '2026-09-25T11:00:00+00:00')
        self.assertEqual(doc['submission_status'], 'UNKNOWN')

    def test_confirmed_target_does_not_mask_other_target_cancel_failure(self):
        doc = document()
        observe(doc, 'order', order('o1', quantity=50))
        observe(doc, 'order', order('o2', quantity=50))
        apply_cancel_request(doc, {'cancel_request_id': 'c1'})
        doc['attempts'].extend([
            {'kind': 'CANCEL_ORDER', 'target_id': 'o1', 'cancel_request_id': 'c1', 'status': 'RETURNED'},
            {'kind': 'CANCEL_ORDER', 'target_id': 'o2', 'cancel_request_id': 'c1', 'status': 'REJECTED'}])
        observe(doc, 'order', order('o1', status=54, quantity=50))
        self.assertEqual(doc['cancel_status'], 'REJECTED')
        apply_cancel_request(doc, {'cancel_request_id': 'c2'})
        self.assertEqual([a['target_id'] for a in pending_cancellations(doc)], ['o2'])

    def test_filled_target_never_hides_live_excess_child(self):
        doc = document()
        observe(doc, 'order', order(quantity=200, filled=100))
        observe(doc, 'deal', deal(quantity=100, amount='1000'))
        apply_cancel_request(doc, {'cancel_request_id': 'c1'})
        mark_reconciled(doc)
        self.assertEqual(doc['execution_status'], 'PARTIALLY_FILLED')
        self.assertEqual(doc['open_quantity'], 100)
        self.assertEqual(doc['cancel_status'], 'REQUESTED')
        self.assertEqual(pending_cancellations(doc)[0]['target_id'], 'o1')
        self.assertTrue(is_order_active(doc))

    def test_filled_target_still_cancels_running_algorithm(self):
        doc = document(algo=True)
        observe(doc, 'task', {'m_nTaskId': 5, 'm_eStatus': 3})
        observe(doc, 'order', order(status=56, filled=100))
        observe(doc, 'deal', deal(quantity=100, amount='1000'))
        mark_reconciled(doc)
        apply_cancel_request(doc, {'cancel_request_id': 'c1'})
        self.assertNotEqual(doc['execution_status'], 'FILLED')
        self.assertEqual(pending_cancellations(doc)[0]['kind'], 'CANCEL_TASK')
        observe(doc, 'task', {'m_nTaskId': 5, 'm_eStatus': 8})
        self.assertNotEqual(doc['cancel_status'], 'NOT_NEEDED')
        mark_reconciled(doc)
        self.assertEqual(doc['cancel_status'], 'NOT_NEEDED')

    def test_aborted_cancel_can_resume_same_intent_and_alias(self):
        for algorithm in (False, True):
            doc = document(algo=algorithm)
            observe(doc, 'order', order())
            if algorithm:
                observe(doc, 'task', {'m_nTaskId': 5, 'm_eStatus': 3})
            apply_cancel_request(doc, {'cancel_request_id': 'c1'})
            action = pending_cancellations(doc)[0]
            doc['attempts'].append(dict(action, status='ABORTED_NO_CALL'))
            recompute_order(doc)
            self.assertEqual(pending_cancellations(doc), [action])
            apply_cancel_request(doc, {'cancel_request_id': 'c2'})
            self.assertEqual(pending_cancellations(doc), [action])
            # 续派一旦进入未知结果，旧ABORTED记录不能放行第三次调用。
            doc['attempts'].append(dict(action, status='UNKNOWN'))
            recompute_order(doc)
            self.assertEqual(pending_cancellations(doc), [])

    def test_direct_amount_fills_after_complete_reconciliation(self):
        doc = document()
        doc['request'].pop('quantity')
        doc['request']['amount'] = '1000'
        doc['requested_quantity'] = None
        doc['requested_amount'] = '1000'
        doc['items'][0].update(requested_quantity=None, requested_amount='1000')
        observe(doc, 'order', order(status=56, filled=100))
        observe(doc, 'deal', deal(quantity=100, amount='990'))
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')
        mark_reconciled(doc)
        self.assertEqual(doc['execution_status'], 'FILLED')
        self.assertEqual(doc['filled_amount'], '990')
        self.assertFalse(is_order_active(doc))
        observe(doc, 'order', order('late', quantity=100))
        self.assertNotEqual(doc['execution_status'], 'FILLED')
        self.assertTrue(is_order_active(doc))

    def test_direct_amount_needs_stable_fills_not_only_filled_order_status(self):
        doc = document()
        doc['items'][0].update(requested_quantity=None, requested_amount='1000')
        observe(doc, 'order', order(status=56, filled=100))
        mark_reconciled(doc)
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')
        self.assertTrue(is_order_active(doc))
        self.assertEqual(doc['filled_quantity'], 0)

    def test_algorithm_amount_task_completion_does_not_prove_target_filled(self):
        doc = document(algo=True)
        doc['items'][0].update(requested_quantity=None, requested_amount='1000')
        observe(doc, 'task', {'m_nTaskId': 5, 'm_eStatus': 7})
        observe(doc, 'order', order(status=56, filled=100))
        observe(doc, 'deal', deal(quantity=100, amount='990'))
        mark_reconciled(doc)
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')
        self.assertFalse(is_order_active(doc))

    def test_completed_underfilled_algorithm_can_finish_without_inventing_fills(self):
        doc = document(algo=True, basket=True)
        observe(doc, 'task', {'m_nTaskId': 5, 'm_eStatus': 7})
        self.assertTrue(is_order_active(doc))
        self.assertFalse(doc['basket_cleanup_eligible'])
        mark_reconciled(doc)
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')
        self.assertEqual(doc['sync_status'], 'COMPLETE')
        self.assertFalse(is_order_active(doc))
        self.assertTrue(doc['basket_cleanup_eligible'])
        self.assertEqual([item['filled_quantity'] for item in doc['items']], [0, 0])
        observe(doc, 'order', order())
        self.assertTrue(is_order_active(doc))
        self.assertFalse(doc['basket_cleanup_eligible'])

    def test_completed_task_with_missing_fill_evidence_cannot_finish_or_clean(self):
        doc = document(algo=True, basket=True)
        observe(doc, 'task', {'m_nTaskId': 5, 'm_eStatus': 7})
        observe(doc, 'order', order(status=56, filled=100))
        mark_reconciled(doc)
        self.assertEqual(doc['execution_status'], 'INCOMPLETE')
        self.assertEqual(doc['sync_status'], 'INCOMPLETE')
        self.assertTrue(is_order_active(doc))
        self.assertFalse(doc['basket_cleanup_eligible'])

    def test_filled_native_status_without_trade_volume_does_not_mask_gap(self):
        doc = document(algo=True)
        observe(doc, 'task', {'m_nTaskId': 5, 'm_eStatus': 7})
        observe(doc, 'order', order(status=56, filled=0))
        mark_reconciled(doc)
        self.assertEqual(doc['sync_status'], 'INCOMPLETE')
        self.assertTrue(is_order_active(doc))


if __name__ == '__main__':
    unittest.main()
