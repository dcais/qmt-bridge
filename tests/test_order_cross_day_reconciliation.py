"""Offline cross-day closure regressions; no QMT or database connections."""
import copy
import importlib
import unittest
from unittest.mock import Mock

from test_order_state import document, order, deal
from tests.order_runtime_helpers import OrderRuntime


class CrossDayReconciliationTests(unittest.TestCase):
    def modules(self):
        for base in ('service.ipc_bridge.order', 'order_bridge'):
            yield (importlib.import_module(base + '.state'),
                   importlib.import_module(base + '.repository'))

    def closed(self, state, status=57, filled=0):
        doc = document()
        doc['created_at'] = '2026-09-25T02:00:00+00:00'
        state.apply_observation(doc, 'order', order(status=status, filled=filled), 'callback')
        if filled:
            state.apply_observation(doc, 'deal', deal(quantity=filled), 'callback')
        state.mark_reconciled(doc, now='2026-09-25T03:00:00+00:00')
        return doc

    def test_closure_restores_four_terminals_without_new_success_timestamp(self):
        for state, unused in self.modules():
            for status, filled in ((57, 0), (54, 0), (53, 40), (56, 100)):
                with self.subTest(module=state.__name__, status=status):
                    doc = self.closed(state, status, filled)
                    checkpoint = copy.deepcopy(doc['terminal_reconciliation_checkpoint'])
                    expected = doc['execution_status']
                    state.mark_reconciled(doc, complete=False)
                    doc['reconcile_requested'] = True
                    self.assertTrue(state.restore_terminal_reconciliation(doc))
                    self.assertEqual(doc['terminal_reconciliation_checkpoint'], checkpoint)
                    self.assertEqual(doc['last_reconciled_at'], checkpoint['reconciled_at'])
                    self.assertEqual(doc['execution_status'], expected)
                    self.assertEqual(doc['sync_status'], 'COMPLETE')
                    self.assertFalse(doc['reconcile_pending'])
                    self.assertFalse(state.recompute_order(doc))

    def test_new_facts_or_cancel_intent_prevent_restoration(self):
        mutations = (
            lambda d: d['attempts'].append({'kind': 'CANCEL_ORDER', 'status': 'UNKNOWN'}),
            lambda d: d['cancel_requests'].append({'cancel_request_id': 'new'}),
            lambda d: d.update(cancel_requested=True),
            lambda d: d.update(active_cancel_request_id='new'),
            lambda d: d['fills'].append({'quantity': 1}),
            lambda d: d['qmt_orders'][0].update(filled_quantity=1),
            lambda d: d.update(error={'message': 'late'}),
            lambda d: d.setdefault('unassociated_evidence', []).append({'reason': 'CONFLICTING_ORDER_REFERENCE'}))
        for state, unused in self.modules():
            for mutate in mutations:
                doc = self.closed(state)
                state.mark_reconciled(doc, complete=False)
                mutate(doc)
                before = copy.deepcopy(doc)
                self.assertFalse(state.restore_terminal_reconciliation(doc))
                self.assertEqual(doc, before)

    def test_no_bootstrap_from_legacy_fingerprint_or_unknown_or_partial_basket(self):
        for state, unused in self.modules():
            legacy = self.closed(state)
            legacy.pop('terminal_reconciliation_checkpoint')
            state.mark_reconciled(legacy, complete=False)
            self.assertFalse(state.restore_terminal_reconciliation(legacy))
            unknown = document()
            unknown['submission_status'] = 'UNKNOWN'
            state.mark_reconciled(unknown, complete=False)
            self.assertFalse(state.restore_terminal_reconciliation(unknown))
            basket = document(basket=True)
            state.apply_observation(basket, 'order', order(status=57), 'callback')
            state.mark_reconciled(basket)
            self.assertNotIn('terminal_reconciliation_checkpoint', basket)
            self.assertFalse(state.restore_terminal_reconciliation(basket))

    def test_stopped_algorithm_without_terminal_result_cannot_mint_checkpoint(self):
        for state, unused in self.modules():
            doc = document(algo=True)
            state.apply_observation(doc, 'task', {'m_nTaskId': 1, 'm_eStatus': 8}, 'callback')
            state.mark_reconciled(doc)
            self.assertEqual(doc['execution_status'], 'INCOMPLETE')
            self.assertNotIn('terminal_reconciliation_checkpoint', doc)
            self.assertFalse(state.restore_terminal_reconciliation(doc))

    def test_terminal_cancel_target_can_retain_unknown_call_without_clearing_it(self):
        for state, unused in self.modules():
            doc = document()
            state.apply_observation(doc, 'order', order(status=50), 'callback')
            state.apply_cancel_request(doc, {'cancel_request_id': 'cancel-1'})
            doc['attempts'].append({'kind': 'CANCEL_ORDER', 'target_id': 'o1',
                                    'cancel_request_id': 'cancel-1', 'status': 'UNKNOWN'})
            state.apply_observation(doc, 'order', order(status=54), 'callback')
            state.mark_reconciled(doc)
            state.mark_reconciled(doc, complete=False)
            self.assertTrue(state.restore_terminal_reconciliation(doc))
            self.assertEqual(doc['cancel_status'], 'CONFIRMED')
            self.assertEqual(doc['attempts'][0]['status'], 'UNKNOWN')

    def test_new_native_reference_conflict_remains_blocked(self):
        for state, unused in self.modules():
            doc = document()
            state.apply_observation(doc, 'order', dict(order(status=57), m_nRef=12,
                                    m_strOrderRef='native-12'), 'callback')
            state.mark_reconciled(doc)
            state.apply_observation(doc, 'order', dict(order(status=57), m_nRef=13,
                                    m_strOrderRef='other'), 'query')
            self.assertTrue(doc['reconcile_pending'])
            self.assertFalse(state.restore_terminal_reconciliation(doc))
            self.assertEqual(doc['unassociated_evidence'][0]['reason'], 'CONFLICTING_ORDER_REFERENCE')

    def test_gap_preserves_closed_order_at_startup_and_runtime(self):
        for state, repository in self.modules():
            for startup in (True, False):
                closed = self.closed(state)
                unknown = document()
                unknown['submission_status'] = 'UNKNOWN'
                cursor = Mock()
                cursor.fetchall.return_value = [('closed', closed), ('unknown', unknown)]
                repo = repository.PostgresRepository({}, 'synthetic-account')
                repo.repo_run = lambda fn, **kw: fn(cursor)
                repo.repo_save = Mock()
                result = repo.mark_reconcile_gap('2026-09-26T00:00:00Z', startup=startup)
                self.assertEqual(result['marked'], 1)
                self.assertEqual(repo.repo_save.call_args[0][1]['submission_status'], 'UNKNOWN')

    def test_locked_finish_revalidates_evidence_and_fact_version(self):
        for state, repository in self.modules():
            doc = self.closed(state)
            state.mark_reconciled(doc, complete=False)
            doc['fact_version'] = 7
            repo = repository.PostgresRepository({}, 'synthetic-account')
            repo.repo_run = lambda fn, **kw: fn(Mock())
            repo.repo_load = Mock(side_effect=lambda *a, **kw: copy.deepcopy(doc))
            repo.repo_save = Mock()
            self.assertFalse(repo.finish_reconcile('id', 6, True, restore_terminal=True))
            self.assertTrue(repo.finish_reconcile('id', 7, True, restore_terminal=True))
            saved = repo.repo_save.call_args[0][1]
            self.assertEqual(saved['last_reconciled_at'], '2026-09-25T03:00:00+00:00')
            self.assertFalse(saved['reconcile_pending'])
            doc['attempts'].append({'kind': 'SUBMIT', 'status': 'UNKNOWN'})
            repo.repo_save.reset_mock()
            self.assertFalse(repo.finish_reconcile('id', 7, True, restore_terminal=True))
            repo.repo_save.assert_not_called()

    def test_background_retained_closure_does_not_claim_history_query_coverage(self):
        from service.ipc_bridge.order import state
        for has_checkpoint in (True, False):
            doc = self.closed(state)
            state.mark_reconciled(doc, complete=False)
            if not has_checkpoint:
                doc.pop('terminal_reconciliation_checkpoint')
            doc['reconcile_round_fact_version'] = 0
            page = {'orders': [doc], 'next_cursor': None, 'has_more': False}
            repo = Mock()
            repo.begin_reconcile_batch.return_value = page
            repo.reconcile_round_batch.return_value = page
            repo.reconcile_history_start.return_value = doc['created_at']
            repo.finish_reconcile.return_value = True
            logs = []
            runtime = OrderRuntime({}, object(), 'synthetic-account', repository=repo,
                                   logger=lambda level, message, **fields: logs.append((message, fields)))
            background = runtime.background
            background._reconcile_step()
            background.round['stage'] = len(background.round['queries'])
            background.round['waiting'] = False
            background._reconcile_step()
            finished = [fields for message, fields in logs if message == 'RECONCILE_FINISHED'][0]
            self.assertEqual(finished['query_scope'], 'CURRENT_DAY')
            self.assertEqual(finished['coverage_gap'], 'HISTORY_NOT_COVERED')
            self.assertFalse(runtime.history_coverage_complete)
            self.assertEqual(finished['complete'], has_checkpoint)
            if has_checkpoint:
                self.assertEqual(finished['outcome'], 'RETAINED_TERMINAL')
                self.assertTrue(repo.finish_reconcile.call_args.kwargs['restore_terminal'])
            else:
                self.assertEqual(finished['action_required'], 'HISTORICAL_EVIDENCE_REVIEW')

    def test_internal_checkpoint_is_hidden_from_public_order(self):
        from service.ipc_bridge.order import state
        from service.ipc_bridge.ipc_protocol.order_common import public_order
        self.assertNotIn('terminal_reconciliation_checkpoint', public_order(self.closed(state)))
