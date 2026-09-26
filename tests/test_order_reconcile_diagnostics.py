# -*- coding: utf-8 -*-
"""账户对账失败后节流与 QMT 查询诊断；Last modified: 2026-09-26。"""
import unittest
from unittest.mock import Mock, patch

from order_bridge.common import parse_timestamp
from order_bridge.runtime import OrderRuntime


class _Clock(object):
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class _Qmt(object):
    def __init__(self):
        self.calls = []
        self.fail = False
        self.history_fail = False

    def query(self, account_id, account_type, kind):
        self.calls.append((account_id, account_type, kind))
        if self.fail and kind == 'task':
            raise RuntimeError('QMT gateway dropped while reading task')
        return []

    def history(self, account_id, account_type, kind, start, end):
        self.calls.append((account_id, account_type, kind, start, end))
        if self.history_fail and kind == 'ORDER':
            raise RuntimeError('QMT archive unavailable')
        return []


class ReconcileDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.clock = _Clock()
        self.qmt = _Qmt()
        self.repo = Mock()
        self.repo.begin_reconcile_batch.return_value = {
            'orders': [], 'next_cursor': None, 'has_more': False}
        self.repo.reconcile_round_batch.return_value = {
            'orders': [], 'next_cursor': None, 'has_more': False}
        self.repo.reconcile_history_start.return_value = None
        self.logs = []
        self.runtime = OrderRuntime(
            {'get_trade_detail_data': self.qmt.query,
             'get_history_trade_detail_data': self.qmt.history}, object(), 'acct-01',
            repository=self.repo, clock=self.clock,
            settings={'reconcile_interval_seconds': 30},
            logger=lambda level, message, **fields: self.logs.append((level, message, fields)))
        self.background = self.runtime.background
        self.background.db_ready = True

    def _finish_round(self):
        """真实 runtime.tick 调用假 QMT；单步驱动数据库工作避免真实线程和账户。"""
        for unused in range(20):
            self.background._reconcile_step()
            if not self.background.queues['query'].empty():
                self.runtime.tick(max_actions=1)
                result = self.background.results.get_nowait()
                self.background._merge(result)
                self.background.capacity.release()
            if self.background.round is None:
                return
        self.fail('reconcile round did not finish')

    def test_failure_waits_configured_interval_and_preserves_success_timestamp(self):
        self.runtime.last_reconciled_at = '2026-09-25T00:00:00+00:00'
        self.qmt.fail = True
        self._finish_round()
        first_count = len(self.qmt.calls)
        self.assertEqual(first_count, 3)
        self.assertEqual(self.runtime.last_reconciled_at, '2026-09-25T00:00:00+00:00')
        health = self.runtime.health()
        self.assertIsNotNone(health['last_reconcile_attempt_at'])
        self.assertIsNotNone(health['last_reconcile_finished_at'])
        self.assertIsNotNone(health['next_reconcile_at'])
        self.assertAlmostEqual(
            (parse_timestamp(health['next_reconcile_at']) -
             parse_timestamp(health['last_reconcile_finished_at'])).total_seconds(),
            30, delta=.1)
        self.assertEqual(health['last_reconcile_error']['type'], 'RuntimeError')
        self.assertIn('gateway dropped', health['last_reconcile_error']['message'])
        self.assertNotIn('traceback', health['last_reconcile_error'])
        self.repo.begin_reconcile_batch.assert_called_with(
            limit=100, round_id=self.repo.begin_reconcile_batch.call_args[1]['round_id'],
            cursor=None, interval_seconds=30)

        # 新回报/缺口和持续回调不能缩短失败后的账户轮次间隔。
        self.background.fact_generation += 1
        self.runtime.observation_gap = True
        self.clock.advance(1)
        self.background._reconcile_step()
        self.assertIsNone(self.background.round)
        self.assertEqual(len(self.qmt.calls), first_count)
        self.clock.advance(28)
        self.background._reconcile_step()
        self.assertEqual(len(self.qmt.calls), first_count)
        self.clock.advance(1)
        self._finish_round()
        self.assertEqual(len(self.qmt.calls), first_count + 3)
        self.assertEqual(self.runtime.last_reconciled_at, '2026-09-25T00:00:00+00:00')

        self.qmt.fail = False
        self.clock.advance(30)
        self._finish_round()
        self.assertEqual(len(self.qmt.calls), first_count + 6)
        self.assertNotEqual(self.runtime.last_reconciled_at, '2026-09-25T00:00:00+00:00')
        self.assertIsNone(self.runtime.health()['last_reconcile_error'])

    def test_query_failure_log_contains_message_stack_and_context(self):
        self.qmt.fail = True
        self._finish_round()
        errors = [fields for level, message, fields in self.logs
                  if message == 'QMT reconciliation incomplete']
        self.assertEqual(len(errors), 1)
        error = errors[0]
        self.assertEqual(error['account_id'], 'acct-01')
        self.assertEqual(error['query_kind'], 'task')
        self.assertTrue(error['round_id'])
        self.assertEqual(error['error_type'], 'RuntimeError')
        self.assertIn('gateway dropped', error['error_message'])
        self.assertIn('RuntimeError: QMT gateway dropped', error['traceback'])
        self.assertNotIn('calls', error)

    def test_nondefault_interval_controls_success_and_failure_rounds(self):
        self.runtime.settings['reconcile_interval_seconds'] = 7
        for fail in (False, True):
            self.qmt.fail = fail
            self._finish_round()
            self.assertEqual(self.background.next_round, self.clock() + 7)
            count = len(self.qmt.calls)
            self.clock.advance(6)
            self.background._reconcile_step()
            self.assertIsNone(self.background.round)
            self.assertEqual(len(self.qmt.calls), count)
            self.clock.advance(1)


    def test_history_failure_log_includes_query_dates(self):
        self.repo.reconcile_history_start.return_value = '2000-01-01T00:00:00+00:00'
        self.qmt.history_fail = True
        self._finish_round()
        errors = [fields for unused, message, fields in self.logs
                  if message == 'QMT reconciliation incomplete']
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]['query_kind'], 'order')
        self.assertEqual(errors[0]['history_start'], '20000101')
        self.assertRegex(errors[0]['history_end'], r'^\d{8}$')

    def test_cooldown_does_not_prevent_dispatch_or_overlap_round(self):
        self.background._reconcile_step()
        self.assertIsNotNone(self.background.round)
        queued = self.background.queues['query'].qsize()
        self.background._reconcile_step()
        self.assertEqual(self.background.queues['query'].qsize(), queued)
        self._finish_round()
        self.runtime.recovery_complete = True
        self.background.grant = (self.runtime.instance_id, 7, self.clock() + 100)
        with patch.object(self.background, '_dispatch') as dispatch:
            self.background._service()
            dispatch.assert_called_once()
        self.assertIsNone(self.background.round)

    def test_finish_passes_interval_to_repository(self):
        self.repo.reconcile_round_batch.return_value = {
            'orders': [{'order_id': 'one', 'submission_status': 'CONFIRMED',
                        'reconcile_round_fact_version': 3}],
            'next_cursor': None, 'has_more': False}
        self._finish_round()
        self.repo.finish_reconcile.assert_called_once()
        self.assertEqual(self.repo.finish_reconcile.call_args[0][:3], ('one', 3, True))
        self.assertEqual(self.repo.finish_reconcile.call_args[1]['interval_seconds'], 30)


if __name__ == '__main__':
    unittest.main()
