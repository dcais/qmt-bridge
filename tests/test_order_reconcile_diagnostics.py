# -*- coding: utf-8 -*-
"""账户对账失败后节流与 QMT 查询诊断；Last modified: 2026-09-28。"""
import datetime as dt
import unittest
from unittest.mock import Mock, patch

from order_bridge.common import parse_timestamp
from order_bridge.background import _background_current_day_covered
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

    def query(self, account_id, account_type, kind):
        self.calls.append((account_id, account_type, kind))
        if self.fail and kind == 'task':
            raise RuntimeError('QMT gateway dropped while reading task')
        return []

class ReconcileDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.now = parse_timestamp('2026-09-28T02:00:00+00:00')
        clock_patch = patch('order_bridge.background.utc_now', return_value=self.now)
        clock_patch.start()
        self.addCleanup(clock_patch.stop)
        stamp_patch = patch('order_bridge.background.iso_datetime', side_effect=lambda value=None:
                            (value or self.now).isoformat())
        stamp_patch.start()
        self.addCleanup(stamp_patch.stop)
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
            {'get_trade_detail_data': self.qmt.query}, object(), 'acct-01',
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


    def test_old_gap_does_not_block_today_or_call_history_api(self):
        self.repo.reconcile_history_start.return_value = '2000-01-01T00:00:00+00:00'
        self.repo.reconcile_round_batch.return_value = {
            'orders': [{'order_id': key, 'submission_status': 'CONFIRMED',
                        'created_at': stamp, 'reconcile_round_fact_version': version}
                       for key, stamp, version in [('old', '2000-01-01T00:00:00+00:00', 2),
                                                   ('today', self.now.isoformat(), 3)]],
            'next_cursor': None, 'has_more': False}
        self._finish_round()
        self.assertEqual(self.qmt.calls, [('acct-01', 'STOCK', kind) for kind in ('task', 'order', 'deal')])
        self.assertEqual([call[0][:3] for call in self.repo.finish_reconcile.call_args_list],
                         [('old', 2, False), ('today', 3, True)])
        health = self.runtime.health()
        self.assertFalse(health['history_coverage_complete'])
        self.assertFalse(health['history_query_available'])
        self.assertEqual(health['reconcile_query_scope'], 'CURRENT_DAY')
        self.assertTrue(health['recovery_complete'])
        self.assertIsNone(health['last_reconcile_error'])
        self.assertEqual(health['last_reconciled_at'], self.now.isoformat())
        finished = [fields for unused, message, fields in self.logs if message == 'RECONCILE_FINISHED']
        self.assertEqual(finished[0]['coverage_gap'], 'HISTORY_NOT_COVERED')
        self.assertIsNone(finished[1]['coverage_gap'])
        self.assertFalse(any(level == 'ERROR' for level, unused, fields in self.logs))

    def test_scope_uses_submit_date_and_rejects_old_or_missing_dates(self):
        day = self.now.date()
        doc = {'submission_status': 'CONFIRMED', 'created_at': '2000-01-01T00:00:00+00:00',
               'attempts': [{'kind': 'SUBMIT', 'created_at': self.now.isoformat()}]}
        self.assertTrue(_background_current_day_covered(doc, day))
        # UTC 前一日的 16:00 已是上海当日；不能按 UTC 日期误报跨日。
        doc['attempts'][0]['created_at'] = '2026-09-27T16:00:00+00:00'
        self.assertTrue(_background_current_day_covered(doc, day))
        for field in ('qmt_tasks', 'qmt_orders', 'fills'):
            doc[field] = [{'trading_day': '20260927'}]
            self.assertFalse(_background_current_day_covered(doc, day))
            doc.pop(field)
        doc['qmt_tasks'] = [{'trading_day': None}]
        self.assertTrue(_background_current_day_covered(doc, day))
        for field in ('qmt_orders', 'fills'):
            doc[field] = [{'trading_day': None}]
            self.assertFalse(_background_current_day_covered(doc, day))
            doc.pop(field)
        doc['attempts'][0]['created_at'] = '2026-09-27T15:59:59+00:00'
        self.assertFalse(_background_current_day_covered(doc, day))
        doc['attempts'][0]['created_at'] = None
        self.assertFalse(_background_current_day_covered(doc, day))

    def test_midnight_round_does_not_confirm_mixed_day_snapshots(self):
        self.background._reconcile_step()
        self.background.round['query_day'] -= dt.timedelta(days=1)
        self._finish_round()
        self.assertIsNone(self.runtime.last_reconciled_at)
        self.assertFalse(self.runtime.history_coverage_complete)
        self.assertFalse(self.runtime.recovery_complete)

    def test_checkpoint_paging_keeps_the_completed_query_date(self):
        docs = [{'order_id': key, 'submission_status': 'CONFIRMED',
                 'created_at': self.now.isoformat(), 'reconcile_round_fact_version': 1}
                for key in ('one', 'two')]
        self.repo.reconcile_round_batch.side_effect = [
            {'orders': docs[:1], 'next_cursor': 'one', 'has_more': True},
            {'orders': docs[1:], 'next_cursor': None, 'has_more': False}]
        self.background._reconcile_step()
        # 查询在第一天已结束，第二页只是数据库写入，不是第二天的新 QMT 查询。
        current = self.background.round
        current.update(stage=3, waiting=False, query_finished_at=self.now.isoformat())
        self.background._reconcile_step()
        self.now += dt.timedelta(days=1)
        self.background._reconcile_step()
        self.assertEqual([call[0][2] for call in self.repo.finish_reconcile.call_args_list], [True, True])

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
                        'created_at': self.now.isoformat(),
                        'reconcile_round_fact_version': 3}],
            'next_cursor': None, 'has_more': False}
        self._finish_round()
        self.repo.finish_reconcile.assert_called_once()
        self.assertEqual(self.repo.finish_reconcile.call_args[0][:3], ('one', 3, True))
        self.assertEqual(self.repo.finish_reconcile.call_args[1]['interval_seconds'], 30)


if __name__ == '__main__':
    unittest.main()
