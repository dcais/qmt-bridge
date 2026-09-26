# -*- coding: utf-8 -*-
"""对账逐单 checkpoint 日志与节流的离线验证；Last modified: 2026-09-26。"""
import unittest
from unittest.mock import Mock

from order_bridge.runtime import OrderRuntime


class Clock(object):
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def document(order_id, version=1):
    return {'order_id': order_id, 'client_order_id': 'client-' + order_id,
            'version': version, 'reconcile_round_fact_version': version + 10,
            'last_reconcile_attempt_at': '2026-09-26T00:00:00+00:00',
            'reconcile_due_at': '2026-09-26T00:00:30+00:00',
            'submission_status': 'CONFIRMED'}


def entries(logs, message):
    return [fields for level, kind, fields in logs if level == 'INFO' and kind == message]


class ReconcileCheckpointLoggingTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.repo = Mock()
        self.repo.reconcile_history_start.return_value = None
        self.repo.finish_reconcile.return_value = True
        self.logs = []
        self.runtime = OrderRuntime({}, object(), 'acct-01', repository=self.repo,
                                    clock=self.clock, settings={'reconcile_interval_seconds': 30,
                                                                'reconcile_batch_size': 2},
                                    logger=lambda level, message, **fields:
                                    self.logs.append((level, message, fields)))
        self.background = self.runtime.background

    def prepare_round(self, begin_pages):
        self.repo.begin_reconcile_batch.side_effect = begin_pages
        for unused in begin_pages:
            self.background._reconcile_step()
        current = self.background.round
        self.assertIsNotNone(current)
        self.assertTrue(current['frozen'])
        # QMT 查询由原有离线测试覆盖；在此仅驱动 checkpoint 的后半段。
        current['stage'] = len(current['queries'])
        current['waiting'] = False
        return current['id']

    def test_each_page_logs_attempt_and_applied_finish_once_with_order_identity(self):
        first = [document('one', 3), document('two', 5)]
        last = [document('three', 7)]
        round_id = self.prepare_round([
            {'orders': first, 'next_cursor': 'next', 'has_more': True},
            {'orders': last, 'next_cursor': None, 'has_more': False}])
        self.repo.reconcile_round_batch.side_effect = [
            {'orders': first, 'next_cursor': 'two', 'has_more': True},
            {'orders': last, 'next_cursor': None, 'has_more': False}]
        self.background._reconcile_step()
        self.background._reconcile_step()
        attempts = entries(self.logs, 'RECONCILE_ATTEMPT')
        finished = entries(self.logs, 'RECONCILE_FINISHED')
        self.assertEqual([row['order_id'] for row in attempts], ['one', 'two', 'three'])
        self.assertEqual([row['order_id'] for row in finished], ['one', 'two', 'three'])
        for row, original in zip(attempts, first + last):
            self.assertEqual(row['account_id'], 'acct-01')
            self.assertEqual(row['client_order_id'], original['client_order_id'])
            self.assertEqual(row['round_id'], round_id)
            self.assertEqual(row['version'], original['version'])
            self.assertEqual(row['fact_version'], original['reconcile_round_fact_version'])
            self.assertEqual(row['last_reconcile_attempt_at'], original['last_reconcile_attempt_at'])
            self.assertEqual(row['reconcile_due_at'], original['reconcile_due_at'])
        for row, original in zip(finished, first + last):
            self.assertEqual(row['account_id'], 'acct-01')
            self.assertEqual(row['round_id'], round_id)
            self.assertEqual(row['source_version'], original['version'])
            self.assertNotIn('version', row)
            self.assertTrue(row['checkpoint_applied'])
            self.assertTrue(row['complete'])
            self.assertEqual(row['outcome'], 'COMPLETE')
        self.assertEqual(self.background.next_round, self.clock() + 30)
        self.background._reconcile_step()
        self.clock.now += 29
        self.background._reconcile_step()
        self.assertEqual(len(entries(self.logs, 'RECONCILE_ATTEMPT')), 3)
        self.assertEqual(len(entries(self.logs, 'RECONCILE_FINISHED')), 3)
        self.assertEqual(self.repo.begin_reconcile_batch.call_count, 2)
        self.assertEqual(self.repo.finish_reconcile.call_count, 3)

    def test_incomplete_and_stale_fact_have_distinct_outcomes(self):
        pending = document('incomplete')
        self.prepare_round([{'orders': [pending], 'next_cursor': None, 'has_more': False}])
        self.background.round['complete'] = False
        self.repo.reconcile_round_batch.return_value = {
            'orders': [pending], 'next_cursor': None, 'has_more': False}
        self.background._reconcile_step()
        first = entries(self.logs, 'RECONCILE_FINISHED')[-1]
        self.assertTrue(first['checkpoint_applied'])
        self.assertFalse(first['complete'])
        self.assertEqual(first['outcome'], 'INCOMPLETE')
        self.assertFalse(self.repo.finish_reconcile.call_args[0][2])

        self.clock.now += 30
        stale = document('stale')
        self.repo.begin_reconcile_batch.side_effect = None
        self.prepare_round([{'orders': [stale], 'next_cursor': None, 'has_more': False}])
        self.repo.reconcile_round_batch.return_value = {
            'orders': [stale], 'next_cursor': None, 'has_more': False}
        self.repo.finish_reconcile.return_value = False
        self.background._reconcile_step()
        second = entries(self.logs, 'RECONCILE_FINISHED')[-1]
        self.assertFalse(second['checkpoint_applied'])
        self.assertIsNone(second['complete'])
        self.assertEqual(second['outcome'], 'STALE_FACT_VERSION')
        self.assertTrue(self.repo.finish_reconcile.call_args[0][2])
        self.assertEqual([row['order_id'] for row in entries(self.logs, 'RECONCILE_ATTEMPT')],
                         ['incomplete', 'stale'])

    def test_repository_failure_has_no_false_success_log_and_logger_failure_isolated(self):
        failure = RuntimeError('begin failed')
        self.repo.begin_reconcile_batch.side_effect = failure
        with self.assertRaises(RuntimeError) as caught:
            self.background._reconcile_step()
        self.assertIs(caught.exception, failure)
        self.assertEqual(entries(self.logs, 'RECONCILE_ATTEMPT'), [])
        self.assertEqual(entries(self.logs, 'RECONCILE_FINISHED'), [])

        # 重新开始单独的离线轮次；日志回调故障不能阻断数据库进度。
        self.background.round = None
        one = document('one')
        self.repo.begin_reconcile_batch.side_effect = None
        self.repo.begin_reconcile_batch.return_value = {
            'orders': [one], 'next_cursor': None, 'has_more': False}
        self.runtime.logger = Mock(side_effect=RuntimeError('log sink failed'))
        self.background._reconcile_step()
        self.assertEqual(entries(self.logs, 'RECONCILE_ATTEMPT'), [])
        self.background.round['stage'] = len(self.background.round['queries'])
        self.background.round['waiting'] = False
        self.repo.reconcile_round_batch.return_value = {
            'orders': [one], 'next_cursor': None, 'has_more': False}
        finish_error = RuntimeError('finish failed')
        self.repo.finish_reconcile.side_effect = finish_error
        with self.assertRaises(RuntimeError) as caught:
            self.background._reconcile_step()
        self.assertIs(caught.exception, finish_error)
        self.assertEqual(entries(self.logs, 'RECONCILE_FINISHED'), [])

        self.repo.finish_reconcile.side_effect = None
        self.background._reconcile_step()
        self.assertIsNone(self.background.round)
        self.repo.finish_reconcile.assert_called()
        self.assertEqual(entries(self.logs, 'RECONCILE_FINISHED'), [])


if __name__ == '__main__':
    unittest.main()
