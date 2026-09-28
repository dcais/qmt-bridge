# -*- coding: utf-8 -*-
"""非阻塞调度与缓冲/授权边界；Last modified: 2026-09-28。"""
import datetime as dt
import queue
import threading
import time
import unittest
from unittest.mock import Mock, patch

from order_bridge.runtime import OrderRuntime
from order_bridge.common import OrderError, iso_datetime, utc_now


class NonblockingRuntimeTests(unittest.TestCase):
    def runtime(self):
        repo = Mock()
        repo.check_schema.return_value = {'ready': True, 'schema_version': 3}
        repo.acquire_executor.return_value = {'epoch': 7}
        repo.recover.return_value = {'next_cursor': None, 'has_more': False}
        repo.health.return_value = {'ready': True}
        repo.mark_reconcile_gap.return_value = {'next_cursor': None, 'has_more': False}
        repo.queued_orders.return_value = []
        repo.cancellation_orders.return_value = []
        repo.reconcile_history_start.return_value = None
        repo.begin_reconcile_batch.return_value = {'orders': [], 'next_cursor': None, 'has_more': False}
        repo.reconcile_round_batch.return_value = {'orders': [], 'next_cursor': None, 'has_more': False}
        runtime = OrderRuntime({}, object(), 'test', repository=repo, local_lock=Mock())
        runtime.adapter = Mock()
        runtime.adapter.query.return_value = []
        self.addCleanup(self.cleanup, runtime)
        return runtime

    def cleanup(self, runtime):
        if not runtime.background.started:
            # 单步测试只验证调度器，不启动持久化线程处理故意精简的假单据。
            return
        runtime.stop()
        self.assertTrue(runtime.stopped_event.wait(2))

    def wait(self, predicate):
        end = time.monotonic() + 2
        while not predicate() and time.monotonic() < end:
            time.sleep(.005)
        self.assertTrue(predicate())

    def ready(self, runtime):
        background = runtime.background
        background.db_ready = True
        background.grant = (runtime.instance_id, 7, runtime.clock() + 100)
        return background

    def test_initialize_and_tick_do_not_wait_for_schema_io(self):
        runtime = self.runtime()
        entered, release = threading.Event(), threading.Event()
        def blocked():
            entered.set()
            release.wait(2)
            return {'ready': True}
        runtime.repo.check_schema.side_effect = blocked
        self.addCleanup(release.set)
        began = time.monotonic()
        runtime.initialize()
        self.assertLess(time.monotonic() - began, .1)
        self.assertTrue(entered.wait(1))
        began = time.monotonic()
        self.assertEqual(runtime.tick(), 0)
        self.assertLess(time.monotonic() - began, .1)
        release.set()

    def test_health_is_a_pure_cache_read(self):
        runtime = self.runtime()
        runtime.repo.health.side_effect = AssertionError('SQL forbidden')
        runtime.health()
        runtime.repo.health.assert_not_called()

    def test_expired_authority_aborts_before_qmt(self):
        runtime = self.runtime()
        background = self.ready(runtime)
        background._enqueue('submit', document={}, pending_key=('submit', 'a'))
        background.grant = (runtime.instance_id, 7, runtime.clock() - 1)
        self.assertEqual(runtime.tick(), 1)
        runtime.adapter.submit.assert_not_called()
        self.assertEqual(background.results.get_nowait()['status'], 'ABORTED_NO_CALL')

    def test_per_callback_quota_shared_across_single_steps(self):
        runtime = self.runtime()
        background = self.ready(runtime)
        runtime.adapter.snapshot.side_effect = lambda value: value
        for index in range(15):
            background._enqueue('submit', document={}, pending_key=('submit', str(index)))
        budget = {'submit': 0, 'cancel': 0}
        for unused in range(20):
            runtime.tick(budget=budget, max_actions=1)
        self.assertEqual(runtime.adapter.submit.call_count, 10)
        self.assertEqual(background.queues['submit'].qsize(), 5)

    def test_reservation_exhaustion_prevents_new_directive(self):
        runtime = self.runtime()
        background = self.ready(runtime)
        for unused in range(128):
            self.assertTrue(background.capacity.acquire(False))
        self.assertFalse(background._enqueue('submit', document={}))
        self.assertEqual(runtime.tick(), 0)
        runtime.adapter.submit.assert_not_called()

    def test_stop_does_not_release_lock_during_qmt_call(self):
        runtime = self.runtime()
        runtime.initialize()
        self.wait(lambda: runtime.initialized)
        background = runtime.background
        entered, release = threading.Event(), threading.Event()
        def submit(document):
            entered.set()
            release.wait(2)
        runtime.adapter.submit.side_effect = submit
        runtime.adapter.snapshot.side_effect = lambda value: value
        background._enqueue('submit', document={'order_id': 'a'}, pending_key=('submit', 'a'))
        callback = threading.Thread(target=runtime.tick)
        callback.start()
        self.assertTrue(entered.wait(1))
        began = time.monotonic()
        runtime.stop()
        self.assertLess(time.monotonic() - began, .1)
        self.assertEqual(runtime.health()['lifecycle'], 'STOPPING')
        runtime.local_lock.release.assert_not_called()
        release.set()
        callback.join(1)
        self.assertTrue(runtime.stopped_event.wait(2))
        runtime.local_lock.release.assert_called_once()

    def test_authority_connection_only_used_by_its_thread(self):
        runtime = self.runtime()
        owners = []
        runtime.repo.check_executor.side_effect = lambda: owners.append(threading.current_thread().name)
        runtime.repo.close.side_effect = lambda: owners.append(threading.current_thread().name)
        runtime.initialize()
        self.wait(lambda: runtime.initialized)
        runtime.health()
        runtime.tick()
        runtime.stop()
        self.assertTrue(runtime.stopped_event.wait(2))
        self.assertTrue(owners)
        self.assertEqual(set(owners), {'order-authority'})

    def test_failed_result_commit_retains_result_and_pauses_trading(self):
        runtime = self.runtime()
        runtime.initialize()
        self.wait(lambda: runtime.initialized)
        failed, release = threading.Event(), threading.Event()
        def update(*args):
            if not release.is_set():
                failed.set()
                raise RuntimeError('database blocked')
            return {}
        runtime.repo.update_order.side_effect = update
        self.addCleanup(release.set)
        background = runtime.background
        background._enqueue('submit', document={'order_id': 'a'}, pending_key=('submit', 'a'))
        runtime.adapter.snapshot.side_effect = lambda value: None
        runtime.tick()
        self.assertTrue(failed.wait(1))
        self.assertFalse(background.db_ready)
        self.assertEqual(runtime.adapter.submit.call_count, 1)
        # 原结果在后台局部保留；队列已出队不代表持久化成功。
        runtime.stop()
        self.assertFalse(runtime.stopped_event.wait(.05))
        release.set()
        self.assertTrue(runtime.stopped_event.wait(2))
        self.assertEqual(runtime.adapter.submit.call_count, 1)

    def test_blocked_result_sql_pauses_but_does_not_consume_directives(self):
        runtime = self.runtime()
        runtime.initialize()
        self.wait(lambda: runtime.initialized)
        entered, release = threading.Event(), threading.Event()
        def blocked(*args):
            entered.set()
            release.wait(2)
            return {}
        runtime.repo.update_order.side_effect = blocked
        self.addCleanup(release.set)
        background = runtime.background
        runtime.adapter.snapshot.side_effect = lambda value: None
        background._enqueue('submit', document={'order_id': 'a'}, pending_key=('submit', 'a'))
        runtime.tick(max_actions=1)
        self.assertTrue(entered.wait(1))
        background._enqueue('submit', document={'order_id': 'b'}, pending_key=('submit', 'b'))
        began = time.monotonic()
        runtime.tick()
        self.assertLess(time.monotonic() - began, .1)
        self.assertEqual(runtime.adapter.submit.call_count, 1)
        self.assertEqual(background.queues['submit'].qsize(), 1)
        release.set()

    def test_stop_waits_for_inflight_http_admission_off_qmt_thread(self):
        runtime = self.runtime()
        runtime.initialize()
        self.wait(lambda: runtime.initialized)
        entered, release = threading.Event(), threading.Event()
        runtime.recovery_complete = True
        runtime.repo.get_order.side_effect = OrderError(404, 'ORDER_NOT_FOUND', 'missing')
        def accept(request):
            entered.set()
            release.wait(2)
            return True, {'order_id': 'accepted', 'client_order_id': request['client_order_id']}
        runtime.repo.accept_order.side_effect = accept
        request = {'client_order_id': 'stop-race', 'account_id': 'test', 'order_type': 'SINGLE',
                   'symbol': '600000.SH', 'side': 'BUY', 'sizing_type': 'QUANTITY', 'quantity': 100,
                   'price_type': 'LIMIT', 'limit_price': '10.50', 'execution': {'type': 'DIRECT'}}
        failures = []
        def http_commit():
            try:
                runtime.handle('submit_order', request, 'POST')
            except Exception as exc:
                failures.append(exc)
        handler = threading.Thread(target=http_commit)
        handler.start()
        self.assertTrue(entered.wait(1))
        self.addCleanup(release.set)
        began = time.monotonic()
        runtime.stop()
        self.assertLess(time.monotonic() - began, .1)
        self.assertFalse(runtime.stopped_event.wait(.05))
        runtime.local_lock.release.assert_not_called()
        release.set()
        handler.join(1)
        self.assertFalse(failures)
        self.assertTrue(runtime.stopped_event.wait(2))

    def test_order_expiry_after_claim_does_not_consume_submit_quota(self):
        runtime = self.runtime()
        background = self.ready(runtime)
        background._enqueue('submit', document={'submit_before': '2000-01-01T00:00:00+00:00'},
                            pending_key=('submit', 'expired'))
        budget = {'submit': 0, 'cancel': 0}
        self.assertEqual(runtime.tick(budget=budget), 1)
        self.assertEqual(budget['submit'], 0)
        runtime.adapter.submit.assert_not_called()
        result = background.results.get_nowait()
        self.assertEqual(result['status'], 'ABORTED_NO_CALL')
        self.assertEqual(result['error']['code'], 'ORDER_EXPIRED')

    def test_reconcile_uses_prequery_per_order_versions(self):
        runtime = self.runtime()
        background = self.ready(runtime)
        background.round = {'id': 'round-a', 'stage': 3, 'queries': ['task', 'order', 'deal'],
                            'query_day': utc_now().astimezone(dt.timezone(dt.timedelta(hours=8))).date(),
                            'history_complete': True,
                            'frozen': True, 'cursor': None, 'waiting': False, 'complete': True,
                            'live_complete': True, 'generation': 0}
        runtime.repo.reconcile_round_batch.return_value = {
            'orders': [{'order_id': 'changed', 'submission_status': 'CONFIRMED',
                        'created_at': iso_datetime(),
                        'reconcile_round_fact_version': 4, 'fact_version': 5},
                       {'order_id': 'unchanged', 'submission_status': 'CONFIRMED',
                        'created_at': iso_datetime(),
                        'reconcile_round_fact_version': 8, 'fact_version': 8}],
            'next_cursor': None, 'has_more': False}
        background._reconcile_step()
        calls = runtime.repo.finish_reconcile.call_args_list
        self.assertEqual(calls[0][0][:3], ('changed', 4, True))
        self.assertEqual(calls[1][0][:3], ('unchanged', 8, True))

    def test_ordinary_and_duplicate_callbacks_do_not_create_global_gap(self):
        runtime = self.runtime()
        runtime.adapter.snapshot.side_effect = lambda value: value
        runtime.observe('order', {'id': 'one'})
        runtime.observe('order', {'id': 'one'})
        self.assertEqual(runtime.background.fact_generation, 0)
        self.assertFalse(runtime.observation_gap)

    def test_gate_change_after_dequeue_preserves_directive(self):
        runtime = self.runtime()
        background = self.ready(runtime)
        background._enqueue('submit', document={}, pending_key=('submit', 'race'))
        source = background.queues['submit']
        original = source.get_nowait
        def dequeue():
            result = original()
            background.db_ready = False
            return result
        with patch.object(source, 'get_nowait', side_effect=dequeue):
            self.assertEqual(runtime.tick(), 0)
        self.assertEqual(source.qsize(), 1)
        self.assertEqual(background.results.qsize(), 0)
        self.assertFalse(background.consumed)
        runtime.adapter.submit.assert_not_called()

    def test_lost_advisory_authority_revokes_grant(self):
        runtime = self.runtime()
        runtime.repo.check_executor.side_effect = [True, RuntimeError('connection lost')]
        runtime.initialize()
        self.wait(lambda: runtime.initialized)
        self.wait(lambda: runtime.background.authority_done.is_set())
        self.assertIsNone(runtime.background.grant)
        self.assertFalse(runtime.background.authorized())

    def test_more_than_64_cancel_claims_have_reserved_queue_slots(self):
        runtime = self.runtime()
        background = self.ready(runtime)
        documents = [{'order_id': str(i), 'active_cancel_request_id': 'c' + str(i)} for i in range(65)]
        runtime.repo.cancellation_orders.side_effect = [documents[i:i + 10] for i in range(0, 65, 10)]
        def claim(order_id, action, authority):
            return True, {'order_id': order_id, 'attempts': [dict(action, attempt_id='attempt' + order_id)]}
        runtime.repo.claim_cancel.side_effect = claim
        with patch('order_bridge.background.pending_cancellations', return_value=[{'kind': 'CANCEL_ORDER', 'target_id': 'target'}]):
            for unused in range(7):
                background._dispatch()
        self.assertEqual(runtime.repo.claim_cancel.call_count, 65)
        self.assertEqual(background.queues['cancel'].qsize(), 65)
        self.assertEqual(len(background.pending), 65)
        runtime.adapter.cancel_action.return_value = True
        runtime.tick()
        self.assertEqual(runtime.adapter.cancel_action.call_count, 10)
        self.assertEqual(background.queues['cancel'].qsize(), 55)

    def test_callback_backlog_yields_to_background_dispatch(self):
        runtime = self.runtime()
        runtime.adapter.snapshot.side_effect = lambda value: value
        runtime.repo.ingest_observation.return_value = None
        for index in range(1000):
            runtime.observe('order', {'id': str(index)})
        serviced = threading.Event()
        remaining = []
        def dispatch():
            remaining.append(runtime.observations.qsize())
            serviced.set()
        runtime.background._dispatch = dispatch
        runtime.recovery_complete = True
        runtime.initialize()
        self.assertTrue(serviced.wait(2))
        self.assertGreater(remaining[0], 0)

    def test_history_gap_is_sampled_without_extra_queries_after_all_freeze_pages(self):
        runtime = self.runtime()
        background = self.ready(runtime)
        runtime.repo.begin_reconcile_batch.side_effect = [
            {'orders': [{'order_id': 'first'}], 'next_cursor': 'next', 'has_more': True},
            {'orders': [{'order_id': 'late-history'}], 'next_cursor': None, 'has_more': False}]
        background._reconcile_step()
        runtime.repo.reconcile_history_start.assert_not_called()
        runtime.repo.reconcile_history_start.return_value = '2000-01-01T00:00:00+00:00'
        background._reconcile_step()
        runtime.repo.reconcile_history_start.assert_called_once()
        queries = background.round['queries']
        self.assertEqual(queries, ['task', 'order', 'deal'])
        self.assertFalse(background.round['history_complete'])

    def test_elapsed_deadline_performs_no_qmt_stage(self):
        runtime = self.runtime()
        background = self.ready(runtime)
        background._enqueue('query', query_kind='order')
        self.assertEqual(runtime.tick(deadline=runtime.clock() - 1), 0)
        runtime.adapter.query.assert_not_called()


if __name__ == '__main__':
    unittest.main()
