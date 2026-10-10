import importlib.util
import time
import datetime as dt
from unittest.mock import Mock, patch
import json
import io
from contextlib import redirect_stdout
import unittest
from pathlib import Path

from service.ipc_bridge.qmt_runtime.executor import QmtRuntime, DeferredResult


class Endpoint:
    def __init__(self, *args, **kwargs):
        self.connected = True
        self.frames = []
        self.sent = []
    def poll(self):
        frames, self.frames = self.frames, []
        return frames
    def send(self, frame):
        self.sent.append(frame)
    def close(self):
        self.connected = False


class QmtRuntimeTests(unittest.TestCase):
    def setUp(self):
        quiet = patch('service.ipc_bridge.qmt_runtime.executor.console_log')
        quiet.start()
        self.addCleanup(quiet.stop)
        self.now = [0.0]
        self.calls = []
        self.runtime = QmtRuntime('order', 'test', '123', 'pipe',
                                  lambda method, params: self.calls.append(method),
                                  endpoint_factory=Endpoint, clock=lambda: self.now[0], event_limit=2)
        self.runtime.tick()
        self.endpoint = self.runtime.endpoint
        self.endpoint.frames.append(dict(type='welcome', protocol=1, role='order',
                                         bridge_id='test', account_id='123', session_id='session'))
        self.runtime.tick()
    def frame(self, **frame):
        frame.setdefault('session_id', 'session')
        self.endpoint.frames.append(frame)
        self.runtime.tick()
    def request(self, ident='r', method='order.submit'):
        self.frame(type='request', request_id=ident, method=method, params={},
                   authority=['bridge', 1], expires_at=time.time()+10)
    def grant(self):
        self.frame(type='authority_grant', challenge_id=self.runtime.challenge_id,
                   authority=['bridge', 1], remaining_seconds=0.5)
    def test_no_authority_no_dispatch_and_replay_consumed(self):
        self.request()
        self.grant()
        self.request()
        self.assertEqual([], self.calls)
        self.request('new')
        self.assertEqual(['order.submit'], self.calls)
    def test_delayed_grant_does_not_extend_local_deadline(self):
        challenge = self.runtime.challenge_id
        self.now[0] = 0.51
        self.frame(type='authority_grant', challenge_id=challenge, authority=['bridge', 1], remaining_seconds=0.5)
        self.request()
        self.assertEqual([], self.calls)
    def test_prepare_fenced_queries_remain_available(self):
        self.request(method='order.prepare_step')
        self.request('query', 'order.query')
        self.assertEqual(['order.query'], self.calls)
    def test_events_ack_and_disconnect_retention(self):
        self.runtime.observe('order', {'a': 1})
        self.runtime.tick()
        self.frame(type='ack', seq=1, boot_id='wrong')
        self.assertEqual(1, len(self.runtime.events))
        self.frame(type='ack', seq=1, boot_id=self.runtime.boot_id)
        self.assertEqual(0, len(self.runtime.events))
        self.runtime.observe('deal', {})
        self.runtime.disconnect()
        self.assertEqual(2, self.runtime.events[0]['seq'])
    def test_overflow_failclosed_query_kept(self):
        self.grant()
        for _ in range(3):
            self.runtime.observe('order', {})
        self.request()
        self.request('query', 'order.query')
        self.assertTrue(self.runtime.gap)
        self.assertEqual(['order.query'], self.calls)
    def test_remaining_lease_never_extends_from_arrival(self):
        self.now[0] = 0.08
        self.frame(type='authority_grant', challenge_id=self.runtime.challenge_id,
                   authority=['bridge', 1], remaining_seconds=0.1)
        self.assertEqual(0.1, self.runtime.authority_until)
        self.now[0] = 0.11
        self.request()
        self.assertEqual([], self.calls)

    def test_close_during_execute_defers_endpoint_release_and_stops_queue(self):
        executed = []
        def execute(method, params):
            executed.append(method)
            self.runtime.close()
            self.assertTrue(self.endpoint.connected)
        self.runtime.execute = execute
        for ident in ('first', 'second'):
            self.endpoint.frames.append(dict(type='request', session_id='session',
                request_id=ident, method='order.query', params={}, expires_at=time.time()+10))
        self.runtime.tick()
        self.assertEqual(['order.query'], executed)
        self.assertFalse(self.endpoint.connected)
        self.assertIsNone(self.runtime.endpoint)
        self.runtime.tick()
        self.assertIsNone(self.runtime.endpoint)

    def test_batch_advances_once_per_tick(self):
        steps = []
        def step():
            steps.append(1)
            return len(steps) == 3, len(steps)
        self.runtime.execute = lambda method, params: DeferredResult(step)
        self.request('batch', 'feed.batch')
        self.assertEqual(1, len(steps))
        self.runtime.tick()
        self.assertEqual(2, len(steps))
        self.runtime.tick()
        self.assertEqual(3, len(steps))
        self.assertEqual(3, [x for x in self.endpoint.sent if x.get('request_id') == 'batch'][0]['payload'])

    def test_deadline_and_capacity(self):
        self.frame(type='request', request_id='expired', method='order.query', params={}, expires_at=0)
        self.runtime.request_limit = 1
        self.grant()
        self.request('first', 'order.submit')
        self.request('full', 'order.submit')
        self.request('read', 'order.query')
        self.request('read', 'order.query')
        self.assertEqual(['order.submit', 'order.query', 'order.query'], self.calls)
        self.assertEqual({'first'}, self.runtime.seen)

    def test_pending_batch_duplicate_does_not_dispatch_again(self):
        calls = []
        def execute(method, params):
            calls.append(method)
            return DeferredResult(lambda: (False, None))
        self.runtime.execute = execute
        self.request('batch', 'feed.batch')
        self.request('batch', 'feed.batch')
        self.assertEqual(['feed.batch'], calls)
        self.assertEqual({'batch'}, self.runtime.pending_ids)


class EntryTests(unittest.TestCase):
    def test_console_metadata_excludes_payload_and_bounds_strings(self):
        from service.ipc_bridge.qmt_runtime.executor import console_log
        output = io.StringIO()
        with redirect_stdout(output):
            console_log('INFO', 'QMT call returned', request_id='x'*1000,
                        elapsed_ms=1.23456, payload={'secret': True},
                        qmt_parameters={'account': 'private'}, return_count=2)
        record = json.loads(output.getvalue())
        self.assertEqual(256, len(record['request_id']))
        self.assertNotIn('payload', record)
        self.assertNotIn('qmt_parameters', record)
        self.assertEqual(1.235, record['elapsed_ms'])

    def test_panel_parameter_normalization(self):
        from service.ipc_bridge.qmt_runtime.entrypoint import _account_id, _positive_integer, start
        self.assertEqual('66027616', _account_id(66027616.0))
        self.assertEqual('00003', _account_id(' 00003 '))
        self.assertEqual(10, _positive_integer(10.0, 'interval'))
        for value in (False, '', 1.5, 0, float('nan')):
            with self.assertRaises(ValueError):
                _account_id(value)
            with self.assertRaises(ValueError):
                _positive_integer(value, 'interval')
        for role in ('feed', 'order'):
            with self.assertRaises(ValueError):
                start(Mock(), {'account_id': '', 'bridge_id': 'ok'}, role, None)
            with self.assertRaises(ValueError):
                start(Mock(), {'account_id': '1', 'bridge_id': 'bad.name'}, role, None)

    def test_business_deadlines_at_submit_boundary(self):
        from service.ipc_bridge.qmt_runtime.order.entrypoint import _submit_document
        from service.ipc_bridge.ipc_protocol.order_common import OrderError
        adapter = Mock()
        now = dt.datetime(2026, 10, 10, 1, 0, tzinfo=dt.timezone.utc)
        cases = [
            ({'submit_before': now.isoformat()}, 'ORDER_EXPIRED'),
            ({'request': {'execution': {'type': 'SMART', 'start_at': '2026-10-10T08:00:00+08:00',
              'end_at': '2026-10-10T09:00:00+08:00'}}}, 'SMART_WINDOW_EXPIRED'),
            ({'request': {'execution': {'type': 'SMART', 'start_at': '2026-10-11T08:00:00+08:00',
              'end_at': '2026-10-11T10:00:00+08:00'}}}, 'SMART_DATE_UNSUPPORTED')]
        for document, code in cases:
            with self.assertRaises(OrderError) as caught:
                _submit_document(adapter, document, now=now)
            self.assertEqual(code, caught.exception.code)
        adapter.submit.assert_not_called()
        document = {'request': {'execution': {'type': 'SMART', 'start_at': '2026-10-10T08:00:00+08:00',
                    'end_at': '2026-10-10T10:00:00+08:00'}}}
        _submit_document(adapter, document, now=now)
        adapter.submit.assert_called_once_with(document)

    def test_stop_signals_before_cancel_then_releases(self):
        from service.ipc_bridge.qmt_runtime import entrypoint
        runtime = QmtRuntime('feed', 'b', 'a', 'pipe', lambda *args: None,
                             endpoint_factory=Endpoint)
        runtime.tick()
        endpoint = runtime.endpoint
        class Context:
            def cancel_schedule_run(inner, timer):
                self.assertTrue(runtime.closed)
                self.assertTrue(endpoint.connected)
                self.assertEqual('timer', timer)
        entrypoint._RUNTIME, entrypoint._TIMER = runtime, 'timer'
        entrypoint.stop(Context())
        self.assertFalse(endpoint.connected)
        self.assertIsNone(entrypoint._RUNTIME)


class BundleTests(unittest.TestCase):
    def test_bundles_current_and_execute_without_source_loading(self):
        root = Path(__file__).resolve().parents[1]
        spec = importlib.util.spec_from_file_location('builder', root / 'tools/build_ipc_strategies.py')
        builder = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(builder)
        for role in ('feed', 'order'):
            content = builder.render(role)
            self.assertEqual(builder.normalized(content.decode('gbk')),
                             builder.normalized((root / 'strategies' / ('ipc_' + role + '.py')).read_bytes().decode('gbk')))
            namespace = {}
            exec(compile(content, '<bundle>', 'exec'), namespace)
            self.assertTrue(callable(namespace['init']))
        with self.assertRaises(ValueError):
            builder.audit('import socket', 'unsafe')
        with self.assertRaises(ValueError):
            builder.audit('open("data")', 'unsafe')


if __name__ == '__main__':
    unittest.main()
