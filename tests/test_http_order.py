import importlib.util
import json
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import Mock, patch


STRATEGY = Path(__file__).resolve().parents[1] / 'strategies' / 'http_order.py'


def load_strategy(parameters=None):
    spec = importlib.util.spec_from_file_location('http_order_test', STRATEGY)
    module = importlib.util.module_from_spec(spec)
    module.__dict__.update(parameters or {})
    spec.loader.exec_module(module)
    return module


class Record:
    def __init__(self, **fields):
        self.__dict__.update(fields)


def ephemeral_listener(module):
    # Keep production port validation; bind test sockets to OS-selected ports.
    server_class = module.ThreadingHTTPServer
    return patch.object(module, 'ThreadingHTTPServer',
                        side_effect=lambda address, handler: server_class((address[0], 0), handler))


class OrderTests(unittest.TestCase):
    def setUp(self):
        self.m = load_strategy()
        self.log_patch = patch.object(self.m, 'log_message')
        self.log_patch.start()
        self.addCleanup(self.log_patch.stop)

    def dispatch(self, method, params=None):
        return self.m.dispatch_request(None, {'method': method, 'params': params or {}})

    def test_account_and_positions_preserve_fields_and_scope(self):
        calls = []
        def query(account, kind, data):
            calls.append((account, kind, data))
            return [Record(m_strAccountID=account, m_nVolume=200, private='omit'),
                    Record(m_nVolume=100)]
        self.m.get_trade_detail_data = query
        self.assertEqual(self.dispatch('account'),
                         {'m_strAccountID': '66027616', 'm_nVolume': 200})
        positions = self.dispatch('positions', {'accountId': 'other', 'accountType': 'credit'})
        self.assertEqual(len(positions), 2)
        self.assertEqual(calls, [('66027616', 'STOCK', 'account'), ('other', 'CREDIT', 'position')])

    def test_empty_account_is_distinct_from_empty_positions(self):
        self.m.get_trade_detail_data = Mock(return_value=[])
        self.assertEqual(self.dispatch('positions'), [])
        with self.assertRaises(self.m.OrderError) as raised:
            self.dispatch('account')
        self.assertEqual(raised.exception.status, 404)
        self.assertEqual(raised.exception.code, 'ACCOUNT_NOT_FOUND')

    def test_invalid_qmt_results_are_not_reported_as_empty_holdings(self):
        for value in (None, {}, 'failed'):
            self.m.get_trade_detail_data = Mock(return_value=value)
            for method in ('account', 'positions'):
                with self.subTest(method=method, value=value):
                    with self.assertRaises(self.m.OrderError) as raised:
                        self.dispatch(method)
                    self.assertEqual(raised.exception.code, 'INVALID_QMT_RESULT')
        self.m.get_smart_algo_param = Mock(return_value=None)
        with self.assertRaises(self.m.OrderError) as raised:
            self.dispatch('get_smart_algo_param')
        self.assertEqual(raised.exception.code, 'INVALID_QMT_RESULT')

    def test_algorithm_query_default_and_selection(self):
        result = {'VWAP': [{'key': 'rate', 'unit': '%', 'defaultValue': '20.00'}]}
        self.m.get_smart_algo_param = Mock(return_value=result)
        self.assertEqual(self.dispatch('get_smart_algo_param'), result)
        self.m.get_smart_algo_param.assert_called_with([])
        self.dispatch('get_smart_algo_param', {'algoList': ['VWAP', 'TWAP']})
        self.m.get_smart_algo_param.assert_called_with(['VWAP', 'TWAP'])

    def test_unavailable_algorithm_api(self):
        with self.assertRaises(self.m.OrderError) as raised:
            self.dispatch('get_smart_algo_param')
        self.assertEqual(raised.exception.status, 501)
        self.assertEqual(raised.exception.code, 'API_UNAVAILABLE')

    def test_invalid_parameters_do_not_call_qmt(self):
        query = self.m.get_trade_detail_data = Mock()
        algo = self.m.get_smart_algo_param = Mock()
        for method, params in [('account', {'accountId': 123}),
                               ('positions', {'accountType': 'INVALID'}),
                               ('account', {'unexpected': True}),
                               ('get_smart_algo_param', {'algoList': [1]}),
                               ('get_smart_algo_param', {'unexpected': True})]:
            with self.subTest(method=method, params=params):
                with self.assertRaises(self.m.OrderError) as raised:
                    self.dispatch(method, params)
                self.assertEqual(raised.exception.status, 400)
        query.assert_not_called()
        algo.assert_not_called()

    def test_nonfinite_values_and_broken_fields(self):
        self.m.get_trade_detail_data = Mock(return_value=[Record(m_value=float('nan'))])
        self.assertIsNone(self.dispatch('account')['m_value'])
        class Broken:
            @property
            def m_value(self):
                raise RuntimeError('cannot read')
        self.m.get_trade_detail_data.return_value = [Broken()]
        state = self.m._ORDER_STATE = self.m.OrderState()
        job = self.m.RequestJob({'method': 'account', 'params': {}})
        state.request_queue.put(job)
        self.m.process_http_requests(None)
        self.assertTrue(job.done.is_set())
        self.assertEqual(job.error_status, 500)
        self.assertEqual(state.request_queue.unfinished_tasks, 0)

    def test_expired_job_never_calls_qmt(self):
        self.m.get_trade_detail_data = Mock()
        state = self.m._ORDER_STATE = self.m.OrderState()
        job = self.m.RequestJob({'method': 'account', 'params': {}})
        job.deadline = time.monotonic() - 1
        state.request_queue.put(job)
        self.m.process_http_requests(None)
        self.assertTrue(job.done.is_set())
        self.assertEqual(job.error_status, 504)
        self.m.get_trade_detail_data.assert_not_called()

    def test_stop_releases_pending_requests(self):
        state = self.m._ORDER_STATE = self.m.OrderState()
        job = self.m.RequestJob({'method': 'positions', 'params': {}})
        state.request_queue.put(job)
        self.m.stop(Mock())
        self.assertTrue(job.done.is_set())
        self.assertEqual(job.error_status, 503)
        self.assertEqual(state.request_queue.unfinished_tasks, 0)

    def test_scheduler_registration_failure_cleans_up_port(self):
        context = Mock()
        context.schedule_run.side_effect = RuntimeError('registration failed')
        with ephemeral_listener(self.m):
            with self.assertRaises(RuntimeError):
                self.m.init(context)
        self.assertIsNone(self.m._ORDER_STATE)
        self.assertIsNone(self.m._ORDER_TIMER_ID)

    def test_tick_request_limit_leaves_remaining_work_queued(self):
        self.m.get_trade_detail_data = Mock(return_value=[])
        self.m.MAX_JOBS_PER_TICK = 2
        self.m.SCHEDULE_BUDGET_MILLISECONDS = 1000
        state = self.m._ORDER_STATE = self.m.OrderState()
        jobs = [self.m.RequestJob({'method': 'positions', 'params': {}}) for _ in range(3)]
        for job in jobs:
            state.request_queue.put(job)
        self.m.process_http_requests(None)
        self.assertEqual([job.done.is_set() for job in jobs], [True, True, False])
        self.m.process_http_requests(None)
        self.assertTrue(jobs[-1].done.is_set())


class RuntimeParameterTests(unittest.TestCase):
    def test_screenshot_lowercase_parameters_and_startup_log(self):
        for early in (True, False):
            with self.subTest(early=early):
                values = {'account_id': 8890763409.0, 'http_port': 8886.0}
                module = load_strategy(values if early else None)
                if not early:
                    module.__dict__.update(values)
                context = Mock()
                context.schedule_run.return_value = 'timer'
                with patch.object(module, 'log_message') as log:
                    with ephemeral_listener(module) as factory:
                        module.init(context)
                    try:
                        factory.assert_called_once_with(('127.0.0.1', 8886), module.OrderRequestHandler)
                        context.set_account.assert_called_once_with('8890763409')
                        actual_port = module._ORDER_STATE.server.server_address[1]
                        log.assert_called_with('INFO', 'QMT HTTP order listening',
                                               host='127.0.0.1', port=actual_port, account_id='8890763409')
                        module.get_trade_detail_data = Mock(return_value=[])
                        module.dispatch_request(None, {'method': 'positions', 'params': {}})
                        module.get_trade_detail_data.assert_called_with('8890763409', 'STOCK', 'position')
                    finally:
                        module.stop(context)

    def test_invalid_lowercase_does_not_fall_back(self):
        for values in ({'account_id': ''}, {'http_port': 0}):
            module = load_strategy(values)
            context = Mock()
            with self.assertRaises(ValueError):
                module.init(context)
            context.set_account.assert_not_called()

    def test_defaults_and_early_injection(self):
        defaults = load_strategy()
        self.assertEqual(defaults.ACCOUNT_ID, '66027616')
        self.assertEqual(defaults.HTTP_PORT, 8888)
        configured = load_strategy({'ACCOUNT_ID': '001234', 'HTTP_PORT': 8899})
        self.assertEqual(configured.ACCOUNT_ID, '001234')
        self.assertEqual(configured.HTTP_PORT, 8899)

    def test_numeric_account_and_port_conversion(self):
        module = load_strategy()
        for raw, expected in [('001234', '001234'), (1234, '1234'), (1234.0, '1234')]:
            self.assertEqual(module.runtime_account_id(raw), expected)
        for raw in (8888, 8888.0, '8888'):
            self.assertEqual(module.runtime_http_port(raw), 8888)

    def test_invalid_configuration_fails_before_account_or_socket_calls(self):
        for name, values in [('ACCOUNT_ID', ['', None, True, 0, -1, 1.5, float('nan'), float(2**53)]),
                             ('HTTP_PORT', [0, 65536, None, True, 1.5, '8888.0', float('inf')])]:
            for value in values:
                with self.subTest(name=name, value=value):
                    module = load_strategy({name: value})
                    context = Mock()
                    with patch.object(module, 'ThreadingHTTPServer') as server:
                        with self.assertRaises(ValueError):
                            module.init(context)
                        context.set_account.assert_not_called()
                        server.assert_not_called()
                    self.assertIsNone(module._ORDER_STATE)

    def test_late_injection_frozen_defaults_and_independent_instances(self):
        modules = []
        for account, port in [(1234.0, '8898'), ('005678', 8899.0)]:
            module = load_strategy()
            module.ACCOUNT_ID, module.HTTP_PORT = account, port
            log_patch = patch.object(module, 'log_message')
            log_patch.start()
            self.addCleanup(log_patch.stop)
            context = Mock()
            context.schedule_run.return_value = 'timer'
            with ephemeral_listener(module) as factory:
                module.init(context)
                factory.assert_called_once_with(('127.0.0.1', int(port)), module.OrderRequestHandler)
            self.addCleanup(module.stop, context)
            modules.append(module)
            expected = module.runtime_account_id(account)
            context.set_account.assert_called_once_with(expected)
            self.assertEqual(module._ORDER_STATE.account_id, expected)
            self.assertEqual(module._ORDER_STATE.http_port, int(port))
            module.ACCOUNT_ID = 'changed-after-start'
            module.get_trade_detail_data = Mock(return_value=[])
            module.dispatch_request(None, {'method': 'positions', 'params': {}})
            module.get_trade_detail_data.assert_called_with(expected, 'STOCK', 'position')
            module.dispatch_request(None, {'method': 'positions', 'params': {'accountId': 'explicit'}})
            module.get_trade_detail_data.assert_called_with('explicit', 'STOCK', 'position')
        self.assertNotEqual(modules[0]._ORDER_STATE.server.server_address,
                            modules[1]._ORDER_STATE.server.server_address)


class HttpOrderTests(unittest.TestCase):
    def setUp(self):
        self.m = load_strategy()
        self.log_patch = patch.object(self.m, 'log_message')
        self.log_patch.start()
        self.addCleanup(self.log_patch.stop)
        listener_patch = ephemeral_listener(self.m)
        listener_patch.start()
        self.addCleanup(listener_patch.stop)
        self.m.REQUEST_TIMEOUT_SECONDS = 0.15
        self.context = Mock()
        self.context.schedule_run.return_value = 'timer-id'
        self.m.init(self.context)
        self.state = self.m._ORDER_STATE
        self.base = 'http://127.0.0.1:{0}'.format(self.state.server.server_address[1])
        self.addCleanup(self.m.stop, self.context)

    def request(self, path, body=None, method=None):
        request = urllib.request.Request(self.base + path, data=body, method=method,
                                         headers={'Content-Type': 'application/json'})
        try:
            response = urllib.request.urlopen(request, timeout=3)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            raw = response.read()
            try:
                payload = json.loads(raw)
            except ValueError:
                payload = raw.decode('utf-8')
            return response.status, payload

    def scheduled_request(self, path, body=None):
        result = []
        worker = threading.Thread(target=lambda: result.append(self.request(path, body)))
        worker.start()
        deadline = time.monotonic() + 2
        while self.state.request_queue.empty() and worker.is_alive() and time.monotonic() < deadline:
            time.sleep(0.002)
        self.m.process_http_requests(self.context)
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertTrue(result)
        return result[0]

    def test_http_get_runs_qmt_only_on_scheduler_thread(self):
        callers = []
        def query(*args):
            callers.append(threading.get_ident())
            return [Record(m_cash=321.5)]
        self.m.get_trade_detail_data = query
        status, payload = self.scheduled_request('/account')
        self.assertEqual((status, payload), (200, {'m_cash': 321.5}))
        self.assertEqual(callers, [threading.get_ident()])

    def test_http_post_positions_and_algorithm_get_list(self):
        self.m.get_trade_detail_data = Mock(return_value=[])
        self.assertEqual(self.scheduled_request('/positions', b'{"accountId":"abc"}'), (200, []))
        self.m.get_trade_detail_data.assert_called_with('abc', 'STOCK', 'position')
        self.m.get_smart_algo_param = Mock(return_value={'VWAP': []})
        self.assertEqual(self.scheduled_request('/get_smart_algo_param?algoList=VWAP&algoList=TWAP'),
                         (200, {'VWAP': []}))
        self.m.get_smart_algo_param.assert_called_with(['VWAP', 'TWAP'])
        self.assertEqual(self.scheduled_request('/get_smart_algo_param', b'{"algoList":[]}'),
                         (200, {'VWAP': []}))
        self.m.get_smart_algo_param.assert_called_with([])

    def test_http_validation_and_timeout(self):
        for path, body in [('/account', b'[]'), ('/account', b'{'), ('/account', b'\xff')]:
            self.assertEqual(self.request(path, body)[0], 400)
        self.assertEqual(self.scheduled_request('/unknown')[0], 404)
        self.assertEqual(self.request('/account', method='DELETE')[0], 405)
        self.assertEqual(self.request('/account')[0], 504)

    def test_init_registers_and_stop_cancels_timer(self):
        self.context.schedule_run.assert_called_once()
        self.assertIs(self.context.schedule_run.call_args[0][0], self.m.process_http_requests)
        self.m.stop(self.context)
        self.context.cancel_schedule_run.assert_called_once_with('timer-id')

    def test_full_queue_rejects_without_qmt_call(self):
        self.m.get_trade_detail_data = Mock()
        for _ in range(self.m.QUEUE_MAX_SIZE):
            self.state.request_queue.put(self.m.RequestJob({'method': 'positions', 'params': {}}))
        status, payload = self.request('/positions')
        self.assertEqual(status, 429)
        self.assertEqual(payload['error']['code'], 'QUEUE_FULL')
        self.m.get_trade_detail_data.assert_not_called()

    def test_stop_unblocks_http_waiter_and_closes_listener(self):
        self.m.REQUEST_TIMEOUT_SECONDS = 3
        result = []
        worker = threading.Thread(target=lambda: result.append(self.request('/positions')))
        worker.start()
        deadline = time.monotonic() + 2
        while self.state.request_queue.empty() and time.monotonic() < deadline:
            time.sleep(0.002)
        self.assertFalse(self.state.request_queue.empty())
        self.m.stop(self.context)
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(result[0][0], 503)
        self.assertFalse(self.state.server_thread.is_alive())


if __name__ == '__main__':
    unittest.main()
