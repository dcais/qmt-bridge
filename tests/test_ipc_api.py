"""HTTP compatibility and full HTTP -> real pipe -> scheduled fake-QMT checks."""
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid

from fastapi.testclient import TestClient

from service.ipc_bridge.app import BridgeHost, create_app
from service.ipc_bridge.config import Config
from service.ipc_bridge.transport.pipe.client import RemoteError
from service.ipc_bridge.qmt.executor import QmtRuntime


class FakeHost:
    def __init__(self):
        self.calls = []
        self.started = self.stopped = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def log(self, *args, **kwargs):
        pass

    def handle(self, method, params, verb):
        self.calls.append((method, params, verb))
        if method == "fail":
            raise RemoteError(409, "IDEMPOTENCY_CONFLICT", "different content")
        if method == "secret":
            raise RuntimeError("password=do-not-expose")
        return (202 if method == "submit_order" else 200), {"method": method, "params": params}


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.host = FakeHost()
        self.client = TestClient(create_app(Config(), self.host))
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def test_single_root_repeated_query_no_success_wrapper(self):
        r = self.client.get('/get_smart_algo_param/?algoList=a&algoList=b')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['params'], {'algoList': ['a', 'b']})
        self.assertNotIn('data', r.json())
        self.assertTrue(self.host.started)

    def test_post_compatibility_feed_ignores_query_order_rejects(self):
        r = self.client.post('/get_sector_list?unused=1', content='{}')
        self.assertEqual(r.status_code, 200)
        r = self.client.post('/account?unused=1', json={})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()['error']['code'], 'INVALID_PARAMS')

    def test_json_error_shape_and_size_guard(self):
        for raw, code in [('{', 'INVALID_JSON'), ('[]', 'INVALID_PARAMS'),
                          ('{"x":NaN}', 'INVALID_JSON')]:
            with self.subTest(raw=raw):
                r = self.client.post('/account', content=raw)
                self.assertEqual(r.status_code, 400)
                self.assertEqual(r.json()['error']['code'], code)
        r = self.client.post('/account', content='x' * (1024 * 1024 + 1))
        self.assertEqual(r.status_code, 413)

    def test_errors_and_methods_not_framework_defaults(self):
        r = self.client.get('/fail')
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()['error']['code'], 'IDEMPOTENCY_CONFLICT')
        for verb in ('put', 'patch', 'delete', 'options'):
            r = getattr(self.client, verb)('/account')
            self.assertEqual(r.status_code, 405)
            self.assertEqual(r.json()['error']['code'], 'METHOD_NOT_ALLOWED')
        self.assertEqual(self.client.post('/health', content='{').status_code, 405)

    def test_durable_status_is_preserved_and_unexpected_error_redacted(self):
        self.assertEqual(self.client.post('/submit_order', json={}).status_code, 202)
        r = self.client.get('/secret')
        self.assertEqual(r.status_code, 503)
        self.assertNotIn('password', r.text)


class ConfigTests(unittest.TestCase):
    def test_defaults_env_and_validation(self):
        cfg = Config.from_env({'QMT_BRIDGE_PORT': '18999', 'QMT_BRIDGE_ID': 'paper01',
                               'QMT_BRIDGE_ACCOUNT_ID': '000001'})
        self.assertEqual(cfg.port, 18999)
        self.assertEqual(cfg.account_id, '000001')
        self.assertEqual(cfg.pipe_name('order'), r'\\.\pipe\qmt-bridge.paper01.order')
        for changes in ({'QMT_BRIDGE_PORT': '0'}, {'QMT_BRIDGE_ID': '../bad'},
                        {'QMT_BRIDGE_PG_DATABASE': 'missing-other-keys'}):
            with self.assertRaises(ValueError):
                Config.from_env(changes)


@unittest.skipUnless(os.name == 'nt', 'Windows pipe integration')
class EndToEndReadOnlyTests(unittest.TestCase):
    def test_http_queries_health_and_shutdown_over_actual_pipes(self):
        with tempfile.TemporaryDirectory() as directory:
            cfg = Config(bridge_id='test_' + uuid.uuid4().hex, account_id='account',
                         log_directory=directory)
            host = BridgeHost(cfg)
            stop = threading.Event()
            peers = []
            threads = []

            def run(peer):
                try:
                    while not stop.is_set():
                        peer.tick()
                        stop.wait(.002)
                finally:
                    peer.close()

            def order(method, params):
                if method == 'order.capabilities':
                    return {'passorder': True}
                if method == 'order.query':
                    query = params['request']
                    return {'m_accountID': query['params']['accountId'], 'm_available': 12.5}
                raise AssertionError('no trading allowed in this test')

            def feed(method, params):
                self.assertEqual(method, 'get_sector_list')
                return ['sector']

            try:
                with TestClient(create_app(cfg, host)) as client:
                    for role, execute in [('feed', feed), ('order', order)]:
                        peer = QmtRuntime(role, cfg.bridge_id, cfg.account_id,
                                          cfg.pipe_name(role), execute)
                        peers.append(peer)
                        thread = threading.Thread(target=run, args=(peer,), daemon=True)
                        threads.append(thread)
                        thread.start()
                    deadline = time.monotonic() + 3
                    while not (host.feed_link.ready and host.order_link.ready):
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(.01)
                    r = client.get('/account?accountType=stock')
                    self.assertEqual(r.status_code, 200, r.text)
                    self.assertEqual(r.json(), {'m_accountID': 'account', 'm_available': 12.5})
                    r = client.get('/get_sector_list')
                    self.assertEqual(r.status_code, 200, r.text)
                    self.assertEqual(r.json(), ['sector'])
                    health = client.get('/health').json()
                    self.assertTrue(health['feed']['ready'])
                    self.assertTrue(health['order']['ready'])
                    self.assertFalse(health['accepting_orders'])
                    self.assertFalse(health['trading_configured'])
                    self.assertEqual(client.get('/health?extra=1').status_code, 400)
                    self.assertEqual(client.get('/no_such_method').status_code, 404)
                    self.assertEqual(client.get('/account?accountId=%20').status_code, 400)
            finally:
                stop.set()
                for thread in threads:
                    thread.join(3)
            self.assertEqual(host.lifecycle, 'STOPPED')
            self.assertTrue(all(not thread.is_alive() for thread in threads))
