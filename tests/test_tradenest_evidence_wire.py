# -*- coding: utf-8 -*-
"""Explicit opt-in cross-repository wire test; random loopback port, in-memory store only."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import threading
import unittest
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

from tests.test_order_sequence_void import VoidRepository, authorize, statements
from tests.order_runtime_helpers import OrderRuntime
from order_bridge.runtime import OrderRuntime as LegacyRuntime
from tools.order_sequence_void_import import import_void


@unittest.skipUnless(os.environ.get('BRIDGE_TEST_TRADENEST_ROOT'), 'explicit cross-repository source root is unset')
class TradeNestWireTests(unittest.TestCase):
    def test_real_readers_accept_persisted_void_tail_across_full_shadow_compact_and_recovery(self):
        source_root = Path(os.environ['BRIDGE_TEST_TRADENEST_ROOT']).resolve()
        if not (source_root / 'src/tradenest').is_dir():
            self.fail('explicit TradeNest source root is invalid')
        sys.path.insert(0, str(source_root / 'src'))
        self.addCleanup(lambda: sys.path.remove(str(source_root / 'src')))
        import httpx
        from tradenest.integrations.qmt_bridge.execution_observation import QmtReadOnlyExecutions
        from tradenest.integrations.qmt_bridge.sequence_void import verify_sequence_void
        from tradenest.runtime.broker_ingestion import BrokerCursor
        from tradenest.runtime.ownership import OwnershipIndex
        from tradenest.trading.identities import BrokerIdentity
        generated_path = Path(__file__).resolve().parents[1] / 'strategies/http_order.py'
        spec = importlib.util.spec_from_file_location('cross_generated_http_order', generated_path)
        generated = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generated)
        for runtime_type in (OrderRuntime, LegacyRuntime, generated.OrderRuntime):
            for mode in ('full', 'shadow', 'compact'):
                with self.subTest(runtime=runtime_type.__module__, mode=mode):
                    repo = VoidRepository()
                    for _ in range(4):
                        repo.append()
                    repo.state['events'].pop(4)
                    repo.void_schema = True
                    proof = statements(repo, 4)
                    authorize(repo, proof[0], proof[1])
                    imported = import_void(repo, *proof, operator='cross-test-importer', apply=True)['receipt']
                    runtime = runtime_type({}, None, repo.account_id, repository=repo)
                    class Handler(BaseHTTPRequestHandler):
                        def do_GET(self):
                            parsed = urlsplit(self.path)
                            params = {key: value[-1] for key, value in parse_qs(parsed.query).items()}
                            try:
                                status, value = runtime.handle(parsed.path.lstrip('/'), params, 'GET')
                            except Exception as exc:
                                status = getattr(exc, 'status', 500)
                                value = {'error': {'code': getattr(exc, 'code', type(exc).__name__), 'message': 'test failure'}}
                            payload = json.dumps(value, allow_nan=False).encode('utf-8')
                            self.send_response(status)
                            self.send_header('Content-Type', 'application/json')
                            self.send_header('Content-Length', str(len(payload)))
                            self.end_headers()
                            self.wfile.write(payload)
                        def log_message(self, *args):
                            pass
                    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
                    worker = threading.Thread(target=server.serve_forever, daemon=True)
                    worker.start()
                    base_url = 'http://127.0.0.1:' + str(server.server_port)
                    broker = BrokerIdentity(namespace='qmt-stock:cross-wire-test', execution_mode='SIMULATED',
                                            external_account_id=repo.account_id)
                    cursor = BrokerCursor(broker=broker, stream_id=repo.state['stream'], sequence=3)
                    reader = QmtReadOnlyExecutions(broker, clock=lambda: datetime.now(timezone.utc),
                        base_url=base_url, allow_local_read=True, evidence_v1=True,
                        include_void_receipts=True, journal_wire_mode=mode)
                    try:
                        first = reader.capture(cursor)
                        self.assertEqual([event.sequence for event in first.events], [4])
                        self.assertEqual(first.read_after, 4)
                        self.assertFalse(first.has_more)
                        plan = first.ingestion_plan(OwnershipIndex(()))
                        self.assertEqual(plan.proposed_cursor.sequence, 4)
                        self.assertFalse(plan.deliveries)
                        self.assertEqual(plan.new_receipts[0].reason, 'NON_FINANCIAL_SEQUENCE_VOID')
                        self.assertIn('SHARED_BROKER_ALLOCATION_UNVERIFIED', plan.blocking)
                        self.assertNotIn('EVIDENCE_SEQUENCE_VOID_INVALID', plan.blocking)
                        self.assertFalse(first.command_ready)
                        second = reader.capture(plan.proposed_cursor)
                        self.assertEqual(second.events, ())
                        self.assertEqual(second.read_after, 4)
                        self.assertTrue(reader.continuity(SimpleNamespace(cursor=plan.proposed_cursor), second,
                            committed_receipts=plan.new_receipts))
                        with httpx.Client(base_url=base_url, trust_env=False) as client:
                            response = client.get('/recovery_evidence_v1', params=dict(after=3, include_void_receipts='true'))
                            self.assertEqual(response.status_code, 200)
                            recovery = response.json()
                        self.assertFalse(recovery['has_more'])
                        self.assertEqual(recovery['recovery_status'], 'incomplete')
                        tail = recovery['events'][0]
                        self.assertEqual(tail['data'], imported)
                        verify_sequence_void(tail, broker, repo.state['stream'], 4)
                    finally:
                        reader.close()
                        server.shutdown()
                        server.server_close()
                        worker.join(timeout=3)

    def test_real_tradenest_notice_parser_verifies_bridge_original_core_and_material(self):
        source_root = Path(os.environ['BRIDGE_TEST_TRADENEST_ROOT']).resolve()
        sys.path.insert(0, str(source_root / 'src'))
        self.addCleanup(lambda: sys.path.remove(str(source_root / 'src')))
        from tests.test_order_event_correction import CorrectionRepository
        from service.ipc_bridge.order.event_correction import append_broker_correction_notice
        from service.ipc_bridge.order.compact_events import compact_source_ref
        from tradenest.integrations.qmt_bridge.correction_notice import parse_notice, verify_source
        from tradenest.integrations.qmt_bridge.execution_observation import _v1_event
        from tradenest.trading.identities import BrokerIdentity
        repo = CorrectionRepository()
        scope = dict(stream_id=repo.state['stream'], account_id=repo.account_id, account_type='STOCK')
        original = repo.journal_source(scope['stream_id'], 3)
        ref = compact_source_ref(scope, original)
        broker = BrokerIdentity(namespace='qmt-stock:cross-notice-test', execution_mode='SIMULATED',
                                external_account_id=repo.account_id)
        result = append_broker_correction_notice(repo, 'order-1', 4, 'cross-notice-1', ref,
            'Independent material requires review', dict(reference='retained:broker-material', sha256='b' * 64),
            'cross-test-reviewer', apply=True)
        wire = repo.journal_source(scope['stream_id'], 5)
        event = _v1_event(broker, scope['stream_id'], wire)
        notice, client = parse_notice(event)
        self.assertEqual(notice.notice_id, result['notice']['notice_id'])
        self.assertEqual(notice.evidence.sha256, 'b' * 64)
        source = SimpleNamespace(event=_v1_event(broker, scope['stream_id'], original),
            delivery=SimpleNamespace(fact=SimpleNamespace(client_order_id=client),
                                     ownership=SimpleNamespace(originating_run='retained-owned-run')))
        checked, run = verify_source(event, source)
        self.assertEqual(checked, notice)
        self.assertEqual(run, 'retained-owned-run')
        self.assertEqual(original, repo.journal_source(scope['stream_id'], 3))
        again = append_broker_correction_notice(repo, 'order-1', 4, 'cross-notice-1', ref,
            'Independent material requires review', dict(reference='new-path:same-material', sha256='b' * 64),
            'another-reviewer', apply=True)
        self.assertTrue(again['replayed'])
        self.assertEqual(repo.state['high'], 5)
