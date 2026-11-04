"""Offline fixture verification of the opt-in journal and single-source routes."""
import copy
import json
import unittest

from service.ipc_bridge.order.compact_events import compact_json, materialize_event
from service.ipc_bridge.ipc_protocol.order_common import OrderError
from order_bridge.runtime import OrderRuntime as NativeRuntime
from tests.order_runtime_helpers import OrderRuntime
from tests.test_order_evidence import MemoryEvidenceRepository


class CompactJournalTests(unittest.TestCase):
    def setUp(self):
        self.repo = MemoryEvidenceRepository()
        self.repo.append()
        for seq in (2, 3):
            self.repo.append()
            old = copy.deepcopy(self.repo.state['events'][seq - 1]['data'])
            self.repo.state['events'][seq]['data'] = old
            old.update(version=seq + 2)
        first, second, third = [row['data'] for row in self.repo.state['events'].values()]
        for item in (first, second, third):
            item['raw_attachment'] = 'immutable-' * 3000
        second.update(fills=[dict(trade_id='t1', quantity=12, raw={'fee': None})],
                      unassociated_evidence=[{'reason': 'CONFLICTING_FILL_IDENTITY'}])
        third.update(fills=[dict(trade_id='t1', quantity=11, raw={'fee': 0.0})], sync_status='INCOMPLETE')
        self.runtime = OrderRuntime({}, None, self.repo.account_id, repository=self.repo)

    def request(self, route, **params):
        return self.runtime.handle(route, params, 'GET')[1]

    def test_both_runtime_routes_roundtrip_and_verify_historical_source(self):
        for runtime in (self.runtime, NativeRuntime({}, None, self.repo.account_id, repository=self.repo)):
            self.runtime = runtime
            self.assertEqual(self.request('capabilities')['order_events_compact_v1'], {
                'mode': 'READ_ONLY', 'default_wire_mode': 'full', 'supported_wire_modes': ['full', 'shadow', 'compact'],
                'compact_primary_opt_in': True, 'grants_trading_authority': False, 'wire_format': 'ORDER_EVENT_DELTA_V1',
                'source_route': 'order_event_source_v1', 'paired_cut': 'COMPACT_START_CURSOR_V1'})
            v1 = self.request('order_events_evidence_v1')['events']
            page = self.request('order_events_compact_v1', limit=1)
            last = materialize_event(page, page['events'][0])
            self.assertEqual(last, v1[0])
            page = self.request('order_events_compact_v1', cursor=page['next_cursor'], limit=2)
            self.assertEqual(page['wire_format'], 'ORDER_EVENT_DELTA_V1')
            for index, wire in enumerate(page['events'], 1):
                ref = wire['base_ref']
                proof = self.request('order_event_source_v1', **{k: str(v) for k, v in ref.items()})
                self.assertEqual(proof['event'], last)
                last = materialize_event(page, wire, proof['event'])
                self.assertEqual(compact_json(last), compact_json(v1[index]))
                self.assertLess(len(json.dumps(wire)), len(json.dumps(last)) // 8)
            self.assertEqual(self.request('recovery_evidence_v1')['events'], v1)

    def test_paired_cut_survives_append_before_first_and_between_later_compact_pages(self):
        for runtime in (self.runtime, NativeRuntime({}, None, self.repo.account_id, repository=self.repo)):
            self.runtime = runtime
            first = self.request('order_events_evidence_v1', after=1, limit=1, pair_compact='true')
            self.repo.append()
            compact = self.request('order_events_compact_v1', cursor=first['compact_start_cursor'], limit=1)
            self.assertEqual(compact['high_watermark'], first['high_watermark'])
            self.assertEqual(compact['events'][0]['event_id'], first['events'][0]['event_id'])
            self.repo.append()
            last = self.request('order_events_compact_v1', cursor=compact['next_cursor'])
            last_full = self.request('order_events_evidence_v1', cursor=first['next_cursor'])
            self.assertEqual(last['next_after'], last_full['next_after'])
            self.assertEqual(last['next_after'], first['high_watermark'])
            self.assertFalse(last['has_more'])
            with self.assertRaises(Exception) as caught:
                self.request('recovery_evidence_v1', cursor=first['compact_start_cursor'])
            self.assertEqual(caught.exception.code, 'EVIDENCE_CURSOR_INVALID')

    def test_paired_cut_rejects_changed_anchor_and_sequence_mode(self):
        first = self.request('order_events_evidence_v1', after=0, limit=1, pair_compact=True)
        cursor = first['compact_start_cursor']
        with self.assertRaises(OrderError) as caught:
            self.request('order_events_compact_v1', cursor=cursor, include_void_receipts=True)
        self.assertEqual(caught.exception.code, 'EVIDENCE_CURSOR_INVALID')
        self.repo.state['events'][first['high_watermark']]['data']['version'] += 1
        with self.assertRaises(OrderError) as caught:
            self.request('order_events_compact_v1', cursor=cursor)
        self.assertEqual(caught.exception.code, 'EVIDENCE_ANCHOR_CHANGED')

    def test_changed_missing_foreign_and_wrong_base_fail_closed(self):
        page = self.request('order_events_compact_v1', after=1, limit=1)
        wire = page['events'][0]
        with self.assertRaises(ValueError):
            materialize_event(page, wire, None)
        for field in ('sha256', 'order_id', 'stream_id', 'account_id'):
            with self.assertRaises(OrderError):
                self.request('order_event_source_v1', **dict(wire['source_ref'], **{field:'changed'}))
        self.repo.state['events'].pop(1)
        with self.assertRaises(OrderError):
            self.request('order_event_source_v1', **wire['base_ref'])
        with self.assertRaises(OrderError):
            self.request('order_events_evidence_v1', cursor=page['next_cursor'])
        with self.assertRaises(OrderError):
            self.runtime.handle('order_events_compact_v1', {}, 'POST')

    def test_interleaved_orders_and_unknown_value_types_remain_distinct(self):
        a, b, c = [row['data'] for row in self.repo.state['events'].values()]
        b['order_id'] = 'another-order'
        a.update(extra=True, signed=0.0, gone='field')
        c.update(extra=1, signed=-0.0)
        page = self.request('order_events_compact_v1')
        v1 = self.request('order_events_evidence_v1')['events']
        wires = page['events']
        self.assertIsNone(wires[1]['base_ref'])
        rebuilt = materialize_event(page, wires[2], v1[0])
        self.assertEqual(compact_json(rebuilt), compact_json(v1[2]))
        tampered = copy.deepcopy(wires[2])
        tampered['changes'].pop()
        with self.assertRaises(ValueError):
            materialize_event(page, tampered, v1[0])


class CompactHttpRoutes(unittest.TestCase):
    def test_ipc_http_uses_journal_rows_for_both_routes(self):
        from fastapi.testclient import TestClient
        from service.ipc_bridge.app import BridgeHost, create_app
        from service.ipc_bridge.config import Config
        from unittest.mock import Mock

        repo = MemoryEvidenceRepository()
        repo.append()
        repo.append()
        host = Mock()
        host.lifecycle = 'RUNNING'
        host.order = OrderRuntime({}, None, repo.account_id, repository=repo)
        host.handle.side_effect = lambda method, params, verb: BridgeHost.handle(host, method, params, verb)
        with TestClient(create_app(Config(), host)) as http:
            compact = http.get('/order_events_compact_v1', params={'after': 1}).json()
            ref = compact['events'][0]['base_ref']
            source = http.get('/order_event_source_v1', params=ref)
            self.assertEqual(source.status_code, 200)
            self.assertEqual(source.json()['event']['event_id'], 1)
            self.assertEqual(http.post('/order_events_compact_v1', json={}).status_code, 405)


class GeneratedCompactHttpRoutes(unittest.TestCase):
    from tests.test_http_order import HttpOrderTests as _http
    setUp = _http.setUp
    request = _http.request

    def test_generated_http_strategy_exposes_the_same_historical_lookup(self):
        from urllib.parse import urlencode

        repo = MemoryEvidenceRepository()
        repo.append()
        repo.append()
        self.state.runtime.repo = repo
        self.state.runtime.account_id = repo.account_id
        self.addCleanup(setattr, self.state.runtime, 'repo', None)
        status, page = self.request('/order_events_compact_v1?after=1')
        self.assertEqual(status, 200)
        ref = page['events'][0]['base_ref']
        status, proof = self.request('/order_event_source_v1?' + urlencode(ref))
        self.assertEqual(status, 200)
        self.assertEqual(proof['source_ref'], ref)
        self.assertEqual(proof['event']['event_id'], 1)


class CompactVoidTests(unittest.TestCase):
    def test_void_is_self_contained_and_immutable_without_order_identity(self):
        import hashlib
        from order_bridge.compact_events import COMPACT_VOID, project_event, materialize_event, compact_json
        from service.ipc_bridge.order.compact_events import project_event as ipc_project

        scope = dict(stream_id='s', account_id='a', account_type='STOCK')
        receipt = dict(scope, kind=COMPACT_VOID, receipt_version=1, event_seq=2,
                       source_statement={'kind': 'RESERVED_VOID'}, approval_statement={'approved': True},
                       authorization={'id': 'synthetic'}, **{'import': {'imported_at':'at', 'operator':'test'}})
        receipt['receipt_sha256'] = hashlib.sha256(compact_json(receipt).encode('utf-8')).hexdigest()
        row = dict(event_id=2, type=COMPACT_VOID, recorded_at='at', data=receipt)
        wire = project_event(scope, row)
        self.assertEqual(wire, ipc_project(scope, row))
        self.assertNotIn('order_id', wire['source_ref'])
        self.assertNotIn('order_version', wire['source_ref'])
        self.assertNotIn('changes', wire)
        self.assertEqual(materialize_event(scope, wire), row)
        wire['receipt']['source_statement'] = {'kind':'changed'}
        with self.assertRaises(ValueError):
            materialize_event(scope, wire)
