# -*- coding: utf-8 -*-
"""Isolated evidence contract tests. No PostgreSQL or native QMT connection."""
import copy
import threading
import time
import unittest
import uuid
from unittest.mock import Mock, patch

from service.ipc_bridge.order.common import OrderError, new_order_document
from service.ipc_bridge.order.evidence import evidence_fee, evidence_sample
from service.ipc_bridge.order.repository import PostgresRepository
from service.ipc_bridge.order.runtime import OrderRuntime
from tests import test_http_order as http_tests
Record = http_tests.Record


class MemoryEvidenceCursor:
    """Model a read transaction at the repository boundary, including concurrent append."""
    def __init__(self, store):
        self.store = store
        self.result = []
        self.sql = []

    def execute(self, sql, params=()):
        self.sql.append((sql, params))
        if sql.startswith('SET TRANSACTION'):
            self.snapshot = copy.deepcopy(self.store.state)
            self.result = []
            return
        state = getattr(self, 'snapshot', self.store.state)
        account, account_type = self.store.account_id, self.store.account_type
        if 'account_runtime r' in sql:
            self.result = [(state['stream'], state['high'], state['epoch'], state['instance'])]
        elif 'min(event_seq)' in sql:
            self.result = [(min(state['events']) if state['events'] else None,)]
        elif 'event_seq=%s' in sql:
            row = state['events'].get(params[-1])
            self.result = [(copy.deepcopy(row['data']), row['type'], row['recorded_at'])] if row else []
        elif 'count(*)' in sql:
            self.result = [(sum(params[-2] < seq <= params[-1] for seq in state['events']),)]
            if self.store.on_count:
                hook, self.store.on_count = self.store.on_count, None
                hook()
        elif 'ORDER BY event_seq LIMIT' in sql:
            after, high, limit = params[-3:]
            self.result = [(seq, row['type'], row['recorded_at'], copy.deepcopy(row['data']))
                           for seq, row in sorted(state['events'].items()) if after < seq <= high][:limit]
        elif 'ORDER BY order_id LIMIT' in sql:
            self.result = [(copy.deepcopy(row),) for row in state['pending'][:params[-1]]]
        else:
            raise AssertionError('unexpected SQL: ' + sql)
        self.assert_scope(params, account, account_type)

    def assert_scope(self, params, account, account_type):
        if params[:2] != (account_type, account):
            raise AssertionError('account query scope mismatch')

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return self.result


class MemoryEvidenceRepository(PostgresRepository):
    def __init__(self):
        super().__init__({}, 'fake-account')
        self.state = dict(stream=str(uuid.uuid4()), high=0, epoch=1, instance='fake-executor', events={}, pending=[])
        self.on_count = None
        self.transactions = []
        self.failed = False

    def repo_run(self, callback, mutation=False):
        if self.failed:
            raise OrderError(503, 'PERSISTENCE_UNAVAILABLE', 'fake failure')
        if mutation:
            raise AssertionError('evidence must not mutate storage')
        cur = MemoryEvidenceCursor(self)
        result = callback(cur)
        self.transactions.append(cur.sql)
        return result

    def append(self, version=None):
        seq = self.state['high'] + 1
        doc = new_order_document(dict(client_order_id='c1', account_id=self.account_id,
                                      order_type='SINGLE', symbol='510300.SH', side='BUY',
                                      quantity=100, execution=dict(type='DIRECT')))
        doc.update(order_id='order-1', version=version or seq)
        self.state['events'][seq] = dict(event_id=seq, type='QMT_OBSERVATION',
            recorded_at='2026-10-08T01:30:00Z', data=doc)
        self.state['high'] = seq
        return doc

    def get_order(self, client):
        if client != 'c1':
            raise OrderError(404, 'ORDER_NOT_FOUND', 'missing original order')
        return copy.deepcopy(self.state['events'][self.state['high']]['data'])

    def events(self, after=0, limit=100):
        rows = [copy.deepcopy(row) for seq, row in sorted(self.state['events'].items()) if seq > after]
        return dict(events=rows[:limit], next_after=rows[min(len(rows), limit)-1]['event_id'] if rows else after,
                    has_more=len(rows)>limit)


class JournalEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.repo = MemoryEvidenceRepository()
        self.runtime = OrderRuntime({}, None, self.repo.account_id, repository=self.repo)
        for _ in range(4):
            self.repo.append()

    def query(self, **params):
        return self.runtime.handle('order_events_evidence_v1', params, 'GET')[1]

    def assert_code(self, code, action):
        with self.assertRaises(OrderError) as caught:
            action()
        self.assertEqual(caught.exception.code, code)

    def test_fixed_high_watermark_excludes_concurrent_append_in_and_between_pages(self):
        self.repo.on_count = self.repo.append
        first = self.query(limit=2)
        self.assertEqual([r['event_id'] for r in first['events']], [1, 2])
        self.assertEqual(first['high_watermark'], 4)
        self.repo.append()
        second = self.query(cursor=first['next_cursor'], limit=2)
        self.assertEqual([r['event_id'] for r in second['events']], [3, 4])
        self.assertEqual(second['high_watermark'], 4)
        self.assertFalse(second['has_more'])
        self.assertTrue(all(sql[0][0] == 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY'
                            for sql in self.repo.transactions))
        self.assertEqual(self.query(after=4)['high_watermark'], 6)

    def test_replaying_page_is_stable_and_legacy_response_shape_is_preserved(self):
        first = self.query(limit=1)
        token = first['next_cursor']
        self.assertEqual(self.query(cursor=token)['events'], self.query(cursor=token)['events'])
        legacy = self.runtime.handle('order_events', dict(after=0, limit=1), 'GET')[1]
        self.assertEqual(set(legacy), {'events', 'next_after', 'has_more'})
        self.assertNotIn('record_id', str(first['events']))

    def test_restart_preserves_stream_but_invalidates_ephemeral_cursor(self):
        first = self.query(limit=1)
        self.runtime = OrderRuntime({}, None, self.repo.account_id, repository=self.repo)
        self.assert_code('EVIDENCE_CURSOR_INVALID', lambda: self.query(cursor=first['next_cursor']))
        self.assertEqual(self.query()['stream_id'], first['stream_id'])

    def test_rebuild_and_epoch_changes_reject_old_cursor(self):
        for key, replacement in [('stream', str(uuid.uuid4())), ('epoch', 2), ('instance', 'replacement')]:
            first = self.query(limit=1)
            self.repo.state[key] = replacement
            self.assert_code('EVIDENCE_STREAM_CHANGED', lambda: self.query(cursor=first['next_cursor']))

    def test_rollback_and_changed_anchor_reject_old_cursor(self):
        first = self.query(limit=1)
        self.repo.state['events'][4]['data']['version'] = 99
        self.assert_code('EVIDENCE_ANCHOR_CHANGED', lambda: self.query(cursor=first['next_cursor']))
        self.repo.state['events'].pop(4)
        self.repo.state['high'] = 3
        self.assert_code('EVIDENCE_ANCHOR_CHANGED', lambda: self.query(cursor=first['next_cursor']))

    def test_internal_gap_retention_gap_and_invalid_bounds_fail_closed(self):
        self.repo.state['events'].pop(2)
        self.assert_code('EVIDENCE_JOURNAL_GAP', self.query)
        self.repo.state['events'].pop(1)
        self.assert_code('EVIDENCE_RETENTION_GAP', self.query)
        self.assert_code('EVIDENCE_ROLLBACK_OR_INVALID_RANGE', lambda: self.query(after=99))

    def test_empty_journal_has_explicit_floor_and_unknown_broker_coverage(self):
        self.repo.state.update(high=0, events={})
        result = self.query()
        self.assertEqual((result['retained_from'], result['high_watermark'], result['events']), (1, 0, []))
        self.assertEqual(result['coverage']['broker_complete'], 'unknown')
        self.assertEqual(result['restore_lineage_proof'], 'unsupported')

    def test_forged_expired_and_cross_route_cursor_are_rejected(self):
        first = self.query(limit=1)
        token = first['next_cursor']
        self.assert_code('EVIDENCE_CURSOR_INVALID', lambda: self.query(cursor=token[:-1] + ('a' if token[-1] != 'a' else 'b')))
        with patch('service.ipc_bridge.order.evidence.time.time', return_value=time.time() + 901):
            self.assert_code('EVIDENCE_CURSOR_INVALID', lambda: self.query(cursor=token))
        self.assert_code('EVIDENCE_CURSOR_INVALID', lambda: self.runtime.handle('recovery_evidence_v1', dict(cursor=token), 'GET'))
        self.assert_code('INVALID_PARAMS', lambda: self.query(cursor=token, after=0))
        for key in ('stream_id', 'sample_id', 'high_watermark', 'executor_epoch'):
            self.assert_code('INVALID_PARAMS', lambda: self.query(**{key: 'caller-assertion'}))

    def test_missing_migration_and_failed_store_do_not_return_evidence(self):
        self.repo.state['stream'] = None
        self.assert_code('EVIDENCE_SCHEMA_REQUIRED', self.query)
        self.repo.failed = True
        self.assert_code('PERSISTENCE_UNAVAILABLE', self.query)

    def test_native_disconnection_or_out_of_order_versions_never_prove_broker_completeness(self):
        self.repo.append(version=1)
        self.runtime.observation_gap = True
        result = self.query()
        self.assertEqual(result['events'][-1]['data']['version'], 1)
        self.assertTrue(result['runtime_health']['observation_gap'])
        self.assertEqual(result['coverage']['broker_complete'], 'unknown')
        self.assertIsNone(result['coverage']['provable_broker_dates'])

    def test_recovery_binds_pending_state_separately_and_keeps_unknown_original_id(self):
        doc = self.repo.get_order('c1')
        doc.update(submission_status='UNKNOWN', attempts=[dict(attempt_id='a', kind='SUBMIT', status='UNKNOWN', executor_epoch=1)])
        self.repo.state['pending'] = [doc, copy.deepcopy(doc)]
        result = self.runtime.handle('recovery_evidence_v1', dict(limit=1), 'GET')[1]
        self.assertTrue(result['pending_truncated'])
        self.assertEqual(result['pending_as_of_high_watermark'], 4)
        self.assertEqual(result['pending_requests'][0]['client_order_id'], 'c1')
        self.assertEqual(result['pending_requests'][0]['unresolved_attempts'][0]['executor_epoch'], 1)
        self.assertEqual(result['recovery_status'], 'incomplete')
        self.assertEqual(result['instruction'], 'QUERY_ORIGINAL_ORDER_ID_DO_NOT_RESUBMIT')
        self.assertIsNone(result['sample_evidence'])
        self.assertIsNone(result['provable_broker_dates'])

    def test_recovery_samples_require_server_capture_and_current_executor_identity(self):
        sample = evidence_sample('account', self.repo.account_id, 'STOCK', {}, 'start', 'end')
        sample['event_watermark'] = dict(before=self.repo.evidence_marker(), after=self.repo.evidence_marker())
        sample['event_relation'] = 'non_atomic_bracket'
        self.runtime.record_evidence_sample(sample)
        result = self.runtime.handle('recovery_evidence_v1', {}, 'GET')[1]
        self.assertEqual(result['sample_evidence']['account']['sample_id'], sample['sample_id'])
        self.assertEqual(result['sample_evidence_consistency'], 'non_atomic')
        self.repo.state['epoch'] += 1
        self.assertIsNone(self.runtime.handle('recovery_evidence_v1', {}, 'GET')[1]['sample_evidence'])

    def test_unknown_or_regressed_sample_brackets_are_not_bound_to_recovery(self):
        for relation, before_high in [('unknown', 4), ('non_atomic_bracket', 5)]:
            sample = evidence_sample('positions', self.repo.account_id, 'STOCK', [], 'start', 'end')
            before = self.repo.evidence_marker()
            before['high_watermark'] = before_high
            sample.update(event_relation=relation, event_watermark=dict(before=before, after=self.repo.evidence_marker()))
            self.runtime.record_evidence_sample(sample)
            self.assertIsNone(self.runtime.handle('recovery_evidence_v1', {}, 'GET')[1]['sample_evidence'])

    def test_returned_cancel_still_requires_fact_confirmation(self):
        doc = self.repo.get_order('c1')
        doc['attempts'] = [dict(attempt_id='cancel-a', kind='CANCEL_ORDER', status='RETURNED', executor_epoch=1)]
        self.repo.state['pending'] = [doc]
        result = self.runtime.handle('recovery_evidence_v1', {}, 'GET')[1]
        self.assertEqual(result['pending_requests'][0]['unresolved_attempts'][0]['status'], 'RETURNED')

    def test_sample_outside_frozen_event_range_is_not_attached(self):
        first = self.runtime.handle('recovery_evidence_v1', dict(limit=1), 'GET')[1]
        self.repo.append()
        marker = self.repo.evidence_marker()
        sample = evidence_sample('positions', self.repo.account_id, 'STOCK', [], 'start', 'end')
        sample.update(event_relation='non_atomic_bracket', event_watermark=dict(before=marker, after=marker))
        self.runtime.record_evidence_sample(sample)
        result = self.runtime.handle('recovery_evidence_v1', dict(cursor=first['next_cursor']), 'GET')[1]
        self.assertEqual(result['high_watermark'], 4)
        self.assertEqual(result['pending_as_of_high_watermark'], 5)
        self.assertIsNone(result['sample_evidence'])

    def test_malformed_asset_fields_remain_unknown_with_raw_values_preserved(self):
        for invalid in (True, '100', float('nan'), None):
            raw = dict(m_dAvailable=invalid)
            result = evidence_sample('account', self.repo.account_id, 'STOCK', raw, 'start', 'end')
            self.assertIsNone(result['assets']['available']['value'])
            self.assertEqual(result['assets']['available']['status'], 'unknown')

    def test_final_database_restore_is_explicitly_unprovable_without_external_witness(self):
        saved = copy.deepcopy(self.repo.state)
        stream = self.query()['stream_id']
        self.repo.append()
        self.repo.state = saved
        self.runtime = OrderRuntime({}, None, self.repo.account_id, repository=self.repo)
        result = self.query()
        self.assertEqual(result['stream_id'], stream)
        self.assertEqual(result['restore_lineage_proof'], 'unsupported')

    def test_fees_remain_unknown_when_aggregate_commission_turnover_or_raw_fees_change(self):
        doc = self.repo.get_order('c1')
        doc['fills'] = [dict(trade_id='t1', raw=dict(m_dTradeAmount=1000, m_dCommission=5))]
        for fee in (5, 8, 0, None):
            doc['fills'][0]['raw']['m_dCommission'] = fee
            result = evidence_fee(doc)
            self.assertEqual(result['status'], 'pending')
            self.assertIsNone(result['cumulative_amount'])
            self.assertIsNone(result['revision'])
            self.assertEqual(result['fills'][0]['finality'], 'unknown')


class SampleEvidenceHttpTests(unittest.TestCase):
    setUp = http_tests.HttpOrderTests.setUp
    request = http_tests.HttpOrderTests.request
    scheduled_request = http_tests.HttpOrderTests.scheduled_request

    def test_old_account_and_positions_shapes_remain_unchanged(self):
        self.m.get_trade_detail_data = Mock(return_value=[Record(m_dAvailable=12)])
        self.assertEqual(self.scheduled_request('/account'), (200, dict(m_dAvailable=12)))
        self.assertEqual(self.scheduled_request('/positions'), (200, [dict(m_dAvailable=12)]))

    def test_financial_changes_have_distinct_samples_and_no_atomic_cash_or_currency(self):
        self.m.get_trade_detail_data = Mock(side_effect=[[Record(m_dAvailable=100, m_dFrozenCash=5, m_dBalance=999)],
                                                       [Record(m_dAvailable=80, m_dFrozenCash=25)]])
        first = self.scheduled_request('/account_evidence_v1')[1]
        second = self.scheduled_request('/account_evidence_v1')[1]
        self.assertNotEqual(first['sample_id'], second['sample_id'])
        self.assertEqual(first['assets']['available']['value'], 100)
        self.assertEqual(second['assets']['frozen']['value'], 25)
        self.assertIsNone(first['assets']['cash']['value'])
        self.assertIsNone(first['assets']['currency'])
        self.assertEqual(first['consistency'], 'non_atomic')
        self.assertLessEqual(first['started_at'], first['finished_at'])
        self.assertEqual(first['event_relation'], 'unknown')

    def test_markers_run_outside_scheduler_and_bind_only_same_account(self):
        repo = MemoryEvidenceRepository()
        self.state.runtime.account_id = repo.account_id
        self.state.account_id = repo.account_id
        self.state.runtime.repo = repo
        self.addCleanup(setattr, self.state.runtime, 'repo', None)
        callers = []
        original = repo.evidence_marker
        def marker():
            callers.append(threading.get_ident())
            return original()
        repo.evidence_marker = marker
        self.m.get_trade_detail_data = Mock(return_value=[])
        result = self.scheduled_request('/positions_evidence_v1')[1]
        self.assertEqual(result['event_relation'], 'non_atomic_bracket')
        self.assertTrue(all(caller != threading.get_ident() for caller in callers))
        self.assertEqual(result['raw'], [])
        other = self.scheduled_request('/positions_evidence_v1?accountId=other')[1]
        self.assertEqual(other['event_relation'], 'unknown')
        self.assertIsNone(other['event_watermark']['before'])

    def test_marker_failure_does_not_promote_sample_to_journal_proof(self):
        repo = MemoryEvidenceRepository()
        repo.failed = True
        self.state.runtime.repo = repo
        self.state.runtime.account_id = repo.account_id
        self.state.account_id = repo.account_id
        self.addCleanup(setattr, self.state.runtime, 'repo', None)
        self.m.get_trade_detail_data = Mock(return_value=[Record(m_dAvailable=0)])
        status, result = self.scheduled_request('/account_evidence_v1')
        self.assertEqual(status, 200)
        self.assertEqual(result['event_relation'], 'unknown')
        self.assertIsNone(result['event_watermark']['after'])

    def test_client_proof_parameters_and_post_evidence_are_rejected(self):
        self.assertEqual(self.request('/account_evidence_v1?sample_id=caller')[0], 400)
        self.assertEqual(self.request('/positions_evidence_v1', b'{}')[0], 405)


class EvidenceHealthTests(unittest.TestCase):
    def runtime(self):
        repo = Mock()
        repo.check_schema.return_value = dict(ready=True, schema_version=3)
        repo.acquire_executor.return_value = dict(epoch=7)
        repo.recover.return_value = dict(next_cursor=None, has_more=False)
        repo.mark_reconcile_gap.return_value = dict(next_cursor=None, has_more=False)
        repo.health.return_value = dict(ready=True)
        repo.queued_orders.return_value = repo.cancellation_orders.return_value = []
        repo.begin_reconcile_batch.return_value = dict(orders=[], next_cursor=None, has_more=False)
        repo.reconcile_round_batch.return_value = dict(orders=[], next_cursor=None, has_more=False)
        repo.reconcile_history_start.return_value = None
        runtime = OrderRuntime({}, None, 'fake', repository=repo, local_lock=Mock())
        runtime.adapter = Mock()
        runtime.adapter.query.return_value = []
        return runtime

    def test_persistence_recovery_clears_current_error_but_keeps_history(self):
        runtime = self.runtime()
        background = runtime.background
        runtime.last_error = background.persistence_error = 'PERSISTENCE_UNAVAILABLE'
        background.last_sample = runtime.clock()
        background._service()
        self.assertIsNone(runtime.health()['error_code'])
        self.assertEqual(runtime.health()['last_error'], 'PERSISTENCE_UNAVAILABLE')
        runtime.repo.health.assert_called_once()

    def test_authority_loss_is_not_cleared_by_successful_persistence_probe(self):
        runtime = self.runtime()
        runtime.background.authority_error = 'EXECUTOR_LOCK_LOST'
        runtime.background.persistence_error = 'PERSISTENCE_UNAVAILABLE'
        runtime.background._service()
        self.assertEqual(runtime.health()['error_code'], 'EXECUTOR_LOCK_LOST')
        self.assertFalse(runtime.health()['accepting_orders'])

    def test_actual_background_recovery_failure_then_success_without_restart(self):
        runtime = self.runtime()
        release = threading.Event()
        failed = threading.Event()
        def recover(**kwargs):
            if not release.is_set():
                failed.set()
                raise OrderError(503, 'PERSISTENCE_UNAVAILABLE', 'fake outage')
            return dict(next_cursor=None, has_more=False)
        runtime.repo.recover.side_effect = recover
        runtime.initialize()
        try:
            self.assertTrue(failed.wait(2))
            deadline = time.monotonic() + 2
            while runtime.health()['error_code'] is None and time.monotonic() < deadline:
                time.sleep(.005)
            self.assertEqual(runtime.health()['error_code'], 'PERSISTENCE_UNAVAILABLE')
            self.assertFalse(runtime.health()['recovery_complete'])
            release.set()
            deadline = time.monotonic() + 2
            while not runtime.recovery_complete and time.monotonic() < deadline:
                runtime.tick()
                time.sleep(.005)
            self.assertTrue(runtime.recovery_complete)
            self.assertIsNone(runtime.health()['error_code'])
            self.assertEqual(runtime.health()['last_error'], 'PERSISTENCE_UNAVAILABLE')
            runtime.adapter.submit.assert_not_called()
            runtime.adapter.cancel.assert_not_called()
        finally:
            release.set()
            runtime.stop()
            self.assertTrue(runtime.stopped_event.wait(2))
