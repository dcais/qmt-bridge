# -*- coding: utf-8 -*-
"""Append-only correction provenance and offline notice checks; no database or QMT."""
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from service.ipc_bridge.ipc_protocol.order_common import OrderError, fingerprint, iso_datetime
from order_bridge.common import OrderError as LegacyOrderError
from service.ipc_bridge.order.event_correction import append_broker_correction_notice, correction_source_refs
from service.ipc_bridge.order.compact_events import compact_source_ref
from tests.test_order_sequence_void import VoidCursor, VoidRepository, LegacyVoidRepository
from tools.order_admin import broker_correction_notice, parser


class CorrectionCursor(VoidCursor):
    def execute(self, sql, params=()):
        if sql.startswith('SELECT event_seq FROM') and 'ORDER BY event_seq DESC LIMIT 1' in sql:
            self.sql.append((sql, params))
            rows = [seq for seq, event in self.store.state['events'].items() if event['data']['order_id'] == params[-1]]
            self.result = [(max(rows),)] if rows else []
        else:
            super().execute(sql, params)


class CorrectionStoreMixin:
    def __init__(self):
        super().__init__()
        self.saved = []
        for _ in range(4):
            self.append()
        self.current = copy.deepcopy(self.state['events'][4]['data'])
        self.current.update(filled_quantity=0, filled_amount='0')

    def repo_run(self, callback, mutation=False):
        cur = CorrectionCursor(self)
        before, current = copy.deepcopy(self.state), copy.deepcopy(self.current)
        try:
            return callback(cur)
        except Exception:
            self.state, self.current = before, current
            raise
        finally:
            self.transactions.append(cur.sql)

    def repo_load(self, cur, order_id, **kwargs):
        return copy.deepcopy(self.current) if self.current['order_id'] == order_id else None

    def repo_save(self, cur, document, event_type, child_fields=None):
        document['version'] += 1
        document['updated_at'] = iso_datetime()
        self.state['high'] += 1
        seq = self.state['high']
        self.state['events'][seq] = dict(event_id=seq, type=event_type,
                                       recorded_at=document['updated_at'], data=copy.deepcopy(document))
        self.current = copy.deepcopy(document)
        self.saved.append((event_type, child_fields))
        return document


class CorrectionRepository(CorrectionStoreMixin, VoidRepository):
    pass


class LegacyCorrectionRepository(CorrectionStoreMixin, LegacyVoidRepository):
    pass


class CorrectionNoticeTests(unittest.TestCase):
    repository_type = CorrectionRepository

    def setUp(self):
        self.repo = self.repository_type()
        self.scope = dict(stream_id=self.repo.state['stream'], account_id=self.repo.account_id, account_type='STOCK')
        self.ref = compact_source_ref(self.scope, self.repo.journal_source(self.scope['stream_id'], 3))
        self.evidence = dict(reference='immutable:broker-settlement-review', sha256='b' * 64)

    def call(self, **overrides):
        params = dict(order_id='order-1', expected_version=4, notice_id='broker-review-1', source_ref=self.ref,
                      reason='Independent broker material needs review', evidence=self.evidence, operator='offline-reviewer', apply=True)
        params.update(overrides)
        return append_broker_correction_notice(self.repo, **params)

    def assert_invalid(self, action):
        with self.assertRaises((OrderError, LegacyOrderError)) as caught:
            action()
        self.assertIn(caught.exception.code, ('EVIDENCE_CORRECTION_INVALID', 'EVIDENCE_SCHEMA_REQUIRED', 'ORDER_NOT_FOUND'))

    def test_notice_appends_new_sequence_preserving_original_wire_hashes_and_economics(self):
        original = copy.deepcopy(self.repo.state['events'])
        economic = {key: copy.deepcopy(self.repo.current.get(key)) for key in
                    ('fills', 'qmt_orders', 'filled_quantity', 'filled_amount', 'submission_status', 'execution_status', 'sync_status')}
        result = self.call()
        self.assertEqual(self.repo.state['high'], 5)
        self.assertEqual({seq: self.repo.state['events'][seq] for seq in original}, original)
        self.assertEqual({key: self.repo.current.get(key) for key in economic}, economic)
        self.assertEqual(self.repo.saved, [('BROKER_CORRECTION_NOTICE', ())])
        notice = result['notice']
        self.assertEqual(notice['source_event_refs'], [self.ref])
        self.assertEqual(notice['verification_status'], 'PENDING_INDEPENDENT_REVIEW')
        self.assertEqual(notice['financial_revision'], 'NOT_AUTHORIZED')
        self.assertEqual(self.ref, compact_source_ref(self.scope, self.repo.journal_source(self.scope['stream_id'], 3)))
        self.assertEqual(self.repo.state['events'][5]['data']['event_corrections_v1'], [notice])

    def test_same_id_retry_is_idempotent_even_with_old_expected_version(self):
        first = self.call()
        original = copy.deepcopy(self.repo.state)
        replay = self.call(operator='another-reviewer', evidence=dict(self.evidence, reference='same-bytes:new-path'))
        self.assertTrue(replay['replayed'])
        self.assertEqual(first['notice'], replay['notice'])
        self.assertEqual(self.repo.state, original)
        self.assert_invalid(lambda: self.call(reason='different claim'))
        self.assertEqual(len(self.repo.saved), 1)

    def test_new_notice_id_appends_and_preserves_prior_notice(self):
        first = self.call()
        second = self.call(notice_id='broker-review-2', expected_version=5)
        self.assertEqual(self.repo.state['high'], 6)
        self.assertEqual(self.repo.current['event_corrections_v1'], [first['notice'], second['notice']])
        self.assertEqual(self.repo.state['events'][5]['data']['event_corrections_v1'], [first['notice']])

    def test_cross_order_client_stream_and_forged_core_digest_are_rejected(self):
        for key, value in [('order_id', 'unowned-order'), ('stream_id', 'foreign-stream'),
                           ('account_id', 'foreign-account'), ('sha256', 'a' * 64), ('order_version', 999)]:
            with self.subTest(field=key):
                self.assert_invalid(lambda: self.call(source_ref=dict(self.ref, **{key: value})))
        self.repo.current['client_order_id'] = 'different-client'
        self.assert_invalid(self.call)
        self.assertEqual(self.repo.saved, [])

    def test_missing_event_stream_or_expected_version_never_creates_history(self):
        self.assert_invalid(lambda: self.call(expected_version=3))
        self.assert_invalid(lambda: self.call(order_id='unowned-order'))
        self.repo.state['events'].pop(3)
        self.assert_invalid(self.call)
        self.repo.state['stream'] = None
        self.assert_invalid(self.call)
        self.assertEqual(self.repo.state['high'], 4)
        self.assertEqual(self.repo.saved, [])

    def test_dry_run_and_invalid_identifiers_do_not_write(self):
        state = copy.deepcopy(self.repo.state)
        result = self.call(apply=False)
        self.assertEqual(result['mode'], 'dry-run')
        self.assertEqual(self.repo.state, state)
        for bad in ('', 'x' * 129, 'has space', '非ASCII', True):
            self.assert_invalid(lambda: self.call(notice_id=bad))
        self.assertEqual(self.repo.saved, [])

    def test_unknown_resolution_carries_retained_original_event_reference(self):
        self.repo.current['submission_status'] = 'UNKNOWN'
        prior = copy.deepcopy(self.repo.state['events'])
        def mutate(document):
            document['submission_status'] = 'REJECTED'
            document.setdefault('manual_resolutions', []).append(dict(reason='positive pre-call proof', operator='reviewer',
                evidence=dict(proof_type='pre_call_boundary', qmt_call_never_started=True)))
        updated = self.repo.update_order('order-1', 'MANUAL_UNKNOWN_RESOLUTION', mutate)
        notice = updated['event_corrections_v1'][-1]
        self.assertEqual(notice['kind'], 'UNKNOWN_OUTCOME_RESOLUTION')
        self.assertEqual(notice['source_event_refs'][0]['event_id'], 4)
        self.assertEqual(notice['source_event_refs'], updated['manual_resolutions'][-1]['source_event_refs'])
        self.assertEqual({seq: self.repo.state['events'][seq] for seq in prior}, prior)

    def test_offline_cli_validates_actual_evidence_bytes_and_keeps_explicit_apply(self):
        raw = b'original broker settlement artifact, not trusted as a financial correction'
        file = Mock()
        file.read_bytes.return_value = raw
        file.resolve.return_value = Path('retained-broker-proof.bin')
        source = Mock()
        source.read_bytes.return_value = json.dumps(self.ref).encode('utf-8')
        args = SimpleNamespace(order_id='order-1', expected_version=4, notice_id='broker-review-cli',
            source_ref_file=source, evidence_file=file, reason='review retained evidence', apply=False)
        preview = broker_correction_notice(self.repo, args)
        self.assertEqual(preview['notice']['evidence']['sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(self.repo.saved, [])
        parsed = parser().parse_args(['correction-notice', '--order-id', 'order-1', '--expected-version', '4',
            '--notice-id', 'cli-id', '--source-ref-file', 'ref.json', '--evidence-file', 'receipt.bin', '--reason', 'review'])
        self.assertFalse(parsed.apply)


class LegacyCorrectionNoticeTests(CorrectionNoticeTests):
    repository_type = LegacyCorrectionRepository
