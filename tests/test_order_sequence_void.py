# -*- coding: utf-8 -*-
"""In-memory receipt/import/merged-journal checks; no PostgreSQL, network or QMT."""
import copy
import hashlib
import json
import unittest
from service.ipc_bridge.ipc_protocol.order_common import OrderError, fingerprint
from order_bridge.common import OrderError as LegacyOrderError
from order_bridge.runtime import OrderRuntime as LegacyRuntime
from tests.order_runtime_helpers import OrderRuntime
from tests.test_order_evidence import (MemoryEvidenceCursor, MemoryEvidenceRepository,
                                      MemoryLegacyEvidenceRepository)
from tools.order_sequence_void_import import import_void

ERRORS = (OrderError, LegacyOrderError)


def statements(repo, seq=2):
    scope = dict(stream_id=repo.state['stream'], account_type=repo.account_type,
                 account_id=repo.account_id, event_seq=seq)
    source_bytes, approval_bytes = b'positive external reservation evidence', b'independent approval artifact'
    source = dict(scope, statement_version=1, kind='RESERVED_SEQUENCE_VOID', source_id='isolated-source-1',
        source_state='RESERVED_VOID', never_published=True, financial_effect='NONE', reservation_id='reservation-1',
        reason='allocated reservation was voided before any publication', issued_at='2026-01-01T00:00:00Z',
        evidence=dict(reference='external:reservation-proof', sha256=hashlib.sha256(source_bytes).hexdigest()))
    approval = dict(scope, statement_version=1, decision='APPROVE_RESERVED_SEQUENCE_VOID_IMPORT',
        source_statement_sha256=fingerprint(source), approval_id='approval-1', approved_by='independent-reviewer',
        reason='verified reservation and no publication in external retained source', approved_at='2026-01-02T00:00:00Z',
        evidence=dict(reference='external:approval-proof', sha256=hashlib.sha256(approval_bytes).hexdigest()))
    return source, approval, source_bytes, approval_bytes


def authorize(repo, source, approval):
    # Test-only independent setup. The production import tool has no equivalent write path.
    scope = (source['stream_id'], source['account_type'], source['account_id'], source['event_seq'])
    repo.authorizations[scope] = ('authorization-1', fingerprint(source), fingerprint(approval),
        'separate-administrator', '2026-01-03T00:00:00Z', 'reviewed external positive source',
        'audit:authorization-proof', 'a' * 64)


class VoidCursor(MemoryEvidenceCursor):
    def execute(self, sql, params=()):
        state = getattr(self, 'snapshot', self.store.state)
        if sql.startswith('SELECT to_regclass'):
            self.sql.append((sql, params))
            self.result = [(params[0] if self.store.void_schema else None,)]
        elif '.event_sequence_void_authorizations WHERE' in sql:
            self.sql.append((sql, params))
            record = self.store.authorizations.get(tuple(params))
            self.result = [record] if record else []
        elif sql.startswith('INSERT INTO') and '.event_sequence_void_receipts ' in sql:
            self.sql.append((sql, params))
            if tuple(params[:3]) != (state['stream'], self.store.account_type, self.store.account_id):
                raise AssertionError('receipt scope changed')
            self.store.state['voids'][params[3]] = json.loads(params[4])
            self.result = []
        elif '.event_sequence_void_receipts ' in sql:
            self.sql.append((sql, params))
            if tuple(params[:3]) != (state['stream'], self.store.account_type, self.store.account_id):
                self.result = [(None,)] if 'min(event_seq)' in sql else []
            elif 'min(event_seq)' in sql:
                self.result = [(min(state['voids']) if state['voids'] else None,)]
            elif 'ORDER BY event_seq LIMIT' in sql:
                after, high, limit = params[-3:]
                self.result = [(seq, copy.deepcopy(row)) for seq, row in sorted(state['voids'].items())
                               if after < seq <= high][:limit]
            else:
                row = state['voids'].get(params[-1])
                self.result = [(copy.deepcopy(row),)] if row else []
        else:
            super().execute(sql, params)


class VoidStoreMixin:
    def __init__(self):
        super().__init__()
        self.void_schema = False
        self.authorizations = {}
        self.state['voids'] = {}

    def repo_run(self, callback, mutation=False):
        cur = VoidCursor(self)
        before = copy.deepcopy(self.state)
        try:
            return callback(cur)
        except Exception:
            self.state = before
            raise
        finally:
            self.transactions.append(cur.sql)


class VoidRepository(VoidStoreMixin, MemoryEvidenceRepository):
    pass


class LegacyVoidRepository(VoidStoreMixin, MemoryLegacyEvidenceRepository):
    pass


class VoidReceiptTests(unittest.TestCase):
    repository_type = VoidRepository
    runtime_type = staticmethod(OrderRuntime)

    def setUp(self):
        self.repo = self.repository_type()
        for _ in range(4):
            self.repo.append()
        self.repo.state['events'].pop(2)
        self.proofs = statements(self.repo)
        self.runtime = self.runtime_type({}, None, self.repo.account_id, repository=self.repo)

    def assert_code(self, code, action):
        with self.assertRaises(ERRORS) as caught:
            action()
        self.assertEqual(caught.exception.code, code)

    def install_authorization(self):
        self.repo.void_schema = True
        authorize(self.repo, self.proofs[0], self.proofs[1])

    def apply(self, **kwargs):
        return import_void(self.repo, *self.proofs, operator='import-operator', apply=True, **kwargs)

    def query(self, route='order_events_evidence_v1', **params):
        return self.runtime.handle(route, params, 'GET')[1]

    def test_missing_tables_and_no_prior_authority_default_closed(self):
        self.assert_code('EVIDENCE_VOID_DISABLED', self.apply)
        self.repo.void_schema = True
        self.assert_code('EVIDENCE_VOID_NOT_AUTHORIZED', self.apply)
        self.assertEqual(self.repo.state['voids'], {})
        self.assertFalse(any('INSERT INTO' in sql for tx in self.repo.transactions for sql, _ in tx))

    def test_preflight_is_read_only_and_import_is_immutable_idempotent(self):
        self.install_authorization()
        original = copy.deepcopy(self.repo.state)
        preview = import_void(self.repo, *self.proofs, operator='reviewer')
        self.assertEqual(preview['result'], 'preflight_ok')
        self.assertEqual(original, self.repo.state)
        result = self.apply()
        self.assertEqual(result['result'], 'imported')
        repeat = import_void(self.repo, *self.proofs, operator='another-operator', apply=True)
        self.assertEqual(repeat['result'], 'already_imported')
        self.assertEqual(result['receipt'], repeat['receipt'])
        self.assertEqual(original['events'], self.repo.state['events'])
        self.assertEqual(original['high'], self.repo.state['high'])
        writes = [sql for tx in self.repo.transactions for sql, _ in tx if sql.startswith('INSERT INTO')]
        self.assertEqual(len(writes), 1)
        self.assertIn('.event_sequence_void_receipts ', writes[0])

    def test_changed_statements_or_external_bytes_cannot_self_authorize(self):
        self.install_authorization()
        source, approval, source_bytes, approval_bytes = copy.deepcopy(self.proofs)
        source['reason'] = 'unapproved replacement claim'
        approval['source_statement_sha256'] = fingerprint(source)
        self.assert_code('EVIDENCE_VOID_INVALID', lambda: import_void(self.repo, source, approval,
            source_bytes, approval_bytes, 'operator', apply=True))
        self.assert_code('EVIDENCE_VOID_INVALID', lambda: import_void(self.repo, self.proofs[0], self.proofs[1],
            b'wrong bytes', approval_bytes, 'operator', apply=True))
        self.assertEqual(self.repo.state['voids'], {})

    def test_unknown_missing_sequence_is_never_a_positive_void_source(self):
        self.install_authorization()
        source, approval, source_bytes, approval_bytes = copy.deepcopy(self.proofs)
        for field, value in [('source_state', 'MISSING'), ('never_published', False), ('financial_effect', 'UNKNOWN')]:
            modified = dict(source, **{field: value})
            changed_approval = dict(approval, source_statement_sha256=fingerprint(modified))
            authorize(self.repo, modified, changed_approval)
            self.assert_code('EVIDENCE_VOID_INVALID', lambda: import_void(self.repo, modified, changed_approval,
                source_bytes, approval_bytes, 'operator', apply=True))
        self.assertEqual(self.repo.state['voids'], {})

    def test_emitted_event_and_future_or_cross_stream_sequences_are_rejected(self):
        self.install_authorization()
        self.repo.state['events'][2] = copy.deepcopy(self.repo.state['events'][1])
        self.assert_code('EVIDENCE_VOID_INVALID', self.apply)
        self.repo.state['events'].pop(2)
        source, approval, source_bytes, approval_bytes = statements(self.repo, 5)
        authorize(self.repo, source, approval)
        self.assert_code('EVIDENCE_VOID_INVALID', lambda: import_void(self.repo, source, approval,
            source_bytes, approval_bytes, 'operator', apply=True))
        self.repo.state['stream'] = '00000000-0000-0000-0000-000000000000'
        self.assert_code('EVIDENCE_VOID_INVALID', self.apply)

    def test_receipt_only_bridges_its_exact_sequence_when_explicitly_requested(self):
        self.install_authorization()
        receipt = self.apply()['receipt']
        ordinary = self.query()
        self.assertEqual([e['event_id'] for e in ordinary['events']], [1])
        self.assert_code('EVIDENCE_JOURNAL_GAP', lambda: self.query(after=1))
        mixed = self.query(include_void_receipts='true')
        self.assertEqual([e['event_id'] for e in mixed['events']], [1, 2, 3, 4])
        self.assertEqual(mixed['events'][1], dict(event_id=2, type='NON_FINANCIAL_SEQUENCE_VOID',
            recorded_at=receipt['import']['imported_at'], data=receipt))
        self.assertFalse(mixed['has_more'])
        self.assertEqual(mixed['coverage']['sequence_kinds'], 'ORIGINAL_OR_ATTESTED_VOID')
        self.assertEqual(mixed['coverage']['broker_complete'], 'unknown')
        self.assertEqual(ordinary['events'][0], mixed['events'][0])

    def test_cursor_persists_mode_and_missing_schema_never_manufactures_receipt(self):
        first = self.query(limit=1, include_void_receipts=True)
        self.assertEqual(self.repo.state['voids'], {})
        self.assert_code('EVIDENCE_JOURNAL_GAP', lambda: self.query(cursor=first['next_cursor']))
        self.assert_code('EVIDENCE_CURSOR_INVALID', lambda: self.query(cursor=first['next_cursor'], include_void_receipts=False))
        for invalid in (1, 'yes', [], None):
            self.assert_code('INVALID_PARAMS', lambda: self.query(include_void_receipts=invalid))
        self.install_authorization()
        self.apply()
        next_page = self.query(cursor=first['next_cursor'])
        self.assertEqual(next_page['events'][0]['type'], 'NON_FINANCIAL_SEQUENCE_VOID')

    def test_void_high_tail_is_stable_in_source_pages_and_new_recovery(self):
        self.repo.state['events'][2] = copy.deepcopy(self.repo.state['events'][1])
        self.repo.state['events'][2]['event_id'] = 2
        self.repo.state['events'].pop(4)
        self.proofs = statements(self.repo, 4)
        self.install_authorization()
        receipt = self.apply()['receipt']
        first = self.query(limit=1, include_void_receipts=True)
        tail = self.query(cursor=first['next_cursor'])
        again = self.query(cursor=first['next_cursor'])
        self.assertEqual(tail['events'], again['events'])
        self.assertFalse(tail['has_more'])
        source = self.repo.journal_source(self.repo.state['stream'], 4, include_void_receipts=True)
        self.assertEqual(source, tail['events'][-1])
        recovery = self.query('recovery_evidence_v1', after=3, include_void_receipts=True)
        self.assertEqual(recovery['events'], [source])
        self.assertEqual(recovery['events'][0]['data'], receipt)
        self.assertEqual(recovery['recovery_status'], 'incomplete')
        self.assertFalse(recovery['has_more'])
        self.assertIsNone(self.repo.journal_source(self.repo.state['stream'], 4))

    def test_corrupted_or_scope_changed_receipt_and_double_occupancy_fail_closed(self):
        self.install_authorization()
        receipt = self.apply()['receipt']
        self.repo.state['voids'][2]['receipt_sha256'] = '0' * 64
        self.assert_code('EVIDENCE_VOID_INVALID', lambda: self.query(include_void_receipts=True))
        changed = copy.deepcopy(receipt)
        changed['account_id'] = 'another-account'
        changed['receipt_sha256'] = fingerprint({k: v for k, v in changed.items() if k != 'receipt_sha256'})
        self.repo.state['voids'][2] = changed
        self.assert_code('EVIDENCE_VOID_INVALID', lambda: self.query(include_void_receipts=True))
        self.repo.state['voids'][2] = receipt
        self.repo.state['events'][2] = copy.deepcopy(self.repo.state['events'][1])
        self.assert_code('EVIDENCE_JOURNAL_CONFLICT', lambda: self.query(include_void_receipts=True))

    def test_changed_pre_authorization_cannot_rewrite_existing_import(self):
        self.install_authorization()
        prior = copy.deepcopy(self.apply()['receipt'])
        source, approval, source_bytes, approval_bytes = copy.deepcopy(self.proofs)
        source['reason'] += ' changed'
        approval['source_statement_sha256'] = fingerprint(source)
        authorize(self.repo, source, approval)
        self.assert_code('EVIDENCE_VOID_INVALID', lambda: import_void(self.repo, source, approval,
            source_bytes, approval_bytes, 'operator', apply=True))
        self.assertEqual(self.repo.state['voids'][2], prior)


class LegacyVoidReceiptTests(VoidReceiptTests):
    repository_type = LegacyVoidRepository
    runtime_type = staticmethod(LegacyRuntime)
