"""Bounded storage replay tests; PostgreSQL exercise owns a freshly named test schema."""
import copy
import hashlib
import json
import os
from pathlib import Path
import unittest
import uuid

from service.ipc_bridge.order.repository import PostgresRepository
from service.ipc_bridge.order.journal_storage import DISK_FORMAT, journal_digest, journal_unpack
from service.ipc_bridge.order.storage_schema import repo_check_storage_schema
from service.ipc_bridge.ipc_protocol.order_common import OrderError, public_order
from tools.order_schema import initialize_schema


ROOT = Path(__file__).resolve().parents[1]


class JournalStorageUnitTests(unittest.TestCase):
    def test_corrupt_digest_and_chained_baseline_fail_without_recursive_reads(self):
        from unittest.mock import Mock
        repo = PostgresRepository({}, 'fixture')
        doc = dict(account_id='fixture', account_type='STOCK', order_id='o', version=1)
        new = dict(doc, version=2)
        meta = dict(seq=2, stream_id='stream-a', order_version=2, order_id='o', account_id='fixture', account_type='STOCK',
                    base_seq=1, distance=1, sha256=journal_digest(new), base_sha256=journal_digest(doc),
                    changes=[dict(op='set', path=['version'], value=2)])
        cur = Mock()
        cur.fetchone.return_value = ('o', doc, 'stream-a', 'stream-a')
        self.assertEqual(journal_unpack(repo, cur, 2, [DISK_FORMAT, meta]), new)
        for bad in (dict(meta, base_seq=2), dict(meta, sha256='0'*64), dict(meta, account_id='elsewhere'),
                    dict(meta, stream_id='different')):
            with self.assertRaises(ValueError):
                journal_unpack(repo, cur, 2, [DISK_FORMAT, bad])
        cur.reset_mock()
        cur.fetchone.return_value = ('o', [DISK_FORMAT, meta], 'stream-a', 'stream-a')
        with self.assertRaises(ValueError):
            journal_unpack(repo, cur, 2, [DISK_FORMAT, meta])
        self.assertEqual(cur.execute.call_count, 1)

    def test_terminal_repair_proof_is_identical_for_packed_history(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from service.ipc_bridge.order.compact_events import compact_changes
        from tests.test_order_terminal_repair import Cursor, Repo, snapshots, tool

        current, events = snapshots()
        args = SimpleNamespace(order_id='order-1', expected_version=12, checkpoint_event_seq=5,
                               reason='synthetic terminal repair', evidence_out=None, apply=False)
        expected = tool.repair(Repo(current, events), args)
        seq, _, _, base = events[0]
        encoded = [events[0]]
        for event_id, kind, at, doc in events[1:]:
            changes = []
            compact_changes(base, doc, [], changes)
            value = [DISK_FORMAT, dict(seq=event_id, stream_id='stream', order_id=doc['order_id'],
                order_version=doc['version'], account_type='STOCK', account_id='account-1',
                base_seq=seq, distance=event_id-seq, base_sha256=journal_digest(base),
                sha256=journal_digest(doc), changes=changes)]
            encoded.append((event_id, kind, at, value))
        class BaselineCursor(Cursor):
            def execute(self, sql, params=None):
                if sql.startswith('SELECT e.order_id,e.document,'):
                    self.rows = [(base['order_id'], base, 'stream', 'stream')]
                else:
                    super().execute(sql, params)
            def fetchone(self):
                return self.rows[0] if self.rows else None
        repo = Repo(current, encoded)
        with patch('tests.test_order_terminal_repair.Cursor', BaselineCursor):
            actual = tool.repair(repo, args)
        self.assertEqual(actual, expected)
        self.assertEqual(repo.mutations, [False])


def exercise_isolated_postgres(config, connect_factory=None):
    # Resolve all SQL identifiers through a fresh schema owned exclusively by this exercise.
    fresh = 'qmt_test_journal_' + uuid.uuid4().hex
    config = dict(config, pg_schema=fresh)
    repo = PostgresRepository(config, 'fixture-journal', connect_factory=connect_factory)
    schema = repo.repo_s
    def migration(name):
        sql = (ROOT / 'sql' / name).read_text(encoding='utf-8').replace('qmt_order.', schema + '.')
        repo.repo_run(lambda cur: cur.execute(sql))
    def rows(query, args=()):
        def read(cur):
            cur.execute(query, args)
            return cur.fetchall()
        return repo.repo_run(read)
    try:
        initialize_schema(repo)
        repo.ensure_account_runtime()
        migration('order_evidence_v1.sql')
        order = repo.accept_order(dict(client_order_id='journal-synthetic', account_id=repo.account_id,
            order_type='SINGLE', symbol='510300.SH', side='BUY', quantity=100, execution={'type':'DIRECT'}))[1]
        original = [copy.deepcopy(order)]
        def change(values):
            new = repo.update_order(order['order_id'], 'TEST_PROGRESS', lambda doc: doc.update(values))
            original.append(copy.deepcopy(new))
        # Deterministic, relatively incompressible native evidence, stable across journal updates.
        raw = ''.join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(750))
        change({'native_proof': raw})
        migration('order_journal_delta_v1.sql')
        assert repo.check_schema()['schema_version'] == 6
        assert initialize_schema(repo)['schema_version'] == 6
        try:
            repo.repo_run(lambda cur: repo_check_storage_schema(cur, schema, 5))
        except OrderError as exc:
            assert exc.code == 'SCHEMA_VERSION_MISMATCH'
        else:
            raise AssertionError('obsolete reader accepted migrated storage')
        change({'test_step': 0})
        def mode(value):
            repo.repo_run(lambda cur: cur.execute('UPDATE ' + schema +
                '.account_runtime SET event_journal_write_mode=%s WHERE account_type=%s AND account_id=%s',
                (value,) + repo.repo_scope))
        mode('BASE_DELTA_V1')
        for index in range(1, 20):
            change({'test_step': index, 'numeric_evidence': [1e20, -0.0, 1.125e-17]})
        mode('FULL')
        change({'test_step': 100})
        physical = rows('SELECT event_seq,base_event_seq,pg_column_size(document) FROM ' + schema +
                        '.order_events ORDER BY event_seq')
        delta = [row for row in physical if row[1] is not None]
        assert delta and all(0 < seq - base < 16 for seq, base, size in delta)
        assert physical[2][1] is None and physical[3][1] is None and physical[-1][1] is None
        assert [r[0] for r in rows('SELECT event_seq FROM ' + schema + '.order_events '
                         'WHERE storage_stream_id IS NULL ORDER BY event_seq')] == [1, 2, 3, physical[-1][0]]
        assert all(size < physical[2][2] // 3 for _, _, size in delta)
        all_rows = repo.events(0, 100)['events']
        assert [r['data'] for r in all_rows] == original
        normalized = rows('SELECT (%s::jsonb)->%s', (json.dumps(original[3]), 'numeric_evidence'))[0][0]
        assert json.dumps(all_rows[3]['data']['numeric_evidence']) == json.dumps(normalized)
        marker = repo.evidence_marker()
        high = marker['high_watermark']
        assert high == len(original)
        page = repo.evidence_events(0, 100, recovery=True)
        assert [r['data'] for r in page['events']] == original
        for seq in (4, 12, 18, high):
            source = repo.journal_source(marker['stream_id'], seq)
            assert source['data'] == public_order(original[seq - 1])
        try:
            repo.repo_run(lambda cur: cur.execute('UPDATE ' + schema +
                ".account_runtime SET evidence_stream_id='00000000-0000-0000-0000-000000000001' WHERE account_id=%s",
                (repo.account_id,)))
        except OrderError:
            pass
        else:
            raise AssertionError('retained journal stream changed')
        base = delta[0][1]
        for statement in ('DELETE FROM ' + schema + '.order_events WHERE event_seq=%s',
                          'UPDATE ' + schema + '.order_events SET event_type=event_type WHERE event_seq=%s'):
            try:
                repo.repo_run(lambda cur: cur.execute(statement, (base,)))
            except OrderError:
                pass
            else:
                raise AssertionError('referenced baseline mutable')
        def abort(cur):
            candidate = repo.repo_load(cur, order['order_id'], lock=True)
            candidate['aborted'] = True
            repo.repo_save(cur, candidate, 'TEST_ROLLBACK')
            raise OrderError(409, 'TEST_ROLLBACK', 'fixture rollback')
        mode('BASE_DELTA_V1')
        try:
            repo.repo_run(abort, mutation=True)
        except OrderError as exc:
            assert exc.code == 'TEST_ROLLBACK'
        assert repo.evidence_marker()['high_watermark'] == high
        assert 'aborted' not in repo.get_order('journal-synthetic')
        return dict(rows=len(physical), delta_rows=len(delta), full_bytes=physical[2][2],
                    largest_delta_bytes=max(r[2] for r in delta), schema=fresh)
    finally:
        assert schema == '"' + fresh + '"' and fresh.startswith('qmt_test_journal_')
        repo.repo_run(lambda cur: cur.execute('DROP SCHEMA IF EXISTS ' + schema + ' CASCADE'))
        repo.close()


@unittest.skipUnless(os.environ.get('ORDER_TEST_PGHOST'), 'requires explicit isolated PostgreSQL test config')
class JournalStoragePostgresTests(unittest.TestCase):
    def test_legacy_migration_default_and_mixed_journal(self):
        from tests.test_order_repository import repo_test_config
        exercise_isolated_postgres(repo_test_config())
