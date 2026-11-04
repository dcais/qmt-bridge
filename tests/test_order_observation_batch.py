"""Batch ingestion transaction and retry tests; no PostgreSQL or QMT connection."""
import copy
import importlib
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.fixture(params=['order_bridge', 'service.ipc_bridge.order'])
def modules(request):
    return (importlib.import_module(request.param + '.repository'),
            importlib.import_module(request.param + '.background'))


class ObservationStore:
    def __init__(self):
        self.state = dict(observations=[], documents={}, events=[])
        self.connections = []
        self.fail_commit = False

    def connect(self, **kwargs):
        connection = ObservationConnection(self)
        self.connections.append(connection)
        return connection


class ObservationConnection:
    def __init__(self, store):
        self.store = store
        self.state = None
        self.sql = []
        self.closed = False
        self.rollbacks = 0

    def cursor(self):
        return ObservationCursor(self)

    def commit(self):
        if self.state is not None:
            self.store.state = copy.deepcopy(self.state)
            self.state = None
            if self.store.fail_commit:
                self.store.fail_commit = False
                raise OSError('reply lost after commit')

    def rollback(self):
        self.rollbacks += 1
        self.state = None

    def close(self):
        self.closed = True


class ObservationCursor:
    def __init__(self, connection):
        self.connection = connection
        self.result = []

    def execute(self, sql, params=()):
        connection = self.connection
        connection.sql.append((sql, params))
        self.result = []
        if sql.startswith('SELECT set_config'):
            return
        if 'account_runtime' in sql and 'FOR UPDATE' in sql:
            connection.state = copy.deepcopy(connection.store.state)
            self.result = [(len(connection.state['events']),)]
            return
        state = connection.state
        if state is None:
            raise AssertionError('observation SQL ran without the account transaction lock')
        if sql.startswith('SELECT observation_hash'):
            self.result = [(row['hash'],) for row in state['observations']
                           if row['scope'] == params[:2] and row['hash'] in params[-1]]
        elif sql.startswith('SELECT order_id'):
            self.result = [(row.get('order_id'),) for row in state['observations']
                           if row['scope'] == params[:2] and row['hash'] == params[-1]]
        elif sql.startswith('INSERT INTO'):
            observation_id = len(state['observations']) + 1
            state['observations'].append(dict(id=observation_id, scope=params[:2],
                kind=params[2], source=params[3], raw=json.loads(params[5]), hash=params[6]))
            self.result = [(observation_id,)]
        elif sql.startswith('UPDATE'):
            row = next(row for row in state['observations'] if row['id'] == params[1])
            row.update(order_id=params[0], applied=True)
        else:
            raise AssertionError('unexpected observation SQL: ' + sql)

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return self.result

    def close(self):
        pass


def repository(module, store, account='test-account', matched=False):
    repo = module.PostgresRepository({}, account, connect_factory=store.connect)
    repo.repo_match = Mock(return_value=None)
    repo.repo_load = Mock()
    repo.repo_apply_pending = Mock()
    if matched:
        doc = module.new_order_document(dict(client_order_id='one', account_id=account,
            order_type='SINGLE', symbol='510300.SH', side='BUY', quantity=100,
            execution=dict(type='DIRECT')))
        doc['submission_status'] = 'SUBMITTING'
        store.state['documents'][doc['order_id']] = doc
        repo.repo_match.side_effect = lambda cur, kind, raw: copy.deepcopy(
            cur.connection.state['documents'][doc['order_id']])
        repo.repo_load.side_effect = lambda cur, order_id: copy.deepcopy(
            cur.connection.state['documents'][order_id])
        def save(cur, row, event_type, fact_source=None):
            cur.connection.state['documents'][row['order_id']] = copy.deepcopy(row)
            cur.connection.state['events'].append((event_type, fact_source, copy.deepcopy(row)))
        repo.repo_save = Mock(side_effect=save)
    return repo


def test_batch_skips_existing_and_repeated_raw_without_loading_orders(modules):
    module, unused = modules
    store = ObservationStore()
    repo = repository(module, store)
    known, fresh = dict(qmt_order_id='known'), dict(qmt_order_id='fresh')
    assert repo.ingest_observations('order', [known]) == 1
    repo.repo_match.reset_mock()
    store.connections.clear()
    assert repo.ingest_observations('order', [known, fresh, fresh, known]) == 1
    assert len(store.connections) == 1
    assert store.connections[0].closed
    assert len([sql for sql, params in store.connections[0].sql if 'FOR UPDATE' in sql]) == 1
    assert len([sql for sql, params in store.connections[0].sql if 'SELECT observation_hash' in sql]) == 1
    repo.repo_match.assert_called_once()
    repo.repo_load.assert_not_called()
    repo.repo_apply_pending.assert_not_called()
    assert [row['raw'] for row in store.state['observations']] == [known, fresh]
    assert repo.ingest_observations('order', [known, fresh]) == 0
    repo.repo_match.assert_called_once()


def test_empty_batch_and_account_source_kind_hash_scopes(modules):
    module, unused = modules
    store = ObservationStore()
    repo = repository(module, store)
    assert repo.ingest_observations('order', []) == 0
    assert not store.connections
    raw = dict(qmt_order_id='same')
    assert repo.ingest_observations('order', [raw]) == 1
    assert repo.ingest_observations('order', [raw], source='history') == 1
    assert repo.ingest_observations('deal', [raw]) == 1
    other = repository(module, store, account='another-account')
    assert other.ingest_observations('order', [raw]) == 1
    assert len(store.state['observations']) == 4


def test_batch_keeps_each_changed_order_event_in_input_order(modules):
    module, unused = modules
    store = ObservationStore()
    repo = repository(module, store, matched=True)
    doc = next(iter(store.state['documents'].values()))
    raw = dict(remark=doc['remark'], qmt_order_id='qmt-one', symbol='510300.SH',
               side='BUY', quantity=100)
    observations = [dict(raw, status=status) for status in (50, 52, 54)]
    assert repo.ingest_observations('order', observations + [observations[-1]]) == 3
    assert [event[2]['qmt_orders'][0]['raw']['status'] for event in store.state['events']] == [50, 52, 54]
    assert all(event[:2] == ('QMT_OBSERVATION', 'query') for event in store.state['events'])
    assert all(row.get('applied') for row in store.state['observations'])
    repo.repo_apply_pending.assert_called_once()
    assert repo.ingest_observations('order', observations) == 0
    assert len(store.state['events']) == 3


def test_batch_failure_rolls_back_all_raw_facts_then_retries(modules):
    module, unused = modules
    store = ObservationStore()
    repo = repository(module, store)
    repo.repo_match.side_effect = [None, RuntimeError('storage unavailable')]
    raws = [dict(qmt_order_id=str(index)) for index in range(2)]
    with pytest.raises(module.OrderError) as caught:
        repo.ingest_observations('order', raws)
    assert caught.value.code == 'PERSISTENCE_UNAVAILABLE'
    assert store.state['observations'] == []
    assert store.connections[-1].rollbacks == 1
    assert store.connections[-1].closed
    repo.repo_match.side_effect = None
    assert repo.ingest_observations('order', raws) == 2
    assert [row['raw'] for row in store.state['observations']] == raws


def test_unknown_commit_retry_uses_persisted_hashes(modules):
    module, unused = modules
    store = ObservationStore()
    repo = repository(module, store, matched=True)
    doc = next(iter(store.state['documents'].values()))
    raw = dict(remark=doc['remark'], qmt_order_id='qmt-one', symbol='510300.SH',
               side='BUY', quantity=100, status=50)
    store.fail_commit = True
    with pytest.raises(module.OrderError) as caught:
        repo.ingest_observations('order', [raw])
    assert caught.value.code == 'PERSISTENCE_OUTCOME_UNKNOWN'
    assert len(store.state['events']) == 1
    repo.repo_load.reset_mock()
    repo.repo_match.reset_mock()
    assert repo.ingest_observations('order', [raw]) == 0
    assert len(store.state['events']) == 1
    assert len(store.state['observations']) == 1
    repo.repo_load.assert_not_called()
    repo.repo_match.assert_not_called()


def test_callback_still_returns_committed_document_and_keeps_distinct_source(modules):
    module, unused = modules
    store = ObservationStore()
    repo = repository(module, store, matched=True)
    doc = next(iter(store.state['documents'].values()))
    raw = dict(remark=doc['remark'], qmt_order_id='qmt-one', symbol='510300.SH',
               side='BUY', quantity=100, status=50)
    assert repo.ingest_observations('order', [raw]) == 1
    returned = repo.ingest_observation('order', raw)
    assert returned['order_id'] == doc['order_id']
    assert repo.ingest_observation('order', raw) == returned
    assert [row['source'] for row in store.state['observations']] == ['query', 'callback']


def test_merge_cursor_advances_only_after_whole_batch_commits(modules):
    unused, background_module = modules
    repo = Mock()
    runtime = SimpleNamespace(repo=repo, settings={'reconcile_batch_size': 2})
    background = object.__new__(background_module.OrderBackground)
    background.r = runtime
    background.round = dict(waiting=True, stage=0, queries=['order'])
    raws = [dict(qmt_order_id=str(index)) for index in range(3)]
    result = dict(kind='query', status='RETURNED', query_kind='order', value=raws)
    repo.ingest_observations.side_effect = RuntimeError('commit failed')
    with pytest.raises(RuntimeError):
        background._merge(result)
    assert 'merge_index' not in result
    assert background.round['stage'] == 0
    repo.ingest_observations.side_effect = None
    with pytest.raises(background_module._ContinueMerge):
        background._merge(result)
    assert result['merge_index'] == 2
    assert repo.ingest_observations.call_args.args == ('order', raws[:2], 'query')
    background._merge(result)
    assert result['merge_index'] == 3
    assert repo.ingest_observations.call_args.args == ('order', raws[2:], 'query')
    assert background.round['stage'] == 1
    assert background.round['waiting'] is False
    repo.ingest_observation.assert_not_called()
