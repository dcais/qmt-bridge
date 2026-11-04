# -*- coding: utf-8 -*-
"""Schema 6 one-hop deltas against an immutable original internal journal document."""
import hashlib
import json

from .compact_events import compact_changes, compact_apply, compact_json


DISK_FORMAT = 'ORDER_JOURNAL_BASE_DELTA_V1'
DISK_BASE_INTERVAL = 16


def journal_digest(document):
    return hashlib.sha256(compact_json(document).encode('utf-8')).hexdigest()


def journal_value(raw):
    return json.loads(raw) if isinstance(raw, str) else raw


def journal_base(repo, cur, seq, order, cache):
    if seq not in cache:
        cur.execute('SELECT e.order_id,e.document,e.storage_stream_id::text,r.evidence_stream_id::text FROM ' + repo.repo_s +
                    '.order_events e JOIN ' + repo.repo_s + '.account_runtime r USING(account_type,account_id) '
                    'WHERE e.account_type=%s AND e.account_id=%s AND e.event_seq=%s',
                    repo.repo_scope + (seq,))
        row = cur.fetchone()
        if not row or row[0] != order or not row[2] or row[2] != row[3]:
            raise ValueError('missing immutable order journal baseline')
        value = journal_value(row[1])
        if not isinstance(value, dict) or value.get('order_id') != order:
            raise ValueError('journal baseline is not an original snapshot')
        cache[seq] = (value, journal_digest(value), row[2])
    return cache[seq]


def journal_unpack(repo, cur, seq, raw, cache=None):
    value = journal_value(raw)
    if isinstance(value, dict):
        return value
    if not isinstance(value, list) or len(value) != 2 or value[0] != DISK_FORMAT:
        raise ValueError('unknown order journal storage format')
    meta = value[1]
    if (meta['seq'] != seq or meta['account_type'] != repo.account_type or
            meta['account_id'] != repo.account_id or not 0 < meta['base_seq'] < seq or
            not 1 <= meta['distance'] < DISK_BASE_INTERVAL):
        raise ValueError('journal delta scope or sequence differs')
    base, base_hash, stream_id = journal_base(repo, cur, meta['base_seq'], meta['order_id'], {} if cache is None else cache)
    if not stream_id or stream_id != meta['stream_id'] or base_hash != meta['base_sha256'] or base['order_id'] != meta['order_id']:
        raise ValueError('order journal baseline stream or digest differs')
    result = compact_apply(base, meta['changes'])
    if (result.get('order_id') != meta['order_id'] or result.get('account_type') != repo.account_type or
            result.get('account_id') != repo.account_id or result.get('version') != meta['order_version'] or
            journal_digest(result) != meta['sha256']):
        raise ValueError('order journal reconstructed document differs')
    return result


def journal_pack(repo, cur, seq, doc, full_json, stream_id):
    if not stream_id:
        raise ValueError('persistent journal stream is required for delta storage')
    order = doc['order_id']
    cur.execute('SELECT event_seq,document,storage_stream_id::text FROM ' + repo.repo_s +
                '.order_events WHERE account_type=%s AND account_id=%s AND order_id=%s '
                'AND event_seq<%s ORDER BY event_seq DESC LIMIT 1', repo.repo_scope + (order, seq))
    previous = cur.fetchone()
    if previous is None or previous[2] is None:
        # Legacy and FULL-mode rows have no per-row stream proof. Begin a new baseline.
        return None, full_json
    if previous[2] != stream_id:
        raise ValueError('previous order journal stream differs')
    prev_seq, prev = previous[0], journal_value(previous[1])
    if isinstance(prev, dict):
        distance, base_seq, base, base_hash = 1, prev_seq, prev, journal_digest(prev)
    else:
        cache = {}
        journal_unpack(repo, cur, prev_seq, prev, cache)
        meta = prev[1]
        distance = meta['distance'] + 1
        if distance >= DISK_BASE_INTERVAL:
            return None, full_json
        base_seq = meta['base_seq']
        base, base_hash, prior_stream = cache[base_seq]
        if prior_stream != stream_id:
            raise ValueError('immutable order journal stream differs')
    if base['order_id'] != order or base['account_id'] != repo.account_id or base['account_type'] != repo.account_type:
        raise ValueError('order journal baseline identity differs')
    # Normalize only changed values through PostgreSQL. The complete current order
    # was already written once to orders; do not send it again for delta encoding.
    changes = []
    compact_changes(base, journal_value(full_json), [], changes)
    patch_json = compact_json(changes)
    full_size = len(full_json.encode('utf-8'))
    if len(patch_json.encode('utf-8')) >= full_size:
        return None, full_json
    cur.execute('SELECT %s::jsonb', (patch_json,))
    changes = journal_value(cur.fetchone()[0])
    stored = compact_apply(base, changes)
    delta = [DISK_FORMAT, dict(seq=seq, stream_id=stream_id, order_id=order, order_version=stored['version'],
        account_type=repo.account_type, account_id=repo.account_id, base_seq=base_seq, distance=distance,
        base_sha256=base_hash, sha256=journal_digest(stored), changes=changes)]
    delta_json = compact_json(delta)
    if len(delta_json.encode('utf-8')) >= full_size:
        return None, full_json
    return base_seq, delta_json
