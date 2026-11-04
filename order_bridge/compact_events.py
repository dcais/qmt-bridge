# -*- coding: utf-8 -*-
"""Lossless, opt-in transport for public order journal rows. No current-order reads."""
import copy
import hashlib
import json


def compact_json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(',', ':'))


COMPACT_VOID = 'NON_FINANCIAL_SEQUENCE_VOID'


def compact_source_ref(scope, event):
    doc = event['data']
    if any(doc.get(key) != scope[key] for key in ('account_id', 'account_type')):
        raise ValueError('journal order account differs from stream')
    ref = {key: scope[key] for key in ('stream_id', 'account_id', 'account_type')}
    ref.update(event_id=event['event_id'], sha256=hashlib.sha256(compact_json(event).encode('utf-8')).hexdigest())
    if event['type'] == COMPACT_VOID:
        payload = dict(doc)
        receipt_hash = payload.pop('receipt_sha256')
        if (doc['kind'] != COMPACT_VOID or doc['receipt_version'] != 1 or
                doc['stream_id'] != ref['stream_id'] or doc['event_seq'] != ref['event_id'] or
                doc['import']['imported_at'] != event['recorded_at'] or
                hashlib.sha256(compact_json(payload).encode('utf-8')).hexdigest() != receipt_hash):
            raise ValueError('void receipt differs from journal source')
        ref['kind'] = COMPACT_VOID
    else:
        ref.update(order_id=doc['order_id'], order_version=doc['version'])
    return ref


def compact_changes(old, new, path, out):
    if isinstance(old, dict) and isinstance(new, dict):
        for key in sorted(set(old) - set(new)):
            out.append(dict(op='remove', path=path + [key]))
        for key in sorted(new):
            if key in old:
                compact_changes(old[key], new[key], path + [key], out)
            else:
                out.append(dict(op='set', path=path + [key], value=new[key]))
    elif isinstance(old, list) and isinstance(new, list):
        common = min(len(old), len(new))
        for index in range(common):
            compact_changes(old[index], new[index], path + [index], out)
        if len(old) != len(new):
            out.append(dict(op='splice', path=path, start=common, delete=len(old)-common, items=new[common:]))
    elif type(old) is not type(new) or old != new or (isinstance(old, float) and repr(old) != repr(new)):
        out.append(dict(op='set', path=path, value=new))


def project_event(scope, row, previous=None):
    ref = compact_source_ref(scope, row)
    if row['type'] == COMPACT_VOID:
        if previous is not None:
            raise ValueError('void receipt has no order predecessor')
        return dict(event_id=row['event_id'], type=COMPACT_VOID, recorded_at=row['recorded_at'],
                    source_ref=ref, base_ref=None, receipt=row['data'])
    base = compact_source_ref(scope, previous) if previous is not None else None
    if base and (base['order_id'] != ref['order_id'] or base['event_id'] >= ref['event_id']):
        raise ValueError('invalid journal predecessor')
    changes = []
    compact_changes(previous['data'] if base else {}, row['data'], [], changes)
    return dict(event_id=row['event_id'], type=row['type'], recorded_at=row['recorded_at'],
                source_ref=ref, base_ref=base, changes=changes)


def materialize_event(scope, compact, previous=None):
    base = compact['base_ref']
    if compact['type'] == COMPACT_VOID:
        row = dict(event_id=compact['event_id'], type=COMPACT_VOID, recorded_at=compact['recorded_at'],
                   data=compact['receipt'])
        if base is not None or previous is not None or compact_source_ref(scope, row) != compact['source_ref']:
            raise ValueError('invalid void source')
        return row
    if (compact_source_ref(scope, previous) if previous is not None else None) != base:
        raise ValueError('journal predecessor missing or changed')
    if base and (base['order_id'] != compact['source_ref']['order_id'] or base['event_id'] >= compact['event_id']):
        raise ValueError('invalid journal predecessor')
    doc = compact_apply(previous['data'] if base else {}, compact['changes'])
    row = dict(event_id=compact['event_id'], type=compact['type'], recorded_at=compact['recorded_at'], data=doc)
    if compact_source_ref(scope, row) != compact['source_ref']:
        raise ValueError('journal event differs from immutable reference')
    return row


def compact_apply(original, changes):
    doc = copy.deepcopy(original)
    for change in changes:
        path = change['path']
        op = change['op']
        node = doc
        for key in path if op == 'splice' else path[:-1]:
            node = node[key]
        if op == 'set':
            node[path[-1]] = copy.deepcopy(change['value'])
        elif op == 'remove':
            del node[path[-1]]
        elif op == 'splice':
            start = change['start']
            node[start:start + change['delete']] = copy.deepcopy(change['items'])
        else:
            raise ValueError('unknown journal change operation')
    return doc
