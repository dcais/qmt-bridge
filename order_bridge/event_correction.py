# -*- coding: utf-8 -*-
"""Append-only references to published order events; never authority to revise fills."""
import re
import uuid
from .common import OrderError, copy_json, fingerprint, iso_datetime, public_order
from .compact_events import compact_source_ref


CORRECTION_NOTICE_KIND = 'EXTERNAL_BROKER_CORRECTION_REVIEW_REQUIRED'


def correction_require(condition, message):
    if not condition:
        raise OrderError(409, 'EVIDENCE_CORRECTION_INVALID', message)


def correction_source_refs(repo, cur, order_id, events=None, event_id=None, client_order_id=None):
    cur.execute("SELECT to_jsonb(r)->>'evidence_stream_id',event_seq FROM " + repo.repo_s +
                '.account_runtime r WHERE account_type=%s AND account_id=%s', repo.repo_scope)
    marker = cur.fetchone()
    if not marker or not marker[0]:
        raise OrderError(503, 'EVIDENCE_SCHEMA_REQUIRED', 'published-event correction requires a durable stream identity')
    scope = dict(stream_id=marker[0], account_id=repo.account_id, account_type=repo.account_type)
    if events is None:
        if event_id is None:
            cur.execute('SELECT event_seq FROM ' + repo.repo_s + '.order_events WHERE account_type=%s '
                        'AND account_id=%s AND order_id=%s ORDER BY event_seq DESC LIMIT 1', repo.repo_scope + (order_id,))
            row = cur.fetchone()
            correction_require(row is not None, 'original published order event is missing')
            event_id = row[0]
        correction_require(type(event_id) is int and 1 <= event_id <= marker[1], 'invalid original event sequence')
        row = repo.repo_journal_row(cur, marker[0], event_id)
        correction_require(row is not None and row['type'] != 'NON_FINANCIAL_SEQUENCE_VOID',
                           'a missing event or void cannot authorize an order correction')
        events = [(row['event_id'], row['type'], row['recorded_at'], row['data'])]
    references = []
    for seq, kind, stamp, document in events:
        correction_require(type(seq) is int and 1 <= seq <= marker[1] and document.get('order_id') == order_id
                           and (client_order_id is None or document.get('client_order_id') == client_order_id),
                           'original event belongs to another order or client identity')
        wire = dict(event_id=seq, type=kind, recorded_at=stamp, data=public_order(document))
        references.append(compact_source_ref(scope, wire))
    correction_require(bool(references), 'at least one retained original event is required')
    return references


def correction_request(notice_id, kind, refs, reason, evidence_hash):
    correction_require(isinstance(notice_id, str) and re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:-]{0,127}', notice_id),
                       'notice_id must contain 1..128 ASCII identifier characters')
    correction_require(isinstance(reason, str) and 0 < len(reason.strip()) <= 4096
                       and isinstance(evidence_hash, str) and re.fullmatch('[0-9a-f]{64}', evidence_hash),
                       'notice reason and evidence SHA256 are required')
    return dict(notice_version=1, notice_id=notice_id, kind=kind, source_event_refs=copy_json(refs),
                reason=reason, evidence_sha256=evidence_hash)


def correction_notice(kind, refs, reason, operator, evidence, notice_id=None):
    notice_id = notice_id or uuid.uuid4().hex
    request = correction_request(notice_id, kind, refs, reason, evidence['sha256'])
    correction_require(isinstance(operator, str) and bool(operator.strip())
                       and isinstance(evidence.get('reference'), str) and bool(evidence['reference'].strip()),
                       'notice operator and retained evidence reference are required')
    return dict(notice_version=1, notice_id=notice_id, kind=kind,
                verification_status=('PENDING_INDEPENDENT_REVIEW' if kind == CORRECTION_NOTICE_KIND else 'AUDITED_STATE_REPAIR'),
                financial_revision='NOT_AUTHORIZED', source_event_refs=copy_json(refs), reason=reason,
                operator=operator, at=iso_datetime(), evidence=copy_json(evidence), request_sha256=fingerprint(request))


def manual_resolution_notice(repo, cur, document, audit):
    refs = correction_source_refs(repo, cur, document['order_id'], client_order_id=document.get('client_order_id'))
    evidence = dict(reference=audit.get('evidence_reference', 'embedded:manual_resolutions.evidence'),
                    sha256=audit.get('evidence_sha256') or fingerprint(audit['evidence']))
    notice = correction_notice('UNKNOWN_OUTCOME_RESOLUTION', refs, audit['reason'], audit['operator'], evidence)
    audit['source_event_refs'] = refs
    document.setdefault('event_corrections_v1', []).append(notice)


def append_broker_correction_notice(repo, order_id, expected_version, notice_id, source_ref, reason,
                                    evidence, operator, apply=False):
    request = correction_request(notice_id, CORRECTION_NOTICE_KIND, [source_ref], reason, evidence['sha256'])
    request_hash = fingerprint(request)
    def run(cur):
        if not apply:
            cur.execute('SET TRANSACTION READ ONLY')
        document = repo.repo_require(repo.repo_load(cur, order_id, lock=apply))
        refs = correction_source_refs(repo, cur, order_id, event_id=source_ref.get('event_id'),
                                      client_order_id=document.get('client_order_id'))
        correction_require(refs == [source_ref], 'original source reference differs from the retained public event')
        prior = [row for row in document.get('event_corrections_v1', []) if row.get('notice_id') == notice_id]
        if prior:
            correction_require(len(prior) == 1 and prior[0].get('request_sha256') == request_hash,
                               'notice_id already binds a different correction request')
            retained = prior[0]
            correction_require(retained.get('kind') == CORRECTION_NOTICE_KIND
                               and retained.get('verification_status') == 'PENDING_INDEPENDENT_REVIEW'
                               and retained.get('financial_revision') == 'NOT_AUTHORIZED'
                               and retained.get('source_event_refs') == refs and retained.get('reason') == reason
                               and retained.get('evidence', {}).get('sha256') == evidence['sha256'],
                               'retained notice differs from its immutable retry identity')
            return dict(order_id=order_id, version=document['version'], replayed=True,
                        event_type='BROKER_CORRECTION_NOTICE', mode='apply' if apply else 'dry-run', notice=copy_json(prior[0]))
        correction_require(type(expected_version) is int and document['version'] == expected_version,
                           'order version changed; inspect the latest state')
        notice = correction_notice(CORRECTION_NOTICE_KIND, refs, reason, operator, evidence, notice_id=notice_id)
        if apply:
            document.setdefault('event_corrections_v1', []).append(notice)
            repo.repo_save(cur, document, 'BROKER_CORRECTION_NOTICE', child_fields=())
        return dict(order_id=order_id, version=document['version'], replayed=False,
                    event_type='BROKER_CORRECTION_NOTICE', mode='apply' if apply else 'dry-run', notice=notice)
    return repo.repo_run(run, mutation=apply)
