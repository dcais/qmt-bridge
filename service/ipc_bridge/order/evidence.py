# -*- coding: utf-8 -*-
"""Conservative v1 evidence: local journal facts are not broker completeness."""
import base64
import hashlib
import hmac
import json
import math
import os
import time

from service.ipc_bridge.ipc_protocol.order_common import OrderError


def evidence_capabilities():
    return dict(version=1, journal_paging='SUPPORTED_WITH_EVIDENCE_MIGRATION',
                source='BRIDGE_PERSISTED_RECORDS', broker_event_completeness='UNSUPPORTED',
                atomic_financial_snapshot='UNSUPPORTED', final_order_fees='UNSUPPORTED',
                native_history='UNSUPPORTED', restore_lineage_proof='UNSUPPORTED',
                sample_event_binding='NON_ATOMIC_BRACKET_ONLY',
                logical_cash_allocation='TRADENEST_OWNED',
                routes=['order_events_evidence_v1', 'account_evidence_v1',
                        'positions_evidence_v1', 'recovery_evidence_v1', 'order_fee_evidence_v1'])


def evidence_fee(document):
    # No documented order-attributable actual fee source is currently available.
    return dict(status='pending', currency=None, cumulative_amount=None,
                revision=None, finality='unknown', source=None,
                reason='ORDER_ATTRIBUTABLE_ACTUAL_FEE_SOURCE_UNAVAILABLE',
                order_id=document.get('order_id'), order_version=document.get('version'),
                fills=[dict(trade_id=row.get('trade_id'), status='pending', currency=None,
                            cumulative_amount=None, revision=None, finality='unknown', source=None)
                       for row in document.get('fills', [])])


def evidence_sample(kind, account_id, account_type, raw, started_at, finished_at):
    result = dict(evidence_version=1, sample_id=os.urandom(16).hex(),
                  source='QMT_GET_TRADE_DETAIL_DATA', account_id=account_id, account_type=account_type,
                  kind=kind, started_at=started_at, finished_at=finished_at,
                  consistency='non_atomic', broker_as_of=None, event_watermark=None,
                  clock_source='BRIDGE_HOST_UTC', account_identity='QUERY_ARGUMENT_ONLY',
                  event_relation='unknown', completeness='unknown', raw=raw)
    if kind == 'account':
        result['assets'] = dict(
            available=evidence_asset(raw, 'm_dAvailable', 'QMT available funds'),
            frozen=evidence_asset(raw, 'm_dFrozenCash', 'QMT frozen funds'),
            cash=dict(value=None, source_field=None, meaning='total cash is unverified; not inferred from balance or available'),
            currency=None, logical_allocation='TRADENEST_OWNED')
    return result


def evidence_asset(raw, field, meaning):
    value = raw.get(field)
    valid = isinstance(value, (int, float)) and not isinstance(value, bool)
    if isinstance(value, float):
        valid = valid and math.isfinite(value)
    return dict(value=value if valid else None, source_field=field, meaning=meaning,
                status='observed' if valid else 'unknown')


class EvidenceCursors(object):
    """Ephemeral server MAC: restart invalidates cursors, never persistent secrets."""
    def __init__(self):
        self.key = os.urandom(32)

    def encode(self, value):
        payload = json.dumps(value, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return base64.urlsafe_b64encode(payload).decode('ascii') + '.' + hmac.new(self.key, payload, hashlib.sha256).hexdigest()

    def decode(self, token):
        try:
            if not isinstance(token, str) or len(token) > 4096:
                raise ValueError()
            body, signature = token.split('.')
            payload = base64.b64decode(body.encode('ascii'), altchars=b'-_', validate=True)
            if not hmac.compare_digest(signature, hmac.new(self.key, payload, hashlib.sha256).hexdigest()):
                raise ValueError()
            value = json.loads(payload.decode('utf-8'))
            if time.time() > value['expires_at']:
                raise ValueError()
            return value
        except (ValueError, TypeError, KeyError, UnicodeError):
            raise OrderError(409, 'EVIDENCE_CURSOR_INVALID', 'cursor expired, altered, or belongs to another process; restart evidence query') from None


def evidence_page(meta, rows, after, high, limit):
    """Check the requested remaining range, not only the returned page."""
    retained = meta['retained_from']
    if high > meta['high_watermark'] or after > high:
        raise OrderError(409, 'EVIDENCE_ROLLBACK_OR_INVALID_RANGE', 'journal watermark regressed or range is invalid')
    if after < retained - 1:
        raise OrderError(409, 'EVIDENCE_RETENTION_GAP', 'requested range precedes retained journal')
    if meta['range_count'] != max(0, high - after):
        raise OrderError(409, 'EVIDENCE_JOURNAL_GAP', 'requested journal range has missing events')
    page = rows[:limit]
    return dict(evidence_version=1, source='BRIDGE_PERSISTED_RECORDS', stream_id=meta['stream_id'],
                stream_identity_origin='DATABASE_GENERATED', authenticity='DEPLOYMENT_TRUST_REQUIRED',
                account_id=meta['account_id'], account_type=meta['account_type'],
                executor_epoch=meta['executor_epoch'], executor_instance=meta['executor_instance'],
                retained_from=retained, high_watermark=high,
                coverage=dict(after_exclusive=after, through_inclusive=high, journal_contiguous=True,
                              start_origin='CLIENT_REQUESTED_BOUND_ONLY',
                              broker_complete='unknown', provable_broker_dates=None),
                restore_lineage_proof='unsupported', events=page,
                next_after=page[-1]['event_id'] if page else after, has_more=len(rows) > limit)
