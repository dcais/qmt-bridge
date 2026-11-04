# -*- coding: utf-8 -*-
"""Imported positive RESERVED_VOID evidence; never an online gap-filling authority."""
import re
import uuid

from service.ipc_bridge.ipc_protocol.order_common import OrderError, copy_json, fingerprint, parse_timestamp


VOID_EVENT_TYPE = 'NON_FINANCIAL_SEQUENCE_VOID'
VOID_SCOPE = ('stream_id', 'account_type', 'account_id', 'event_seq')


def void_require(condition, message):
    if not condition:
        raise OrderError(409, 'EVIDENCE_VOID_INVALID', message)


def void_text(value):
    return isinstance(value, str) and 0 < len(value.strip()) <= 4096


def void_hash(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def void_evidence(value):
    void_require(isinstance(value, dict) and set(value) == {'reference', 'sha256'}
                 and void_text(value['reference']) and void_hash(value['sha256']),
                 'positive evidence reference and SHA256 are required')


def void_scope(value):
    void_require(isinstance(value, dict) and all(key in value for key in VOID_SCOPE), 'void scope is missing')
    try:
        valid_stream = str(uuid.UUID(value['stream_id'])) == value['stream_id']
    except (ValueError, TypeError, AttributeError):
        valid_stream = False
    void_require(valid_stream and value['account_type'] == 'STOCK' and void_text(value['account_id'])
                 and type(value['event_seq']) is int and 1 <= value['event_seq'] <= 9223372036854775807,
                 'invalid void account, stream or sequence')
    return {key: value[key] for key in VOID_SCOPE}


def void_validate_statements(source, approval):
    scope = void_scope(source)
    void_require(void_scope(approval) == scope, 'source and approval scopes differ')
    void_require(set(source) == set(VOID_SCOPE) | {'statement_version', 'kind', 'source_id', 'source_state',
                 'never_published', 'financial_effect', 'reservation_id', 'reason', 'issued_at', 'evidence'},
                 'source statement fields differ from the RESERVED_VOID contract')
    void_require(source['statement_version'] == 1 and type(source['statement_version']) is int
                 and source['kind'] == 'RESERVED_SEQUENCE_VOID' and source['source_state'] == 'RESERVED_VOID'
                 and source['never_published'] is True and source['financial_effect'] == 'NONE'
                 and all(void_text(source[key]) for key in ('source_id', 'reservation_id', 'reason')),
                 'missing positive never-published non-financial reservation evidence')
    void_evidence(source['evidence'])
    parse_timestamp(source['issued_at'])
    void_require(set(approval) == set(VOID_SCOPE) | {'statement_version', 'decision', 'source_statement_sha256',
                 'approval_id', 'approved_by', 'reason', 'approved_at', 'evidence'}, 'approval statement fields differ')
    void_require(approval['statement_version'] == 1 and type(approval['statement_version']) is int
                 and approval['decision'] == 'APPROVE_RESERVED_SEQUENCE_VOID_IMPORT'
                 and approval['source_statement_sha256'] == fingerprint(source)
                 and all(void_text(approval[key]) for key in ('approval_id', 'approved_by', 'reason')),
                 'approval is not bound to the exact source statement')
    void_evidence(approval['evidence'])
    void_require(parse_timestamp(source['issued_at']) <= parse_timestamp(approval['approved_at']),
                 'approval precedes the source statement')
    return scope


def void_validate_receipt(receipt, expected_scope=None):
    void_require(isinstance(receipt, dict), 'void receipt must be an object')
    required = set(VOID_SCOPE) | {'receipt_version', 'kind', 'source_statement', 'approval_statement',
                                'authorization', 'import', 'receipt_sha256'}
    void_require(set(receipt) == required and type(receipt['receipt_version']) is int
                 and receipt['receipt_version'] == 1 and receipt['kind'] == VOID_EVENT_TYPE,
                 'void receipt version or fields differ')
    scope = void_scope(receipt)
    void_require(expected_scope is None or scope == expected_scope, 'void receipt scope differs from the journal')
    void_require(void_validate_statements(receipt['source_statement'], receipt['approval_statement']) == scope,
                 'void statements differ from the journal scope')
    authorization = receipt['authorization']
    void_require(isinstance(authorization, dict) and set(authorization) == set(VOID_SCOPE) |
                 {'authorization_id', 'source_statement_sha256', 'approval_statement_sha256', 'authorized_by',
                  'authorized_at', 'reason', 'evidence'}, 'independent authorization fields differ')
    void_require(void_scope(authorization) == scope
                 and authorization['source_statement_sha256'] == fingerprint(receipt['source_statement'])
                 and authorization['approval_statement_sha256'] == fingerprint(receipt['approval_statement'])
                 and all(void_text(authorization[key]) for key in ('authorization_id', 'authorized_by', 'reason')),
                 'independent authorization is not bound to both statements')
    void_evidence(authorization['evidence'])
    imported = receipt['import']
    void_require(isinstance(imported, dict) and set(imported) == {'operator', 'imported_at'}
                 and void_text(imported['operator']), 'import identity is missing')
    void_require(parse_timestamp(receipt['approval_statement']['approved_at']) <= parse_timestamp(authorization['authorized_at'])
                 <= parse_timestamp(imported['imported_at']), 'authorization/import chronology is invalid')
    payload = {key: value for key, value in receipt.items() if key != 'receipt_sha256'}
    void_require(void_hash(receipt['receipt_sha256']) and fingerprint(payload) == receipt['receipt_sha256'],
                 'void receipt digest differs')
    return receipt


def void_event(receipt, expected_scope):
    void_validate_receipt(receipt, expected_scope)
    return dict(event_id=receipt['event_seq'], type=VOID_EVENT_TYPE,
                recorded_at=receipt['import']['imported_at'], data=copy_json(receipt))
