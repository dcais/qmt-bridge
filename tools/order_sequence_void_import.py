#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline import of independently preauthorized RESERVED_VOID evidence. No QMT or trust issuance."""
import argparse
import getpass
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from service.ipc_bridge.ipc_protocol.order_common import OrderError, copy_json, fingerprint, iso_datetime, json_text
from service.ipc_bridge.order.repository import PostgresRepository, repo_json, repo_timestamp
from service.ipc_bridge.order.sequence_void import VOID_EVENT_TYPE, void_require, void_validate_receipt, void_validate_statements
from tools.order_admin import config_from_args


def import_void(repo, source, approval, source_evidence, approval_evidence, operator, apply=False):
    scope = void_validate_statements(source, approval)
    void_require(scope['account_id'] == repo.account_id and scope['account_type'] == repo.account_type,
                 'import account differs from repository account')
    for statement, artifact in ((source, source_evidence), (approval, approval_evidence)):
        void_require(isinstance(artifact, bytes) and 0 < len(artifact) <= 1048576
                     and hashlib.sha256(artifact).hexdigest() == statement['evidence']['sha256'],
                     'external positive evidence bytes differ from the authorized statement')
    source_hash, approval_hash = fingerprint(source), fingerprint(approval)
    def run(cur):
        if not apply:
            cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        for table in ('event_sequence_void_authorizations', 'event_sequence_void_receipts'):
            cur.execute('SELECT to_regclass(%s)', (repo.repo_s + '.' + table,))
            if not cur.fetchone()[0]:
                raise OrderError(409, 'EVIDENCE_VOID_DISABLED', 'optional void tables are not installed')
        cur.execute("SELECT to_jsonb(r)->>'evidence_stream_id',event_seq FROM " + repo.repo_s +
                    '.account_runtime r WHERE account_type=%s AND account_id=%s', repo.repo_scope)
        marker = cur.fetchone()
        void_require(marker and marker[0] == scope['stream_id'] and scope['event_seq'] <= marker[1],
                     'import stream differs or sequence was never allocated')
        params = (scope['stream_id'],) + repo.repo_scope + (scope['event_seq'],)
        cur.execute('SELECT authorization_id,source_statement_sha256,approval_statement_sha256,authorized_by,'
                    'authorized_at,reason,evidence_reference,evidence_sha256 FROM ' + repo.repo_s +
                    '.event_sequence_void_authorizations WHERE stream_id=%s AND account_type=%s AND account_id=%s '
                    'AND event_seq=%s' + (' FOR SHARE' if apply else ''), params)
        row = cur.fetchone()
        if row is None:
            raise OrderError(409, 'EVIDENCE_VOID_NOT_AUTHORIZED', 'independent prior per-sequence authorization is required')
        void_require(row[1] == source_hash and row[2] == approval_hash,
                     'statements differ from independent prior authorization')
        authorization = dict(scope, authorization_id=row[0], source_statement_sha256=row[1],
            approval_statement_sha256=row[2], authorized_by=row[3], authorized_at=repo_timestamp(row[4]),
            reason=row[5], evidence=dict(reference=row[6], sha256=row[7]))
        cur.execute('SELECT event_type FROM ' + repo.repo_s +
                    '.order_events WHERE account_type=%s AND account_id=%s AND event_seq=%s',
                    repo.repo_scope + (scope['event_seq'],))
        void_require(cur.fetchone() is None, 'an emitted original event cannot be replaced or voided')
        cur.execute('SELECT document FROM ' + repo.repo_s + '.event_sequence_void_receipts '
                    'WHERE stream_id=%s AND account_type=%s AND account_id=%s AND event_seq=%s', params)
        existing = cur.fetchone()
        if existing:
            receipt = void_validate_receipt(repo_json(existing[0]), scope)
            void_require(receipt['source_statement'] == source and receipt['approval_statement'] == approval
                         and receipt['authorization'] == authorization, 'conflicting immutable void receipt already exists')
            return dict(result='already_imported', committed=False, receipt=receipt)
        receipt = dict(scope, receipt_version=1, kind=VOID_EVENT_TYPE, source_statement=copy_json(source),
                       approval_statement=copy_json(approval), authorization=authorization,
                       **{'import': dict(operator=operator, imported_at=iso_datetime())})
        receipt['receipt_sha256'] = fingerprint(receipt)
        void_validate_receipt(receipt, scope)
        if apply:
            cur.execute('INSERT INTO ' + repo.repo_s + '.event_sequence_void_receipts '
                        '(stream_id,account_type,account_id,event_seq,document,recorded_at) VALUES(%s,%s,%s,%s,%s::jsonb,%s)',
                        params + (json_text(receipt), receipt['import']['imported_at']))
        return dict(result='imported' if apply else 'preflight_ok', committed=apply, receipt=receipt)
    return repo.repo_run(run, mutation=apply)


def read_artifact(path):
    data = path.read_bytes()
    if not 0 < len(data) <= 1048576:
        raise ValueError('evidence must contain 1..1048576 bytes')
    return data


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path)
    parser.add_argument('--account-id')
    parser.add_argument('--statements', required=True, type=Path)
    parser.add_argument('--source-evidence', required=True, type=Path)
    parser.add_argument('--approval-evidence', required=True, type=Path)
    parser.add_argument('--operator', default=getpass.getuser())
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(read_artifact(args.statements).decode('utf-8-sig'))
        if not isinstance(manifest, dict) or set(manifest) != {'source_statement', 'approval_statement'}:
            raise ValueError('manifest must contain exactly source_statement and approval_statement')
        config = config_from_args(args)
        repo = PostgresRepository(config, config['account_id'])
        result = import_void(repo, manifest['source_statement'], manifest['approval_statement'],
                             read_artifact(args.source_evidence), read_artifact(args.approval_evidence),
                             args.operator, apply=args.apply)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except (OrderError, ValueError, OSError, KeyError, TypeError) as exc:
        print(exc.code if isinstance(exc, OrderError) else type(exc).__name__, file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
