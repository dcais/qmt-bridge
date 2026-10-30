#!/usr/bin/env python3
"""Offline v4 -> v5 durable submission bans. Read-only unless --apply with backup.

Stop ALL old and new ORDER writers before applying; old binaries must not resume.
Optional evidence_stream_id migration is separately required to issue ban proof.
"""
import argparse
import json
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from service.ipc_bridge.order.repository import repo_schema
from service.ipc_bridge.order.storage_schema import repo_check_storage_schema
from tools.order_admin import config_from_args
from tools.order_storage_backfill import connect, MigrationError
from tools.order_schema import sql_statements


def run(config, schema_name=None, apply=False, backup=None):
    schema = repo_schema(schema_name or config.get("pg_schema", "qmt_order"))
    path = Path(backup) if backup else None
    if apply and (path is None or not path.is_file() or path.stat().st_size <= 0):
        raise MigrationError("--apply requires a nonempty existing --backup file")
    conn = connect(config)
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" + ("" if apply else " READ ONLY"))
            if apply:
                cur.execute("LOCK TABLE " + schema + ".schema_version," + schema +
                            ".account_runtime IN ACCESS EXCLUSIVE MODE")
            cur.execute("SELECT version FROM " + schema + ".schema_version")
            versions = [r[0] for r in cur.fetchall()]
            if versions not in ([4], [5]):
                raise MigrationError("expected exactly schema version 4 or 5")
            version = versions[0]
            repo_check_storage_schema(cur, schema, version)
            if apply and version == 4:
                script = (ROOT / "sql/order_init.sql").read_text(encoding="utf-8-sig")
                for statement in sql_statements(script.replace('"qmt_order"', schema)):
                    if ".submission_bans" in statement:
                        cur.execute(statement)
                cur.execute("UPDATE " + schema + ".schema_version SET version=5 WHERE version=4")
                if cur.rowcount != 1:
                    raise MigrationError("schema version changed during migration")
                repo_check_storage_schema(cur, schema, 5)
        if apply:
            try:
                conn.commit()
            except Exception:
                raise MigrationError("COMMIT_OUTCOME_UNKNOWN: reconnect and run read-only preflight") from None
        else:
            conn.rollback()
        return dict(before_version=version, schema=schema[1:-1],
                    committed=bool(apply and version == 4),
                    result="already_v5" if version == 5 else "migrated" if apply else "preflight_ok")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup", type=Path)
    args = parser.parse_args(argv)
    args.command = "schema"
    args.account_id = None
    try:
        print(json.dumps(run(config_from_args(args), apply=args.apply, backup=args.backup), sort_keys=True))
        return 0
    except Exception as exc:
        print(str(exc) if isinstance(exc, (MigrationError, ValueError)) else type(exc).__name__, file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
