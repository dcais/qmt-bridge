#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Stopped-writer ORDER v3 to v4 timestamp migration; read-only unless --apply.

Stop all ORDER writers and retain a pg_dump backup before applying. The bridge
never runs this tool automatically. JSON evidence is deliberately untouched.
"""
import argparse
from collections import Counter
from datetime import timezone
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.ipc_bridge.ipc_protocol.order_common import parse_timestamp  # noqa: E402
from service.ipc_bridge.order.repository import repo_schema  # noqa: E402
from service.ipc_bridge.order.storage_schema import repo_check_storage_schema  # noqa: E402
from tools.order_admin import config_from_args  # noqa: E402
from tools.order_storage_backfill import connect, MigrationError, TABLES  # noqa: E402


TIMESTAMPS = {"order_events": ("occurred_at", "account_type,account_id,event_seq"),
              "qmt_observations": ("observed_at", "observation_id")}


def _version(cur, schema):
    cur.execute("SELECT version FROM " + schema + ".schema_version")
    versions = [row[0] for row in cur.fetchall()]
    if versions not in ([3], [4]):
        raise MigrationError("expected one schema_version row with version 3 or 4")
    return versions[0]


def _sequences(cur, schema):
    cur.execute("SELECT c.relname FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=%s AND c.relkind='S' ORDER BY c.relname", (schema[1:-1],))
    names = [row[0] for row in cur.fetchall()]
    states = {}
    for name in names:
        quoted = '"' + name.replace('"', '""') + '"'
        cur.execute("SELECT last_value,is_called FROM " + schema + "." + quoted)
        states[name] = cur.fetchone()
    return states


def inspect(cur, schema):
    """Validate types and finite aware instants, and retain every business fact."""
    version = _version(cur, schema)
    repo_check_storage_schema(cur, schema, expected_version=version)
    facts, times, counts = {}, {}, {}
    # ponytail: snapshots use memory proportional to stored evidence; stream to
    # disk if a stopped database becomes too large for a maintenance process.
    for table in TABLES:
        excluded = "version" if table == "schema_version" else TIMESTAMPS.get(table, (None,))[0]
        expression = "to_jsonb(t)" + (" - '" + excluded + "'" if excluded else "")
        cur.execute("SELECT (" + expression + ")::text FROM " + schema + "." + table + " AS t")
        rows = cur.fetchall()
        facts[table] = Counter(row[0] for row in rows)
        counts[table] = len(rows)
        if table not in TIMESTAMPS:
            continue
        column, keys = TIMESTAMPS[table]
        if version == 4:
            cur.execute("SELECT count(*) FROM " + schema + "." + table +
                        " WHERE NOT isfinite(" + column + ")")
            if cur.fetchone()[0]:
                raise MigrationError("non-finite timestamp in " + table + "." + column)
        cur.execute("SELECT " + keys + "," + column + " FROM " + schema + "." + table)
        values = {}
        for row in cur.fetchall():
            try:
                instant = parse_timestamp(row[-1]) if version == 3 else row[-1].astimezone(timezone.utc)
            except Exception:
                raise MigrationError("invalid timezone-aware ISO timestamp in " + table + "." + column) from None
            key = tuple(row[:-1])
            if key in values:
                raise MigrationError("duplicate timestamp row key in " + table)
            values[key] = instant
        times[table] = values
    return {"version": version, "facts": facts, "times": times,
            "counts": counts, "sequences": _sequences(cur, schema)}


def _verify(cur, schema, before):
    after = inspect(cur, schema)
    if after["version"] != 4:
        raise MigrationError("schema version did not reach 4")
    for field in ("facts", "times", "counts", "sequences"):
        if after[field] != before[field]:
            raise MigrationError(field + " changed during timestamp migration")


def run(config, schema_name=None, apply=False, backup=None):
    """Migrate only an explicitly selected database; pg_schema supports test isolation."""
    schema_name = schema_name if schema_name is not None else config.get("pg_schema", "qmt_order")
    schema = repo_schema(schema_name)
    path = Path(backup) if backup else None
    if apply and (path is None or not path.is_file() or path.stat().st_size <= 0):
        raise MigrationError("--apply requires a nonempty existing --backup file")
    conn = connect(config)
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cur.execute("SET LOCAL TIME ZONE 'UTC'")
            before = inspect(cur, schema)
        conn.rollback()
        report = {"database": config["pg_database"], "schema": schema_name,
                  "before_version": before["version"], "counts": before["counts"],
                  "committed": False,
                  "result": "already_v4" if before["version"] == 4 else "preflight_ok"}
        if not apply or before["version"] == 4:
            return report
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            cur.execute("SET LOCAL TIME ZONE 'UTC'")
            cur.execute("LOCK TABLE " + ",".join(schema + "." + t for t in TABLES) +
                        " IN ACCESS EXCLUSIVE MODE")
            if inspect(cur, schema) != before:
                raise MigrationError("database changed after preflight; stop writers and inspect again")
            for table, (column, _) in TIMESTAMPS.items():
                cur.execute("ALTER TABLE " + schema + "." + table + " ALTER COLUMN " + column +
                            " TYPE timestamptz USING " + column + "::timestamptz")
            cur.execute("UPDATE " + schema + ".schema_version SET version=4 WHERE version=3")
            if cur.rowcount != 1:
                raise MigrationError("schema version changed during migration")
            _verify(cur, schema, before)
        try:
            conn.commit()
        except Exception:
            raise MigrationError("COMMIT_OUTCOME_UNKNOWN: reconnect and run read-only preflight; "
                                 "do not blindly rerun --apply") from None
        report.update(result="migrated", committed=True, backup=str(path))
        return report
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, help="same JSON connection config as order_admin")
    parser.add_argument("--account-id", help=argparse.SUPPRESS)
    parser.add_argument("--apply", action="store_true", help="after stopping writers, migrate v3 to v4")
    parser.add_argument("--backup", type=Path, help="existing nonempty pg_dump backup required for --apply")
    args = parser.parse_args(argv)
    args.command = "schema"
    try:
        report = run(config_from_args(args), apply=args.apply, backup=args.backup)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        detail = str(exc) if isinstance(exc, (MigrationError, ValueError)) else type(exc).__name__
        print("ORDER timestamp migration failed: " + detail, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
