#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-time, stopped-writer ORDER v2 to v3 migration. Never called by Bridge."""
import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import sys
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from order_bridge.common import fingerprint, iso_datetime, json_text, parse_timestamp  # noqa: E402
from order_bridge.repository import repo_schema  # noqa: E402
from tools.order_admin import config_from_args  # noqa: E402
from tools.order_schema import DEFAULT_SCHEMA, DEFAULT_SQL_PATH, sql_statements  # noqa: E402


CHILDREN = {"order_items": ("items", "item_id"),
            "execution_attempts": ("attempts", "attempt_id"),
            "cancel_requests": ("cancel_requests", "cancel_request_id"),
            "qmt_tasks": ("qmt_tasks", "qmt_task_id"),
            "qmt_orders": ("qmt_orders", "qmt_order_id"),
            "fills": ("fills", "trade_id")}
TABLES = ("schema_version", "account_runtime", "orders", "order_events",
          "qmt_observations") + tuple(CHILDREN)
QUERY_COLUMNS = {
    "order_items": ("item_id", "symbol", "side"),
    "execution_attempts": ("attempt_id", "kind", "status", "target_id", "cancel_request_id"),
    "cancel_requests": ("cancel_request_id", "status"),
    "qmt_tasks": ("qmt_task_id", "trading_day", "market", "status"),
    "qmt_orders": ("qmt_order_id", "qmt_task_id", "item_id", "trading_day", "market",
                   "symbol", "side", "status", "native_ref", "native_order_ref"),
    "fills": ("trade_id", "qmt_order_id", "item_id", "trading_day", "market",
              "symbol", "side", "quantity", "amount")}
NUMERIC_COLUMNS = frozenset(("quantity", "amount"))


class MigrationError(ValueError):
    pass


def _table(schema, table):
    return schema + "." + table


def connect(config):
    """Maintenance connection; runtime's short statement/lock limits do not apply."""
    import psycopg2
    conn = psycopg2.connect(host=config["pg_host"], port=int(config["pg_port"]),
                            dbname=config["pg_database"], user=config["pg_user"],
                            password=config["pg_password"], connect_timeout=30)
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '30min'")
            cur.execute("SET lock_timeout = '5min'")
        conn.commit()
        return conn
    except Exception:
        conn.close()
        raise


def _version(cur, schema):
    cur.execute("SELECT version FROM " + _table(schema, "schema_version"))
    versions = [row[0] for row in cur.fetchall()]
    if versions not in ([2], [3]):
        raise MigrationError("expected one schema_version row with version 2 or 3: " + str(versions))
    return versions[0]


def _columns(cur, schema):
    cur.execute("SELECT table_name,column_name,data_type,is_nullable FROM information_schema.columns "
                "WHERE table_schema=%s", (schema[1:-1],))
    found = {}
    for table, name, kind, nullable in cur.fetchall():
        found.setdefault(table, {})[name] = (kind, nullable)
    return found


def _structure(cur, schema, version):
    columns = _columns(cur, schema)
    for table in TABLES:
        if table not in columns:
            raise MigrationError("missing table " + table)
        if ("created_at" in columns[table]) != (version == 3 or table == "orders"):
            raise MigrationError("unexpected created_at layout in " + table)
    for table in CHILDREN:
        expected = "text" if version == 2 else "uuid"
        if columns[table].get("record_id", (None,))[0] != expected:
            raise MigrationError("unexpected record_id type in " + table)
        for name in QUERY_COLUMNS[table]:
            if (name in columns[table]) != (version == 3):
                raise MigrationError("unexpected generated column layout in " + table + "." + name)
    return columns


def _document(value):
    return json.loads(value) if isinstance(value, str) else value


def _old_key(field, name, row, index):
    value = str(row.get(name) or row.get("id") or index)
    return fingerprint([row.get("trading_day"), row.get("market"), value]) if field in (
        "qmt_tasks", "qmt_orders", "fills") else value


def _timestamp(value, label):
    try:
        return parse_timestamp(value)
    except Exception:
        raise MigrationError("invalid timezone-aware created_at in " + label) from None


def _validate_query_values(table, record, label):
    for name in QUERY_COLUMNS[table]:
        value = record.get(name)
        if name == CHILDREN[table][1] and (
                not isinstance(value, str) or not value.strip()):
            raise MigrationError("missing business id " + name + " in " + label)
        if value is None:
            continue
        if name in NUMERIC_COLUMNS:
            if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
                raise MigrationError("invalid numeric " + name + " in " + label)
            if value == "":
                raise MigrationError("empty numeric " + name + " in " + label)
            try:
                number = Decimal(str(value))
            except InvalidOperation:
                raise MigrationError("invalid numeric " + name + " in " + label) from None
            if not number.is_finite():
                raise MigrationError("non-finite numeric " + name + " in " + label)
        elif not isinstance(value, str):
            raise MigrationError("invalid text " + name + " in " + label)


def _read(cur, schema, version):
    """Read all old facts once; no row is changed by this function."""
    rows = {}
    for table in TABLES:
        if table in CHILDREN:
            cur.execute("SELECT account_type,account_id,order_id,record_id,document" +
                        (",created_at" if version == 3 else "") + " FROM " + _table(schema, table))
        elif table == "orders":
            cur.execute("SELECT account_type,account_id,order_id,document,created_at FROM " + _table(schema, table))
        else:
            cur.execute("SELECT * FROM " + _table(schema, table))
        rows[table] = cur.fetchall()
    return rows


def _analyze(rows, version):
    """Prove each projected row has exactly one matching parent record."""
    parents = {}
    mapping = {}
    sources = Counter()
    for account_type, account_id, order_id, raw_doc, created_at in rows["orders"]:
        key = (account_type, account_id, order_id)
        doc = _document(raw_doc)
        if not isinstance(doc, dict) or key in parents:
            raise MigrationError("invalid or duplicate order document " + str(key))
        if created_at is None or created_at.tzinfo is None or _timestamp(doc.get("created_at"), str(key)) != created_at.astimezone(timezone.utc):
            raise MigrationError("orders created_at mismatch " + str(key))
        parents[key] = (doc, created_at)

    cancel_scope = set()
    for table, (field, name) in CHILDREN.items():
        projected = {}
        for row in rows[table]:
            scope = tuple(row[:3])
            record_key = str(row[3])
            key = scope + (record_key,)
            if key in projected or scope not in parents:
                raise MigrationError("orphan or duplicate projection " + table + " " + str(key))
            projected[key] = (_document(row[4]), row[5] if version == 3 else None)
        matched = set()
        for scope, (parent, parent_time) in parents.items():
            collection = parent.get(field, [])
            if not isinstance(collection, list):
                raise MigrationError("invalid collection " + field + " " + str(scope))
            business = set()
            for index, record in enumerate(collection):
                label = table + " " + str(scope) + "[" + str(index) + "]"
                if not isinstance(record, dict):
                    raise MigrationError("invalid child document " + label)
                _validate_query_values(table, record, label)
                old = _old_key(field, name, record, index) if version == 2 else record.get("record_id")
                if version == 3:
                    try:
                        old = str(uuid.UUID(str(old)))
                    except (ValueError, AttributeError, TypeError):
                        raise MigrationError("invalid UUID " + label) from None
                key = scope + (str(old),)
                if key not in projected or key in matched or projected[key][0] != record:
                    raise MigrationError("parent/projection mismatch " + label)
                matched.add(key)
                identity = record.get(name)
                if table in ("order_items", "execution_attempts", "cancel_requests"):
                    if identity in business:
                        raise MigrationError("duplicate business id " + label)
                    business.add(identity)
                elif identity:
                    natural = (record.get("trading_day"), record.get("market"), identity)
                    if natural in business:
                        raise MigrationError("duplicate QMT identity " + label)
                    business.add(natural)
                if table == "cancel_requests":
                    cancel_key = scope[:2] + (identity,)
                    if cancel_key in cancel_scope:
                        raise MigrationError("duplicate account cancel_request_id " + label)
                    cancel_scope.add(cancel_key)
                if version == 2:
                    stamp = record.get("created_at")
                    if stamp is not None:
                        created = _timestamp(stamp, label)
                        sources["child_document"] += 1
                    elif table == "order_items":
                        created = parent_time
                        sources["parent_order"] += 1
                    else:
                        created = None
                        sources["migration_at"] += 1
                    mapping[(table, key)] = (record, created, stamp if stamp is not None else
                                             parent.get("created_at") if table == "order_items" else None)
                else:
                    stamp = _timestamp(record.get("created_at"), label)
                    stored_time = projected[key][1]
                    if stored_time is None or stored_time.tzinfo is None or stored_time.astimezone(timezone.utc) != stamp:
                        raise MigrationError("child created_at mismatch " + label)
        if matched != set(projected):
            raise MigrationError("unmatched projected rows in " + table)
    return parents, mapping, dict(sources)


def inspect(cur, schema):
    cur.execute("SHOW server_version_num")
    if int(cur.fetchone()[0]) < 120000:
        raise MigrationError("PostgreSQL 12 or newer is required for stored generated columns")
    version = _version(cur, schema)
    _structure(cur, schema, version)
    rows = _read(cur, schema, version)
    parents, mapping, sources = _analyze(rows, version)
    return {"version": version, "rows": rows, "parents": parents,
            "mapping": mapping, "sources": sources,
            "counts": {table: len(rows[table]) for table in TABLES}}


def _order_business_rows(cur, schema):
    cur.execute("SELECT to_jsonb(t) - 'document' - 'created_at' FROM " +
                _table(schema, "orders") + " AS t")
    return Counter(json_text(row[0]) for row in cur.fetchall())


def _target_ddl(cur, schema):
    """Use the sole current initializer for constraints and indexes after ALTERs."""
    script = DEFAULT_SQL_PATH.read_text(encoding="utf-8-sig")
    if DEFAULT_SCHEMA not in script:
        raise MigrationError("current initializer is missing default schema")
    statements = sql_statements(script.replace(DEFAULT_SCHEMA, schema))
    for statement in statements:
        if statement.lstrip().upper().startswith("CREATE"):
            # Existing tables remain intact; CREATE TABLE IF NOT EXISTS is a no-op.
            cur.execute(statement)


def _convert(cur, schema, original, migration_at):
    at_text = iso_datetime(migration_at)
    metadata = {}
    for (table, key), (_, created, existing_text) in original["mapping"].items():
        metadata[(table, key)] = (str(uuid.uuid4()), existing_text or at_text, created or migration_at)
    # Add ordinary columns before data updates. The new UUID column is temporary.
    for table in TABLES:
        if table != "orders":
            cur.execute("ALTER TABLE " + _table(schema, table) + " ADD COLUMN created_at timestamptz")
    for table in CHILDREN:
        cur.execute("ALTER TABLE " + _table(schema, table) + " ADD COLUMN record_id_new uuid")

    for scope, (doc, _) in original["parents"].items():
        updated = json.loads(json_text(doc))
        for table, (field, name) in CHILDREN.items():
            for index, record in enumerate(updated.get(field, [])):
                old = _old_key(field, name, record, index)
                uuid_text, created_text, created_dt = metadata[(table, scope + (old,))]
                record["record_id"] = uuid_text
                record["created_at"] = created_text
                cur.execute("UPDATE " + _table(schema, table) +
                            " SET record_id_new=%s,created_at=%s,document=%s::jsonb "
                            "WHERE account_type=%s AND account_id=%s AND order_id=%s AND record_id=%s",
                            (uuid_text, created_dt, json_text(record)) + scope + (old,))
                if cur.rowcount != 1:
                    raise MigrationError("child row vanished during migration")
        cur.execute("UPDATE " + _table(schema, "orders") +
                    " SET document=%s::jsonb WHERE account_type=%s AND account_id=%s AND order_id=%s",
                    (json_text(updated),) + scope)
        if cur.rowcount != 1:
            raise MigrationError("order row vanished during migration")

    for table in ("schema_version", "account_runtime", "order_events", "qmt_observations"):
        cur.execute("UPDATE " + _table(schema, table) + " SET created_at=%s", (migration_at,))
    for table in CHILDREN:
        target = _table(schema, table)
        cur.execute("ALTER TABLE " + target + " DROP CONSTRAINT " + table + "_pkey")
        cur.execute("ALTER TABLE " + target + " DROP COLUMN record_id")
        cur.execute("ALTER TABLE " + target + " RENAME COLUMN record_id_new TO record_id")
        cur.execute("ALTER TABLE " + target + " ALTER COLUMN record_id SET NOT NULL")
        cur.execute("ALTER TABLE " + target + " ADD PRIMARY KEY(account_type,account_id,order_id,record_id)")
    # Dropping the old record_id column already removes this dependent index.
    cur.execute("DROP INDEX IF EXISTS " + schema + ".cancel_request_scope_id")
    for table in TABLES:
        cur.execute("ALTER TABLE " + _table(schema, table) + " ALTER COLUMN created_at SET DEFAULT now()")
        cur.execute("ALTER TABLE " + _table(schema, table) + " ALTER COLUMN created_at SET NOT NULL")

    for table, names in QUERY_COLUMNS.items():
        for name in names:
            expression = "(document->>'" + name + "')::numeric" if name in NUMERIC_COLUMNS else "document->>'" + name + "'"
            kind = "numeric" if name in NUMERIC_COLUMNS else "text"
            cur.execute("ALTER TABLE " + _table(schema, table) + " ADD COLUMN " + name +
                        " " + kind + " GENERATED ALWAYS AS (" + expression + ") STORED")
    for table, name in (("order_items", "item_id"), ("execution_attempts", "attempt_id"),
                        ("cancel_requests", "cancel_request_id")):
        cur.execute("ALTER TABLE " + _table(schema, table) + " ALTER COLUMN " + name + " SET NOT NULL")
        check = ("cancel_requests_request_id_nonempty" if table == "cancel_requests" else
                 table + "_" + name + "_nonempty")
        cur.execute("ALTER TABLE " + _table(schema, table) + " ADD CONSTRAINT " + check +
                    " CHECK (btrim(" + name + ") <> '')")
    _target_ddl(cur, schema)


def _verify(cur, schema, before, migration_at, business_rows):
    after = inspect(cur, schema) if _version(cur, schema) == 3 else None
    if after is None:
        cur.execute("UPDATE " + _table(schema, "schema_version") + " SET version=3 WHERE version=2")
        if cur.rowcount != 1:
            raise MigrationError("schema version changed during migration")
        after = inspect(cur, schema)
    if after["counts"] != before["counts"]:
        raise MigrationError("row counts changed during migration")
    if _order_business_rows(cur, schema) != business_rows:
        raise MigrationError("order business columns changed during migration")
    # Old business documents must differ only by the new internal metadata.
    for scope, (old_doc, old_time) in before["parents"].items():
        new_doc, new_time = after["parents"][scope]
        if new_time != old_time:
            raise MigrationError("order created_at changed")
        restored = json.loads(json_text(new_doc))
        for field, _ in CHILDREN.values():
            for index, record in enumerate(restored.get(field, [])):
                record.pop("record_id", None)
                if "created_at" not in old_doc.get(field, [])[index]:
                    record.pop("created_at", None)
        if restored != old_doc:
            raise MigrationError("business document changed " + str(scope))
    # All untouched columns, event snapshots, raw observations, sequence and executor state.
    for table in ("account_runtime", "order_events", "qmt_observations"):
        original = [tuple(row) for row in before["rows"][table]]
        current = [tuple(row[:-1]) for row in after["rows"][table]]
        if Counter(map(repr, original)) != Counter(map(repr, current)):
            raise MigrationError("business rows changed in " + table)
    if migration_at is not None:
        for table in TABLES:
            cur.execute("SELECT count(*) FROM " + _table(schema, table) + " WHERE created_at IS NULL")
            if cur.fetchone()[0]:
                raise MigrationError("null created_at in " + table)
    from order_bridge.storage_schema import repo_check_storage_schema
    repo_check_storage_schema(cur, schema, expected_version=3)
    return after


def run(config, schema_name="qmt_order", apply=False, backup=None):
    schema = repo_schema(schema_name)
    if apply:
        path = Path(backup) if backup else None
        if path is None or not path.is_file() or path.stat().st_size <= 0:
            raise MigrationError("--apply requires a nonempty existing --backup file")
    conn = connect(config)
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            before = inspect(cur, schema)
            report = {"database": config["pg_database"], "schema": schema_name,
                      "before_version": before["version"], "counts": before["counts"],
                      "time_sources": before["sources"], "committed": False}
            if before["version"] == 3:
                from order_bridge.storage_schema import repo_check_storage_schema
                repo_check_storage_schema(cur, schema, expected_version=3)
                report["result"] = "already_v3"
            else:
                report["result"] = "preflight_ok"
        conn.rollback()
        if not apply or before["version"] == 3:
            return report
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            cur.execute("LOCK TABLE " + ",".join(_table(schema, t) for t in TABLES) + " IN ACCESS EXCLUSIVE MODE")
            before = inspect(cur, schema)
            if before["version"] != 2 or before["counts"] != report["counts"]:
                raise MigrationError("database changed after preflight; inspect again")
            business_rows = _order_business_rows(cur, schema)
            cur.execute("SELECT transaction_timestamp()")
            migration_at = cur.fetchone()[0]
            _convert(cur, schema, before, migration_at)
            _verify(cur, schema, before, migration_at, business_rows)
            report.update(result="migrated", migration_at=iso_datetime(migration_at),
                          backup=str(path))
        try:
            conn.commit()
        except Exception:
            # A broken connection can leave COMMIT outcome unknown. Reconnect and check v3.
            raise MigrationError("COMMIT_OUTCOME_UNKNOWN: reconnect and run read-only preflight; "
                                 "do not blindly rerun --apply") from None
        report["committed"] = True
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
    parser.add_argument("--apply", action="store_true", help="write v2 to v3 in one transaction")
    parser.add_argument("--backup", type=Path, help="existing pg_dump backup required for --apply")
    args = parser.parse_args(argv)
    args.command = "schema"  # Reuse database config parsing without requiring an account.
    try:
        config = config_from_args(args)
        report = run(config, apply=args.apply, backup=args.backup)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        # Driver messages may contain credentials; only expected validation errors are printable.
        detail = str(exc) if isinstance(exc, (MigrationError, ValueError)) else type(exc).__name__
        print("ORDER backfill failed: " + detail, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
