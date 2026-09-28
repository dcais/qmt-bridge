# -*- coding: utf-8 -*-
"""Read-only PostgreSQL ORDER storage contract shared by runtime and installer."""
import re
from datetime import datetime, timezone

from .common import OrderError


def repo_column(kind, required=True, default=None, generated=None):
    return (kind, required, default, generated)


repo_SCHEMA_COLUMNS = {
    "schema_version": {
        "version": repo_column("integer"),
        "created_at": repo_column("timestamp with time zone", default="now()")},
    "account_runtime": {
        "account_type": repo_column("text"), "account_id": repo_column("text"),
        "event_seq": repo_column("bigint", default="0"),
        "executor_host": repo_column("text", False),
        "executor_instance": repo_column("text", False),
        "executor_epoch": repo_column("bigint", default="0"),
        "created_at": repo_column("timestamp with time zone", default="now()")},
    "orders": {
        "account_type": repo_column("text"), "account_id": repo_column("text"),
        "order_id": repo_column("text"), "client_order_id": repo_column("text"),
        "request_hash": repo_column("text"), "remark": repo_column("text"),
        "active": repo_column("boolean"), "document": repo_column("jsonb"),
        "submission_status": repo_column("text"),
        "cancel_ready": repo_column("boolean", default="false"),
        "reconcile_pending": repo_column("boolean", default="false"),
        "reconcile_priority": repo_column("boolean", default="false"),
        "reconcile_due_at": repo_column("timestamp with time zone", default="epoch"),
        "last_reconcile_attempt_at": repo_column("timestamp with time zone", False),
        "last_reconciled_at": repo_column("timestamp with time zone", False),
        "fact_version": repo_column("bigint", default="0"),
        "created_at": repo_column("timestamp with time zone", default="now()")},
    "order_events": {
        "account_type": repo_column("text"), "account_id": repo_column("text"),
        "event_seq": repo_column("bigint"), "order_id": repo_column("text"),
        "event_type": repo_column("text"), "occurred_at": repo_column("text"),
        "document": repo_column("jsonb"),
        "created_at": repo_column("timestamp with time zone", default="now()")},
    "qmt_observations": {
        "observation_id": repo_column("bigint", default="nextval"),
        "account_type": repo_column("text"), "account_id": repo_column("text"),
        "kind": repo_column("text"), "source": repo_column("text"),
        "observed_at": repo_column("text"), "raw": repo_column("jsonb"),
        "order_id": repo_column("text", False),
        "applied": repo_column("boolean", default="false"),
        "observation_hash": repo_column("text", False),
        "created_at": repo_column("timestamp with time zone", default="now()")},
}

repo_CHILD_GENERATED = {
    "order_items": {"item_id": "text", "symbol": "text", "side": "text"},
    "execution_attempts": {"attempt_id": "text", "kind": "text", "status": "text",
                           "target_id": "text", "cancel_request_id": "text"},
    "cancel_requests": {"cancel_request_id": "text", "status": "text"},
    "qmt_tasks": {"qmt_task_id": "text", "trading_day": "text", "market": "text", "status": "text"},
    "qmt_orders": {"qmt_order_id": "text", "qmt_task_id": "text", "item_id": "text",
                   "trading_day": "text", "market": "text", "symbol": "text",
                   "side": "text", "status": "text", "native_ref": "text",
                   "native_order_ref": "text"},
    "fills": {"trade_id": "text", "qmt_order_id": "text", "item_id": "text",
              "trading_day": "text", "market": "text", "symbol": "text",
              "side": "text", "quantity": "numeric", "amount": "numeric"},
}
repo_BUSINESS_IDS = {"order_items": "item_id", "execution_attempts": "attempt_id",
                     "cancel_requests": "cancel_request_id"}
for repo_table, repo_fields in repo_CHILD_GENERATED.items():
    repo_columns = {
        "account_type": repo_column("text"), "account_id": repo_column("text"),
        "order_id": repo_column("text"), "record_id": repo_column("uuid"),
        "document": repo_column("jsonb"),
        "created_at": repo_column("timestamp with time zone", default="now()")}
    for repo_field, repo_kind in repo_fields.items():
        repo_columns[repo_field] = repo_column(repo_kind,
            repo_field == repo_BUSINESS_IDS.get(repo_table), generated=repo_field)
    repo_SCHEMA_COLUMNS[repo_table] = repo_columns

repo_SCHEMA_INDEXES = {
    "order_items_business_id": ("order_items", True, ("account_type", "account_id", "order_id", "item_id")),
    "execution_attempts_business_id": ("execution_attempts", True, ("account_type", "account_id", "order_id", "attempt_id")),
    "cancel_request_scope_id": ("cancel_requests", True, ("account_type", "account_id", "cancel_request_id")),
    "qmt_tasks_identity_lookup": ("qmt_tasks", False, ("account_type", "account_id", "qmt_task_id", "trading_day", "market")),
    "qmt_orders_identity_lookup": ("qmt_orders", False, ("account_type", "account_id", "qmt_order_id", "trading_day", "market")),
    "fills_trade_identity_lookup": ("fills", False, ("account_type", "account_id", "trade_id", "trading_day", "market")),
    "fills_order_identity_lookup": ("fills", False, ("account_type", "account_id", "qmt_order_id", "trading_day", "market")),
}
repo_PRIMARY_KEYS = {
    "schema_version": ("version",),
    "account_runtime": ("account_type", "account_id"),
    "orders": ("account_type", "account_id", "order_id"),
    "order_events": ("account_type", "account_id", "event_seq"),
    "qmt_observations": ("observation_id",),
}
for repo_table in repo_CHILD_GENERATED:
    repo_PRIMARY_KEYS[repo_table] = ("account_type", "account_id", "order_id", "record_id")
repo_EXISTING_UNIQUE_INDEXES = {
    "orders_account_type_account_id_client_order_id_key":
        ("orders", ("account_type", "account_id", "client_order_id")),
    "orders_remark_key": ("orders", ("remark",)),
}


def repo_storage_error(message):
    return OrderError(503, "SCHEMA_NOT_READY", message)


def repo_normalize_default(value):
    if value is None:
        return None
    normalized = "".join(value.lower().split())
    if normalized in ("now()", "current_timestamp"):
        return "now()"
    if normalized.startswith("nextval("):
        return "nextval"
    if normalized.startswith("'epoch'::") and normalized.endswith(("timestamptz", "timestampwithtimezone")):
        return "epoch"
    timestamp = re.match(r"^'(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})([+-]\d{2}(?::?\d{2})?)'::timestamp with time zone$",
                         value.strip().lower())
    if timestamp:
        offset = timestamp.group(3)
        if len(offset) == 3:
            offset += "00"
        elif len(offset) == 6:
            offset = offset[:3] + offset[4:]
        try:
            instant = datetime.strptime(timestamp.group(1) + " " + timestamp.group(2) + offset,
                                        "%Y-%m-%d %H:%M:%S%z")
            if instant.astimezone(timezone.utc) == datetime(1970, 1, 1, tzinfo=timezone.utc):
                return "epoch"
        except ValueError:
            pass
    return normalized


def repo_generated_matches(expression, field, kind):
    if expression is None:
        return False
    # pg_get_expr may add grouping parentheses and an explicit ::text cast on the JSON key.
    compact = re.sub(r"\s+", "", expression.lower()).replace("(", "").replace(")", "")
    expected = "document->>'{0}'::text".format(field)
    if kind == "numeric":
        return compact in ("document->>'{0}'::numeric".format(field), expected + "::numeric")
    return compact in ("document->>'{0}'".format(field), expected)


def repo_check_storage_schema(cur, schema, expected_version=3):
    """Validate the full v3 contract with catalog reads; never performs DDL or repairs."""
    if not isinstance(schema, str) or not re.match(r'^"[A-Za-z_][A-Za-z0-9_]{0,62}"$', schema):
        raise OrderError(503, "INVALID_PERSISTENCE_CONFIG", "invalid PostgreSQL schema identifier")
    schema_name = schema[1:-1]
    cur.execute("""SELECT c.relname, a.attname, pg_catalog.format_type(a.atttypid,a.atttypmod),
                          a.attnotnull, a.attgenerated, pg_get_expr(d.adbin,d.adrelid)
                   FROM pg_catalog.pg_class c
                   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                   JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid
                   LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
                   WHERE n.nspname=%s AND c.relkind IN ('r','p')
                     AND a.attnum>0 AND NOT a.attisdropped""", (schema_name,))
    actual = {}
    for table, column, kind, not_null, generated, expression in cur.fetchall():
        actual.setdefault(table, {})[column] = (kind, bool(not_null), generated or "", expression)
    if "version" in actual.get("schema_version", {}):
        cur.execute("SELECT version FROM " + schema + ".schema_version")
        if [row[0] for row in cur.fetchall()] != [expected_version]:
            raise OrderError(503, "SCHEMA_VERSION_MISMATCH", "unsupported order schema version")
    for table, columns in repo_SCHEMA_COLUMNS.items():
        if table not in actual:
            raise repo_storage_error("missing required table: " + table)
        for column, (kind, required, default, generated) in columns.items():
            found = actual[table].get(column)
            if found is None:
                raise repo_storage_error("missing required column: " + table + "." + column)
            found_kind, found_required, found_generated, expression = found
            if found_kind != kind or found_required != required:
                raise repo_storage_error("incorrect column type or nullability: " + table + "." + column)
            if generated:
                if found_generated != "s" or not repo_generated_matches(expression, generated, kind):
                    raise repo_storage_error("incorrect generated column: " + table + "." + column)
            elif found_generated or repo_normalize_default(expression) != default:
                raise repo_storage_error("incorrect column default: " + table + "." + column)
    cur.execute("""SELECT tab.relname, idx.relname, i.indisunique, i.indisvalid, i.indisready,
                          pg_get_expr(i.indpred,i.indrelid),
                          ARRAY(SELECT pg_get_indexdef(i.indexrelid,k,true)
                                FROM generate_series(1,i.indnkeyatts) AS k ORDER BY k)
                   FROM pg_catalog.pg_index i
                   JOIN pg_catalog.pg_class tab ON tab.oid=i.indrelid
                   JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid
                   JOIN pg_catalog.pg_namespace n ON n.oid=tab.relnamespace
                   WHERE n.nspname=%s""", (schema_name,))
    indexes = {name: (table, bool(unique), bool(valid), bool(ready), predicate, tuple(columns))
               for table, name, unique, valid, ready, predicate, columns in cur.fetchall()}
    for name, (table, unique, columns) in repo_SCHEMA_INDEXES.items():
        found = indexes.get(name)
        if found != (table, unique, True, True, None, columns):
            raise repo_storage_error("missing or incorrect required index: " + name)
    for table, columns in repo_PRIMARY_KEYS.items():
        name = table + "_pkey"
        found = indexes.get(name)
        if found != (table, True, True, True, None, columns):
            raise repo_storage_error("missing or incorrect primary key: " + name)
    for name, (table, columns) in repo_EXISTING_UNIQUE_INDEXES.items():
        if indexes.get(name) != (table, True, True, True, None, columns):
            raise repo_storage_error("missing or incorrect unique index: " + name)
    observation_index = indexes.get("qmt_observation_content")
    if (observation_index is None or observation_index[:4] !=
            ("qmt_observations", True, True, True) or
            observation_index[5] != ("account_type", "account_id", "observation_hash") or
            not re.fullmatch(r"\(?observation_hash IS NOT NULL\)?", observation_index[4] or "", re.I)):
        raise repo_storage_error("missing or incorrect unique index: qmt_observation_content")
    cur.execute("""SELECT c.relname, con.conname, con.convalidated, pg_get_constraintdef(con.oid)
                   FROM pg_catalog.pg_constraint con
                   JOIN pg_catalog.pg_class c ON c.oid=con.conrelid
                   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                   WHERE n.nspname=%s AND con.contype='c'""", (schema_name,))
    checks = {(table, name): (validated, definition)
              for table, name, validated, definition in cur.fetchall()}
    for table, field in repo_BUSINESS_IDS.items():
        name = ("cancel_requests_request_id_nonempty" if table == "cancel_requests" else
                table + "_" + field + "_nonempty")
        validated, definition = checks.get((table, name), (False, ""))
        normalized_check = re.sub(r"[\s()]", "", definition.lower())
        if not validated or normalized_check not in (
                "checkbtrim{0}<>''".format(field),
                "checkbtrim{0}<>''::text".format(field)):
            raise repo_storage_error("missing or incorrect nonempty check: " + name)
    return {"schema_version": expected_version, "ready": True}
