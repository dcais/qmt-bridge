#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Explicit PostgreSQL ORDER v2 installation and v1 migration, outside QMT."""
from pathlib import Path

from order_bridge.common import OrderError
from order_bridge.repository import repo_SCHEMA_VERSION, repo_schema


DEFAULT_SQL_PATH = Path(__file__).resolve().parents[1] / "sql" / "order_v2.sql"
MIGRATION_SQL_PATH = Path(__file__).resolve().parents[1] / "sql" / "order_v1_to_v2.sql"
DEFAULT_SCHEMA = '"qmt_order"'
V2_ORDER_COLUMNS = ("submission_status", "cancel_ready", "reconcile_pending",
                    "reconcile_priority", "reconcile_due_at", "last_reconcile_attempt_at",
                    "last_reconciled_at", "fact_version", "created_at")


def sql_statements(script):
    """Split the installer SQL without treating semicolons in comments as SQL."""
    statements, current = [], []
    state = "sql"
    position = 0
    while position < len(script):
        char = script[position]
        ahead = script[position:position + 2]
        if state == "sql":
            if ahead == "--":
                state = "line_comment"
                position += 2
                continue
            if ahead == "/*":
                state = "block_comment"
                position += 2
                continue
            if char in ("'", '"'):
                state = char
            elif char == ";":
                statement = "".join(current).strip()
                if statement:
                    statements.append(statement)
                current = []
                position += 1
                continue
            current.append(char)
        elif state == "line_comment":
            if char == "\n":
                current.append(char)
                state = "sql"
        elif state == "block_comment":
            if ahead == "*/":
                current.append(" ")
                state = "sql"
                position += 2
                continue
        else:
            current.append(char)
            if char == state:
                if position + 1 < len(script) and script[position + 1] == state:
                    current.append(state)
                    position += 1
                else:
                    state = "sql"
        position += 1
    if state not in ("sql", "line_comment"):
        raise ValueError("unterminated SQL string or comment")
    statement = "".join(current).strip()
    if statement:
        statements.append(statement)
    return statements


def _schema_sql(repo, source):
    schema = repo.repo_s
    if not isinstance(schema, str) or len(schema) < 3 or repo_schema(schema[1:-1]) != schema:
        raise OrderError(503, "INVALID_PERSISTENCE_CONFIG", "invalid PostgreSQL schema identifier")
    script = source.read_text(encoding="utf-8-sig")
    if DEFAULT_SCHEMA not in script:
        raise ValueError("ORDER SQL does not reference the default schema")
    statements = sql_statements(script.replace(DEFAULT_SCHEMA, schema))
    if not statements:
        raise ValueError("ORDER SQL has no statements")
    return schema, statements


def _version(cur, schema):
    cur.execute("SELECT version FROM " + schema + ".schema_version FOR UPDATE")
    return [row[0] for row in cur.fetchall()]


def _check_v2_orders(cur, schema):
    cur.execute("SELECT " + ",".join(V2_ORDER_COLUMNS) + " FROM " + schema + ".orders LIMIT 0")


def initialize_schema(repo, sql_path=None):
    """Install fresh v2 schema or verify compatible v2, without account writes."""
    source = DEFAULT_SQL_PATH if sql_path is None else Path(sql_path)
    schema, statements = _schema_sql(repo, source)

    def install(cur):
        # Inspect an existing version before the first DDL statement.
        cur.execute("SELECT to_regclass(%s)", (schema + ".schema_version",))
        if cur.fetchone()[0] is not None:
            versions = _version(cur, schema)
            if versions == [1]:
                raise OrderError(503, "SCHEMA_VERSION_MISMATCH",
                                 "ORDER schema v1 requires explicit schema migrate")
            if versions != [repo_SCHEMA_VERSION]:
                raise OrderError(503, "SCHEMA_VERSION_MISMATCH", "unsupported order schema version")

        for statement in statements:
            cur.execute(statement)
        cur.execute("INSERT INTO " + schema + ".schema_version(version) VALUES(%s) ON CONFLICT DO NOTHING",
                    (repo_SCHEMA_VERSION,))
        cur.execute("SELECT version FROM " + schema + ".schema_version")
        if [row[0] for row in cur.fetchall()] != [repo_SCHEMA_VERSION]:
            raise OrderError(503, "SCHEMA_VERSION_MISMATCH", "unsupported order schema version")
        _check_v2_orders(cur, schema)
        for table in ("orders", "order_events", "qmt_observations"):
            cur.execute("SELECT 1 FROM " + schema + "." + table + " LIMIT 0")
        return {"schema_version": repo_SCHEMA_VERSION}

    return repo.repo_run(install)


def migrate_schema(repo, sql_path=None):
    """Explicit, atomic v1 -> v2 migration. A second v2 invocation is read-only."""
    source = MIGRATION_SQL_PATH if sql_path is None else Path(sql_path)
    schema, statements = _schema_sql(repo, source)

    def migrate(cur):
        cur.execute("SELECT to_regclass(%s)", (schema + ".schema_version",))
        if cur.fetchone()[0] is None:
            raise OrderError(503, "SCHEMA_NOT_READY", "ORDER schema is not installed; run schema init")
        versions = _version(cur, schema)
        if versions == [repo_SCHEMA_VERSION]:
            _check_v2_orders(cur, schema)
            return {"schema_version": repo_SCHEMA_VERSION, "migrated": False}
        if versions != [1]:
            raise OrderError(503, "SCHEMA_VERSION_MISMATCH", "unsupported order schema version")
        for statement in statements:
            cur.execute(statement)
        _check_v2_orders(cur, schema)
        cur.execute("UPDATE " + schema + ".schema_version SET version=%s WHERE version=1",
                    (repo_SCHEMA_VERSION,))
        if _version(cur, schema) != [repo_SCHEMA_VERSION]:
            raise OrderError(503, "SCHEMA_VERSION_MISMATCH", "ORDER schema migration failed")
        return {"schema_version": repo_SCHEMA_VERSION, "migrated": True}

    return repo.repo_run(migrate)
