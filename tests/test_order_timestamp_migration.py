# -*- coding: utf-8 -*-
"""Timestamp migration checks; PostgreSQL writes use a disposable random schema."""
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from service.ipc_bridge.ipc_protocol.order_common import OrderError
from tools import order_timestamp_migration as migration
from tools.order_schema import DEFAULT_SCHEMA, DEFAULT_SQL_PATH, sql_statements


class GuardTests(unittest.TestCase):
    def test_apply_requires_nonempty_existing_backup_before_connect(self):
        with tempfile.TemporaryDirectory() as directory:
            empty = Path(directory) / "empty.dump"
            empty.touch()
            for backup in (None, Path(directory) / "absent.dump", empty, Path(directory)):
                with self.subTest(backup=backup), patch.object(migration, "connect") as connect:
                    with self.assertRaisesRegex(migration.MigrationError, "nonempty existing --backup"):
                        migration.run({}, apply=True, backup=backup)
                    connect.assert_not_called()

    def test_cli_defaults_to_readonly_and_reuses_admin_config(self):
        config = {"pg_database": "isolated"}
        with patch.object(migration, "config_from_args", return_value=config) as parse, \
                patch.object(migration, "run", return_value={"result": "preflight_ok"}) as run, \
                patch("builtins.print"):
            self.assertEqual(migration.main([]), 0)
        self.assertEqual(parse.call_args[0][0].command, "schema")
        run.assert_called_once_with(config, apply=False, backup=None)


@unittest.skipUnless(os.environ.get("ORDER_TEST_PGHOST"),
                     "ORDER_TEST_PGHOST unset: isolated PostgreSQL timestamp migration skipped")
class PostgresTimestampMigrationTests(unittest.TestCase):
    def setUp(self):
        self.config = {"pg_host": os.environ["ORDER_TEST_PGHOST"],
                       "pg_port": int(os.environ.get("ORDER_TEST_PGPORT", "5432")),
                       "pg_database": os.environ.get("ORDER_TEST_PGDATABASE", "postgres"),
                       "pg_user": os.environ.get("ORDER_TEST_PGUSER", "postgres"),
                       "pg_password": os.environ.get("ORDER_TEST_PGPASSWORD", ""),
                       "pg_schema": "qmt_timestamp_test_" + uuid.uuid4().hex}
        self.schema = migration.repo_schema(self.config["pg_schema"])
        self.conn = migration.connect(self.config)
        self.addCleanup(self._drop_schema)
        self.backup_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.backup_dir.cleanup)
        self.backup = Path(self.backup_dir.name) / "isolated.dump"
        self.backup.write_bytes(b"isolated test backup")
        self.stamps = ("2026-10-14T23:59:59.123456Z", "2026-10-15T08:00:00.654321+08:00")
        self._make_v3()
        self._seed()

    def _drop_schema(self):
        try:
            self.conn.rollback()
            with self.conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS " + self.schema + " CASCADE")
            self.conn.commit()
        finally:
            self.conn.close()

    def _make_v3(self):
        script = DEFAULT_SQL_PATH.read_text(encoding="utf-8-sig").replace(DEFAULT_SCHEMA, self.schema)
        # Keep this fixture v3 when the sole current initializer advances to v4.
        script = script.replace("occurred_at timestamptz", "occurred_at text")
        script = script.replace("observed_at timestamptz", "observed_at text")
        with self.conn.cursor() as cur:
            for statement in sql_statements(script):
                cur.execute(statement)
            cur.execute("INSERT INTO " + self.schema + ".schema_version(version) VALUES(3)")
        self.conn.commit()

    def _seed(self):
        self.document = {"evidence": {"timestamp": self.stamps[1], "nested": [1, None, False]},
                         "submission_status": "UNKNOWN", "version": 12, "fact_version": 8}
        document = json.dumps(self.document)
        scope = ("STOCK", "isolated", "order-1")
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO " + self.schema + ".account_runtime"
                        "(account_type,account_id,event_seq,executor_host,executor_instance,executor_epoch) "
                        "VALUES('STOCK','isolated',29,'host','instance',7)")
            cur.execute("INSERT INTO " + self.schema + ".orders"
                        "(account_type,account_id,order_id,client_order_id,request_hash,remark,active,"
                        "document,submission_status,fact_version,reconcile_due_at) "
                        "VALUES(%s,%s,%s,'client','hash','remark',true,%s::jsonb,'UNKNOWN',8,%s)",
                        scope + (document, self.stamps[0]))
            for table, identity in (("order_items", "item_id"), ("execution_attempts", "attempt_id"),
                                    ("cancel_requests", "cancel_request_id"), ("qmt_tasks", "qmt_task_id"),
                                    ("qmt_orders", "qmt_order_id"), ("fills", "trade_id")):
                record = dict(self.document, **{identity: "0007", "record_id": str(uuid.uuid4()),
                                                "created_at": self.stamps[0]})
                cur.execute("INSERT INTO " + self.schema + "." + table +
                            "(account_type,account_id,order_id,record_id,document) "
                            "VALUES(%s,%s,%s,%s,%s::jsonb)", scope + (record["record_id"], json.dumps(record)))
            for index, stamp in enumerate(self.stamps):
                cur.execute("INSERT INTO " + self.schema + ".order_events"
                            "(account_type,account_id,event_seq,order_id,event_type,occurred_at,document) "
                            "VALUES('STOCK','isolated',%s,'order-1','QMT_OBSERVATION',%s,%s::jsonb)",
                            (28 + index, stamp, document))
                cur.execute("INSERT INTO " + self.schema + ".qmt_observations"
                            "(account_type,account_id,kind,source,observed_at,raw,order_id,applied,observation_hash) "
                            "VALUES('STOCK','isolated','order','callback',%s,%s::jsonb,'order-1',%s,%s)",
                            (stamp, document, bool(index), "hash-" + str(index)))
            # A state different from max(observation_id) proves no setval/reset occurs.
            cur.execute("SELECT setval(pg_get_serial_sequence(%s,'observation_id'),97,false)",
                        (self.schema + ".qmt_observations",))
        self.conn.commit()

    def _state(self):
        with self.conn.cursor() as cur:
            cur.execute("SET LOCAL TIME ZONE 'UTC'")
            state = {}
            for table in migration.TABLES:
                cur.execute("SELECT to_jsonb(t)::text FROM " + self.schema + "." + table + " t")
                state[table] = sorted(row[0] for row in cur.fetchall())
            cur.execute("SELECT table_name,column_name,data_type FROM information_schema.columns "
                        "WHERE table_schema=%s ORDER BY table_name,column_name", (self.config["pg_schema"],))
            state["columns"] = cur.fetchall()
            state["sequences"] = migration._sequences(cur, self.schema)
        self.conn.rollback()
        return state

    def test_preflight_apply_preserves_all_facts_instants_and_sequences_and_repeat_is_readonly(self):
        original = self._state()
        report = migration.run(self.config)
        self.assertEqual(report["result"], "preflight_ok")
        self.assertFalse(report["committed"])
        self.assertEqual(self._state(), original)
        with self.conn.cursor() as cur:
            before = migration.inspect(cur, self.schema)
        self.conn.rollback()
        verify = migration._verify

        def require_locks(cur, schema, original):
            cur.execute("SELECT c.relname FROM pg_catalog.pg_locks l "
                        "JOIN pg_catalog.pg_class c ON c.oid=l.relation "
                        "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                        "WHERE l.pid=pg_backend_pid() AND l.mode='AccessExclusiveLock' "
                        "AND l.granted AND n.nspname=%s AND c.relkind='r'", (schema[1:-1],))
            self.assertEqual({row[0] for row in cur.fetchall()}, set(migration.TABLES))
            verify(cur, schema, original)

        with patch.object(migration, "_verify", side_effect=require_locks):
            report = migration.run(self.config, apply=True, backup=self.backup)
        self.assertEqual(report["result"], "migrated")
        self.assertTrue(report["committed"])
        with self.conn.cursor() as cur:
            after = migration.inspect(cur, self.schema)
            self.assertEqual(after["version"], 4)
            for field in ("facts", "times", "counts", "sequences"):
                self.assertEqual(after[field], before[field], field)
            for table, (column, keys) in migration.TIMESTAMPS.items():
                cur.execute("SELECT " + column + " FROM " + self.schema + "." + table + " ORDER BY " + keys)
                self.assertEqual([row[0] for row in cur.fetchall()],
                                 [migration.parse_timestamp(stamp) for stamp in self.stamps])
        self.conn.rollback()
        migrated = self._state()
        original_inspect = migration.inspect

        def require_readonly(cur, schema):
            cur.execute("SHOW transaction_read_only")
            self.assertEqual(cur.fetchone()[0], "on")
            return original_inspect(cur, schema)

        with patch.object(migration, "inspect", side_effect=require_readonly):
            for apply in (False, True):
                repeat = migration.run(self.config, apply=apply, backup=self.backup)
                self.assertEqual(repeat["result"], "already_v4")
                self.assertFalse(repeat["committed"])
        self.assertEqual(self._state(), migrated)

    def test_invalid_times_block_preflight_and_apply_without_changes(self):
        for table, (column, _) in migration.TIMESTAMPS.items():
            for value in ("2026-10-15T08:00:00", "infinity", "-infinity", "bad", "2026-02-30T00:00:00Z"):
                with self.subTest(table=table, value=value):
                    with self.conn.cursor() as cur:
                        cur.execute("UPDATE " + self.schema + "." + table + " SET " + column + "=%s", (value,))
                    self.conn.commit()
                    original = self._state()
                    for apply in (False, True):
                        with self.assertRaisesRegex(migration.MigrationError, "timezone-aware ISO"):
                            migration.run(self.config, apply=apply, backup=self.backup)
                        self.assertEqual(self._state(), original)
            with self.conn.cursor() as cur:
                cur.execute("UPDATE " + self.schema + "." + table + " SET " + column + "=%s", (self.stamps[0],))
            self.conn.commit()

    def test_verification_failure_after_ddl_rolls_back_columns_version_and_evidence(self):
        original = self._state()
        verify = migration._verify

        def corrupt_then_verify(cur, schema, before):
            cur.execute("UPDATE " + schema + ".order_events SET document='{}'::jsonb")
            verify(cur, schema, before)

        with patch.object(migration, "_verify", side_effect=corrupt_then_verify):
            with self.assertRaisesRegex(migration.MigrationError, "facts changed"):
                migration.run(self.config, apply=True, backup=self.backup)
        self.assertEqual(self._state(), original)

    def test_verification_rejects_instant_change_and_rolls_back(self):
        original = self._state()
        verify = migration._verify

        def change_then_verify(cur, schema, before):
            cur.execute("UPDATE " + schema + ".qmt_observations "
                        "SET observed_at=observed_at + interval '1 microsecond'")
            verify(cur, schema, before)

        with patch.object(migration, "_verify", side_effect=change_then_verify):
            with self.assertRaisesRegex(migration.MigrationError, "times changed"):
                migration.run(self.config, apply=True, backup=self.backup)
        self.assertEqual(self._state(), original)

    def test_unsupported_version_and_wrong_v3_column_type_reject(self):
        with self.conn.cursor() as cur:
            cur.execute("UPDATE " + self.schema + ".schema_version SET version=2")
        self.conn.commit()
        original = self._state()
        with self.assertRaisesRegex(migration.MigrationError, "version 3 or 4"):
            migration.run(self.config, apply=True, backup=self.backup)
        self.assertEqual(self._state(), original)
        with self.conn.cursor() as cur:
            cur.execute("UPDATE " + self.schema + ".schema_version SET version=3")
            cur.execute("ALTER TABLE " + self.schema + ".order_events ALTER COLUMN occurred_at "
                        "TYPE timestamptz USING occurred_at::timestamptz")
        self.conn.commit()
        original = self._state()
        with self.assertRaisesRegex(OrderError, "incorrect column type or nullability"):
            migration.run(self.config, apply=True, backup=self.backup)
        self.assertEqual(self._state(), original)

    def test_v4_infinity_rejects_readonly_validation(self):
        migration.run(self.config, apply=True, backup=self.backup)
        with self.conn.cursor() as cur:
            cur.execute("UPDATE " + self.schema + ".qmt_observations SET observed_at='infinity'")
        self.conn.commit()
        original = self._state()
        with self.assertRaisesRegex(migration.MigrationError, "non-finite timestamp"):
            migration.run(self.config)
        self.assertEqual(self._state(), original)


if __name__ == "__main__":
    unittest.main()
