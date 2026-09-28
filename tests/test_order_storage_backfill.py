# -*- coding: utf-8 -*-
"""One-time ORDER migration tests; PostgreSQL cases use a random disposable schema."""
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from order_bridge.common import fingerprint, json_text
from tools import order_storage_backfill as migration
from tools.order_schema import DEFAULT_SCHEMA, DEFAULT_SQL_PATH, sql_statements


def test_config():
    return {"pg_host": os.environ.get("ORDER_TEST_PGHOST", "127.0.0.1"),
            "pg_port": int(os.environ.get("ORDER_TEST_PGPORT", "5432")),
            "pg_database": os.environ.get("ORDER_TEST_PGDATABASE", "postgres"),
            "pg_user": os.environ.get("ORDER_TEST_PGUSER", "postgres"),
            "pg_password": os.environ.get("ORDER_TEST_PGPASSWORD", "")}


class MappingTests(unittest.TestCase):
    def test_old_key_matches_repository_save(self):
        record = {"qmt_order_id": "0007", "trading_day": "20260928", "market": "SH"}
        self.assertEqual(migration._old_key("qmt_orders", "qmt_order_id", record, 0),
                         fingerprint(["20260928", "SH", "0007"]))
        self.assertEqual(migration._old_key("items", "item_id", {"item_id": "0007"}, 0), "0007")

    def test_generated_inputs_reject_unrepresentable_values(self):
        with self.assertRaises(migration.MigrationError):
            migration._validate_query_values("fills", {"amount": "NaN"}, "fill")
        with self.assertRaises(migration.MigrationError):
            migration._validate_query_values("fills", {"quantity": {}}, "fill")
        with self.assertRaises(migration.MigrationError):
            migration._validate_query_values("order_items", {"item_id": " "}, "item")


@unittest.skipUnless(os.environ.get("ORDER_TEST_PGHOST"),
                     "ORDER_TEST_PGHOST unset: isolated PostgreSQL migration skipped")
class PostgresMigrationTests(unittest.TestCase):
    def setUp(self):
        self.config = test_config()
        self.schema_name = "qmt_migration_test_" + uuid.uuid4().hex
        self.schema = migration.repo_schema(self.schema_name)
        self.conn = migration.connect(self.config)
        self.backup_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.backup_dir.cleanup)
        self.backup = Path(self.backup_dir.name) / "isolated.dump"
        self.backup.write_bytes(b"isolated test backup")
        try:
            self._make_v2()
            self._seed()
        except Exception:
            self.tearDown()
            raise

    def tearDown(self):
        try:
            self.conn.rollback()
            with self.conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS " + self.schema + " CASCADE")
            self.conn.commit()
        finally:
            self.conn.close()

    def _make_v2(self):
        """Reverse an empty current schema to the exact v2 column/key shape."""
        script = DEFAULT_SQL_PATH.read_text(encoding="utf-8-sig").replace(DEFAULT_SCHEMA, self.schema)
        with self.conn.cursor() as cur:
            for statement in sql_statements(script):
                cur.execute(statement)
            cur.execute("INSERT INTO " + self.schema + ".schema_version(version) VALUES(2)")
            for index in ("order_items_business_id", "execution_attempts_business_id",
                          "cancel_request_scope_id", "qmt_tasks_identity_lookup",
                          "qmt_orders_identity_lookup", "fills_trade_identity_lookup",
                          "fills_order_identity_lookup"):
                cur.execute("DROP INDEX " + self.schema + "." + index)
            for table, names in migration.QUERY_COLUMNS.items():
                if table in ("order_items", "execution_attempts", "cancel_requests"):
                    check = ("cancel_requests_request_id_nonempty" if table == "cancel_requests" else
                             table + "_" + migration.CHILDREN[table][1] + "_nonempty")
                    cur.execute("ALTER TABLE " + self.schema + "." + table + " DROP CONSTRAINT " + check)
                for name in names:
                    cur.execute("ALTER TABLE " + self.schema + "." + table + " DROP COLUMN " + name)
                cur.execute("ALTER TABLE " + self.schema + "." + table + " DROP CONSTRAINT " + table + "_pkey")
                cur.execute("ALTER TABLE " + self.schema + "." + table +
                            " ALTER COLUMN record_id TYPE text USING record_id::text")
                cur.execute("ALTER TABLE " + self.schema + "." + table +
                            " ADD PRIMARY KEY(account_type,account_id,order_id,record_id)")
            for table in migration.TABLES:
                if table != "orders":
                    cur.execute("ALTER TABLE " + self.schema + "." + table + " DROP COLUMN created_at")
            cur.execute("CREATE UNIQUE INDEX cancel_request_scope_id ON " + self.schema +
                        ".cancel_requests(account_type,account_id,record_id)")
        self.conn.commit()

    def _seed(self):
        self.created = "2026-09-25T12:00:00+00:00"
        item = {"item_id": "0001", "symbol": "510300.SH", "side": "BUY"}
        attempt = {"attempt_id": "try-1", "kind": "ORDER", "status": "DONE",
                   "created_at": "2026-09-26T13:00:00+08:00"}
        cancel = {"cancel_request_id": "cancel-1", "status": "REQUESTED"}
        task = {"qmt_task_id": "0002", "trading_day": "20260928", "market": "SH", "status": "OPEN"}
        order = {"qmt_order_id": "0003", "trading_day": "20260928", "market": "SH",
                 "symbol": "510300.SH", "side": "BUY", "status": "OPEN"}
        fill = {"trade_id": "0004", "qmt_order_id": "0003", "trading_day": "20260928",
                "market": "SH", "quantity": 1, "amount": "10.001"}
        self.doc = {"created_at": self.created, "items": [item], "attempts": [attempt],
                    "cancel_requests": [cancel], "qmt_tasks": [task],
                    "qmt_orders": [order], "fills": [fill], "version": 9,
                    "fact_version": 2, "order_id": "order-1"}
        scope = ("STOCK", "account-1", "order-1")
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO " + self.schema + ".account_runtime"
                        "(account_type,account_id,event_seq,executor_host,executor_epoch) "
                        "VALUES('STOCK','account-1',7,'host-1',3)")
            cur.execute("INSERT INTO " + self.schema + ".orders"
                        "(account_type,account_id,order_id,client_order_id,request_hash,remark,active,"
                        "document,submission_status,created_at,fact_version) "
                        "VALUES(%s,%s,%s,'client-1','hash-1','remark-1',true,%s::jsonb,'CONFIRMED',%s,2)",
                        scope + (json_text(self.doc), self.created))
            for table, (field, name) in migration.CHILDREN.items():
                record = self.doc[field][0]
                key = migration._old_key(field, name, record, 0)
                cur.execute("INSERT INTO " + self.schema + "." + table +
                            "(account_type,account_id,order_id,record_id,document) "
                            "VALUES(%s,%s,%s,%s,%s::jsonb)", scope + (key, json_text(record)))
            cur.execute("INSERT INTO " + self.schema + ".order_events"
                        "(account_type,account_id,event_seq,order_id,event_type,occurred_at,document) "
                        "VALUES('STOCK','account-1',7,'order-1','OLD','old',%s::jsonb)",
                        (json_text(self.doc),))
            cur.execute("INSERT INTO " + self.schema + ".qmt_observations"
                        "(account_type,account_id,kind,source,observed_at,raw,order_id) "
                        "VALUES('STOCK','account-1','order','callback','old',%s::jsonb,'order-1')",
                        (json_text({"evidence": "raw"}),))
        self.conn.commit()

    def _version(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT version FROM " + self.schema + ".schema_version")
            version = cur.fetchone()[0]
        self.conn.rollback()
        return version

    def test_preflight_apply_preserve_and_repeat(self):
        report = migration.run(self.config, self.schema_name)
        self.assertEqual(report["result"], "preflight_ok")
        self.assertFalse(report["committed"])
        self.assertEqual(self._version(), 2)
        result = migration.run(self.config, self.schema_name, apply=True, backup=self.backup)
        self.assertEqual(result["result"], "migrated")
        self.assertTrue(result["committed"])
        self.assertEqual(self._version(), 3)
        with self.conn.cursor() as cur:
            cur.execute("SELECT document,created_at FROM " + self.schema + ".orders")
            doc, created = cur.fetchone()
            self.assertEqual(created.isoformat(), self.created)
            self.assertEqual(doc["version"], 9)
            self.assertEqual(doc["attempts"][0]["created_at"],
                             self.doc["attempts"][0]["created_at"])
            self.assertEqual(doc["items"][0]["created_at"], self.created)
            self.assertEqual(doc["qmt_tasks"][0]["created_at"], result["migration_at"])
            for table, (field, _) in migration.CHILDREN.items():
                cur.execute("SELECT record_id,created_at,document FROM " + self.schema + "." + table)
                record_id, stamped, projected = cur.fetchone()
                self.assertEqual(str(record_id), doc[field][0]["record_id"])
                self.assertEqual(projected, doc[field][0])
                self.assertEqual(stamped.isoformat(),
                                 migration._timestamp(projected["created_at"], table).isoformat())
            cur.execute("SELECT document FROM " + self.schema + ".order_events")
            self.assertEqual(cur.fetchone()[0], self.doc)
            cur.execute("SELECT raw FROM " + self.schema + ".qmt_observations")
            self.assertEqual(cur.fetchone()[0], {"evidence": "raw"})
            cur.execute("SELECT event_seq,executor_host,executor_epoch FROM " + self.schema +
                        ".account_runtime")
            self.assertEqual(cur.fetchone(), (7, "host-1", 3))
        self.conn.rollback()
        repeat = migration.run(self.config, self.schema_name, apply=True, backup=self.backup)
        self.assertEqual(repeat["result"], "already_v3")
        self.assertFalse(repeat["committed"])

    def test_inconsistent_projection_blocks_preflight(self):
        with self.conn.cursor() as cur:
            cur.execute("UPDATE " + self.schema + ".order_items SET document='{" +
                        '"item_id":"other"' + "}'::jsonb")
        self.conn.commit()
        with self.assertRaisesRegex(migration.MigrationError, "parent/projection mismatch"):
            migration.run(self.config, self.schema_name)
        self.assertEqual(self._version(), 2)

    def test_failure_after_ddl_rolls_back_everything(self):
        with patch.object(migration, "_verify", side_effect=migration.MigrationError("forced failure")):
            with self.assertRaisesRegex(migration.MigrationError, "forced failure"):
                migration.run(self.config, self.schema_name, apply=True, backup=self.backup)
        self.assertEqual(self._version(), 2)
        with self.conn.cursor() as cur:
            cur.execute("SELECT data_type FROM information_schema.columns "
                        "WHERE table_schema=%s AND table_name='order_items' AND column_name='record_id'",
                        (self.schema_name,))
            self.assertEqual(cur.fetchone()[0], "text")
            cur.execute("SELECT count(*) FROM " + self.schema + ".order_events")
            self.assertEqual(cur.fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
