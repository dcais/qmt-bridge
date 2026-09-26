# -*- coding: utf-8 -*-
"""ORDER v1 -> v2 migration against a disposable PostgreSQL schema."""
import json
import os
from pathlib import Path
import unittest
import uuid
from datetime import timezone

from order_bridge.common import OrderError
from order_bridge.repository import PostgresRepository
from tools.order_schema import initialize_schema, migrate_schema, sql_statements


ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.environ.get("ORDER_TEST_PGHOST"),
                     "ORDER_TEST_PGHOST unset: real PostgreSQL migration skipped")
class OrderMigrationTests(unittest.TestCase):
    def setUp(self):
        self.config = {"pg_host": os.environ["ORDER_TEST_PGHOST"],
                       "pg_port": int(os.environ.get("ORDER_TEST_PGPORT", "5432")),
                       "pg_database": os.environ.get("ORDER_TEST_PGDATABASE", "postgres"),
                       "pg_user": os.environ.get("ORDER_TEST_PGUSER", "postgres"),
                       "pg_password": os.environ.get("ORDER_TEST_PGPASSWORD", ""),
                       "pg_schema": "qmt_migrate_" + uuid.uuid4().hex,
                       "pg_lock_timeout_ms": 5000, "pg_statement_timeout_ms": 30000}
        self.repo = PostgresRepository(self.config, "", connect_factory=None)
        script = (ROOT / "sql" / "order_v1.sql").read_text(encoding="utf-8")
        statements = sql_statements(script.replace('"qmt_order"', self.repo.repo_s))

        def install_v1(cur):
            for statement in statements:
                cur.execute(statement)
            cur.execute("INSERT INTO " + self.repo.repo_s + ".schema_version(version) VALUES(1)")
        self.repo.repo_run(install_v1)

    def tearDown(self):
        self.repo.close()
        conn = self.repo.repo_connect()
        try:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA " + self.repo.repo_s + " CASCADE")
            conn.commit()
        finally:
            conn.close()

    def seed(self):
        documents = {
            "queued": {"submission_status": "QUEUED", "created_at": "2026-09-25T01:02:03+00:00",
                       "attempts": [], "qmt_tasks": [], "qmt_orders": [], "fills": []},
            "local": {"submission_status": "CANCELLED_LOCAL", "created_at": "2026-09-25T01:02:04+00:00",
                      "attempts": [], "qmt_tasks": [], "qmt_orders": [], "fills": []},
            "rejected": {"submission_status": "REJECTED", "created_at": "2026-09-25T01:02:05+00:00",
                         "attempts": [], "qmt_tasks": [], "qmt_orders": [], "fills": []},
            "filled": {"submission_status": "CONFIRMED", "execution_status": "FILLED",
                       "created_at": "2026-09-25T01:02:06+00:00", "attempts": [], "qmt_tasks": [],
                       "qmt_orders": [{"qmt_order_id": "exchange-1", "terminal": True}], "fills": []},
            "uncertain": {"submission_status": "UNKNOWN", "created_at": "2026-09-25T01:02:07+00:00",
                          "attempts": [], "qmt_tasks": [], "qmt_orders": [], "fills": []},
        }

        def write(cur):
            cur.execute("INSERT INTO " + self.repo.repo_s +
                        ".account_runtime(account_type,account_id,event_seq,executor_epoch) "
                        "VALUES('STOCK','account-1',47,9)")
            for order_id, doc in documents.items():
                cur.execute("INSERT INTO " + self.repo.repo_s +
                            ".orders(account_type,account_id,order_id,client_order_id,request_hash,remark,active,document) "
                            "VALUES('STOCK','account-1',%s,%s,'hash',%s,true,%s::jsonb)",
                            (order_id, "client-" + order_id, "remark-" + order_id, json.dumps(doc)))
            cur.execute("INSERT INTO " + self.repo.repo_s +
                        ".qmt_orders(account_type,account_id,order_id,record_id,document) "
                        "VALUES('STOCK','account-1','filled','exchange-1','{}'::jsonb)")
            cur.execute("INSERT INTO " + self.repo.repo_s +
                        ".order_events(account_type,account_id,event_seq,order_id,event_type,occurred_at,document) "
                        "VALUES('STOCK','account-1',47,'filled','FILL','2026-09-25T01:02:06+00:00','{}'::jsonb)")
        self.repo.repo_run(write)

    def snapshot(self):
        def read(cur):
            cur.execute("SELECT order_id,submission_status,cancel_ready,reconcile_pending,reconcile_priority,"
                        "reconcile_due_at,last_reconcile_attempt_at,last_reconciled_at,fact_version,created_at,"
                        "document::text FROM " + self.repo.repo_s + ".orders ORDER BY order_id")
            return {row[0]: row[1:] for row in cur.fetchall()}
        return self.repo.repo_run(read)

    def test_migrate_preserves_existing_facts_and_backfills_projection(self):
        self.seed()
        with self.assertRaises(OrderError) as caught:
            initialize_schema(self.repo)
        self.assertEqual(caught.exception.code, "SCHEMA_VERSION_MISMATCH")
        self.assertIn("schema migrate", str(caught.exception))

        self.assertEqual(migrate_schema(self.repo), {"schema_version": 2, "migrated": True})
        rows = self.snapshot()
        self.assertEqual(set(rows), {"queued", "local", "rejected", "filled", "uncertain"})
        for order_id in ("queued", "local", "rejected"):
            self.assertFalse(rows[order_id][2], order_id)
        for order_id in ("filled", "uncertain"):
            self.assertTrue(rows[order_id][2], order_id)
        for order_id, row in rows.items():
            doc = json.loads(row[9])
            self.assertEqual(row[0], doc["submission_status"])
            self.assertEqual(row[1], doc["cancel_ready"])
            self.assertEqual(row[2], doc["reconcile_pending"])
            self.assertEqual(row[3], doc["reconcile_priority"])
            self.assertEqual(doc["reconcile_requested"], row[2])
            self.assertEqual(row[8].astimezone(timezone.utc).isoformat(), "2026-09-25T01:02:0" +
                             str({"queued": 3, "local": 4, "rejected": 5, "filled": 6,
                                  "uncertain": 7}[order_id]) + "+00:00")
            self.assertEqual(doc["fact_version"], 0)

        def metadata(cur):
            cur.execute("SELECT event_seq,executor_epoch FROM " + self.repo.repo_s +
                        ".account_runtime WHERE account_type='STOCK' AND account_id='account-1'")
            account = cur.fetchone()
            cur.execute("SELECT event_seq,event_type FROM " + self.repo.repo_s + ".order_events")
            events = cur.fetchall()
            cur.execute("SELECT count(*) FROM " + self.repo.repo_s + ".qmt_orders")
            children = cur.fetchone()[0]
            cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname=%s",
                        (self.config["pg_schema"],))
            indexes = {row[0] for row in cur.fetchall()}
            return account, events, children, indexes
        account, events, children, indexes = self.repo.repo_run(metadata)
        self.assertEqual(tuple(account), (47, 9))
        self.assertEqual([tuple(row) for row in events], [(47, "FILL")])
        self.assertEqual(children, 1)
        self.assertTrue({"order_queued_v2", "order_cancel_ready_v2",
                         "order_reconcile_rotation_v2", "order_reconcile_due_v2"} <= indexes)

        self.assertEqual(migrate_schema(self.repo), {"schema_version": 2, "migrated": False})
        self.assertEqual(self.snapshot(), rows)
