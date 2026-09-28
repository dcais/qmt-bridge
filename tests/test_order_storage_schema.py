# -*- coding: utf-8 -*-
"""Current ORDER schema behavior on an isolated PostgreSQL schema."""
import json
import os
import unittest
import uuid

from order_bridge.common import OrderError
from order_bridge.repository import repo_SCHEMA_VERSION
from order_bridge.storage_schema import repo_check_storage_schema
from tools.order_schema import initialize_schema


@unittest.skipUnless(os.environ.get("ORDER_TEST_PGHOST"),
                     "ORDER_TEST_PGHOST unset: real PostgreSQL integration skipped")
class StorageSchemaTests(unittest.TestCase):
    def setUp(self):
        import psycopg2
        self.connection = psycopg2.connect(
            host=os.environ["ORDER_TEST_PGHOST"],
            port=int(os.environ.get("ORDER_TEST_PGPORT", "5432")),
            dbname=os.environ.get("ORDER_TEST_PGDATABASE", "postgres"),
            user=os.environ.get("ORDER_TEST_PGUSER", "postgres"),
            password=os.environ.get("ORDER_TEST_PGPASSWORD", ""))
        self.schema_name = "qmt_storage_test_" + uuid.uuid4().hex
        self.repo_s = '"' + self.schema_name + '"'

    def tearDown(self):
        self.connection.rollback()
        try:
            with self.connection.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS " + self.repo_s + " CASCADE")
            self.connection.commit()
        finally:
            self.connection.close()

    def repo_run(self, callback, mutation=False):
        try:
            with self.connection.cursor() as cur:
                result = callback(cur)
            self.connection.commit()
            return result
        except Exception:
            self.connection.rollback()
            raise

    def test_install_repeat_and_read_only_contract(self):
        self.assertEqual(initialize_schema(self), {"schema_version": repo_SCHEMA_VERSION})
        with self.connection.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_constraint con "
                        "JOIN pg_namespace n ON n.oid=con.connamespace "
                        "WHERE n.nspname=%s AND con.contype='f'", (self.schema_name,))
            self.assertEqual(cur.fetchone()[0], 0)
            cur.execute("SELECT count(*) FROM information_schema.columns "
                        "WHERE table_schema=%s AND column_name='created_at'", (self.schema_name,))
            self.assertEqual(cur.fetchone()[0], 11)
            cur.execute("INSERT INTO " + self.repo_s + ".account_runtime(account_type,account_id,event_seq) "
                        "VALUES('STOCK','test-account',37)")
            cur.execute("SELECT created_at FROM " + self.repo_s + ".account_runtime")
            created_at = cur.fetchone()[0]
        self.connection.commit()
        self.assertEqual(initialize_schema(self), {"schema_version": repo_SCHEMA_VERSION})
        with self.connection.cursor() as cur:
            cur.execute("SELECT event_seq,created_at FROM " + self.repo_s + ".account_runtime")
            self.assertEqual(cur.fetchone(), (37, created_at))
        self.connection.rollback()
        with self.connection.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            self.assertEqual(repo_check_storage_schema(cur, self.repo_s),
                             {"schema_version": 3, "ready": True})
        self.connection.rollback()
        with self.connection.cursor() as cur:
            cur.execute("UPDATE " + self.repo_s + ".schema_version SET version=2")
            with self.assertRaises(OrderError) as caught:
                repo_check_storage_schema(cur, self.repo_s)
            self.assertEqual(caught.exception.code, "SCHEMA_VERSION_MISMATCH")
        self.connection.rollback()

    def test_generated_values_business_constraints_and_catalog_drift(self):
        initialize_schema(self)
        order_id = "test-order"
        with self.connection.cursor() as cur:
            scope = ("STOCK", "test-account", order_id)
            item = {"item_id": "0001", "symbol": "510300.SH", "side": "BUY"}
            cur.execute("INSERT INTO " + self.repo_s + ".order_items "
                        "(account_type,account_id,order_id,record_id,document) "
                        "VALUES(%s,%s,%s,%s,%s::jsonb)",
                        scope + (str(uuid.uuid4()), json.dumps(item)))
            cur.execute("SELECT item_id,symbol,side FROM " + self.repo_s + ".order_items")
            self.assertEqual(cur.fetchone(), ("0001", "510300.SH", "BUY"))
            fill = {"quantity": "1.2500", "amount": "123.4500", "trade_id": "0007"}
            cur.execute("INSERT INTO " + self.repo_s + ".fills "
                        "(account_type,account_id,order_id,record_id,document) "
                        "VALUES(%s,%s,%s,%s,%s::jsonb)",
                        scope + (str(uuid.uuid4()), json.dumps(fill)))
            cur.execute("SELECT quantity,amount,trade_id FROM " + self.repo_s + ".fills")
            quantity, amount, trade_id = cur.fetchone()
            self.assertEqual((str(quantity), str(amount), trade_id), ("1.2500", "123.4500", "0007"))
        self.connection.commit()
        from psycopg2 import IntegrityError
        with self.assertRaises(IntegrityError):
            with self.connection.cursor() as cur:
                cur.execute("INSERT INTO " + self.repo_s + ".order_items "
                            "(account_type,account_id,order_id,record_id,document) "
                            "VALUES(%s,%s,%s,%s,%s::jsonb)",
                            scope + (str(uuid.uuid4()), json.dumps(item)))
        self.connection.rollback()
        with self.assertRaises(IntegrityError):
            with self.connection.cursor() as cur:
                cur.execute("INSERT INTO " + self.repo_s + ".order_items "
                            "(account_type,account_id,order_id,record_id,document) "
                            "VALUES(%s,%s,%s,%s,%s::jsonb)",
                            scope + (str(uuid.uuid4()), json.dumps({"item_id": "  "})))
        self.connection.rollback()
        with self.connection.cursor() as cur:
            cur.execute("DROP INDEX " + self.repo_s + ".fills_order_identity_lookup")
            with self.assertRaises(OrderError) as caught:
                repo_check_storage_schema(cur, self.repo_s)
            self.assertEqual(caught.exception.code, "SCHEMA_NOT_READY")
        self.connection.rollback()
        with self.connection.cursor() as cur:
            cur.execute("ALTER TABLE " + self.repo_s + ".order_events "
                        "ALTER COLUMN created_at DROP DEFAULT")
            with self.assertRaises(OrderError) as caught:
                repo_check_storage_schema(cur, self.repo_s)
            self.assertEqual(caught.exception.code, "SCHEMA_NOT_READY")
        self.connection.rollback()
        with self.connection.cursor() as cur:
            cur.execute("ALTER TABLE " + self.repo_s + ".qmt_tasks DROP COLUMN status")
            cur.execute("ALTER TABLE " + self.repo_s + ".qmt_tasks ADD COLUMN status text "
                        "GENERATED ALWAYS AS (document->>'market') STORED")
            with self.assertRaises(OrderError) as caught:
                repo_check_storage_schema(cur, self.repo_s)
            self.assertEqual(caught.exception.code, "SCHEMA_NOT_READY")
        self.connection.rollback()
