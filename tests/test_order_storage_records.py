# -*- coding: utf-8 -*-
"""Real PostgreSQL checks for stable child identity and incremental projections."""
import copy
import os
import unittest
import uuid

from order_bridge.common import OrderError, new_record_metadata
from order_bridge.repository import PostgresRepository
from tests.test_order_repository import repo_test_config
from tools.order_schema import initialize_schema


@unittest.skipUnless(os.environ.get("ORDER_TEST_PGHOST"),
                     "ORDER_TEST_PGHOST unset: real PostgreSQL integration skipped")
class StorageRecordTests(unittest.TestCase):
    fields = {"order_items": "items", "execution_attempts": "attempts",
              "cancel_requests": "cancel_requests", "qmt_tasks": "qmt_tasks",
              "qmt_orders": "qmt_orders", "fills": "fills"}

    def setUp(self):
        self.config = repo_test_config()
        self.repo = PostgresRepository(self.config, "test-account")
        initialize_schema(self.repo)
        self.repo.ensure_account_runtime()

    def tearDown(self):
        self.repo.close()
        conn = self.repo.repo_connect()
        try:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA " + self.repo.repo_s + " CASCADE")
            conn.commit()
        finally:
            conn.close()

    def order(self, client="storage-records"):
        request = {"client_order_id": client, "account_id": "test-account",
                   "order_type": "SINGLE", "symbol": "510300.SH", "side": "BUY",
                   "quantity": 100, "execution": {"type": "DIRECT"}}
        return self.repo.accept_order(request)[1]

    def save(self, doc, child_fields=None):
        return self.repo.repo_run(
            lambda cur: self.repo.repo_save(cur, doc, "STORAGE_TEST", child_fields=child_fields),
            mutation=True)

    def snapshot(self, order_id):
        def read(cur):
            cur.execute("SELECT document,created_at,xmin::text,ctid::text FROM " + self.repo.repo_s +
                        ".orders WHERE account_type=%s AND account_id=%s AND order_id=%s",
                        self.repo.repo_scope + (order_id,))
            order_row = cur.fetchone()
            cur.execute("SELECT event_seq FROM " + self.repo.repo_s +
                        ".account_runtime WHERE account_type=%s AND account_id=%s", self.repo.repo_scope)
            sequence = cur.fetchone()[0]
            children = {}
            for table in self.fields:
                cur.execute("SELECT record_id::text,document,created_at,xmin::text,ctid::text FROM " +
                            self.repo.repo_s + "." + table +
                            " WHERE account_type=%s AND account_id=%s AND order_id=%s ORDER BY record_id",
                            self.repo.repo_scope + (order_id,))
                children[table] = cur.fetchall()
            return order_row, sequence, children
        return self.repo.repo_run(read)

    def populated_order(self):
        doc = self.order()
        doc["attempts"].append(dict(new_record_metadata(), attempt_id="attempt-1",
                                    kind="SUBMIT", status="RETURNED"))
        doc["cancel_requests"].append(dict(new_record_metadata(), cancel_request_id="cancel-1",
                                           status="REQUESTED"))
        doc["qmt_tasks"].append(dict(new_record_metadata(), qmt_task_id="task-1",
                                    trading_day="20260926", market="SH", status="RUNNING"))
        doc["qmt_orders"].append(dict(new_record_metadata(), qmt_order_id="order-1",
                                     trading_day="20260926", market="SH", item_id="single",
                                     symbol="510300.SH", side="BUY", status="WORKING"))
        doc["fills"].append(dict(new_record_metadata(), trade_id="trade-1",
                                qmt_order_id="order-1", item_id="single", trading_day="20260926",
                                market="SH", symbol="510300.SH", side="BUY",
                                quantity=10, amount="123.45"))
        self.save(doc)
        return self.repo.get_by_id(doc["order_id"])

    def assert_rejected_without_db_change(self, order_id, change, child_fields=None):
        before = self.snapshot(order_id)
        candidate = copy.deepcopy(self.repo.get_by_id(order_id))
        change(candidate)
        with self.assertRaises(OrderError) as caught:
            self.save(candidate, child_fields=child_fields)
        self.assertEqual(caught.exception.code, "PERSISTENCE_INCONSISTENT")
        self.assertEqual(self.snapshot(order_id), before)

    def test_all_six_child_rows_keep_metadata_and_skip_unchanged_updates(self):
        doc = self.populated_order()
        initial = self.snapshot(doc["order_id"])
        for table, field in self.fields.items():
            row = initial[2][table][0]
            record = doc[field][0]
            self.assertEqual(len(initial[2][table]), 1)
            self.assertEqual(row[0], record["record_id"])
            self.assertEqual(str(uuid.UUID(row[0])), row[0])
            self.assertEqual(row[1]["created_at"], record["created_at"])
            self.assertEqual(row[2].isoformat(), record["created_at"])
        self.save(copy.deepcopy(doc))
        unchanged = self.snapshot(doc["order_id"])
        for table in self.fields:
            self.assertEqual(unchanged[2][table], initial[2][table], table)

        changed = self.repo.get_by_id(doc["order_id"])
        changed["qmt_orders"][0]["status"] = "CANCELLED"
        self.save(changed, child_fields=("qmt_orders",))
        updated = self.snapshot(doc["order_id"])
        for table in self.fields:
            before_row, after_row = initial[2][table][0], updated[2][table][0]
            self.assertEqual(after_row[0], before_row[0], table)
            self.assertEqual(after_row[2], before_row[2], table)
            if table != "qmt_orders":
                self.assertEqual(after_row, before_row, table)
            else:
                self.assertEqual(after_row[1]["status"], "CANCELLED")

    def test_removed_replaced_or_retimed_children_roll_back(self):
        doc = self.populated_order()
        order_id = doc["order_id"]
        for field in self.fields.values():
            with self.subTest(field=field, change="removed"):
                self.assert_rejected_without_db_change(order_id, lambda row, field=field: row[field].pop())
            with self.subTest(field=field, change="empty"):
                self.assert_rejected_without_db_change(order_id, lambda row, field=field: row.update({field: []}))
            with self.subTest(field=field, change="uuid"):
                self.assert_rejected_without_db_change(
                    order_id, lambda row, field=field: row[field][0].update(record_id=str(uuid.uuid4())))
            with self.subTest(field=field, change="created_at"):
                self.assert_rejected_without_db_change(
                    order_id, lambda row, field=field: row[field][0].update(
                        created_at="2000-01-01T00:00:00+00:00"))

    def test_duplicate_business_identity_and_unselected_changes_roll_back(self):
        doc = self.populated_order()
        order_id = doc["order_id"]
        for field in self.fields.values():
            with self.subTest(field=field):
                def duplicate(row, field=field):
                    repeated = copy.deepcopy(row[field][0])
                    repeated["record_id"] = str(uuid.uuid4())
                    row[field].append(repeated)
                self.assert_rejected_without_db_change(order_id, duplicate)
        self.assert_rejected_without_db_change(
            order_id, lambda row: row["items"][0].update(symbol="159001.SZ"),
            child_fields=("fills",))
        self.assert_rejected_without_db_change(
            order_id, lambda row: row.update(created_at="2000-01-01T00:00:00+00:00"))

    def test_qmt_identity_completion_reuses_row_and_uuid(self):
        doc = self.order("identity-completion")
        base = {"remark": doc["remark"], "account_id": "test-account",
                "qmt_order_id": "same-order", "symbol": "510300.SH", "side": "BUY",
                "quantity": 100, "status": 50}
        first = self.repo.ingest_observation("order", base)
        self.assertEqual(len(first["qmt_orders"]), 1)
        original = self.snapshot(doc["order_id"])[2]["qmt_orders"][0]
        self.repo.ingest_observation("order", dict(base, trading_day="20260926", market="SH"))
        stored = self.repo.get_by_id(doc["order_id"])
        completed = self.snapshot(doc["order_id"])[2]["qmt_orders"][0]
        self.assertEqual(len(stored["qmt_orders"]), 1)
        self.assertEqual(completed[0], original[0])
        self.assertEqual(completed[2], original[2])
        self.assertEqual(stored["qmt_orders"][0]["trading_day"], "20260926")
        self.assertEqual(stored["qmt_orders"][0]["market"], "SH")

        task = {"remark": doc["remark"], "account_id": "test-account",
                "qmt_task_id": "same-task", "status": 3}
        self.repo.ingest_observation("task", task)
        original_task = self.snapshot(doc["order_id"])[2]["qmt_tasks"][0]
        self.repo.ingest_observation("task", dict(task, trading_day="20260926", market="SH"))
        completed_task = self.snapshot(doc["order_id"])[2]["qmt_tasks"][0]
        self.assertEqual(completed_task[0], original_task[0])
        self.assertEqual(completed_task[2], original_task[2])
        self.assertEqual(completed_task[1]["trading_day"], "20260926")
        self.assertEqual(completed_task[1]["market"], "SH")

    def test_non_uuid_cancel_request_id_replays_without_rewriting_child(self):
        doc = self.order("cancel-replay")
        request = {"client_order_id": doc["client_order_id"],
                   "cancel_request_id": "human-readable-cancel"}
        status, response = self.repo.request_cancel(request)
        self.assertIn(status, (200, 202))
        self.assertEqual(response["cancel_request_id"], request["cancel_request_id"])
        self.assertNotIn("record_id", response)
        first = self.snapshot(doc["order_id"])[2]["cancel_requests"][0]
        replay_status, replay = self.repo.request_cancel(request)
        self.assertEqual(replay_status, status)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["created_at"], response["created_at"])
        self.assertEqual(self.snapshot(doc["order_id"])[2]["cancel_requests"][0], first)

    def test_invalid_qmt_identity_keeps_raw_observation_without_child(self):
        doc = self.order("invalid-qmt-identity")
        raw = {"remark": doc["remark"], "account_id": "test-account",
               "qmt_order_id": "bad-market-order", "market": ["SH"],
               "symbol": "510300", "side": "BUY", "status": 50,
               "trading_day": "20260926", "quantity": 100}
        associated = self.repo.ingest_observation("order", raw)
        self.assertIsNotNone(associated)
        self.assertEqual(associated["qmt_orders"], [])
        self.assertEqual(associated["unassociated_evidence"][0]["reason"],
                         "INVALID_OBSERVATION_IDENTITY")

        def stored_raw(cur):
            cur.execute("SELECT raw,applied,order_id FROM " + self.repo.repo_s +
                        ".qmt_observations WHERE account_type=%s AND account_id=%s",
                        self.repo.repo_scope)
            return cur.fetchall()
        rows = self.repo.repo_run(stored_raw)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], (raw, True, doc["order_id"]))


if __name__ == "__main__":
    unittest.main()
