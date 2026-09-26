# -*- coding: utf-8 -*-
"""真实 PostgreSQL 隔离 schema 验证；未配置数据库时明确跳过。"""
import os
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

from order_bridge.common import OrderError, json_text, new_order_document
from order_bridge.repository import PostgresRepository
from tools.order_schema import initialize_schema


def repo_test_config():
    return {"pg_host": os.environ.get("ORDER_TEST_PGHOST", "127.0.0.1"),
            "pg_port": int(os.environ.get("ORDER_TEST_PGPORT", "5432")),
            "pg_database": os.environ.get("ORDER_TEST_PGDATABASE", "postgres"),
            "pg_user": os.environ.get("ORDER_TEST_PGUSER", "postgres"),
            "pg_password": os.environ.get("ORDER_TEST_PGPASSWORD", ""),
            "pg_schema": "qmt_test_" + uuid.uuid4().hex,
            "pg_lock_timeout_ms": 5000, "pg_statement_timeout_ms": 10000}


class RepositoryConfigurationTests(unittest.TestCase):
    def test_schema_identifier_rejects_injection(self):
        with self.assertRaises(OrderError):
            PostgresRepository({"pg_schema": 'a"; DROP SCHEMA public;--'}, "account")

    def test_connection_error_does_not_disclose_password(self):
        def connect(**kwargs):
            raise RuntimeError("secret_password sensitive server detail")
        repo = PostgresRepository({"pg_password": "secret_password"}, "account", connect_factory=connect)
        with self.assertRaises(OrderError) as caught:
            repo.check_schema()
        self.assertNotIn("secret", str(caught.exception))
        self.assertEqual(caught.exception.status, 503)


@unittest.skipUnless(os.environ.get("ORDER_TEST_PGHOST"), "ORDER_TEST_PGHOST unset: real PostgreSQL integration skipped")
class PostgresRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.config = repo_test_config()
        self.repo = PostgresRepository(self.config, "test-account")
        initialize_schema(self.repo)
        self.repo.ensure_account_runtime()
        self.extra = []

    def tearDown(self):
        for repo in self.extra + [self.repo]:
            repo.close()
        conn = self.repo.repo_connect()
        try:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA " + self.repo.repo_s + " CASCADE")
            conn.commit()
        finally:
            conn.close()

    def other(self, account="test-account", factory=None):
        repo = PostgresRepository(self.config, account, connect_factory=factory)
        self.extra.append(repo)
        return repo

    def request(self, client="order-one"):
        return {"client_order_id": client, "account_id": "test-account", "order_type": "SINGLE",
                "symbol": "510300.SH", "side": "BUY", "quantity": 100,
                "execution": {"type": "DIRECT"}}

    def order(self, client="order-one"):
        return self.repo.accept_order(self.request(client))[1]

    def owner(self):
        self.repo.acquire_executor("instance-one", "host-one")

    def test_schema_check_needs_no_account_and_does_not_insert_one(self):
        other = self.other("new-startup-account")
        self.assertEqual(other.check_schema(), {"schema_version": 2, "ready": True})
        def records(cur):
            cur.execute("SELECT account_id FROM " + other.repo_s +
                        ".account_runtime WHERE account_type=%s AND account_id=%s", other.repo_scope)
            return [tuple(row) for row in cur.fetchall()]
        self.assertEqual(other.repo_run(records), [])
        self.assertTrue(other.ensure_account_runtime())
        self.assertEqual(other.repo_run(records), [("new-startup-account",)])

    def test_concurrent_account_creation_has_one_row_and_preserves_existing_state(self):
        other = self.other("concurrent-startup-account")
        barrier = threading.Barrier(8)
        def ensure(index):
            barrier.wait()
            return other.ensure_account_runtime()
        with ThreadPoolExecutor(max_workers=8) as pool:
            created = list(pool.map(ensure, range(8)))
        self.assertEqual(sum(created), 1)
        def initial(cur):
            cur.execute("SELECT event_seq,executor_host,executor_instance,executor_epoch FROM " + other.repo_s +
                        ".account_runtime WHERE account_type=%s AND account_id=%s", other.repo_scope)
            return [tuple(row) for row in cur.fetchall()]
        self.assertEqual(other.repo_run(initial), [(0, None, None, 0)])
        def set_existing(cur):
            cur.execute("UPDATE " + other.repo_s + ".account_runtime SET event_seq=37,executor_host='bound-host',"
                        "executor_instance='running-instance',executor_epoch=8 "
                        "WHERE account_type=%s AND account_id=%s", other.repo_scope)
        other.repo_run(set_existing)
        self.assertFalse(other.ensure_account_runtime())
        self.assertEqual(other.repo_run(initial), [(37, "bound-host", "running-instance", 8)])
        with self.assertRaises(OrderError) as caught:
            other.acquire_executor("new-instance", "different-host")
        self.assertEqual(caught.exception.code, "EXECUTOR_HOST_MISMATCH")

    def test_concurrent_idempotent_accept_and_conflict(self):
        barrier = threading.Barrier(8)
        def accept(index):
            barrier.wait()
            return self.repo.accept_order(self.request())
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(accept, range(8)))
        self.assertEqual(sum(int(row[0]) for row in results), 1)
        self.assertEqual(len(set(row[1]["order_id"] for row in results)), 1)
        changed = self.request()
        changed["quantity"] = 200
        with self.assertRaises(OrderError) as caught:
            self.repo.accept_order(changed)
        self.assertEqual(caught.exception.status, 409)
        self.assertEqual(len(self.repo.events()["events"]), 1)

    def test_event_sequence_atomic_with_projection_and_rollback(self):
        doc = self.order()
        self.repo.update_order(doc["order_id"], "NO_CHANGE", lambda row: None)
        self.assertEqual(len(self.repo.events()["events"]), 1)
        def failure(row):
            row["submission_status"] = "UNKNOWN"
            raise OrderError(400, "TEST", "deliberate rollback")
        with self.assertRaises(OrderError):
            self.repo.update_order(doc["order_id"], "FAILED", failure)
        self.assertEqual(self.repo.get_by_id(doc["order_id"])["submission_status"], "QUEUED")
        def mutate(index):
            self.repo.update_order(doc["order_id"], "CHANGE", lambda row: row.update(counter=row.get("counter", 0) + 1))
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(mutate, range(12)))
        events = self.repo.events(limit=100)["events"]
        self.assertEqual([row["event_seq"] for row in events], list(range(1, 14)))
        self.assertEqual(events[-1]["order"], self.repo.get_by_id(doc["order_id"]))
        self.assertEqual(events[-1]["order"]["counter"], 12)

    def test_executor_exclusion_host_binding_and_lost_session(self):
        self.owner()
        second = self.other()
        with self.assertRaises(OrderError) as caught:
            second.acquire_executor("other", "host-one")
        self.assertEqual(caught.exception.code, "EXECUTOR_ALREADY_RUNNING")
        self.repo.release_executor()
        with self.assertRaises(OrderError) as caught:
            second.acquire_executor("other", "host-two")
        self.assertEqual(caught.exception.code, "EXECUTOR_HOST_MISMATCH")
        second.acquire_executor("other", "host-one")
        second.repo_executor.close()
        with self.assertRaises(OrderError):
            second.check_executor()
        with self.assertRaises(OrderError):
            second.acquire_executor("third", "host-one")

    def test_cancel_before_claim_and_cancel_id_conflict(self):
        doc = self.order()
        self.owner()
        request = {"client_order_id": doc["client_order_id"], "cancel_request_id": "cancel-one"}
        status, response = self.repo.request_cancel(request)
        self.assertEqual(status, 200)
        self.assertEqual(response["cancel_status"], "CONFIRMED")
        self.assertFalse(self.repo.claim_submission(doc["order_id"])[0])
        self.assertTrue(self.repo.request_cancel(request)[1]["replayed"])
        self.order("order-two")
        with self.assertRaises(OrderError) as caught:
            self.repo.request_cancel(dict(request, client_order_id="order-two"))
        self.assertEqual(caught.exception.status, 409)

    def test_claim_once_recover_unknown_and_no_reissue(self):
        self.owner()
        doc = self.order()
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda n: self.repo.claim_submission(doc["order_id"]), range(4)))
        self.assertEqual(sum(int(row[0]) for row in results), 1)
        self.repo.recover()
        current = self.repo.get_by_id(doc["order_id"])
        self.assertEqual(current["submission_status"], "UNKNOWN")
        self.assertEqual(current["attempts"][0]["status"], "UNKNOWN")
        self.assertFalse(self.repo.claim_submission(doc["order_id"])[0])

    def test_recover_queued_basket_rechecks_existing_qmt_basket(self):
        self.owner()
        def basket(client, stage, complete, state):
            request = {"client_order_id": client, "account_id": "test-account", "order_type": "BASKET",
                       "items": [{"item_id": "first", "symbol": "510300.SH", "side": "BUY", "quantity": 100}],
                       "execution": {"type": "DIRECT"}}
            doc = self.repo.accept_order(request)[1]
            frozen = {"orderCode": doc["basket_name"], "basket_payload": {"stock": "510300.SH", "quantity": 100}}
            def prepared(row):
                row.update(resolved_request=frozen, preparation_stage=stage,
                           preparation_complete=complete, basket_state=state)
            return self.repo.update_order(doc["order_id"], "BASKET_PREPARED", prepared), frozen
        in_set, set_frozen = basket("basket-set", "BASKET_SET", False, "PENDING")
        verified, verified_frozen = basket("basket-verified", None, True, "VERIFIED")
        claimed, claimed_frozen = basket("basket-submitting", None, True, "VERIFIED")
        self.assertTrue(self.repo.claim_submission(claimed["order_id"])[0])
        self.repo.recover()
        for before, frozen in ((in_set, set_frozen), (verified, verified_frozen)):
            after = self.repo.get_by_id(before["order_id"])
            self.assertEqual(after["submission_status"], "QUEUED")
            self.assertEqual(after["preparation_stage"], "BASKET_GET")
            self.assertFalse(after["preparation_complete"])
            self.assertEqual(after["basket_state"], "PENDING")
            self.assertEqual(after["resolved_request"], frozen)
            self.assertEqual(after["basket_name"], before["basket_name"])
        after_claim = self.repo.get_by_id(claimed["order_id"])
        self.assertEqual(after_claim["submission_status"], "UNKNOWN")
        self.assertEqual(after_claim["resolved_request"], claimed_frozen)
        self.assertFalse(self.repo.claim_submission(claimed["order_id"])[0])

    def test_claim_requires_executor_and_deadline(self):
        doc = self.order()
        with self.assertRaises(OrderError):
            self.repo.claim_submission(doc["order_id"])
        self.owner()
        self.repo.update_order(doc["order_id"], "DEADLINE", lambda row: row.update(submit_before="2000-01-01T00:00:00Z"))
        claimed, doc = self.repo.claim_submission(doc["order_id"])
        self.assertFalse(claimed)
        self.assertEqual(doc["submission_status"], "EXPIRED")
        self.assertEqual(doc["attempts"], [])

    def test_pending_callback_replayed_and_fill_deduplicated(self):
        doc = self.order()
        fill = {"qmt_order_id": "qmt-one", "trade_id": "trade-one", "trading_day": "20260925",
                "market": "SH", "symbol": "510300.SH", "side": "BUY", "quantity": 100, "amount": "420"}
        self.assertIsNone(self.repo.ingest_observation("deal", fill))
        raw = {"remark": doc["remark"], "qmt_order_id": "qmt-one", "trading_day": "20260925", "market": "SH",
               "symbol": "510300.SH", "side": "BUY", "quantity": 100, "filled_quantity": 100, "status": 56}
        current = self.repo.ingest_observation("order", raw)
        self.assertEqual(current["filled_quantity"], 100)
        current = self.repo.ingest_observation("deal", fill)
        self.assertEqual(len(current["fills"]), 1)
        self.assertEqual(len(self.repo.lookup_observations(doc["order_id"])), 2)
        counts = self.repo.repo_run(lambda cur: self.counts(cur))
        self.assertEqual(counts, [1, 1, 1])

    def counts(self, cur):
        result = []
        for table in ("order_items", "qmt_orders", "fills"):
            cur.execute("SELECT count(*) FROM " + self.repo.repo_s + "." + table)
            result.append(cur.fetchone()[0])
        return result

    def test_account_guard_and_no_symbol_guess(self):
        doc = self.order()
        raw = {"remark": doc["remark"], "account_id": "wrong-account", "qmt_order_id": "qmt-one"}
        self.assertIsNone(self.repo.ingest_observation("order", raw))
        self.assertIsNone(self.repo.ingest_observation("order", {"symbol": "510300.SH", "quantity": 100}))
        self.assertEqual(self.repo.get_by_id(doc["order_id"])["qmt_orders"], [])

    def test_reused_qmt_id_requires_unambiguous_day_and_market(self):
        first, second = self.order("first"), self.order("second")
        base = {"qmt_order_id": "reused", "market": "SH", "symbol": "510300.SH", "side": "BUY",
                "quantity": 100, "status": 50}
        self.repo.ingest_observation("order", dict(base, remark=first["remark"], trading_day="20260924"))
        self.repo.ingest_observation("order", dict(base, remark=second["remark"], trading_day="20260925"))
        self.assertIsNone(self.repo.ingest_observation("order", base))
        matched = self.repo.ingest_observation("order", dict(base, trading_day="20260925"))
        self.assertEqual(matched["order_id"], second["order_id"])

    def test_cancel_recovery_preserves_submit_confirmation(self):
        self.owner()
        doc = self.order()
        self.repo.claim_submission(doc["order_id"])
        raw = {"remark": doc["remark"], "qmt_order_id": "qmt-one", "trading_day": "20260925", "market": "SH",
               "symbol": "510300.SH", "side": "BUY", "quantity": 100, "status": 50}
        self.repo.ingest_observation("order", raw)
        self.repo.request_cancel({"client_order_id": "order-one", "cancel_request_id": "cancel-one"})
        def cancelling(row):
            row["attempts"][0]["status"] = "RETURNED"
            row["attempts"].append({"attempt_id": "cancel-attempt", "kind": "CANCEL_ORDER", "target_id": "qmt-one",
                                    "cancel_request_id": "cancel-one", "status": "CALLING"})
        self.repo.update_order(doc["order_id"], "CANCEL_CALLING", cancelling)
        self.repo.recover()
        current = self.repo.get_by_id(doc["order_id"])
        self.assertEqual(current["submission_status"], "CONFIRMED")
        self.assertEqual(current["attempts"][0]["status"], "RETURNED")
        self.assertEqual(current["attempts"][1]["status"], "UNKNOWN")
        self.assertEqual(current["cancel_status"], "UNKNOWN")

    def test_commit_ambiguity_is_error_and_original_id_reads_back(self):
        from pg8000 import dbapi
        class AmbiguousConnection(object):
            def __init__(self, conn):
                self.conn, self.commits = conn, 0
            def cursor(self):
                return self.conn.cursor()
            def commit(self):
                self.commits += 1
                self.conn.commit()
                if self.commits == 2:
                    raise OSError("lost commit reply")
            def rollback(self):
                return self.conn.rollback()
            def close(self):
                return self.conn.close()
        repo = self.other(factory=lambda **kwargs: AmbiguousConnection(dbapi.connect(**kwargs)))
        with self.assertRaises(OrderError) as caught:
            repo.accept_order(self.request())
        self.assertEqual(caught.exception.code, "PERSISTENCE_OUTCOME_UNKNOWN")
        self.assertEqual(self.repo.get_order("order-one")["client_order_id"], "order-one")
        self.assertFalse(self.repo.accept_order(self.request())[0])

    def test_account_isolation_pagination_and_queue_fairness(self):
        first = self.order("first")
        self.repo.update_order(first["order_id"], "UNKNOWN", lambda row: row.update(submission_status="UNKNOWN"))
        second = self.order("second")
        self.assertEqual(self.repo.queued_orders(limit=1)[0]["order_id"], second["order_id"])
        page = self.repo.list_orders(active=False, limit=1)
        self.assertTrue(page["has_more"])
        following = self.repo.list_orders(active=False, limit=1, cursor=page["next_cursor"])
        self.assertFalse(following["has_more"])
        self.assertNotEqual(page["orders"][0]["order_id"], following["orders"][0]["order_id"])
        other = self.other("different-account")
        other.ensure_account_runtime()
        self.assertEqual(other.events()["events"], [])
        with self.assertRaises(OrderError) as caught:
            other.get_by_id(first["order_id"])
        self.assertEqual(caught.exception.status, 404)

    def test_health_and_canonical_event_contract(self):
        self.assertEqual(self.repo.health(), {"schema_version": 2, "ready": True,
                                             "executor": False, "unknown_order_count": 0, "pending_count": 0})
        self.owner()
        doc = self.order()
        self.repo.update_order(doc["order_id"], "UNKNOWN", lambda row: row.update(submission_status="UNKNOWN"))
        health = self.repo.health()
        self.assertTrue(health["executor"])
        self.assertEqual(health["unknown_order_count"], 1)
        self.assertEqual(health["pending_count"], 1)
        event = self.repo.events(after=1)["events"][0]
        self.assertEqual(event["event_id"], 2)
        self.assertEqual(event["order_version"], 2)
        self.assertEqual(event["type"], "UNKNOWN")
        self.assertEqual(event["recorded_at"], event["data"]["updated_at"])
        self.assertEqual(event["data"], self.repo.get_by_id(doc["order_id"]))

    def test_startup_check_does_not_create_missing_schema(self):
        config = dict(self.config, pg_schema="qmt_absent_" + uuid.uuid4().hex)
        missing = PostgresRepository(config, "test-account")
        self.extra.append(missing)
        with self.assertRaises(OrderError) as caught:
            missing.check_schema()
        self.assertEqual(caught.exception.code, "SCHEMA_NOT_READY")
        def exists(cur):
            cur.execute("SELECT 1 FROM pg_namespace WHERE nspname=%s", (config["pg_schema"],))
            return cur.fetchone()
        self.assertIsNone(self.repo.repo_run(exists))

    def test_startup_check_reports_missing_table_and_column_without_repair(self):
        def remove_table(cur):
            cur.execute("DROP TABLE " + self.repo.repo_s + ".fills")
        self.repo.repo_run(remove_table)
        with self.assertRaises(OrderError) as caught:
            self.repo.check_schema()
        self.assertEqual(caught.exception.code, "SCHEMA_NOT_READY")
        self.assertIn("fills", caught.exception.message)
        initialize_schema(self.repo)
        def remove_column(cur):
            cur.execute("ALTER TABLE " + self.repo.repo_s + ".orders DROP COLUMN request_hash")
        self.repo.repo_run(remove_column)
        with self.assertRaises(OrderError) as caught:
            self.repo.check_schema()
        self.assertIn("request_hash", caught.exception.message)

    def test_installer_rejects_incompatible_version_before_ddl(self):
        def incompatible(cur):
            cur.execute("UPDATE " + self.repo.repo_s + ".schema_version SET version=999")
            cur.execute("DROP TABLE " + self.repo.repo_s + ".fills")
        self.repo.repo_run(incompatible)
        with self.assertRaises(OrderError) as caught:
            initialize_schema(self.repo)
        self.assertEqual(caught.exception.code, "SCHEMA_VERSION_MISMATCH")
        def table_exists(cur):
            cur.execute("SELECT to_regclass(%s)", (self.repo.repo_s + ".fills",))
            return cur.fetchone()[0]
        self.assertIsNone(self.repo.repo_run(table_exists))

    def test_new_cancel_precedes_unresolved_unknown_cancel(self):
        frozen, fresh = self.order("frozen"), self.order("fresh")
        for doc in (frozen, fresh):
            self.repo.ingest_observation("order", {"remark": doc["remark"], "qmt_order_id": doc["client_order_id"],
                                                 "trading_day": "20260925", "market": "SH", "symbol": "510300.SH",
                                                 "side": "BUY", "quantity": 100, "status": 50})
            self.repo.request_cancel({"client_order_id": doc["client_order_id"],
                                      "cancel_request_id": "cancel-" + doc["client_order_id"]})
        self.repo.update_order(frozen["order_id"], "CANCEL_UNKNOWN", lambda row: row["attempts"].append(
            {"kind": "CANCEL_ORDER", "target_id": "frozen", "cancel_request_id": "cancel-frozen", "status": "UNKNOWN"}))
        self.assertEqual(self.repo.cancellation_orders(limit=1)[0]["order_id"], fresh["order_id"])

    def test_selectors_rounds_and_stale_reconcile_fact_version(self):
        docs = [self.order("page-" + str(index)) for index in range(5)]
        ids = sorted(row["order_id"] for row in docs)
        queued = []
        cursor = None
        while True:
            page = self.repo.queued_orders(limit=2, cursor=cursor)
            queued.extend(row["order_id"] for row in page)
            if len(page) < 2:
                break
            cursor = page[-1]["order_id"]
        self.assertEqual(queued, ids)
        first = docs[0]
        raw = {"remark": first["remark"], "qmt_order_id": "qmt-" + first["client_order_id"],
               "trading_day": "20260925", "market": "SH", "symbol": "510300.SH", "side": "BUY",
               "quantity": 100, "filled_quantity": 100, "status": 56}
        current = self.repo.ingest_observation("order", raw)
        self.assertEqual(current["fact_version"], 1)
        self.assertEqual([row["order_id"] for row in self.repo.reconcile_orders(limit=10)], [first["order_id"]])
        batch = self.repo.begin_reconcile_batch(limit=1, round_id="round-one")
        self.assertEqual(len(batch["orders"]), 1)
        self.assertEqual(batch["orders"][0]["order_id"], first["order_id"])
        self.assertEqual(self.repo.begin_reconcile_batch(limit=1, round_id="round-one")["orders"], [])
        self.repo.ingest_observation("deal", {"qmt_order_id": raw["qmt_order_id"], "trade_id": "trade-one",
                                               "trading_day": "20260925", "market": "SH", "symbol": "510300.SH",
                                               "side": "BUY", "quantity": 100, "amount": "420"})
        self.assertFalse(self.repo.finish_reconcile(first["order_id"], current["fact_version"], True))
        newer = self.repo.get_by_id(first["order_id"])
        self.assertTrue(newer["reconcile_pending"])
        self.assertEqual(newer["fact_version"], 2)
        self.assertTrue(self.repo.finish_reconcile(first["order_id"], newer["fact_version"], True))
        finished = self.repo.get_by_id(first["order_id"])
        self.assertFalse(finished["reconcile_pending"])
        self.assertIsNotNone(finished["last_reconciled_at"])
        self.assertEqual(self.repo.ingest_observation("deal", {"qmt_order_id": raw["qmt_order_id"], "trade_id": "trade-one",
                                                                "trading_day": "20260925", "market": "SH", "symbol": "510300.SH",
                                                                "side": "BUY", "quantity": 100, "amount": "420"})["fact_version"], 2)
        self.assertFalse(self.repo.get_by_id(first["order_id"])["reconcile_pending"])
        reopened = self.repo.ingest_observation("order", dict(raw, qmt_order_id="late-child", status=50,
                                                                 filled_quantity=0))
        self.assertTrue(reopened["reconcile_pending"])
        self.assertEqual(reopened["fact_version"], 3)
        self.assertIsNotNone(self.repo.reconcile_history_start())

    def test_restart_gap_reopens_terminal_in_bounded_pages(self):
        docs = [self.order("gap-" + str(index)) for index in range(3)]
        for doc in docs:
            raw = {"remark": doc["remark"], "qmt_order_id": "qmt-" + doc["client_order_id"],
                   "trading_day": "20260925", "market": "SH", "symbol": "510300.SH", "side": "BUY",
                   "quantity": 100, "filled_quantity": 100, "status": 56}
            self.repo.ingest_observation("order", raw)
            current = self.repo.ingest_observation("deal", {"qmt_order_id": raw["qmt_order_id"],
                                                       "trade_id": "fill-" + doc["client_order_id"],
                                                       "trading_day": "20260925", "market": "SH", "symbol": "510300.SH",
                                                       "side": "BUY", "quantity": 100, "amount": "420"})
            self.assertTrue(self.repo.finish_reconcile(doc["order_id"], current["fact_version"], True))
            self.assertFalse(self.repo.get_by_id(doc["order_id"])["reconcile_pending"])
        cursor, marked = None, 0
        while True:
            page = self.repo.mark_reconcile_gap("2026-09-25T00:00:00Z", limit=1, cursor=cursor)
            marked += page["marked"]
            if not page["has_more"]:
                break
            cursor = page["next_cursor"]
        self.assertEqual(marked, 3)
        for doc in docs:
            current = self.repo.get_by_id(doc["order_id"])
            self.assertTrue(current["reconcile_pending"])
            self.assertTrue(current["reconcile_requested"])

    def test_authority_claims_do_not_touch_advisory_connection(self):
        self.owner()
        authority = (self.repo.repo_executor_instance, self.repo.repo_executor_epoch)
        queued = self.order("authority-submit")
        original_check = self.repo.check_executor
        self.repo.check_executor = lambda: self.fail("DB claim touched advisory connection")
        try:
            claimed, submitted = self.repo.claim_submission(queued["order_id"], authority=authority)
            self.assertTrue(claimed)
            self.assertEqual(submitted["submission_status"], "SUBMITTING")
            with self.assertRaises(OrderError) as caught:
                self.repo.claim_submission(queued["order_id"], authority=(authority[0], authority[1] + 1))
            self.assertEqual(caught.exception.code, "EXECUTOR_LOCK_LOST")
            live = self.order("authority-cancel")
            self.repo.ingest_observation("order", {"remark": live["remark"], "qmt_order_id": "cancel-target",
                                                   "trading_day": "20260925", "market": "SH", "symbol": "510300.SH",
                                                   "side": "BUY", "quantity": 100, "status": 50})
            self.repo.request_cancel({"client_order_id": live["client_order_id"], "cancel_request_id": "cancel-one"})
            action = self.repo.cancellation_orders(limit=1)[0]
            from order_bridge.state import pending_cancellations
            target = pending_cancellations(action)[0]
            claimed, current = self.repo.claim_cancel(live["order_id"], target, authority=authority)
            self.assertTrue(claimed)
            self.assertEqual(current["attempts"][-1]["status"], "CALLING")
            self.assertEqual(self.repo.cancellation_orders(limit=1), [])
            self.assertFalse(self.repo.claim_cancel(live["order_id"], target, authority=authority)[0])
        finally:
            self.repo.check_executor = original_check

    def test_query_merge_after_round_freeze_does_not_hide_callback(self):
        doc = self.order("round-fact")
        raw = {"remark": doc["remark"], "qmt_order_id": "qmt-round", "trading_day": "20260925",
               "market": "SH", "symbol": "510300.SH", "side": "BUY", "quantity": 100,
               "filled_quantity": 100, "status": 56}
        initial = self.repo.ingest_observation("order", raw, source="callback")
        self.assertEqual(initial["fact_version"], 1)
        self.repo.begin_reconcile_batch(limit=1, round_id="round-frozen")
        from_query = self.repo.ingest_observation("deal", {"qmt_order_id": "qmt-round", "trade_id": "q-fill",
                                                             "trading_day": "20260925", "market": "SH",
                                                             "symbol": "510300.SH", "side": "BUY", "quantity": 100,
                                                             "amount": "420"}, source="query")
        self.assertEqual(from_query["fact_version"], 1)
        frozen = self.repo.reconcile_round_batch("round-frozen", limit=1)["orders"][0]
        self.assertEqual(frozen["reconcile_round_fact_version"], 1)
        self.assertTrue(self.repo.finish_reconcile(doc["order_id"], frozen["reconcile_round_fact_version"], True))
        self.assertFalse(self.repo.get_by_id(doc["order_id"])["reconcile_pending"])

        callback = self.repo.ingest_observation("order", dict(raw, qmt_order_id="late-qmt", status=50,
                                                                 filled_quantity=0), source="callback")
        self.assertEqual(callback["fact_version"], 2)
        self.repo.begin_reconcile_batch(limit=1, round_id="next-frozen")
        from_query = self.repo.ingest_observation("order", dict(raw, qmt_order_id="late-qmt", status=54,
                                                                   filled_quantity=0), source="query")
        self.assertEqual(from_query["fact_version"], 2)
        during_round = self.repo.ingest_observation("order", dict(raw, qmt_order_id="third-qmt", status=50,
                                                                    filled_quantity=0), source="callback")
        self.assertEqual(during_round["fact_version"], 3)
        frozen = self.repo.reconcile_round_batch("next-frozen", limit=1)["orders"][0]
        self.assertEqual(frozen["reconcile_round_fact_version"], 2)
        self.assertFalse(self.repo.finish_reconcile(doc["order_id"], frozen["reconcile_round_fact_version"], True))
        self.assertTrue(self.repo.get_by_id(doc["order_id"])["reconcile_pending"])

    def test_thousand_pending_reconcile_pages_without_starvation(self):
        # 批量装载合法文档，验证 100 限额游标及索引可用性；业务受理另有并发测试。
        docs = []
        for index in range(1000):
            doc = new_order_document(self.request("bulk-" + str(index)))
            doc["account_type"] = "STOCK"
            doc["submission_status"] = "UNKNOWN"
            doc["reconcile_pending"] = True
            doc["reconcile_requested"] = True
            doc["reconcile_due_at"] = "1970-01-01T00:00:00Z"
            doc["reconcile_priority"] = False
            doc["last_reconcile_attempt_at"] = None
            doc["fact_version"] = 0
            docs.append(doc)
        def insert(cur):
            sql = ("INSERT INTO " + self.repo.repo_s + ".orders "
                   "(account_type,account_id,order_id,client_order_id,request_hash,remark,active,document,"
                   "submission_status,cancel_ready,reconcile_pending,reconcile_priority,reconcile_due_at,"
                   "last_reconcile_attempt_at,last_reconciled_at,fact_version,created_at) "
                   "VALUES(%s,%s,%s,%s,%s,%s,true,%s::jsonb,'UNKNOWN',false,true,false,'epoch',null,null,0,%s)")
            for doc in docs:
                cur.execute(sql, self.repo.repo_scope +
                            (doc["order_id"], doc["client_order_id"], doc["request_hash"], doc["remark"],
                             json_text(doc), doc["created_at"]))
            cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname=%s AND tablename='orders'",
                        (self.config["pg_schema"],))
            return {row[0] for row in cur.fetchall()}
        indexes = self.repo.repo_run(insert)
        self.assertIn("order_reconcile_rotation_v2", indexes)
        self.assertIn("order_reconcile_due_v2", indexes)
        def explain(cur):
            # 小测试表不代表生产代价；关闭顺序扫描/排序仅证明选择器可使用对应索引。
            cur.execute("SET LOCAL enable_seqscan=off")
            cur.execute("SET LOCAL enable_bitmapscan=off")
            cur.execute("SET LOCAL enable_sort=off")
            plans = {}
            for name, suffix in (
                    ("queued", "submission_status='QUEUED' ORDER BY order_id"),
                    ("cancel", "cancel_ready=true ORDER BY order_id"),
                    ("reconcile", "reconcile_pending=true AND reconcile_due_at<=now() "
                                  "ORDER BY reconcile_priority DESC,"
                                  "COALESCE(last_reconcile_attempt_at,'epoch'::timestamptz),order_id")):
                cur.execute("EXPLAIN (COSTS OFF) SELECT document FROM " + self.repo.repo_s +
                            ".orders WHERE account_type=%s AND account_id=%s AND " + suffix + " LIMIT 100",
                            self.repo.repo_scope)
                plans[name] = "\n".join(row[0] for row in cur.fetchall())
            return plans
        plans = self.repo.repo_run(explain)
        self.assertIn("order_queued_v2", plans["queued"])
        self.assertIn("order_cancel_ready_v2", plans["cancel"])
        self.assertIn("order_reconcile_rotation_v2", plans["reconcile"])
        cursor, chosen, pages = None, [], 0
        while True:
            batch = self.repo.begin_reconcile_batch(limit=100, round_id="bulk-round", cursor=cursor)
            self.assertLessEqual(len(batch["orders"]), 100)
            chosen.extend(row["order_id"] for row in batch["orders"])
            pages += 1
            if not batch["has_more"]:
                break
            cursor = batch["next_cursor"]
        self.assertEqual(pages, 10)
        self.assertEqual(len(chosen), 1000)
        self.assertEqual(len(set(chosen)), 1000)
        cursor, processed, pages = None, [], 0
        while True:
            batch = self.repo.reconcile_round_batch("bulk-round", limit=100, cursor=cursor)
            processed.extend(row["order_id"] for row in batch["orders"])
            pages += 1
            if not batch["has_more"]:
                break
            cursor = batch["next_cursor"]
        self.assertEqual(pages, 10)
        self.assertEqual(set(processed), set(chosen))


    def manual_fixture(self, client="manual"):
        doc = self.order(client)
        doc = self.repo.update_order(doc["order_id"], "UNKNOWN", lambda row: row.update(submission_status="UNKNOWN"))
        raw = {"account_id": "test-account", "qmt_order_id": "manual-qmt", "symbol": "510300.SH", "side": "BUY",
               "trading_day": "20260925", "market": "SH", "quantity": 100, "status": 50}
        self.assertIsNone(self.repo.ingest_observation("order", raw))
        candidates = self.repo.lookup_observations(doc["order_id"], include_unassociated=True)
        obs_id = candidates[-1]["observation_id"]
        audit = {"operator": "test-human", "reason": "verified QMT persisted raw evidence",
                 "qmt_order_ids": ["manual-qmt"], "qmt_task_ids": [],
                 "evidence": {"manual_attribution": {"account_id": "test-account", "order_id": doc["order_id"],
                             "observation_ids": [obs_id], "basis": "human verified broker screen and exact submission chronology"}}}
        return doc, raw, obs_id, audit

    def test_manual_missing_remark_association_is_audited_raw_unchanged(self):
        doc, raw, obs_id, audit = self.manual_fixture()
        result = self.repo.manual_associate(doc["order_id"], [obs_id], doc["version"], audit)
        self.assertEqual(result["submission_status"], "CONFIRMED")
        self.assertEqual(result["qmt_orders"][0]["qmt_order_id"], "manual-qmt")
        self.assertEqual(result["manual_resolutions"][0]["observation_ids"], [obs_id])
        self.assertEqual(result["manual_resolutions"][0]["operator"], "test-human")
        observations = self.repo.lookup_observations(doc["order_id"])
        self.assertEqual(observations[0]["raw"], raw)
        self.assertTrue(observations[0]["applied"])
        self.assertEqual(self.repo.events()["events"][-1]["type"], "MANUAL_UNKNOWN_RESOLUTION")

    def test_manual_association_rejects_wrong_ids_missing_proof_and_stale_version(self):
        doc, raw, obs_id, audit = self.manual_fixture()
        for bad_audit in (dict(audit, qmt_order_ids=["invented"]), dict(audit, evidence="no attribution")):
            with self.assertRaises(OrderError) as caught:
                self.repo.manual_associate(doc["order_id"], [obs_id], doc["version"], bad_audit)
            self.assertEqual(caught.exception.status, 409)
        with self.assertRaises(OrderError) as caught:
            self.repo.manual_associate(doc["order_id"], [obs_id], doc["version"] - 1, audit)
        self.assertEqual(caught.exception.code, "ORDER_VERSION_CONFLICT")
        self.assertEqual(self.repo.lookup_observations(doc["order_id"]), [])
        self.assertEqual(self.repo.get_by_id(doc["order_id"])["submission_status"], "UNKNOWN")

    def test_manual_association_concurrency_exactly_one_resolution(self):
        doc, raw, obs_id, audit = self.manual_fixture()
        def apply(index):
            try:
                self.repo.manual_associate(doc["order_id"], [obs_id], doc["version"], audit)
                return "ok"
            except OrderError as exc:
                return exc.code
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(apply, range(2)))
        self.assertEqual(sorted(outcomes), ["ORDER_VERSION_CONFLICT", "ok"])
        self.assertEqual(len(self.repo.get_by_id(doc["order_id"])["manual_resolutions"]), 1)

    def test_manual_observation_owned_by_other_order_is_rejected(self):
        doc, raw, obs_id, audit = self.manual_fixture()
        other = self.order("real-owner")
        self.repo.ingest_observation("order", dict(raw, remark=other["remark"]))
        with self.assertRaises(OrderError) as caught:
            self.repo.manual_associate(doc["order_id"], [obs_id], doc["version"], audit)
        self.assertEqual(caught.exception.code, "OBSERVATION_CONFLICT")
        self.assertEqual(self.repo.get_by_id(doc["order_id"])["submission_status"], "UNKNOWN")

    def test_manual_rejects_contradictory_raw_identity_and_hides_bad_candidates(self):
        doc, raw, obs_id, audit = self.manual_fixture()
        for changes in ({"remark": "contradictory"}, {"side": "SELL"}, {"account_id": "other-account"}):
            self.repo.ingest_observation("order", dict(raw, **changes))
            def latest(cur):
                cur.execute("SELECT max(observation_id) FROM " + self.repo.repo_s + ".qmt_observations")
                return cur.fetchone()[0]
            invalid_id = self.repo.repo_run(latest)
            candidate_ids = [row["observation_id"] for row in self.repo.lookup_observations(doc["order_id"], True)]
            self.assertNotIn(invalid_id, candidate_ids)
            proof = dict(audit)
            proof["evidence"] = {"manual_attribution": dict(audit["evidence"]["manual_attribution"], observation_ids=[invalid_id])}
            with self.assertRaises(OrderError) as caught:
                self.repo.manual_associate(doc["order_id"], [invalid_id], doc["version"], proof)
            self.assertEqual(caught.exception.code, "OBSERVATION_CONFLICT")

    def test_repeated_external_queries_deduplicate_without_pending_replay(self):
        raw = {"qmt_order_id": "external", "symbol": "510300.SH", "side": "BUY", "status": 50}
        original = self.repo.repo_apply_pending
        def forbidden(cur):
            raise AssertionError("unrelated or duplicate observation must not scan pending records")
        self.repo.repo_apply_pending = forbidden
        try:
            for index in range(10):
                self.assertIsNone(self.repo.ingest_observation("order", raw, source="query"))
        finally:
            self.repo.repo_apply_pending = original
        def count(cur):
            cur.execute("SELECT count(*) FROM " + self.repo.repo_s + ".qmt_observations")
            return cur.fetchone()[0]
        self.assertEqual(self.repo.repo_run(count), 1)
        self.assertEqual(self.repo.events()["events"], [])

    def test_changed_status_of_known_identity_does_not_scan_pending(self):
        doc = self.order()
        raw = {"remark": doc["remark"], "qmt_order_id": "known", "symbol": "510300.SH", "side": "BUY", "status": 50}
        self.repo.ingest_observation("order", raw)
        def forbidden(cur):
            raise AssertionError("known identity change must not scan pending records")
        self.repo.repo_apply_pending = forbidden
        updated = self.repo.ingest_observation("order", dict(raw, status=51))
        self.assertEqual(updated["order_id"], doc["order_id"])


if __name__ == "__main__":
    unittest.main()
