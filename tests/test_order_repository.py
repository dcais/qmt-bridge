# -*- coding: utf-8 -*-
"""真实 PostgreSQL 隔离 schema 验证；未配置数据库时明确跳过。"""
import os
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

from order_bridge.common import OrderError
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
        self.assertEqual(other.check_schema(), {"schema_version": 1, "ready": True})
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
        self.assertEqual(self.repo.health(), {"schema_version": 1, "ready": True,
                                             "executor": False, "unknown_order_count": 0})
        self.owner()
        doc = self.order()
        self.repo.update_order(doc["order_id"], "UNKNOWN", lambda row: row.update(submission_status="UNKNOWN"))
        health = self.repo.health()
        self.assertTrue(health["executor"])
        self.assertEqual(health["unknown_order_count"], 1)
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
        def freeze(row):
            row.update(submission_status="UNKNOWN", cancel_requested=True, cancel_status="UNKNOWN")
        def requested(row):
            row.update(submission_status="CONFIRMED", cancel_requested=True, cancel_status="REQUESTED")
        self.repo.update_order(frozen["order_id"], "UNKNOWN", freeze)
        self.repo.update_order(fresh["order_id"], "CANCEL_REQUESTED", requested)
        self.assertEqual(self.repo.cancellation_orders(limit=1)[0]["order_id"], fresh["order_id"])

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
