"""Ban contract and opt-in isolated PostgreSQL serialization tests."""
import json
import os
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from service.ipc_bridge.ipc_protocol.order_common import OrderError
from service.ipc_bridge.order.repository import PostgresRepository
import test_order_repository as fixtures

class BanCursor:
    def __init__(self):
        self.bans, self.row = {}, None
    def execute(self, sql, params=None):
        self.row = None
        if sql.startswith("SELECT event_seq"):
            self.row = (0,)
        elif "SELECT to_jsonb" in sql:
            self.row = ("77777777-7777-4777-8777-777777777777",)
        elif sql.startswith("SELECT request_hash"):
            for digest, doc in self.bans.values():
                key = "cancel_request_id" if "cancel_request_id=%s" in sql else "client_order_id"
                if doc[key] == params[-1]:
                    self.row = (digest, doc)
        elif sql.startswith("INSERT INTO") and ".submission_bans" in sql:
            self.bans[params[2]] = (params[4], json.loads(params[5]))
        elif ".cancel_requests" not in sql:
            raise AssertionError(sql)
    def fetchone(self):
        return self.row

class SubmissionBanUnitTests(unittest.TestCase):
    def setUp(self):
        self.cursor = BanCursor()
        self.repo = PostgresRepository({}, "test-account")
        self.repo.repo_run = lambda callback, mutation=False: callback(self.cursor)
        self.repo.repo_load = lambda *args, **kwargs: None
        self.cancel = dict(account_id="test-account", client_order_id="before-submit", cancel_request_id="cancel-one")
    def test_receipt_replay_restart_conflicts_and_submit_ban(self):
        status, proof = self.repo.request_cancel(self.cancel)
        self.assertEqual(status, 200)
        self.assertTrue(proof["no_submission"])
        self.assertEqual(proof["execution_attempt_count"], 0)
        self.assertEqual(proof["source"], "BRIDGE_PERSISTED_SUBMISSION_BAN")
        self.assertNotIn("order_id", proof)
        self.assertEqual(self.repo.get_order("before-submit"), proof)
        self.assertEqual(self.repo.request_cancel(self.cancel), (200, proof))
        for changed in (dict(self.cancel, client_order_id="different"), dict(self.cancel, cancel_request_id="different")):
            with self.assertRaises(OrderError) as error:
                self.repo.request_cancel(changed)
            self.assertEqual(error.exception.code, "IDEMPOTENCY_CONFLICT")
        with self.assertRaises(OrderError) as error:
            self.repo.accept_order(dict(account_id="test-account", client_order_id="before-submit"))
        self.assertEqual(error.exception.code, "SUBMISSION_BANNED")
        restarted = PostgresRepository({}, "test-account")
        restarted.repo_run, restarted.repo_load = self.repo.repo_run, self.repo.repo_load
        self.assertEqual(restarted.get_order("before-submit"), proof)
    def test_missing_stream_never_creates_proof(self):
        execute = self.cursor.execute
        def without_stream(sql, params=None):
            execute(sql, params)
            if "SELECT to_jsonb" in sql:
                self.cursor.row = (None,)
        self.cursor.execute = without_stream
        with self.assertRaises(OrderError) as error:
            self.repo.request_cancel(self.cancel)
        self.assertEqual(error.exception.code, "EVIDENCE_SCHEMA_REQUIRED")
        self.assertEqual(self.cursor.bans, {})

    def test_commit_unknown_recovery(self):
        outer = self
        class Connection:
            def cursor(self): return outer.cursor
            def commit(self): raise RuntimeError("lost commit response")
            def rollback(self): pass
            def close(self): pass
        self.repo.repo_run = PostgresRepository.repo_run.__get__(self.repo)
        self.repo.repo_connect = lambda: Connection()
        with self.assertRaises(OrderError) as error:
            self.repo.request_cancel(self.cancel)
        self.assertEqual(error.exception.code, "PERSISTENCE_OUTCOME_UNKNOWN")
        self.repo.repo_run = lambda callback, mutation=False: callback(self.cursor)
        self.assertEqual(self.repo.request_cancel(self.cancel)[1], self.repo.get_order("before-submit"))

@unittest.skipUnless(os.environ.get("ORDER_TEST_PGHOST"), "ORDER_TEST_PGHOST unset: isolated PostgreSQL skipped")
class SubmissionBanPostgresTests(unittest.TestCase):
    setUp = fixtures.PostgresRepositoryTests.setUp
    tearDown = fixtures.PostgresRepositoryTests.tearDown
    other = fixtures.PostgresRepositoryTests.other
    request = fixtures.PostgresRepositoryTests.request
    def add_stream(self):
        self.repo.repo_run(lambda cur: cur.execute("ALTER TABLE " + self.repo.repo_s +
            ".account_runtime ADD COLUMN evidence_stream_id uuid NOT NULL DEFAULT md5(random()::text || clock_timestamp()::text)::uuid"))
    def test_cancel_first_restart_and_account_scope(self):
        self.add_stream()
        cancel = dict(account_id="test-account", client_order_id="one", cancel_request_id="cancel-one")
        proof = self.repo.request_cancel(cancel)[1]
        restarted = self.other()
        self.assertEqual(restarted.get_order("one"), proof)
        self.assertEqual(restarted.request_cancel(cancel)[1], proof)
        with self.assertRaises(OrderError) as error:
            restarted.accept_order(self.request("one"))
        self.assertEqual(error.exception.code, "SUBMISSION_BANNED")
        other = self.other("another-account")
        other.ensure_account_runtime()
        self.assertTrue(other.accept_order(dict(self.request("one"), account_id="another-account"))[0])
    def test_cancel_accept_concurrency(self):
        self.add_stream()
        for index in range(12):
            client = "race-" + str(index)
            barrier = threading.Barrier(2)
            other = self.other()
            def accept():
                barrier.wait()
                try: return other.accept_order(self.request(client))
                except OrderError as exc: return exc.code
            def cancel():
                barrier.wait()
                return self.repo.request_cancel(dict(account_id="test-account", client_order_id=client, cancel_request_id="cancel-"+client))
            with ThreadPoolExecutor(max_workers=2) as pool:
                a, b = pool.submit(accept), pool.submit(cancel)
                accepted, cancelled = a.result(), b.result()
            saved = self.repo.get_order(client)
            self.assertEqual(cancelled[0], 200)
            if saved.get("receipt_type") == "SUBMISSION_BAN":
                self.assertEqual(accepted, "SUBMISSION_BANNED")
            else:
                self.assertEqual(saved["submission_status"], "CANCELLED_LOCAL")
                self.assertEqual(saved["attempts"], [])
    def test_v4_migration_preserves_orders_and_is_idempotent(self):
        from tools import order_submission_ban_migration as migration
        before = self.repo.accept_order(self.request("migration-order"))[1]
        def downgrade(cur):
            cur.execute("DROP TABLE " + self.repo.repo_s + ".submission_bans")
            cur.execute("UPDATE " + self.repo.repo_s + ".schema_version SET version=4")
        self.repo.repo_run(downgrade)
        self.assertEqual(migration.run(self.config)["result"], "preflight_ok")
        # Only this disposable schema uses a nonempty fixture as the backup gate.
        self.assertEqual(migration.run(self.config, apply=True, backup=__file__)["result"], "migrated")
        self.assertEqual(self.repo.get_order("migration-order"), before)
        self.assertEqual(migration.run(self.config, apply=True, backup=__file__)["result"], "already_v5")

    def test_claim_before_cancel_does_not_return_ban(self):
        self.add_stream()
        self.repo.acquire_executor("instance-one", "host-one")
        order = self.repo.accept_order(self.request("claimed"))[1]
        self.assertTrue(self.repo.claim_submission(order["order_id"])[0])
        unused, receipt = self.repo.request_cancel(dict(account_id="test-account", client_order_id="claimed", cancel_request_id="cancel-claimed"))
        self.assertNotEqual(receipt.get("receipt_type"), "SUBMISSION_BAN")
        self.assertNotEqual(self.repo.get_order("claimed")["submission_status"], "CANCELLED_LOCAL")


class SubmissionBanMigrationTests(unittest.TestCase):
    def test_idempotent_v5_and_rollback_on_ddl_failure(self):
        from unittest.mock import Mock, patch
        from tools import order_submission_ban_migration as migration
        conn, cur = Mock(), Mock()
        conn.cursor.return_value.__enter__ = Mock(return_value=cur)
        conn.cursor.return_value.__exit__ = Mock(return_value=False)
        cur.fetchall.return_value = [(5,)]
        with patch.object(migration, "connect", return_value=conn), patch.object(migration, "repo_check_storage_schema"):
            result = migration.run({}, apply=False)
        self.assertEqual(result["result"], "already_v5")
        self.assertFalse(result["committed"])
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()
        cur.fetchall.return_value = [(4,)]
        def fail_ddl(sql, *args):
            if sql.startswith("CREATE TABLE"):
                raise RuntimeError("DDL failed")
        cur.execute.side_effect = fail_ddl
        with patch.object(migration, "connect", return_value=conn), patch.object(migration, "repo_check_storage_schema"), \
             patch.object(migration.Path, "is_file", return_value=True), patch.object(migration.Path, "stat", return_value=Mock(st_size=1)):
            with self.assertRaises(RuntimeError):
                migration.run({}, apply=True, backup="explicit-test-backup")
        conn.commit.assert_not_called()
        self.assertEqual(conn.rollback.call_count, 2)
