# -*- coding: utf-8 -*-
"""本机执行锁、配置冻结及停止失败的资源释放。"""
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock

from order_bridge.common import OrderError, public_order
from order_bridge.runtime import LocalExecutorLock, OrderRuntime, read_pg_config


class LifecycleTests(unittest.TestCase):
    qmt_queries = {"get_trade_detail_data": lambda *args: []}

    def _store(self):
        store = Mock()
        store.check_schema.return_value = {"schema_version": 3, "ready": True}
        store.ensure_account_runtime.return_value = True
        store.acquire_executor.return_value = {"epoch": 1, "instance_id": "test"}
        store.check_executor.return_value = True
        store.recover.return_value = {"recovered": 0, "next_cursor": None, "has_more": False}
        store.mark_reconcile_gap.return_value = {"marked": 0, "next_cursor": None, "has_more": False}
        store.reconcile_history_start.return_value = None
        store.begin_reconcile_batch.return_value = {"orders": [], "next_cursor": None, "has_more": False}
        store.reconcile_round_batch.return_value = {"orders": [], "next_cursor": None, "has_more": False}
        store.queued_orders.return_value = []
        store.cancellation_orders.return_value = []
        store.health.return_value = {"ready": True, "executor": True, "unknown_order_count": 0}
        return store

    def _until(self, predicate, runtime, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            runtime.tick()
            if predicate():
                return
            time.sleep(0.005)
        self.fail("runtime did not converge: {!r}".format(runtime.health()))

    def test_startup_checks_schema_then_registers_account_before_execution(self):
        store, lock = self._store(), Mock()
        runtime = OrderRuntime(self.qmt_queries, object(), "configured-account", repository=store, local_lock=lock)
        runtime.initialize()
        self._until(lambda: runtime.recovery_complete, runtime)
        self.assertTrue(runtime.initialized)
        names = [call[0] for call in store.method_calls]
        self.assertLess(names.index("check_schema"), names.index("ensure_account_runtime"))
        self.assertLess(names.index("ensure_account_runtime"), names.index("acquire_executor"))
        self.assertLess(names.index("acquire_executor"), names.index("recover"))
        store.acquire_executor.assert_called_once_with(runtime.instance_id, runtime.host_id)
        runtime.stop()
        self._until(lambda: runtime.stopped_event.is_set(), runtime)

    def test_failed_schema_check_or_account_insert_blocks_execution_and_recovery(self):
        for failed_method in ("check_schema", "ensure_account_runtime"):
            store, lock = self._store(), Mock()
            getattr(store, failed_method).side_effect = OrderError(503, "TEST_STORE_FAILURE", "test failure")
            runtime = OrderRuntime(self.qmt_queries, object(), "configured-account", repository=store, local_lock=lock)
            runtime.initialize()
            self._until(lambda: runtime.last_error == "TEST_STORE_FAILURE", runtime)
            self.assertFalse(runtime.initialized)
            self.assertFalse(runtime.recovery_complete)
            self.assertEqual(runtime.last_error, "TEST_STORE_FAILURE")
            store.acquire_executor.assert_not_called()
            store.recover.assert_not_called()
            if failed_method == "check_schema":
                store.ensure_account_runtime.assert_not_called()
            runtime.stop()
            self._until(lambda: runtime.stopped_event.is_set(), runtime)

    def test_initialize_returns_while_schema_check_is_blocked(self):
        store, lock = self._store(), Mock()
        entered, release = threading.Event(), threading.Event()

        def blocked_schema():
            entered.set()
            release.wait(2)
            return {"schema_version": 3, "ready": True}

        store.check_schema.side_effect = blocked_schema
        runtime = OrderRuntime(self.qmt_queries, object(), "account", repository=store, local_lock=lock)
        started = time.monotonic()
        runtime.initialize()
        self.assertLess(time.monotonic() - started, 0.2)
        try:
            self.assertTrue(entered.wait(1))
            self.assertFalse(runtime.recovery_complete)
            self.assertFalse(runtime.health()["accepting_orders"])
            store.ensure_account_runtime.assert_not_called()
            started = time.monotonic()
            runtime.stop()
            self.assertLess(time.monotonic() - started, 0.2)
        finally:
            release.set()
        self._until(lambda: runtime.stopped_event.is_set(), runtime)
        lock.release.assert_called_once_with()

    def test_query_only_and_lowercase_configuration(self):
        self.assertIsNone(read_pg_config({}))
        config = read_pg_config({"PG_DATABASE": "ignored", "pg_database": "orders",
                                 "pg_user": "runner", "pg_password": "private", "pg_port": 5432.0})
        self.assertEqual(config["pg_database"], "orders")
        self.assertEqual(config["pg_schema"], "qmt_order")
        self.assertEqual(config["pg_port"], 5432)
        with self.assertRaises(ValueError):
            read_pg_config({"pg_database": "orders"})

    def test_schema_is_fixed_and_databases_select_paper_or_live(self):
        base = {"pg_user": "runner", "pg_password": "private"}
        paper = read_pg_config(dict(base, pg_database="paper"))
        live = read_pg_config(dict(base, pg_database="live"))
        self.assertEqual(paper["pg_schema"], "qmt_order")
        self.assertEqual(live["pg_schema"], "qmt_order")
        self.assertNotEqual(paper["pg_database"], live["pg_database"])
        for key in ("pg_schema", "PG_SCHEMA"):
            with self.assertRaisesRegex(ValueError, "fixed to qmt_order"):
                read_pg_config(dict(base, pg_database="paper", **{key: "custom"}))

    def test_local_executor_excludes_second_instance_and_releases(self):
        config = {"pg_host": "local", "pg_port": 5432, "pg_database": "db", "pg_schema": "isolated"}
        with tempfile.TemporaryDirectory() as directory:
            first, second = LocalExecutorLock(config, "account"), LocalExecutorLock(config, "account")
            first.path = second.path = os.path.join(directory, "executor.lock")
            try:
                first.acquire()
                with self.assertRaises(OrderError):
                    second.acquire()
                first.release()
                second.acquire()
            finally:
                first.release()
                second.release()

    def test_stop_releases_local_ownership_even_if_database_close_fails(self):
        store, lock = self._store(), Mock()
        close_started, allow_close = threading.Event(), threading.Event()

        def slow_failed_close():
            close_started.set()
            allow_close.wait(2)
            raise RuntimeError("closed connection")

        store.close.side_effect = slow_failed_close
        runtime = OrderRuntime(self.qmt_queries, object(), "account", repository=store, local_lock=lock)
        runtime.initialize()
        self._until(lambda: runtime.recovery_complete, runtime)
        started = time.monotonic()
        runtime.stop()
        self.assertLess(time.monotonic() - started, 0.2)
        self.assertTrue(runtime.stop_event.is_set())
        try:
            self.assertTrue(close_started.wait(2))
            lock.release.assert_not_called()
        finally:
            allow_close.set()
        self._until(lambda: runtime.stopped_event.is_set(), runtime)
        lock.release.assert_called_once_with()
        self.assertFalse(runtime.initialized)

    def test_health_does_not_treat_nonempty_database_status_as_ready(self):
        store = self._store()
        runtime = OrderRuntime(self.qmt_queries, object(), "account", repository=store, local_lock=Mock())
        runtime.initialize()
        self._until(lambda: runtime.recovery_complete, runtime)
        store.health.return_value = {"ready": False, "executor": False, "unknown_order_count": 2}
        runtime.background.last_sample = 0
        self._until(lambda: runtime.health()["unknown_order_count"] == 2, runtime)
        result = runtime.health()
        self.assertFalse(result["database_available"])
        self.assertFalse(result["accepting_orders"])
        self.assertEqual(result["unknown_order_count"], 2)
        runtime.stop()
        self._until(lambda: runtime.stopped_event.is_set(), runtime)

    def test_public_payload_does_not_disclose_operator_evidence(self):
        result = public_order({"order_id": "o", "manual_resolutions": [{"evidence": "private"}],
                               "request_hash": "hash", "remark": "tag"})
        self.assertEqual(result, {"order_id": "o"})


if __name__ == "__main__":
    unittest.main()
