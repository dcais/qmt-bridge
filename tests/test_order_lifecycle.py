# -*- coding: utf-8 -*-
"""本机执行锁、配置冻结及停止失败的资源释放。"""
import os
import tempfile
import unittest
from unittest.mock import Mock

from order_bridge.common import OrderError, public_order
from order_bridge.runtime import LocalExecutorLock, OrderRuntime, read_pg_config


class LifecycleTests(unittest.TestCase):
    def test_startup_checks_schema_then_registers_account_before_execution(self):
        store, lock = Mock(), Mock()
        store.check_schema.return_value = {"schema_version": 1, "ready": True}
        runtime = OrderRuntime({}, object(), "configured-account", repository=store, local_lock=lock)
        runtime.initialize()
        self.assertTrue(runtime.initialized)
        self.assertFalse(runtime.recovery_complete)
        self.assertEqual([call[0] for call in store.method_calls],
                         ["check_schema", "ensure_account_runtime", "acquire_executor", "recover"])
        store.acquire_executor.assert_called_once_with(runtime.instance_id, runtime.host_id)

    def test_failed_schema_check_or_account_insert_blocks_execution_and_recovery(self):
        for failed_method in ("check_schema", "ensure_account_runtime"):
            store, lock = Mock(), Mock()
            store.check_schema.return_value = {"schema_version": 1, "ready": True}
            getattr(store, failed_method).side_effect = OrderError(503, "TEST_STORE_FAILURE", "test failure")
            runtime = OrderRuntime({}, object(), "configured-account", repository=store, local_lock=lock)
            runtime.initialize()
            self.assertFalse(runtime.initialized)
            self.assertFalse(runtime.recovery_complete)
            self.assertEqual(runtime.last_error, "TEST_STORE_FAILURE")
            store.acquire_executor.assert_not_called()
            store.recover.assert_not_called()
            if failed_method == "check_schema":
                store.ensure_account_runtime.assert_not_called()

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
        class Store:
            def release_executor(self):
                pass
            def close(self):
                raise RuntimeError("closed connection")
        class Lock:
            released = False
            def release(self):
                self.released = True
        lock = Lock()
        runtime = OrderRuntime({}, object(), "account", repository=Store(), local_lock=lock)
        runtime.initialized = runtime.recovery_complete = True
        with self.assertRaises(RuntimeError):
            runtime.stop()
        self.assertTrue(lock.released)
        self.assertFalse(runtime.initialized)
        self.assertTrue(runtime.stop_event.is_set())

    def test_health_does_not_treat_nonempty_database_status_as_ready(self):
        class Store:
            def health(self):
                return {"ready": False, "executor": False, "unknown_order_count": 2}
        runtime = OrderRuntime({}, object(), "account", repository=Store())
        runtime.initialized = runtime.recovery_complete = True
        runtime.last_tick = runtime.clock()
        result = runtime.health()
        self.assertFalse(result["database_available"])
        self.assertFalse(result["accepting_orders"])
        self.assertEqual(result["unknown_order_count"], 2)

    def test_public_payload_does_not_disclose_operator_evidence(self):
        result = public_order({"order_id": "o", "manual_resolutions": [{"evidence": "private"}],
                               "request_hash": "hash", "remark": "tag"})
        self.assertEqual(result, {"order_id": "o"})


if __name__ == "__main__":
    unittest.main()
