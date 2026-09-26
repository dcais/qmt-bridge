# -*- coding: utf-8 -*-
"""调度尝试关联和异步 QMT 调用日志集成；Last modified: 2026-09-26。"""
import tempfile
import unittest
from unittest.mock import Mock

from order_bridge.async_log import AsyncOrderLogger
from order_bridge.runtime import OrderRuntime


class CallTraceIntegrationTests(unittest.TestCase):
    def test_cancel_trace_and_following_query_keep_separate_context(self):
        with tempfile.TemporaryDirectory() as directory:
            records = []
            logger = AsyncOrderLogger(directory, sink=records.append)
            cancel = Mock(return_value=True)
            runtime = OrderRuntime({"cancel": cancel, "get_trade_detail_data": lambda *args: []},
                                   object(), "account", repository=Mock(), logger=logger)
            bg = runtime.background
            bg.db_ready = True
            bg.grant = (runtime.instance_id, 7, runtime.clock() + 100)
            prior = {"request_id": "prior-context"}
            runtime.adapter.log_context = prior
            bg._enqueue("cancel", document={"order_id": "order-1", "client_order_id": "client-1"},
                        action={"kind": "CANCEL_ORDER", "target_id": "qmt-order-1",
                                "cancel_request_id": "cancel-1"})
            self.assertEqual(runtime.tick(max_actions=1), 1)
            result = bg.results.get_nowait()
            bg.capacity.release()
            self.assertTrue(result["value"])
            self.assertIs(runtime.adapter.log_context, prior)
            attempt_id = result["token"]
            bg._enqueue("query", query_kind="order", dates=(), round_id="round-1")
            self.assertEqual(runtime.tick(max_actions=1), 1)
            bg.results.get_nowait()
            bg.capacity.release()
            logger.start()
            logger.request_stop()
            self.assertTrue(logger.join(2))
            self.assertEqual(len(records), 4)
            before, after, query_before, query_after = [row["fields"] for row in records]
            self.assertEqual(before["qmt_method"], "cancel")
            self.assertEqual(before["attempt_id"], attempt_id)
            self.assertEqual(before["client_order_id"], "client-1")
            self.assertEqual(before["order_id"], "order-1")
            self.assertEqual(before["cancel_request_id"], "cancel-1")
            self.assertEqual(before["qmt_call_id"], after["qmt_call_id"])
            self.assertTrue(after["return_value"])
            self.assertIsInstance(before["qmt_parameters"], dict)
            self.assertEqual(query_before["round_id"], "round-1")
            self.assertNotIn("order_id", query_before)
            self.assertNotIn("cancel_request_id", query_before)
            self.assertEqual(query_after["return_count"], 0)
            cancel.assert_called_once()

    def test_abort_before_call_does_not_log_native_start(self):
        logs = Mock()
        native = Mock(return_value=True)
        runtime = OrderRuntime({"cancel": native}, object(), "account", repository=Mock(), logger=logs)
        bg = runtime.background
        bg.db_ready = True
        bg.grant = (runtime.instance_id, 7, runtime.clock() - 1)
        bg._enqueue("cancel", document={}, action={"kind": "CANCEL_ORDER", "target_id": "1"})
        self.assertEqual(runtime.tick(max_actions=1), 1)
        self.assertEqual(bg.results.get_nowait()["status"], "ABORTED_NO_CALL")
        bg.capacity.release()
        native.assert_not_called()
        logs.assert_not_called()

    def test_failed_native_call_restores_context_without_duplicate_call(self):
        error = RuntimeError("native cancel failed")
        native = Mock(side_effect=error)
        runtime = OrderRuntime({"cancel": native}, object(), "account", repository=Mock(), logger=Mock())
        bg = runtime.background
        bg.db_ready = True
        bg.grant = (runtime.instance_id, 7, runtime.clock() + 100)
        prior = {"request_id": "prior"}
        runtime.adapter.log_context = prior
        bg._enqueue("cancel", document={}, action={"kind": "CANCEL_ORDER", "target_id": "1"})
        self.assertEqual(runtime.tick(max_actions=1), 1)
        self.assertEqual(bg.results.get_nowait()["status"], "UNKNOWN")
        bg.capacity.release()
        self.assertIs(runtime.adapter.log_context, prior)
        native.assert_called_once()


if __name__ == "__main__":
    unittest.main()
