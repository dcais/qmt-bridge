"""Native history is unsupported; local retained records are a distinct source."""
import threading
import time
import unittest
from unittest.mock import Mock, patch

from test_http_order import load_strategy
from service.ipc_bridge.qmt_runtime.order.adapter import QmtAdapter, qmt_history_capabilities


class HistoryCapabilityTests(unittest.TestCase):
    def setUp(self):
        self.module = load_strategy()
        self.module.log_message = Mock()
        self.params = {"accountId": "synthetic-account", "accountType": "stock",
                       "dataType": "deal", "startDate": "20260928", "endDate": "20260929"}

    def test_explicit_unsupported_even_if_a_function_or_builtin_is_injected(self):
        native = Mock(return_value=[])
        for apis in ({}, {"get_history_trade_detail_data": native},
                     {"__builtins__": {"get_history_trade_detail_data": native}}):
            capabilities = qmt_history_capabilities(apis)
            self.assertEqual(capabilities["availability"], "UNSUPPORTED")
            self.assertFalse(capabilities["implemented"])
            self.assertFalse(capabilities["function_available"])
            adapter = QmtAdapter(apis, None, "synthetic-account")
            self.assertFalse(adapter.available("get_history_trade_detail_data"))
            with self.assertRaises(Exception) as caught:
                adapter.history_query(self.params)
            self.assertEqual(caught.exception.code, "NATIVE_HISTORY_UNSUPPORTED")
            self.assertEqual(caught.exception.status, 501)
        native.assert_not_called()

    def test_capability_without_database_does_not_report_retention_or_final_fees(self):
        result = self.module.dispatch_request(None, {"method": "history_capabilities", "params": {}})
        self.assertEqual(result["source"], "QMT_NATIVE_HISTORY")
        self.assertEqual(result["local_history"]["source"], "BRIDGE_PERSISTED_RECORDS")
        self.assertEqual(result["local_history"]["coverage"], "UNVERIFIED")
        self.assertEqual(result["local_history"]["retention_start"], "UNVERIFIED")
        self.assertEqual(result["local_history"]["fee_finality"], "UNVERIFIED")
        self.assertIn("/orders", result["local_history"]["routes"])

    def test_history_request_is_501_not_successful_empty_data(self):
        self.module.get_history_trade_detail_data = Mock(return_value=[])
        with self.assertRaises(self.module.OrderError) as caught:
            self.module.dispatch_request(None, {"method": "get_history_trade_detail_data", "params": self.params})
        self.assertEqual(caught.exception.status, 501)
        self.assertEqual(caught.exception.code, "NATIVE_HISTORY_UNSUPPORTED")
        self.module.get_history_trade_detail_data.assert_not_called()

    def test_parameter_validation_before_any_native_work(self):
        invalid = ({"startDate": 20260928}, {"startDate": "20260230"},
                   {"startDate": "2026-09-28"}, {"startDate": "２０２６０９２８"},
                   {"startDate": "20260930"}, {"startDate": "20260801"},
                   {"dataType": "TASK"}, {"dataType": False},
                   {"accountType": "invalid"}, {"accountId": 1},
                   {"cursor": "not-native-pagination"}, {"endDate": None})
        for updates in invalid:
            with self.subTest(updates=updates):
                params = dict(self.params, **updates)
                with self.assertRaises(self.module.OrderError) as caught:
                    self.module.normalize_request("get_history_trade_detail_data", params)
                self.assertEqual(caught.exception.status, 400)
        with self.assertRaises(self.module.OrderError):
            self.module.normalize_request("history_capabilities", {"extra": 1})

    def test_date_normalization_is_only_request_validation_not_native_support(self):
        result = self.module.normalize_request("get_history_trade_detail_data", self.params)
        self.assertEqual(result["params"]["accountType"], "STOCK")
        self.assertEqual(result["params"]["dataType"], "DEAL")
        self.assertEqual(result["params"]["startDate"], "20260928")

    def test_expired_read_and_shutdown_do_not_run_query(self):
        state = self.module._ORDER_STATE = self.module.OrderState()
        self.module.get_history_trade_detail_data = Mock()
        job = self.module.RequestJob({"method": "get_history_trade_detail_data", "params": self.params})
        job.deadline = time.monotonic() - 1
        state.request_queue.put(job)
        self.module.process_http_requests(None)
        self.assertEqual(job.error_status, 504)
        self.assertTrue(job.done.is_set())
        self.module.get_history_trade_detail_data.assert_not_called()
        self.assertEqual(state.request_queue.unfinished_tasks, 0)

    def test_callback_returns_unsupported_and_completes_queue(self):
        state = self.module._ORDER_STATE = self.module.OrderState()
        job = self.module.RequestJob({"method": "get_history_trade_detail_data", "params": self.params})
        state.request_queue.put(job)
        self.module.process_http_requests(None)
        self.assertEqual(job.error_status, 501)
        self.assertEqual(job.error["code"], "NATIVE_HISTORY_UNSUPPORTED")
        self.assertTrue(job.done.is_set())
        self.assertEqual(state.request_queue.unfinished_tasks, 0)

    def test_existing_queries_remain_unchanged(self):
        self.module.get_trade_detail_data = Mock(return_value=[])
        self.assertEqual(self.module.dispatch_request(None, {"method": "positions", "params": {}}), [])
        self.module.get_trade_detail_data.assert_called_once_with("66027616", "STOCK", "position")
        with self.assertRaises(self.module.OrderError) as caught:
            self.module.dispatch_request(None, {"method": "not-a-query", "params": {}})
        self.assertEqual(caught.exception.code, "METHOD_NOT_FOUND")
