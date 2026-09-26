# -*- coding: utf-8 -*-
"""ORDER HTTP 的 QMT 调用边界日志；所有 QMT 对象均为离线替身。Last modified: 2026-09-26。"""
import importlib.util
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


SOURCE = Path(__file__).resolve().parents[1] / "order_bridge" / "http.py"


def load_http():
    spec = importlib.util.spec_from_file_location("order_bridge.http_logging_test", SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class NativeRow:
    def __init__(self, value):
        self.m_value = value

    def __repr__(self):
        raise AssertionError("原生 QMT 对象不得被日志 repr")


def calls(records, method, message=None):
    return [item for item in records if item["fields"].get("qmt_method") == method
            and (message is None or item["message"] == message)]


class HttpQmtLoggingTests(unittest.TestCase):
    def setUp(self):
        self.module = load_http()
        self.records = []
        self.logger = self.module.AsyncOrderLogger("unused", sink=self.records.append)
        self.logger.start()
        self.addCleanup(self.close_logger)

    def close_logger(self):
        self.logger.request_stop()
        self.assertTrue(self.logger.join(3))

    def test_legacy_queries_keep_results_and_log_named_parameters(self):
        module = self.module
        query = module.get_trade_detail_data = Mock(return_value=[NativeRow(7)])
        algo = module.get_smart_algo_param = Mock(return_value={"VWAP": [1]})
        self.assertEqual(module.dispatch_request(None, {"method": "account", "params": {}},
                                                 logger=self.logger, correlation={"request_id": "req-a"}),
                         {"m_value": 7})
        self.assertEqual(module.dispatch_request(None, {"method": "positions",
                                                  "params": {"accountId": "other", "accountType": "CREDIT"}},
                                                 logger=self.logger, correlation={"request_id": "req-p"}),
                         [{"m_value": 7}])
        self.assertEqual(module.dispatch_request(None, {"method": "get_smart_algo_param",
                                                  "params": {"algoList": ["VWAP"]}},
                                                 logger=self.logger, correlation={"request_id": "req-s"}),
                         {"VWAP": [1]})
        self.assertEqual(query.call_args_list[0][0], ("66027616", "STOCK", "account"))
        self.assertEqual(query.call_args_list[1][0], ("other", "CREDIT", "position"))
        algo.assert_called_once_with(["VWAP"])
        self.close_logger()
        started = calls(self.records, "get_trade_detail_data", "QMT call started")
        returned = calls(self.records, "get_trade_detail_data", "QMT call returned")
        self.assertEqual(len(started), len(returned))
        self.assertEqual(len(started), 2)
        self.assertEqual([item["fields"]["request_id"] for item in started], ["req-a", "req-p"])
        self.assertEqual([item["fields"]["query_kind"] for item in started], ["account", "position"])
        self.assertEqual(started[1]["fields"]["qmt_parameters"],
                         {"account_id": "other", "account_type": "CREDIT", "query_kind": "position"})
        self.assertEqual([item["fields"]["return_count"] for item in returned], [1, 1])
        self.assertEqual({item["fields"]["qmt_call_id"] for item in started},
                         {item["fields"]["qmt_call_id"] for item in returned})
        smart = calls(self.records, "get_smart_algo_param")
        self.assertEqual(len(smart), 2)
        self.assertEqual(smart[0]["fields"]["qmt_parameters"], {"algo_list": ["VWAP"]})
        self.assertEqual(smart[1]["fields"]["return_count"], 1)

    def test_queued_query_passes_request_id_and_failure_unchanged(self):
        module = self.module
        state = module._ORDER_STATE = module.OrderState()
        state.logger = self.logger
        state.scheduler_turn = 1
        failure = RuntimeError("query failed")
        module.get_trade_detail_data = Mock(side_effect=failure)
        request = module.RequestJob({"method": "positions", "params": {}})
        request.request_id = "queued-42"
        state.request_queue.put(request)
        module.process_http_requests(None)
        self.assertTrue(request.done.is_set())
        self.assertEqual(request.error["code"], "QMT_ERROR")
        self.close_logger()
        failed = calls(self.records, "get_trade_detail_data", "QMT call failed")
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["fields"]["request_id"], "queued-42")
        self.assertEqual(failed[0]["fields"]["account_id"], "66027616")
        self.assertEqual(failed[0]["fields"]["query_kind"], "position")
        self.assertGreaterEqual(failed[0]["fields"]["elapsed_ms"], 0)


class HttpLifecycleLoggingTests(unittest.TestCase):
    def setUp(self):
        self.module = load_http()
        self.records = []
        self.loggers = []
        logger_class = self.module.AsyncOrderLogger

        def make_logger(directory):
            logger = logger_class(directory, sink=self.records.append)
            self.loggers.append(logger)
            return logger

        self.logger_patch = patch.object(self.module, "AsyncOrderLogger", side_effect=make_logger)
        self.logger_patch.start()
        self.addCleanup(self.logger_patch.stop)

    def ephemeral_server(self):
        server_class = self.module.ThreadingHTTPServer
        return patch.object(self.module, "ThreadingHTTPServer",
                            side_effect=lambda address, handler: server_class((address[0], 0), handler))

    def test_start_and_stop_trace_context_calls_without_callback_repr(self):
        module = self.module
        module.SCHEDULE_INTERVAL = module.dt.timedelta(milliseconds=25)
        context = Mock()
        context.schedule_run.return_value = "timer-7"
        with self.ephemeral_server():
            module.init(context)
        state = module._ORDER_STATE
        module.stop(context)
        self.assertTrue(state.cleanup_done.wait(3))
        self.assertIsNone(module._ORDER_STATE)
        self.assertEqual(context.set_account.call_args[0], ("66027616",))
        self.assertIs(context.schedule_run.call_args[0][0], module.process_http_requests)
        self.assertEqual(context.schedule_run.call_args[0][3], module.SCHEDULE_INTERVAL)
        context.cancel_schedule_run.assert_called_once_with("timer-7")
        self.assertEqual(calls(self.records, "ContextInfo.set_account", "QMT call started")[0]
                         ["fields"]["qmt_parameters"], {"account_id": "66027616"})
        self.assertEqual(calls(self.records, "ContextInfo.schedule_run", "QMT call started")[0]
                         ["fields"]["qmt_parameters"],
                         {"callback": "process_http_requests", "start_time": "20200101000000",
                          "repeat_times": -1, "interval_ms": 25, "timer_name": "http_order_timer"})
        returned = calls(self.records, "ContextInfo.schedule_run", "QMT call returned")
        self.assertEqual(returned[0]["fields"]["return_value"], "timer-7")
        self.assertEqual(calls(self.records, "ContextInfo.cancel_schedule_run", "QMT call started")[0]
                         ["fields"]["qmt_parameters"], {"timer_id": "timer-7"})
        self.assertTrue(all(logger.health()["stopped"] for logger in self.loggers))

    def test_account_binding_failure_logs_and_releases_logger_without_server(self):
        module = self.module
        context = Mock()
        failure = RuntimeError("binding failed")
        context.set_account.side_effect = failure
        with patch.object(module, "ThreadingHTTPServer") as server:
            with self.assertRaises(RuntimeError) as caught:
                module.init(context)
        self.assertIs(caught.exception, failure)
        server.assert_not_called()
        context.schedule_run.assert_not_called()
        self.assertIsNone(module._ORDER_STATE)
        self.assertTrue(self.loggers[0].health()["stopped"])
        self.assertEqual(len(calls(self.records, "ContextInfo.set_account", "QMT call failed")), 1)

    def test_server_failure_keeps_original_error_and_releases_logger(self):
        module = self.module
        context = Mock()
        failure = OSError("port busy")
        with patch.object(module, "ThreadingHTTPServer", side_effect=failure):
            with self.assertRaises(OSError) as caught:
                module.init(context)
        self.assertIs(caught.exception, failure)
        context.schedule_run.assert_not_called()
        self.assertIsNone(module._ORDER_STATE)
        self.assertTrue(self.loggers[0].health()["stopped"])

    def test_failure_after_registration_traces_cancel_and_cleans_up(self):
        module = self.module
        context = Mock()
        context.schedule_run.return_value = 0
        failure = RuntimeError("executor init failed")
        with patch.object(module.OrderRuntime, "initialize", side_effect=failure):
            with self.ephemeral_server():
                with self.assertRaises(RuntimeError) as caught:
                    module.init(context)
        self.assertIs(caught.exception, failure)
        state = module._ORDER_STATE
        if state is not None:
            self.assertTrue(state.cleanup_done.wait(3))
        context.cancel_schedule_run.assert_called_once_with(0)
        self.assertEqual(calls(self.records, "ContextInfo.schedule_run", "QMT call returned")[0]
                         ["fields"]["return_value"], 0)
        self.assertEqual(calls(self.records, "ContextInfo.cancel_schedule_run", "QMT call started")[0]
                         ["fields"]["qmt_parameters"], {"timer_id": 0})
        self.assertIsNone(module._ORDER_STATE)


if __name__ == "__main__":
    unittest.main()
