import ast
import importlib.util
import json
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from service.ipc_bridge.feed import FEED_METHODS, FeedService, parse_query_params
from service.ipc_bridge.feed import qmt_api, tasks


# Reuse the existing QMT-shaped fixtures without starting the legacy HTTP server.
_spec = importlib.util.spec_from_file_location(
    "legacy_feed_fixtures", Path(__file__).with_name("test_http_feed.py")
)
fixtures = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fixtures)


class FeedExtractionContractTest(unittest.TestCase):
    def test_existing_query_results_and_errors_are_preserved(self):
        legacy = fixtures.load_strategy()
        cases = {
            "get_stock_list_in_sector": {"sectorname": "沪深A股", "realtime": "1782871200000"},
            "get_trading_dates": {"stockcode": "600000.SH", "count": "3"},
            "get_instrument_detail": {"stockcode": "600000.SH", "iscomplete": "false"},
            "get_divid_factors": {"stockcode": "600000.SH"},
            "get_weight_in_index": {"indexcode": "000300.SH", "stockcode": "600000.SH"},
            "get_full_tick": {"stock_code": ["600000.SH"]},
            "get_his_index_data": {"stockcode": "000300.SH"},
            "get_his_contract_list": {"market": "SHO"},
            "get_longhubang": {"stock_list": ["000002.SZ"], "startTime": "20260701", "endTime": "20260701"},
            "get_market_data_ex": {"fields": ["close"], "stock_code": ["600000.SH"], "period": "1d", "count": "2"},
            "get_financial_data": {"mode": "range", "fieldList": ["ASHAREBALANCESHEET.fix_assets"], "stockList": ["600000.SH"], "startDate": "20260701", "endDate": "20260702"},
        }
        for method, params in cases.items():
            with self.subTest(method=method):
                request = {"method": method, "params": params}
                actual = qmt_api.dispatch_request(fixtures.FakeContext(), request)
                expected = legacy.dispatch_request(fixtures.FakeContext(), request)
                self.assertEqual(expected, actual)
                json.dumps(actual, allow_nan=False)
                bad = {"method": method, "params": dict(params, unknown="value")}
                with self.assertRaises(legacy.FeedError) as before:
                    legacy.dispatch_request(fixtures.FakeContext(), bad)
                with self.assertRaises(qmt_api.FeedError) as after:
                    qmt_api.dispatch_request(fixtures.FakeContext(), bad)
                self.assertEqual(
                    (before.exception.status, before.exception.code, before.exception.message),
                    (after.exception.status, after.exception.code, after.exception.message),
                )

    def test_sector_global_is_resolved_at_call_time(self):
        with patch.object(qmt_api, "get_sector_list", return_value=[["行业"], ["node"]], create=True) as native:
            self.assertEqual([["行业"], ["node"]], qmt_api.dispatch_request(None, {"method": "get_sector_list", "params": {}}))
            native.assert_called_once_with("")

    def test_batch_state_advances_one_item_and_preserves_partial_errors(self):
        class PartialContext(fixtures.FakeContext):
            def get_instrument_detail(self, stockcode, iscomplete):
                if stockcode == "000001.SZ":
                    raise RuntimeError("fixture unavailable")
                return super().get_instrument_detail(stockcode, iscomplete)

        request = {"method": "get_instrument_details", "params": {"stock_code": ["600000.SH", "000001.SZ"]}}
        old = fixtures.load_strategy()
        old_job = old.RequestJob(request)
        new_job = qmt_api.BatchJob(request)
        old_context, new_context = PartialContext(), PartialContext()
        self.assertFalse(old.process_batch_request(old_context, old_job))
        self.assertFalse(qmt_api.process_batch_request(new_context, new_job))
        self.assertEqual(1, len(new_context.instrument_detail_calls))
        self.assertTrue(old.process_batch_request(old_context, old_job))
        self.assertTrue(qmt_api.process_batch_request(new_context, new_job))
        self.assertEqual(old_job.result, new_job.result)
        self.assertEqual("QMT_ERROR", new_job.result["errors"]["000001.SZ"]["code"])
        self.assertEqual(new_job.result, qmt_api.dispatch_request(PartialContext(), request))

    def test_each_batch_method_matches_existing_result(self):
        legacy = fixtures.load_strategy()
        for method in qmt_api.BATCH_METHODS:
            params = {"stock_code": ["600000.SH", "000001.SZ"]}
            if method == "get_weights_in_index":
                params["indexcode"] = "000300.SH"
            request = {"method": method, "params": params}
            job = legacy.RequestJob(request)
            context = fixtures.FakeContext()
            while not legacy.process_batch_request(context, job):
                pass
            self.assertEqual(job.result, qmt_api.dispatch_request(fixtures.FakeContext(), request))

    def test_single_argument_instrument_compatibility(self):
        result = qmt_api.dispatch_request(fixtures.SingleArgumentInstrumentContext(), {
            "method": "get_instrument_detail", "params": {"stockcode": "600000.SH"},
        })
        self.assertEqual("legacy instrument", result["InstrumentName"])

    def test_result_shape_limit_checked_before_materializing_qmt_values(self):
        context = fixtures.FakeContext()
        context.get_market_data_ex = lambda *args, **kwargs: {
            "600000.SH": fixtures.OversizedTable(1001)
        }
        with self.assertRaises(qmt_api.FeedError) as raised:
            qmt_api.dispatch_request(context, {"method": "get_market_data_ex", "params": {
                "stock_code": ["600000.SH"], "fields": ["close"], "count": 2,
            }})
        self.assertEqual("INVALID_QMT_RESULT", raised.exception.code)

    def test_native_download_preserves_omitted_and_explicit_null_incrementally(self):
        params = {"stockcode": "600000.sh", "period": "1d"}
        with patch.object(qmt_api, "download_history_data", create=True) as native:
            qmt_api.dispatch_request(None, {"method": "download_history_data", "params": params})
            native.assert_called_once_with("600000.sh", "1d", "", "")
            native.reset_mock()
            qmt_api.dispatch_request(None, {"method": "download_history_data", "params": dict(params, incrementally=None)})
            native.assert_called_once_with("600000.sh", "1d", "", "", incrementally=None)

    def test_qmt_source_is_python36_and_only_imports_safe_standard_library(self):
        source = Path(qmt_api.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source, feature_version=(3, 6))
        imports = [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names]
        self.assertEqual(["datetime", "math"], imports)
        self.assertFalse(any(isinstance(node, ast.ImportFrom) for node in ast.walk(tree)))
        forbidden = {"open", "eval", "exec", "compile", "__import__"}
        self.assertFalse(any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in forbidden for node in ast.walk(tree)))


class FeedServiceContractTest(unittest.TestCase):
    def make_service(self, call, **kwargs):
        service = FeedService(call, **kwargs)
        def cleanup():
            service.close()
            service._worker.join(2)
        self.addCleanup(cleanup)
        return service

    def wait_status(self, service, task_id, expected):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            status, task = service.handle("get_download_status", {"task_id": task_id})
            if task.get("status") == expected:
                self.assertEqual(200, status)
                return task
            threading.Event().wait(0.005)
        self.fail("task did not reach " + expected)

    def test_query_parser_and_whitelist_prevent_arbitrary_native_calls(self):
        calls = []
        service = self.make_service(lambda method, params, **kw: calls.append((method, params, kw)) or [])
        params = parse_query_params("stock_code=600000.SH&stock_code=000001.SZ&count=2&fill_data=false&start_time=")
        self.assertEqual(["600000.SH", "000001.SZ"], params["stock_code"])
        self.assertEqual("false", params["fill_data"])
        self.assertEqual((200, []), service.handle("get_market_data_ex", params))
        self.assertEqual({"timeout": 10}, calls[0][2])
        for method in ("eval", "passorder", "account", "getattr", "unknown"):
            self.assertEqual(404, service.handle(method, {})[0])
        self.assertEqual(1, len(calls))
        self.assertEqual(17, len(FEED_METHODS))

    def test_native_error_status_code_message_pass_through(self):
        def native(*args, **kwargs):
            raise qmt_api.FeedError(400, "INVALID_PARAMS", "bad count")
        service = self.make_service(native)
        self.assertEqual((400, {"error": {"code": "INVALID_PARAMS", "message": "bad count"}}), service.handle("get_trading_dates", {}))

    def test_download_returns_202_and_status_stays_available_while_native_busy(self):
        started, release = threading.Event(), threading.Event()
        calls = []
        def native(method, params, **kwargs):
            calls.append((method, params, kwargs))
            started.set()
            release.wait(2)
        service = self.make_service(native)
        self.addCleanup(release.set)
        status, queued = service.handle("download_history_data", {"stockcode": "600000.sh", "period": "1d"}, "POST")
        self.assertEqual(202, status)
        self.assertEqual("queued", queued["status"])
        self.assertTrue(started.wait(1))
        self.wait_status(service, queued["task_id"], "running")
        release.set()
        task = self.wait_status(service, queued["task_id"], "completed")
        self.assertEqual("download_history_data returned; data coverage was not verified", task["message"])
        self.assertEqual({"timeout": 120}, calls[0][2])
        self.assertEqual("600000.sh", calls[0][1]["stockcode"])
        self.assertEqual("DOWNLOAD_TASK_NOT_FOUND", service.handle("get_download_status", {"task_id": "old-process"})[1]["error"]["code"])

    def test_download_failure_and_timeout_never_retry(self):
        for exc, code in ((RuntimeError("native failed"), "QMT_ERROR"), (TimeoutError("still running remotely"), "QMT_TIMEOUT")):
            calls = []
            def native(*args, **kwargs):
                calls.append(args)
                raise exc
            service = self.make_service(native)
            _, task = service.handle("download_history_data", {"stockcode": "600000.SH", "period": "1d"}, "POST")
            task = self.wait_status(service, task["task_id"], "failed")
            self.assertEqual(code, task["error"]["code"])
            self.assertEqual(1, len(calls))

    def test_queue_full_rolls_back_task_and_stop_cannot_be_overwritten_by_completion(self):
        started, release = threading.Event(), threading.Event()
        def native(*args, **kwargs):
            started.set()
            release.wait(2)
        service = self.make_service(native, queue_size=1)
        self.addCleanup(release.set)
        params = {"stockcode": "600000.SH", "period": "1d"}
        _, first = service.handle("download_history_data", params, "POST")
        self.assertTrue(started.wait(1))
        _, second = service.handle("download_history_data", params, "POST")
        self.assertEqual(429, service.handle("download_history_data", params, "POST")[0])
        self.assertEqual(2, len(service.download_tasks))
        service.close()
        self.assertEqual(503, service.handle("get_full_tick", {})[0])
        release.set()
        service._worker.join(2)
        for task in (first, second):
            failed = self.wait_status(service, task["task_id"], "failed")
            self.assertEqual("FEED_STOPPING", failed["error"]["code"])

    def test_task_cache_only_evicts_terminal_tasks(self):
        service = self.make_service(lambda *args, **kw: None)
        params = qmt_api.normalize_download_history_params({"stockcode": "600000.SH", "period": "1d"})
        with patch.object(tasks, "MAX_DOWNLOAD_TASKS", 1):
            first = tasks.create_download_task(service, params)
            with self.assertRaises(qmt_api.FeedError) as full:
                tasks.create_download_task(service, params)
            self.assertEqual("DOWNLOAD_TASKS_FULL", full.exception.code)
            tasks.update_download_task(service, first["task_id"], "completed", "returned")
            tasks.create_download_task(service, params)
            self.assertNotIn(first["task_id"], service.download_tasks)

    def test_invalid_download_rejected_before_any_task_or_native_call(self):
        service = self.make_service(lambda *args, **kwargs: self.fail("native called"))
        self.assertEqual(405, service.handle("download_history_data", {}, "GET")[0])
        self.assertEqual(400, service.handle("download_history_data", {"stockcode": "600000.SH", "period": "1d", "startTime": "20260702", "endTime": "20260701"}, "POST")[0])
        self.assertEqual(0, len(service.download_tasks))


if __name__ == "__main__":
    unittest.main()
