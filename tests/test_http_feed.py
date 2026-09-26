import copy
import datetime as dt
import importlib.util
import json
import io
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from contextlib import redirect_stdout
from unittest.mock import patch
from urllib.parse import urlencode


STRATEGY_PATH = Path(__file__).resolve().parents[1] / "strategies" / "http_feed.py"


def load_strategy():
    spec = importlib.util.spec_from_file_location("http_feed", STRATEGY_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeAccount:
    m_accountID = "66027616"
    m_available = 1234.5


class BrokenAccount:
    @property
    def m_broken(self):
        raise RuntimeError("broken field")


class FakeValues:
    def __init__(self, values):
        self.values = values

    def tolist(self):
        return self.values


class FakeArray:
    def __init__(self, values):
        self.values = values

    def tolist(self):
        return self.values


class ExplodingValues:
    def tolist(self):
        raise AssertionError("values must not be materialized")


class FakeMarketFrame:
    ndim = 2

    def __init__(self):
        self.index = ["20260701093000", "20260701093100"]
        self.columns = ["close", "askPrice"]
        self.values = FakeValues(
            [
                [10.1, FakeArray([10.2, 10.3])],
                [float("nan"), FakeArray([10.3, 10.4])],
            ]
        )


class FakeTable:
    ndim = 2

    def __init__(self, index, columns, data):
        self.index = index
        self.columns = columns
        self.values = FakeValues(data)


class OversizedTable:
    ndim = 2

    def __init__(self, row_count):
        self.index = list(range(row_count))
        self.columns = ["value"]
        self.values = ExplodingValues()


class FakeFinancialFrame:
    ndim = 2

    def __init__(self):
        self.index = ["20260701", "20260702"]
        self.columns = ["fix_assets"]
        self.values = FakeValues([[100.5], [float("nan")]])


class FakeFinancialSeries:
    ndim = 1

    def __init__(self):
        self.index = ["fix_assets", "total_assets"]
        self.values = FakeValues([100.5, float("inf")])


class FakeFinancialPanel:
    ndim = 3

    def __init__(self):
        self.items = ["600000.SH", "000001.SZ"]
        self.major_axis = ["20260701"]
        self.minor_axis = ["fix_assets"]
        self.values = FakeValues([[[100.5]], [[200.5]]])


class FakeContext:
    def __init__(self):
        self.account_id = None
        self.period = "1d"
        self.callback = None
        self.timer_id = "timer-1"
        self.cancelled_timer_id = None
        self.sector_calls = []
        self.trading_dates_calls = []
        self.instrument_detail_calls = []
        self.divid_factor_calls = []
        self.index_weight_calls = []
        self.financial_data_calls = []
        self.financial_data_keyword_calls = []
        self.market_data_ex_calls = []
        self.full_tick_calls = []
        self.his_index_data_calls = []
        self.his_contract_list_calls = []
        self.longhubang_calls = []

    def set_account(self, account_id):
        self.account_id = account_id

    def schedule_run(self, callback, time_point, repeat_times, interval, name):
        self.callback = callback
        self.schedule_args = (time_point, repeat_times, interval, name)
        return self.timer_id

    def cancel_schedule_run(self, timer_id):
        self.cancelled_timer_id = timer_id

    def get_stock_list_in_sector(self, *args):
        self.sector_calls.append((threading.get_ident(),) + args)
        return ["600000.SH", "000001.SZ"]

    def get_trading_dates(self, *args):
        self.trading_dates_calls.append((threading.get_ident(),) + args)
        return ["20260701", "20260702", "20260703"]

    def get_instrument_detail(self, *args):
        self.instrument_detail_calls.append((threading.get_ident(),) + args)
        stockcode = args[0]
        code, market = stockcode.split(".", 1)
        return {
            "ExchangeID": market,
            "InstrumentID": code,
            "InstrumentName": "浦发银行",
        }

    def get_financial_data(self, *args, **kwargs):
        self.financial_data_calls.append((threading.get_ident(),) + args)
        self.financial_data_keyword_calls.append(kwargs)
        if isinstance(args[0], list):
            return FakeFinancialFrame()
        return 42758000000.0

    def get_divid_factors(self, *args):
        self.divid_factor_calls.append((threading.get_ident(),) + args)
        return {
            1689868800000: [0.32, 0.0, 0.0, 0.0, 0.0, 0, 1.04507]
        }

    def get_weight_in_index(self, *args):
        self.index_weight_calls.append((threading.get_ident(),) + args)
        return 0.438

    def get_market_data_ex(self, *args, **kwargs):
        self.market_data_ex_calls.append(
            (threading.get_ident(), args, kwargs)
        )
        frame = FakeMarketFrame()
        if kwargs.get("count") == 1:
            frame.index = frame.index[:1]
            frame.values = FakeValues(frame.values.tolist()[:1])
        return {"600000.SH": frame}

    def get_full_tick(self, *args):
        self.full_tick_calls.append((threading.get_ident(),) + args)
        return {
            stockcode: {
                "time": 1782871200000,
                "lastPrice": 10.1,
                "lastClose": 10.0,
                "askPrice": FakeArray([10.2, 10.3]),
                "bidPrice": [10.0, 9.9],
                "volume": 1234,
                "amount": float("nan"),
            }
            for stockcode in args[0]
        }

    def get_his_index_data(self, *args):
        self.his_index_data_calls.append((threading.get_ident(),) + args)
        return {
            "20260701": {"600000.SH": 0.0438},
            "20260702": {"600000.SH": float("nan")},
        }

    def get_his_contract_list(self, *args):
        self.his_contract_list_calls.append(
            (threading.get_ident(),) + args
        )
        return ["10000001.SHO", "10000002.SHO"]

    def get_longhubang(self, *args):
        self.longhubang_calls.append((threading.get_ident(),) + args)
        booth_columns = ["traderName", "buyAmount", "sellAmount", "rank"]
        buy_booth = FakeTable(
            [0],
            booth_columns,
            [["example buy booth", 1000000.0, 0.0, 1]],
        )
        sell_booth = FakeTable(
            [0],
            booth_columns,
            [["example sell booth", 0.0, float("nan"), 1]],
        )
        return FakeTable(
            [0],
            [
                "stockCode",
                "date",
                "close",
                "buyTraderBooth",
                "sellTraderBooth",
            ],
            [
                [
                    "000002.SZ",
                    dt.datetime(2026, 7, 1),
                    12.5,
                    buy_booth,
                    sell_booth,
                ]
            ],
        )


class SingleArgumentInstrumentContext(FakeContext):
    def get_instrument_detail(self, stockcode):
        self.instrument_detail_calls.append(
            (threading.get_ident(), stockcode)
        )
        code, market = stockcode.split(".", 1)
        return {
            "ExchangeID": market,
            "InstrumentID": code,
            "InstrumentName": "legacy instrument",
            "OpenDate": "19911101",
            "InstrumentType": "stock",
        }


class BrokenInstrumentContext(FakeContext):
    def get_instrument_detail(self, stockcode, iscomplete):
        self.instrument_detail_calls.append(
            (threading.get_ident(), stockcode, iscomplete)
        )

        def decode_instrument_detail(value):
            return value

        return decode_instrument_detail(stockcode, iscomplete)


class FeedLoggingTest(unittest.TestCase):
    def setUp(self):
        self.strategy = load_strategy()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.strategy.LOG_DIRECTORY = self.temp.name

    def test_console_and_file_have_identical_timestamped_records(self):
        console = io.StringIO()
        with redirect_stdout(console):
            self.strategy.log_message("info", "Download started", stockcode="000001.SZ")
        files = list(Path(self.temp.name).glob("feed-*.log"))
        self.assertEqual(1, len(files))
        record = files[0].read_text(encoding="utf-8")
        self.assertEqual(console.getvalue(), record)
        self.assertRegex(record, r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \[INFO\] Download started')
        self.assertIn('"stockcode": "000001.SZ"', record)

    def test_debug_is_filtered_until_enabled(self):
        console = io.StringIO()
        with redirect_stdout(console):
            self.strategy.log_message("DEBUG", "hidden")
            self.assertEqual([], list(Path(self.temp.name).glob("*.log")))
            self.strategy.LOG_LEVEL = "DEBUG"
            self.strategy.log_message("DEBUG", "visible")
        self.assertNotIn("hidden", console.getvalue())
        self.assertIn("[DEBUG] visible", console.getvalue())

    def test_file_failure_is_reported_without_raising(self):
        console = io.StringIO()
        with patch.object(self.strategy.os, "makedirs", side_effect=PermissionError("denied")):
            with redirect_stdout(console):
                self.strategy.log_message("info", "test")
        self.assertIn("[INFO] test", console.getvalue())
        self.assertIn("[ERROR] log file write failed", console.getvalue())

    def test_console_failure_does_not_prevent_file_write(self):
        with patch("builtins.print", side_effect=OSError("console unavailable")):
            self.strategy.log_message("warning", "test")
        record = next(Path(self.temp.name).glob("*.log")).read_text(encoding="utf-8")
        self.assertIn("[WARNING] test", record)

    def test_download_success_and_failure_records_include_request(self):
        state = self.strategy.FeedState()
        params = {"stockcode": "000001.SZ", "period": "1m",
                  "startTime": "20260921000000", "endTime": "20260921235959"}
        for fail in (False, True):
            task = self.strategy.create_download_task(state, params)
            with patch.object(self.strategy, "download_history_data", create=True,
                              side_effect=RuntimeError("download error") if fail else None):
                with patch.object(self.strategy, "log_message") as log:
                    self.strategy.handle_download_history_data(
                        state, {"params": params, "task_id": task["task_id"]})
            self.assertEqual(2, log.call_count)
            first, last = log.call_args_list
            self.assertEqual(("INFO", "Download started"), first.args)
            self.assertEqual("ERROR" if fail else "INFO", last.args[0])
            self.assertIn("failed" if fail else "completed", last.args[1])
            for call in (first, last):
                for key, value in params.items():
                    self.assertEqual(value, call.kwargs[key])


class HttpFeedTest(unittest.TestCase):
    def setUp(self):
        self.strategy = load_strategy()
        log_temp = tempfile.TemporaryDirectory()
        self.addCleanup(log_temp.cleanup)
        self.strategy.LOG_DIRECTORY = log_temp.name
        self.strategy.HTTP_PORT = 0
        self.context = FakeContext()
        self.query_calls = []
        self.sector_list_calls = []

        def fake_get_trade_detail_data(account_id, account_type, detail_type):
            self.query_calls.append(
                (
                    threading.get_ident(),
                    account_id,
                    account_type,
                    detail_type,
                )
            )
            return [FakeAccount()]

        self.strategy.get_trade_detail_data = fake_get_trade_detail_data

        def fake_get_sector_list(node):
            self.sector_list_calls.append((threading.get_ident(), node))
            return [["沪深300", "上证50"], ["行业", "概念"]]

        self.strategy.get_sector_list = fake_get_sector_list
        self.download_history_calls = []

        def fake_download_history_data(*args, **kwargs):
            self.download_history_calls.append(
                (threading.get_ident(), args, kwargs)
            )

        self.strategy.download_history_data = fake_download_history_data
        self.strategy.init(self.context)
        self.state = self.strategy._FEED_STATE

    def tearDown(self):
        self.strategy.stop(self.context)
        self.state.server_thread.join(timeout=2)
        self.assertFalse(self.state.server_thread.is_alive())

    def base_url(self):
        port = self.state.server.server_address[1]
        return "http://127.0.0.1:{0}".format(port)

    def start_request(self, path, body=None):
        result = {}

        def send_request():
            data = None
            headers = {}
            if body is not None:
                data = json.dumps(body).encode("utf-8")
                headers["Content-Type"] = "application/json"
            request = urllib.request.Request(
                self.base_url() + path,
                data=data,
                headers=headers,
            )
            try:
                with urllib.request.urlopen(request, timeout=2) as response:
                    result["status"] = response.status
                    result["body"] = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                result["status"] = exc.code
                result["body"] = json.loads(exc.read().decode("utf-8"))

        thread = threading.Thread(target=send_request)
        thread.start()
        return thread, result

    def request_json(self, path, body=None, method=None):
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.base_url() + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=2) as response:
                return (
                    response.status,
                    json.loads(response.read().decode("utf-8")),
                )
        except urllib.error.HTTPError as exc:
            return (
                exc.code,
                json.loads(exc.read().decode("utf-8")),
            )

    def wait_for_queued_job(self):
        request_queue = self.state.request_queue
        deadline = time.time() + 2
        while request_queue.empty() and time.time() < deadline:
            time.sleep(0.01)
        self.assertFalse(request_queue.empty())
        with request_queue.mutex:
            return request_queue.queue[0]

    def test_account_request_is_normalized_then_dispatched_by_schedule(self):
        client_thread, response = self.start_request("/account")
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {"method": "account", "params": {}},
            queued_job.request,
        )
        schedule_thread_id = threading.get_ident()
        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual("66027616", response["body"]["m_accountID"])
        self.assertEqual(1234.5, response["body"]["m_available"])
        self.assertEqual(schedule_thread_id, self.query_calls[0][0])
        self.assertEqual(
            ("66027616", "STOCK", "account"),
            self.query_calls[0][1:],
        )

    def test_context_remains_deepcopyable_after_feed_init(self):
        copied_context = copy.deepcopy(self.context)

        self.assertEqual("66027616", copied_context.account_id)
        self.assertFalse(hasattr(copied_context, "http_feed_state"))
        self.assertFalse(hasattr(copied_context, "http_feed_timer_id"))

    def test_get_stock_list_in_sector_uses_current_schedule_context(self):
        query = urlencode({"sectorname": "沪深300"})
        client_thread, response = self.start_request(
            "/get_stock_list_in_sector?" + query
        )
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {
                "method": "get_stock_list_in_sector",
                "params": {"sectorname": "沪深300"},
            },
            queued_job.request,
        )
        reset_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual(["600000.SH", "000001.SZ"], response["body"])
        self.assertEqual(
            [(schedule_thread_id, "沪深300")],
            reset_context.sector_calls,
        )
        self.assertEqual([], self.context.sector_calls)

    def test_get_sector_list_defaults_to_top_level_and_runs_in_schedule(self):
        client_thread, response = self.start_request("/get_sector_list")
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {
                "method": "get_sector_list",
                "params": {},
            },
            queued_job.request,
        )
        schedule_thread_id = threading.get_ident()
        self.context.callback(FakeContext())
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual(
            [["沪深300", "上证50"], ["行业", "概念"]],
            response["body"],
        )
        self.assertEqual(
            [(schedule_thread_id, "")],
            self.sector_list_calls,
        )

    def test_get_sector_list_passes_requested_node(self):
        client_thread, response = self.start_request(
            "/get_sector_list?" + urlencode({"node": "我的"})
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            [(threading.get_ident(), "我的")],
            self.sector_list_calls,
        )

    def test_get_sector_list_rejects_non_string_node(self):
        client_thread, response = self.start_request(
            "/get_sector_list",
            {"node": 123},
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(400, response["status"])
        self.assertEqual("INVALID_PARAMS", response["body"]["error"]["code"])
        self.assertEqual([], self.sector_list_calls)

    def test_get_sector_list_rejects_invalid_qmt_result_shape(self):
        self.strategy.get_sector_list = lambda node: ["沪深300"]
        client_thread, response = self.start_request("/get_sector_list")
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(500, response["status"])
        self.assertEqual(
            "INVALID_QMT_RESULT",
            response["body"]["error"]["code"],
        )

    def test_get_trading_dates_uses_current_schedule_context(self):
        query = urlencode(
            {
                "stockcode": "600000.SH",
                "start_date": "20260701",
                "end_date": "20260731",
                "count": "10",
                "period": "1d",
            }
        )
        client_thread, response = self.start_request(
            "/get_trading_dates?" + query
        )
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {
                "method": "get_trading_dates",
                "params": {
                    "stockcode": "600000.SH",
                    "start_date": "20260701",
                    "end_date": "20260731",
                    "count": "10",
                    "period": "1d",
                },
            },
            queued_job.request,
        )
        reset_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual(
            ["20260701", "20260702", "20260703"],
            response["body"],
        )
        self.assertEqual(
            [
                (
                    schedule_thread_id,
                    "600000.SH",
                    "20260701",
                    "20260731",
                    10,
                    "1d",
                )
            ],
            reset_context.trading_dates_calls,
        )
        self.assertEqual([], self.context.trading_dates_calls)

    def test_get_trading_dates_applies_official_defaults(self):
        client_thread, response = self.start_request(
            "/get_trading_dates",
            {"count": 3},
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            [(threading.get_ident(), "", "", "", 3, "1d")],
            self.context.trading_dates_calls,
        )

    def test_get_trading_dates_requires_positive_integer_count(self):
        for count in (
            None,
            0,
            -1,
            10001,
            True,
            1.5,
            "1.5",
            "²",
            "9" * 5000,
        ):
            body = {} if count is None else {"count": count}
            client_thread, response = self.start_request(
                "/get_trading_dates",
                body,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.trading_dates_calls)

    def test_get_trading_dates_rejects_invalid_date_or_period(self):
        invalid_params = (
            {"count": 3, "start_date": "2026-07-01"},
            {"count": 3, "end_date": "202607"},
            {"count": 3, "period": "2m"},
        )
        for params in invalid_params:
            client_thread, response = self.start_request(
                "/get_trading_dates",
                params,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.trading_dates_calls)

    def test_get_trading_dates_rejects_invalid_qmt_result(self):
        self.context.get_trading_dates = lambda *args: [20260701]
        client_thread, response = self.start_request(
            "/get_trading_dates",
            {"count": 1},
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(500, response["status"])
        self.assertEqual(
            "INVALID_QMT_RESULT",
            response["body"]["error"]["code"],
        )

    def test_get_instrument_detail_uses_current_schedule_context(self):
        query = urlencode(
            {
                "stockcode": "600000.SH",
                "iscomplete": "true",
            }
        )
        client_thread, response = self.start_request(
            "/get_instrument_detail?" + query
        )
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {
                "method": "get_instrument_detail",
                "params": {
                    "stockcode": "600000.SH",
                    "iscomplete": "true",
                },
            },
            queued_job.request,
        )
        reset_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual("SH", response["body"]["ExchangeID"])
        self.assertEqual("600000", response["body"]["InstrumentID"])
        self.assertEqual("浦发银行", response["body"]["InstrumentName"])
        self.assertEqual(
            [(schedule_thread_id, "600000.SH", True)],
            reset_context.instrument_detail_calls,
        )
        self.assertEqual([], self.context.instrument_detail_calls)

    def test_get_instrument_detail_supports_single_argument_qmt_client(self):
        client_thread, response = self.start_request(
            "/get_instrument_detail",
            {"stockcode": "600000.SH", "iscomplete": True},
        )
        self.wait_for_queued_job()

        legacy_context = SingleArgumentInstrumentContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(legacy_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual("legacy instrument", response["body"]["InstrumentName"])
        self.assertEqual("19911101", response["body"]["OpenDate"])
        self.assertEqual("stock", response["body"]["InstrumentType"])
        self.assertEqual(
            [(schedule_thread_id, "600000.SH")],
            legacy_context.instrument_detail_calls,
        )

    def test_get_instrument_detail_does_not_mask_internal_type_error(self):
        client_thread, response = self.start_request(
            "/get_instrument_detail",
            {"stockcode": "600000.SH", "iscomplete": True},
        )
        self.wait_for_queued_job()

        broken_context = BrokenInstrumentContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(broken_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(500, response["status"])
        self.assertEqual("QMT_ERROR", response["body"]["error"]["code"])
        self.assertIn(
            "positional argument",
            response["body"]["error"]["message"],
        )
        self.assertEqual(
            [(schedule_thread_id, "600000.SH", True)],
            broken_context.instrument_detail_calls,
        )

    def test_get_instrument_details_processes_one_stock_per_tick(self):
        body = {
            "stock_code": ["600000.SH", "000001.SZ"],
            "iscomplete": True,
        }
        client_thread, response = self.start_request(
            "/get_instrument_details",
            body,
        )
        self.wait_for_queued_job()

        first_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(first_context)

        self.assertTrue(client_thread.is_alive())
        self.assertEqual(
            1,
            self.strategy._FEED_STATE.request_queue.unfinished_tasks,
        )
        self.assertEqual(
            [(schedule_thread_id, "600000.SH", True)],
            first_context.instrument_detail_calls,
        )

        second_context = FakeContext()
        self.context.callback(second_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual({}, response["body"]["errors"])
        self.assertEqual(
            {
                "600000.SH": {
                    "ExchangeID": "SH",
                    "InstrumentID": "600000",
                    "InstrumentName": "浦发银行",
                },
                "000001.SZ": {
                    "ExchangeID": "SZ",
                    "InstrumentID": "000001",
                    "InstrumentName": "浦发银行",
                },
            },
            response["body"]["results"],
        )
        self.assertEqual(
            [(schedule_thread_id, "000001.SZ", True)],
            second_context.instrument_detail_calls,
        )
        self.assertEqual(
            0,
            self.strategy._FEED_STATE.request_queue.unfinished_tasks,
        )
        self.assertEqual([], self.context.instrument_detail_calls)

    def test_get_instrument_details_supports_single_argument_qmt_client(self):
        client_thread, response = self.start_request(
            "/get_instrument_details",
            {
                "stock_code": ["600000.SH", "000001.SZ"],
                "iscomplete": True,
            },
        )
        self.wait_for_queued_job()

        first_context = SingleArgumentInstrumentContext()
        second_context = SingleArgumentInstrumentContext()
        self.context.callback(first_context)
        self.context.callback(second_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual({}, response["body"]["errors"])
        self.assertEqual(
            "legacy instrument",
            response["body"]["results"]["600000.SH"]["InstrumentName"],
        )
        self.assertEqual(
            "19911101",
            response["body"]["results"]["000001.SZ"]["OpenDate"],
        )

    def test_get_instrument_details_enforces_aggregate_json_limit(self):
        original_limit = self.strategy.MAX_BATCH_INSTRUMENT_DETAIL_JSON_VALUES
        self.strategy.MAX_BATCH_INSTRUMENT_DETAIL_JSON_VALUES = 4
        try:
            client_thread, response = self.start_request(
                "/get_instrument_details",
                {"stock_code": ["600000.SH", "000001.SZ"]},
            )
            self.wait_for_queued_job()

            self.context.callback(FakeContext())
            self.context.callback(FakeContext())
            client_thread.join(timeout=2)

            self.assertFalse(client_thread.is_alive())
            self.assertEqual(200, response["status"])
            self.assertIn("600000.SH", response["body"]["results"])
            self.assertNotIn("000001.SZ", response["body"]["results"])
            self.assertEqual(
                "RESULT_LIMIT_EXCEEDED",
                response["body"]["errors"]["000001.SZ"]["code"],
            )
        finally:
            self.strategy.MAX_BATCH_INSTRUMENT_DETAIL_JSON_VALUES = (
                original_limit
            )

    def test_get_divid_factors_uses_current_schedule_context(self):
        client_thread, response = self.start_request(
            "/get_divid_factors",
            {"stockcode": "600000.SH"},
        )
        self.wait_for_queued_job()

        reset_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            {
                "1689868800000": [
                    0.32,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0,
                    1.04507,
                ]
            },
            response["body"],
        )
        self.assertEqual(
            [(schedule_thread_id, "600000.SH")],
            reset_context.divid_factor_calls,
        )
        self.assertEqual([], self.context.divid_factor_calls)

    def test_get_divid_factors_batch_isolates_item_errors_across_ticks(self):
        body = {"stock_code": ["600000.SH", "000001.SZ"]}
        client_thread, response = self.start_request(
            "/get_divid_factors_batch",
            body,
        )
        self.wait_for_queued_job()

        first_context = FakeContext()
        self.context.callback(first_context)

        self.assertTrue(client_thread.is_alive())
        self.assertEqual(
            [(threading.get_ident(), "600000.SH")],
            first_context.divid_factor_calls,
        )

        second_context = FakeContext()
        second_context.get_divid_factors = lambda stockcode: []
        self.context.callback(second_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual(
            {
                "600000.SH": {
                    "1689868800000": [
                        0.32,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0,
                        1.04507,
                    ]
                }
            },
            response["body"]["results"],
        )
        self.assertEqual(
            "INVALID_QMT_RESULT",
            response["body"]["errors"]["000001.SZ"]["code"],
        )

    def test_get_divid_factors_batch_enforces_aggregate_record_limit(self):
        original_limit = self.strategy.MAX_BATCH_DIVID_FACTOR_RECORDS
        self.strategy.MAX_BATCH_DIVID_FACTOR_RECORDS = 1
        try:
            client_thread, response = self.start_request(
                "/get_divid_factors_batch",
                {"stock_code": ["600000.SH", "000001.SZ"]},
            )
            self.wait_for_queued_job()

            self.context.callback(FakeContext())
            self.context.callback(FakeContext())
            client_thread.join(timeout=2)

            self.assertFalse(client_thread.is_alive())
            self.assertEqual(200, response["status"])
            self.assertIn("600000.SH", response["body"]["results"])
            self.assertNotIn("000001.SZ", response["body"]["results"])
            self.assertEqual(
                "RESULT_LIMIT_EXCEEDED",
                response["body"]["errors"]["000001.SZ"]["code"],
            )
        finally:
            self.strategy.MAX_BATCH_DIVID_FACTOR_RECORDS = original_limit

    def test_get_weight_in_index_uses_current_schedule_context(self):
        client_thread, response = self.start_request(
            "/get_weight_in_index",
            {
                "indexcode": "000300.SH",
                "stockcode": "000002.SZ",
            },
        )
        self.wait_for_queued_job()

        reset_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(0.438, response["body"])
        self.assertEqual(
            [(schedule_thread_id, "000300.SH", "000002.SZ")],
            reset_context.index_weight_calls,
        )
        self.assertEqual([], self.context.index_weight_calls)

    def test_get_weights_in_index_processes_one_stock_per_tick(self):
        body = {
            "indexcode": "000300.SH",
            "stock_code": ["000002.SZ", "600000.SH"],
        }
        client_thread, response = self.start_request(
            "/get_weights_in_index",
            body,
        )
        self.wait_for_queued_job()

        first_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(first_context)

        self.assertTrue(client_thread.is_alive())
        self.assertEqual(
            [(schedule_thread_id, "000300.SH", "000002.SZ")],
            first_context.index_weight_calls,
        )

        second_context = FakeContext()
        self.context.callback(second_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual(
            {"000002.SZ": 0.438, "600000.SH": 0.438},
            response["body"]["results"],
        )
        self.assertEqual({}, response["body"]["errors"])
        self.assertEqual(
            [(schedule_thread_id, "000300.SH", "600000.SH")],
            second_context.index_weight_calls,
        )

    def test_batch_endpoints_reject_invalid_params_before_qmt_calls(self):
        too_many_stocks = [
            "{0:06d}.SZ".format(index) for index in range(21)
        ]
        cases = (
            ("/get_instrument_details", {}),
            (
                "/get_instrument_details",
                {"stock_code": ["600000.SH", "600000.SH"]},
            ),
            (
                "/get_instrument_details",
                {"stock_code": too_many_stocks},
            ),
            (
                "/get_instrument_details",
                {"stock_code": ["600000.SH"], "iscomplete": "yes"},
            ),
            ("/get_divid_factors_batch", {"stock_code": []}),
            (
                "/get_divid_factors_batch",
                {"stock_code": ["600000"], "unknown": True},
            ),
            (
                "/get_weights_in_index",
                {"stock_code": ["000002.SZ"]},
            ),
            (
                "/get_weights_in_index",
                {
                    "indexcode": "000300",
                    "stock_code": ["000002.SZ"],
                },
            ),
        )
        for path, body in cases:
            with self.subTest(path=path, body=body):
                client_thread, response = self.start_request(path, body)
                self.wait_for_queued_job()

                self.context.callback(self.context)
                client_thread.join(timeout=2)

                self.assertFalse(client_thread.is_alive())
                self.assertEqual(400, response["status"])
                self.assertEqual(
                    "INVALID_PARAMS",
                    response["body"]["error"]["code"],
                )
        self.assertEqual([], self.context.instrument_detail_calls)
        self.assertEqual([], self.context.divid_factor_calls)
        self.assertEqual([], self.context.index_weight_calls)

    def test_stop_wakes_an_active_batch_request(self):
        client_thread, response = self.start_request(
            "/get_instrument_details",
            {"stock_code": ["600000.SH", "000001.SZ"]},
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        self.assertTrue(client_thread.is_alive())

        self.strategy.stop(self.context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(503, response["status"])
        self.assertEqual(
            "FEED_STOPPING",
            response["body"]["error"]["code"],
        )
        self.assertEqual(0, self.state.request_queue.unfinished_tasks)

    def test_batch_is_not_reactivated_after_stop_begins_during_item(self):
        client_thread, response = self.start_request(
            "/get_instrument_details",
            {"stock_code": ["600000.SH", "000001.SZ"]},
        )
        self.wait_for_queued_job()

        stopping_context = FakeContext()
        original_get_detail = stopping_context.get_instrument_detail

        def get_detail_and_stop(stockcode, iscomplete):
            result = original_get_detail(stockcode, iscomplete)
            self.strategy._FEED_STATE.stop_event.set()
            return result

        stopping_context.get_instrument_detail = get_detail_and_stop
        self.context.callback(stopping_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(503, response["status"])
        self.assertEqual(
            "FEED_STOPPING",
            response["body"]["error"]["code"],
        )
        self.assertIsNone(self.strategy._FEED_STATE.active_job)
        self.assertEqual(
            0,
            self.strategy._FEED_STATE.request_queue.unfinished_tasks,
        )

    def test_expired_active_batch_is_cleared_before_next_request(self):
        active_job = self.strategy.RequestJob(
            {
                "method": "get_instrument_details",
                "params": {"stock_code": ["600000.SH", "000001.SZ"]},
            }
        )
        active_job.batch_state = (
            self.strategy.init_get_instrument_details_batch(
                active_job.request["params"]
            )
        )
        active_job.batch_state["position"] = 1
        active_job.deadline = 0
        self.strategy._FEED_STATE.active_job = active_job

        next_job = self.strategy.RequestJob(
            {
                "method": "get_weight_in_index",
                "params": {
                    "indexcode": "000300.SH",
                    "stockcode": "000002.SZ",
                },
            }
        )
        self.strategy._FEED_STATE.request_queue.put_nowait(next_job)

        self.context.callback(FakeContext())

        self.assertTrue(active_job.done.is_set())
        self.assertEqual("REQUEST_EXPIRED", active_job.error["code"])
        self.assertIsNone(self.strategy._FEED_STATE.active_job)
        self.assertFalse(next_job.done.is_set())

        self.context.callback(FakeContext())

        self.assertTrue(next_job.done.is_set())
        self.assertEqual(0.438, next_job.result)

    def test_get_weight_in_index_rejects_invalid_params(self):
        invalid_params = (
            {},
            {"indexcode": "000300.SH"},
            {"stockcode": "000002.SZ"},
            {"indexcode": "000300", "stockcode": "000002.SZ"},
            {"indexcode": "000300.SH", "stockcode": "000002"},
            {
                "indexcode": "000300.SH",
                "stockcode": "000002.SZ",
                "date": "20260724",
            },
        )
        for params in invalid_params:
            client_thread, response = self.start_request(
                "/get_weight_in_index",
                params,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.index_weight_calls)

    def test_get_weight_in_index_rejects_invalid_qmt_result(self):
        invalid_results = (
            None,
            True,
            "0.438",
            float("nan"),
            float("inf"),
            10 ** 10000,
        )
        for result in invalid_results:
            self.context.get_weight_in_index = (
                lambda indexcode, stockcode, value=result: value
            )
            client_thread, response = self.start_request(
                "/get_weight_in_index",
                {
                    "indexcode": "000300.SH",
                    "stockcode": "000002.SZ",
                },
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(500, response["status"])
            self.assertEqual(
                "INVALID_QMT_RESULT",
                response["body"]["error"]["code"],
            )

    def test_get_divid_factors_rejects_invalid_params(self):
        invalid_params = (
            {},
            {"stockcode": "600000"},
            {"stockcode": 600000},
            {"stockcode": "600000.SH", "date": "20230721"},
        )
        for params in invalid_params:
            client_thread, response = self.start_request(
                "/get_divid_factors",
                params,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.divid_factor_calls)

    def test_get_divid_factors_rejects_invalid_qmt_result(self):
        invalid_results = (
            [],
            {"1689868800000": [0.0] * 7},
            {1689868800000: [0.0] * 6},
            {1689868800000: [0.0] * 6 + [float("nan")]},
            {1689868800000: [0.0] * 6 + [True]},
        )
        for result in invalid_results:
            self.context.get_divid_factors = (
                lambda stockcode, value=result: value
            )
            client_thread, response = self.start_request(
                "/get_divid_factors",
                {"stockcode": "600000.SH"},
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(500, response["status"])
            self.assertEqual(
                "INVALID_QMT_RESULT",
                response["body"]["error"]["code"],
            )

    def test_get_divid_factors_rejects_too_many_records(self):
        self.context.get_divid_factors = lambda stockcode: {
            timestamp: [0.0] * 7
            for timestamp in range(
                self.strategy.MAX_DIVID_FACTOR_RECORDS + 1
            )
        }
        client_thread, response = self.start_request(
            "/get_divid_factors",
            {"stockcode": "600000.SH"},
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(500, response["status"])
        self.assertEqual(
            "INVALID_QMT_RESULT",
            response["body"]["error"]["code"],
        )

    def test_get_divid_factors_processes_at_most_one_job_per_tick(self):
        first_thread, first_response = self.start_request(
            "/get_divid_factors",
            {"stockcode": "600000.SH"},
        )
        self.wait_for_queued_job()
        second_thread, second_response = self.start_request(
            "/get_divid_factors",
            {"stockcode": "000001.SZ"},
        )
        deadline = time.time() + 2
        while self.state.request_queue.qsize() < 2 and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(2, self.state.request_queue.qsize())

        self.context.callback(self.context)
        first_thread.join(timeout=2)

        self.assertFalse(first_thread.is_alive())
        self.assertTrue(second_thread.is_alive())
        self.assertEqual(1, self.state.request_queue.qsize())
        self.assertEqual(1, len(self.context.divid_factor_calls))

        self.context.callback(self.context)
        second_thread.join(timeout=2)

        self.assertFalse(second_thread.is_alive())
        self.assertEqual(200, first_response["status"])
        self.assertEqual(200, second_response["status"])
        self.assertEqual(2, len(self.context.divid_factor_calls))

    def test_get_instrument_detail_defaults_iscomplete_to_false(self):
        client_thread, response = self.start_request(
            "/get_instrument_detail",
            {"stockcode": "600000.SH"},
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            [(threading.get_ident(), "600000.SH", False)],
            self.context.instrument_detail_calls,
        )

    def test_get_instrument_detail_rejects_invalid_stockcode(self):
        invalid_stockcodes = (
            None,
            600000,
            "",
            "600000",
            ".SH",
            "600000.",
            "600000. SH",
            "600 000.SH",
            "600000.\x00SH",
            "X" * 65 + ".SH",
        )
        for stockcode in invalid_stockcodes:
            body = {} if stockcode is None else {"stockcode": stockcode}
            client_thread, response = self.start_request(
                "/get_instrument_detail",
                body,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.instrument_detail_calls)

    def test_get_instrument_detail_rejects_invalid_iscomplete(self):
        for iscomplete in (1, 0, "1", "yes", "", None):
            client_thread, response = self.start_request(
                "/get_instrument_detail",
                {
                    "stockcode": "600000.SH",
                    "iscomplete": iscomplete,
                },
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.instrument_detail_calls)

    def test_get_instrument_detail_rejects_invalid_qmt_result(self):
        self.context.get_instrument_detail = lambda *args: []
        client_thread, response = self.start_request(
            "/get_instrument_detail",
            {"stockcode": "600000.SH"},
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(500, response["status"])
        self.assertEqual(
            "INVALID_QMT_RESULT",
            response["body"]["error"]["code"],
        )

    def test_get_financial_data_range_uses_current_schedule_context(self):
        body = {
            "mode": "range",
            "fieldList": ["ASHAREBALANCESHEET.fix_assets"],
            "stockList": ["600000.SH"],
            "startDate": "20260701",
            "endDate": "20260702",
            "report_type": "announce_time",
        }
        client_thread, response = self.start_request(
            "/get_financial_data",
            body,
        )
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {"method": "get_financial_data", "params": body},
            queued_job.request,
        )
        reset_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual(
            {
                "type": "dataframe",
                "index": ["20260701", "20260702"],
                "columns": ["fix_assets"],
                "data": [[100.5], [None]],
            },
            response["body"],
        )
        self.assertEqual(
            [
                (
                    schedule_thread_id,
                    ["ASHAREBALANCESHEET.fix_assets"],
                    ["600000.SH"],
                    "20260701",
                    "20260702",
                    "announce_time",
                )
            ],
            reset_context.financial_data_calls,
        )
        self.assertEqual([], self.context.financial_data_calls)

    def test_get_financial_data_bar_mode_returns_scalar(self):
        client_thread, response = self.start_request(
            "/get_financial_data",
            {
                "mode": "bar",
                "tabname": "ASHAREBALANCESHEET",
                "colname": "fix_assets",
                "market": "SH",
                "code": "600000",
                "barpos": "12",
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(42758000000.0, response["body"])
        self.assertEqual(
            [
                (
                    threading.get_ident(),
                    "ASHAREBALANCESHEET",
                    "fix_assets",
                    "SH",
                    "600000",
                    12,
                )
            ],
            self.context.financial_data_calls,
        )
        self.assertEqual([{}], self.context.financial_data_keyword_calls)

    def test_get_financial_data_bar_report_type_uses_keyword(self):
        client_thread, response = self.start_request(
            "/get_financial_data",
            {
                "mode": "bar",
                "tabname": "ASHAREBALANCESHEET",
                "colname": "fix_assets",
                "market": "SH",
                "code": "600000",
                "barpos": 12,
                "report_type": "announce_time",
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            [
                (
                    threading.get_ident(),
                    "ASHAREBALANCESHEET",
                    "fix_assets",
                    "SH",
                    "600000",
                    12,
                )
            ],
            self.context.financial_data_calls,
        )
        self.assertEqual(
            [{"report_type": "announce_time"}],
            self.context.financial_data_keyword_calls,
        )

    def test_get_financial_data_serializes_panel_axes(self):
        self.context.get_financial_data = lambda *args: FakeFinancialPanel()
        client_thread, response = self.start_request(
            "/get_financial_data",
            {
                "mode": "range",
                "fieldList": ["ASHAREBALANCESHEET.fix_assets"],
                "stockList": ["600000.SH", "000001.SZ"],
                "startDate": "20260701",
                "endDate": "20260701",
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            {
                "type": "panel",
                "items": ["600000.SH", "000001.SZ"],
                "major_axis": ["20260701"],
                "minor_axis": ["fix_assets"],
                "data": [[[100.5]], [[200.5]]],
            },
            response["body"],
        )

    def test_get_financial_data_serializes_series(self):
        self.context.get_financial_data = lambda *args: FakeFinancialSeries()
        client_thread, response = self.start_request(
            "/get_financial_data",
            {
                "mode": "range",
                "fieldList": [
                    "ASHAREBALANCESHEET.fix_assets",
                    "ASHAREBALANCESHEET.total_assets",
                ],
                "stockList": ["600000.SH"],
                "startDate": "20260701",
                "endDate": "20260701",
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            {
                "type": "series",
                "index": ["fix_assets", "total_assets"],
                "data": [100.5, None],
            },
            response["body"],
        )

    def test_get_financial_data_rejects_invalid_range_params(self):
        valid = {
            "mode": "range",
            "fieldList": ["ASHAREBALANCESHEET.fix_assets"],
            "stockList": ["600000.SH"],
            "startDate": "20260701",
            "endDate": "20260702",
        }
        invalid_params = (
            dict(valid, fieldList=[]),
            dict(valid, stockList=["600000"]),
            dict(valid, startDate="2026-07-01"),
            dict(valid, endDate="20260630"),
            dict(valid, report_type="future_time"),
            dict(
                valid,
                fieldList=["T.f{0}".format(index) for index in range(16)],
                stockList=["600000.SH"] * 20,
                endDate="20260901",
            ),
        )
        for params in invalid_params:
            client_thread, response = self.start_request(
                "/get_financial_data",
                params,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.financial_data_calls)

    def test_get_financial_data_rejects_invalid_bar_params(self):
        valid = {
            "mode": "bar",
            "tabname": "ASHAREBALANCESHEET",
            "colname": "fix_assets",
            "market": "SH",
            "code": "600000",
            "barpos": 12,
        }
        invalid_params = (
            dict(valid, barpos=-1),
            dict(valid, barpos=True),
            dict(valid, barpos="9" * 100),
            dict(valid, tabname=""),
            dict(valid, report_type="future_time"),
            dict(valid, mode="unknown"),
        )
        for params in invalid_params:
            client_thread, response = self.start_request(
                "/get_financial_data",
                params,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.financial_data_calls)

    def test_get_financial_data_rejects_invalid_qmt_result(self):
        self.context.get_financial_data = lambda *args: {}
        client_thread, response = self.start_request(
            "/get_financial_data",
            {
                "mode": "range",
                "fieldList": ["ASHAREBALANCESHEET.fix_assets"],
                "stockList": ["600000.SH"],
                "startDate": "20260701",
                "endDate": "20260702",
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(500, response["status"])
        self.assertEqual(
            "INVALID_QMT_RESULT",
            response["body"]["error"]["code"],
        )

    def test_get_market_data_ex_uses_current_schedule_context(self):
        body = {
            "fields": ["close", "askPrice"],
            "stock_code": ["600000.SH"],
            "period": "1m",
            "start_time": "20260701093000",
            "end_time": "20260701150000",
            "count": 2,
            "dividend_type": "none",
            "fill_data": False,
            "subscribe": False,
        }
        client_thread, response = self.start_request(
            "/get_market_data_ex",
            body,
        )
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {"method": "get_market_data_ex", "params": body},
            queued_job.request,
        )
        reset_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual(
            {
                "600000.SH": {
                    "index": ["20260701093000", "20260701093100"],
                    "columns": ["close", "askPrice"],
                    "data": [
                        [10.1, [10.2, 10.3]],
                        [None, [10.3, 10.4]],
                    ],
                }
            },
            response["body"],
        )
        self.assertEqual(
            [
                (
                    schedule_thread_id,
                    (["close", "askPrice"], ["600000.SH"]),
                    {
                        "period": "1m",
                        "start_time": "20260701093000",
                        "end_time": "20260701150000",
                        "count": 2,
                        "dividend_type": "none",
                        "fill_data": False,
                        "subscribe": False,
                    },
                )
            ],
            reset_context.market_data_ex_calls,
        )
        self.assertEqual([], self.context.market_data_ex_calls)

    def test_get_market_data_ex_applies_bounded_feed_defaults(self):
        client_thread, response = self.start_request(
            "/get_market_data_ex",
            {"stock_code": "600000.SH"},
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            [
                (
                    threading.get_ident(),
                    (
                        [
                            "time",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "amount",
                            "settle",
                            "openInterest",
                            "preClose",
                            "suspendFlag",
                        ],
                        ["600000.SH"],
                    ),
                    {
                        "period": "follow",
                        "start_time": "",
                        "end_time": "",
                        "count": 1,
                        "dividend_type": "follow",
                        "fill_data": False,
                        "subscribe": False,
                    },
                )
            ],
            self.context.market_data_ex_calls,
        )

    def test_get_market_data_ex_allows_explicit_fill_data_true(self):
        client_thread, response = self.start_request(
            "/get_market_data_ex",
            {
                "fields": ["close"],
                "stock_code": "600000.SH",
                "period": "1d",
                "fill_data": True,
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertTrue(
            self.context.market_data_ex_calls[-1][2]["fill_data"]
        )

    def test_get_market_data_ex_expands_tick_and_level2_default_fields(self):
        cases = (
            (
                "tick",
                [
                    "time",
                    "lastPrice",
                    "lastClose",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "settle",
                    "openInterest",
                    "stockStatus",
                ],
            ),
            (
                "l2transaction",
                [
                    "time",
                    "price",
                    "volume",
                    "amount",
                    "tradeIndex",
                    "buyNo",
                    "sellNo",
                    "tradeType",
                    "tradeFlag",
                ],
            ),
        )
        for period, expected_fields in cases:
            client_thread, response = self.start_request(
                "/get_market_data_ex",
                {
                    "fields": [],
                    "stock_code": "600000.SH",
                    "period": period,
                },
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(200, response["status"])
            self.assertEqual(
                expected_fields,
                self.context.market_data_ex_calls[-1][1][0],
            )

    def test_default_market_data_fields_supports_synthesized_bars(self):
        expected_fields = list(self.strategy.MARKET_DATA_BAR_FIELDS)
        for period in (
            "3m",
            "10m",
            "60m",
            "2h",
            "3h",
            "4h",
            "2d",
            "3d",
            "5d",
        ):
            self.assertEqual(
                expected_fields,
                self.strategy.default_market_data_fields(period),
            )

    def test_get_market_data_ex_resolves_follow_profile_from_context(self):
        client_thread, response = self.start_request(
            "/get_market_data_ex",
            {"fields": [], "stock_code": "600000.SH"},
        )
        self.wait_for_queued_job()

        reset_context = FakeContext()
        reset_context.period = "tick"
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            list(self.strategy.MARKET_DATA_TICK_FIELDS),
            reset_context.market_data_ex_calls[-1][1][0],
        )
        self.assertEqual(
            "follow",
            reset_context.market_data_ex_calls[-1][2]["period"],
        )
        self.assertEqual([], self.context.market_data_ex_calls)

    def test_get_market_data_ex_rejects_unknown_empty_field_profile(self):
        for request_params in (
            {
                "fields": [],
                "stock_code": "600000.SH",
                "period": "special-data",
            },
            {"fields": [], "stock_code": "600000.SH"},
        ):
            if "period" not in request_params:
                self.context.period = None
            client_thread, response = self.start_request(
                "/get_market_data_ex",
                request_params,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )

    def test_get_market_data_ex_accepts_explicit_special_period_fields(self):
        client_thread, response = self.start_request(
            "/get_market_data_ex",
            {
                "fields": ["question"],
                "stock_code": "600000.SH",
                "period": "interactiveqa",
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            ["question"],
            self.context.market_data_ex_calls[-1][1][0],
        )

    def test_get_market_data_ex_sizes_expanded_fields_before_qmt_call(self):
        client_thread, response = self.start_request(
            "/get_market_data_ex",
            {
                "fields": [],
                "stock_code": ["600000.SH", "000001.SZ"],
                "period": "1d",
                "count": 1000,
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(400, response["status"])
        self.assertEqual(
            "INVALID_PARAMS",
            response["body"]["error"]["code"],
        )
        self.assertEqual([], self.context.market_data_ex_calls)

    def test_get_market_data_ex_parses_get_booleans_and_lists(self):
        query = urlencode(
            [
                ("fields", "close"),
                ("fields", "open"),
                ("stock_code", "600000.SH"),
                ("count", "2"),
                ("fill_data", "false"),
                ("subscribe", "false"),
                ("start_time", "20260701093000"),
                ("end_time", "20260701"),
            ]
        )
        client_thread, response = self.start_request(
            "/get_market_data_ex?" + query
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        call = self.context.market_data_ex_calls[0]
        self.assertEqual((["close", "open"], ["600000.SH"]), call[1])
        self.assertEqual(2, call[2]["count"])
        self.assertFalse(call[2]["fill_data"])
        self.assertFalse(call[2]["subscribe"])

    def test_get_market_data_ex_rejects_unbounded_or_invalid_params(self):
        valid = {
            "fields": ["close"],
            "stock_code": ["600000.SH"],
            "period": "1d",
            "count": 10,
        }
        invalid_params = (
            {},
            dict(valid, stock_code=["600000"]),
            dict(valid, fields=["f{0}".format(index) for index in range(33)]),
            dict(valid, count=-1),
            dict(valid, count=True),
            dict(valid, count=1001),
            dict(valid, period=""),
            dict(valid, start_time="2026-07-01"),
            dict(valid, start_time="20260702", end_time="20260701"),
            dict(valid, dividend_type="future"),
            dict(valid, fill_data="yes"),
            dict(valid, subscribe=1),
            dict(valid, subscribe=True),
            dict(valid, unknown="value"),
            {
                "fields": ["f{0}".format(index) for index in range(32)],
                "stock_code": ["600000.SH"] * 20,
                "count": 1000,
            },
        )
        for params in invalid_params:
            client_thread, response = self.start_request(
                "/get_market_data_ex",
                params,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.market_data_ex_calls)

    def test_get_market_data_ex_rejects_invalid_qmt_result(self):
        invalid_results = (
            [],
            {"600000.SH": {}},
        )
        for result in invalid_results:
            self.context.get_market_data_ex = lambda *args, **kwargs: result
            client_thread, response = self.start_request(
                "/get_market_data_ex",
                {
                    "fields": ["close"],
                    "stock_code": ["600000.SH"],
                },
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(500, response["status"])
            self.assertEqual(
                "INVALID_QMT_RESULT",
                response["body"]["error"]["code"],
            )

    def test_get_market_data_ex_rejects_oversized_nested_result(self):
        frame = FakeMarketFrame()
        frame.index = ["20260701093000"]
        frame.columns = ["askPrice"]
        frame.values = FakeValues(
            [[FakeArray([1] * 50001)]]
        )
        self.context.get_market_data_ex = (
            lambda *args, **kwargs: {"600000.SH": frame}
        )
        client_thread, response = self.start_request(
            "/get_market_data_ex",
            {
                "fields": ["askPrice"],
                "stock_code": ["600000.SH"],
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(500, response["status"])
        self.assertEqual(
            "INVALID_QMT_RESULT",
            response["body"]["error"]["code"],
        )

    def test_get_market_data_ex_accepts_exact_request_cell_limit(self):
        fields = ["field{0}".format(index) for index in range(20)]
        client_thread, response = self.start_request(
            "/get_market_data_ex",
            {
                "fields": fields,
                "stock_code": ["600000.SH"],
                "count": 1000,
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(fields, self.context.market_data_ex_calls[0][1][0])

    def test_get_market_data_ex_checks_shape_before_materializing_values(self):
        oversized_frames = []
        too_many_rows = FakeMarketFrame()
        too_many_rows.index = list(range(1001))
        too_many_rows.columns = ["close"]
        too_many_rows.values = ExplodingValues()
        oversized_frames.append(too_many_rows)

        too_many_columns = FakeMarketFrame()
        too_many_columns.index = ["20260701"]
        too_many_columns.columns = list(range(129))
        too_many_columns.values = ExplodingValues()
        oversized_frames.append(too_many_columns)

        for frame in oversized_frames:
            self.context.get_market_data_ex = (
                lambda *args, **kwargs: {"600000.SH": frame}
            )
            client_thread, response = self.start_request(
                "/get_market_data_ex",
                {
                    "fields": ["close"],
                    "stock_code": ["600000.SH"],
                    "count": 1000,
                },
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(500, response["status"])
            self.assertEqual(
                "INVALID_QMT_RESULT",
                response["body"]["error"]["code"],
            )

    def test_get_market_data_ex_rejects_aggregate_response_over_cell_limit(self):
        rows = list(range(1000))
        columns = ["field{0}".format(index) for index in range(11)]
        data = [[0] * 11 for _ in rows]
        first_frame = FakeMarketFrame()
        first_frame.index = rows
        first_frame.columns = columns
        first_frame.values = FakeValues(data)
        second_frame = FakeMarketFrame()
        second_frame.index = rows
        second_frame.columns = columns
        second_frame.values = ExplodingValues()
        self.context.get_market_data_ex = lambda *args, **kwargs: {
            "600000.SH": first_frame,
            "000001.SZ": second_frame,
        }
        client_thread, response = self.start_request(
            "/get_market_data_ex",
            {
                "fields": ["close"],
                "stock_code": ["600000.SH", "000001.SZ"],
                "count": 1000,
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(500, response["status"])
        self.assertEqual(
            "INVALID_QMT_RESULT",
            response["body"]["error"]["code"],
        )

    def test_get_market_data_ex_processes_at_most_one_job_per_tick(self):
        first_thread, first_response = self.start_request(
            "/get_market_data_ex",
            {
                "fields": ["close"],
                "stock_code": ["600000.SH"],
            },
        )
        self.wait_for_queued_job()
        second_thread, second_response = self.start_request(
            "/get_market_data_ex",
            {
                "fields": ["close"],
                "stock_code": ["000001.SZ"],
            },
        )
        deadline = time.time() + 2
        while self.state.request_queue.qsize() < 2 and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(2, self.state.request_queue.qsize())

        self.context.callback(self.context)
        first_thread.join(timeout=2)

        self.assertFalse(first_thread.is_alive())
        self.assertTrue(second_thread.is_alive())
        self.assertEqual(1, self.state.request_queue.qsize())
        self.assertEqual(1, len(self.context.market_data_ex_calls))

        self.context.callback(self.context)
        second_thread.join(timeout=2)

        self.assertFalse(second_thread.is_alive())
        self.assertEqual(200, first_response["status"])
        self.assertEqual(200, second_response["status"])
        self.assertEqual(2, len(self.context.market_data_ex_calls))

    def test_download_history_data_returns_task_and_runs_in_schedule(self):
        status, body = self.request_json(
            "/download_history_data",
            {
                "stockcode": "600000.SH",
                "period": "1m",
                "startTime": "20260701093000",
                "endTime": "20260701150000",
            },
        )

        self.assertEqual(202, status)
        self.assertEqual("queued", body["status"])
        task_id = body["task_id"]
        self.assertEqual(1, self.state.request_queue.qsize())
        self.assertEqual([], self.download_history_calls)

        queued_status, queued_body = self.request_json(
            "/get_download_status?task_id={0}".format(task_id)
        )
        self.assertEqual(200, queued_status)
        self.assertEqual("queued", queued_body["status"])
        self.assertEqual(1, self.state.request_queue.qsize())

        schedule_thread_id = threading.get_ident()
        self.context.callback(self.context)

        completed_status, completed_body = self.request_json(
            "/get_download_status?task_id={0}".format(task_id)
        )
        self.assertEqual(200, completed_status)
        self.assertEqual("completed", completed_body["status"])
        self.assertIn("coverage was not verified", completed_body["message"])
        self.assertEqual(
            [
                (
                    schedule_thread_id,
                    (
                        "600000.SH",
                        "1m",
                        "20260701093000",
                        "20260701150000",
                    ),
                    {},
                )
            ],
            self.download_history_calls,
        )

    def test_download_history_data_passes_incrementally_only_when_explicit(self):
        status, body = self.request_json(
            "/download_history_data",
            {
                "stockcode": "600000.SH",
                "period": "1d",
                "startTime": "",
                "endTime": "",
                "incrementally": True,
            },
        )

        self.assertEqual(202, status)
        self.assertTrue(body["incrementally"])
        self.context.callback(self.context)

        self.assertEqual(
            [
                (
                    threading.get_ident(),
                    (
                        "600000.SH",
                        "1d",
                        "",
                        "",
                    ),
                    {"incrementally": True},
                )
            ],
            self.download_history_calls,
        )

    def test_download_history_data_preserves_explicit_incrementally_null(self):
        status, body = self.request_json(
            "/download_history_data",
            {
                "stockcode": "600000.SH",
                "period": "1d",
                "incrementally": None,
            },
        )

        self.assertEqual(202, status)
        self.assertIsNone(body["incrementally"])
        self.context.callback(self.context)

        self.assertEqual(
            [
                (
                    threading.get_ident(),
                    (
                        "600000.SH",
                        "1d",
                        "",
                        "",
                    ),
                    {"incrementally": None},
                )
            ],
            self.download_history_calls,
        )

    def test_download_history_data_marks_qmt_errors_failed_without_retry(self):
        def unsupported_incrementally(*args, **kwargs):
            raise TypeError("too many arguments")

        self.strategy.download_history_data = unsupported_incrementally
        status, body = self.request_json(
            "/download_history_data",
            {
                "stockcode": "600000.SH",
                "period": "1d",
                "incrementally": True,
            },
        )

        self.assertEqual(202, status)
        self.context.callback(self.context)
        failed_status, failed_body = self.request_json(
            "/get_download_status?task_id={0}".format(body["task_id"])
        )

        self.assertEqual(200, failed_status)
        self.assertEqual("failed", failed_body["status"])
        self.assertEqual("QMT_ERROR", failed_body["error"]["code"])
        self.assertIn("too many arguments", failed_body["error"]["message"])
        self.assertEqual(0, self.state.request_queue.qsize())

    def test_download_history_data_rejects_invalid_or_unordered_params(self):
        valid = {
            "stockcode": "600000.SH",
            "period": "1m",
            "startTime": "20260701093000",
            "endTime": "20260701150000",
        }
        invalid_params = (
            {},
            dict(valid, stockcode="600000"),
            dict(valid, period="15m"),
            dict(valid, startTime="2026-07-01"),
            dict(valid, startTime="20260702", endTime="20260701"),
            dict(valid, incrementally="yes"),
            dict(valid, unknown="value"),
        )
        for params in invalid_params:
            status, body = self.request_json("/download_history_data", params)
            self.assertEqual(400, status)
            self.assertEqual("INVALID_PARAMS", body["error"]["code"])

        self.assertEqual(0, self.state.request_queue.qsize())
        self.assertEqual([], self.download_history_calls)

    def test_download_status_remains_available_while_qmt_is_blocked(self):
        entered = threading.Event()
        release = threading.Event()

        def blocking_download(*args):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test did not release download")

        self.strategy.download_history_data = blocking_download
        _, task = self.request_json(
            "/download_history_data", {"stockcode": "600000.SH", "period": "1d"}
        )
        worker = threading.Thread(target=self.context.callback, args=(self.context,))
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            status, snapshot = self.request_json(
                "/get_download_status?task_id=" + task["task_id"]
            )
            self.assertEqual(200, status)
            self.assertEqual("running", snapshot["status"])
        finally:
            release.set()
            worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual("completed", self.strategy.get_download_task_snapshot(
            self.state, task["task_id"]
        )["status"])

    def test_download_ignores_normal_request_deadline_and_stop_fails_queued(self):
        _, task = self.request_json(
            "/download_history_data", {"stockcode": "600000.SH", "period": "1d"}
        )
        with self.state.request_queue.mutex:
            job = self.state.request_queue.queue[0]
            job.deadline = 0
        self.context.callback(self.context)
        self.assertEqual("completed", self.strategy.get_download_task_snapshot(
            self.state, task["task_id"]
        )["status"])
        _, queued = self.request_json(
            "/download_history_data", {"stockcode": "000001.SZ", "period": "1d"}
        )
        self.strategy.stop(self.context)
        snapshot = self.strategy.get_download_task_snapshot(self.state, queued["task_id"])
        self.assertEqual("failed", snapshot["status"])
        self.assertEqual("FEED_STOPPING", snapshot["error"]["code"])
        self.assertEqual(1, len(self.download_history_calls))
        self.assertEqual(0, self.state.request_queue.unfinished_tasks)

    def test_download_cache_preserves_active_tasks_and_evicts_terminal_tasks(self):
        self.strategy.MAX_DOWNLOAD_TASKS = 1
        params = {"stockcode": "600000.SH", "period": "1d"}
        _, first = self.request_json("/download_history_data", params)
        status, error = self.request_json("/download_history_data", params)
        self.assertEqual(429, status)
        self.assertEqual("DOWNLOAD_TASKS_FULL", error["error"]["code"])
        self.context.callback(self.context)
        status, second = self.request_json("/download_history_data", params)
        self.assertEqual(202, status)
        self.assertNotEqual(first["task_id"], second["task_id"])
        self.assertNotIn(first["task_id"], self.state.download_tasks)
        self.assertEqual(1, len(self.state.download_tasks))

    def test_stop_before_download_enqueue_rejects_without_orphaned_job(self):
        created = threading.Event()
        release = threading.Event()
        original = self.strategy.create_download_task

        def paused_create(state, params):
            task = original(state, params)
            created.set()
            if not release.wait(2):
                raise RuntimeError("test did not release download admission")
            return task

        with patch.object(self.strategy, "create_download_task", side_effect=paused_create):
            client, response = self.start_request(
                "/download_history_data", {"stockcode": "600000.SH", "period": "1d"}
            )
            try:
                self.assertTrue(created.wait(1))
                self.strategy.stop(self.context)
            finally:
                release.set()
                client.join(2)
        self.assertFalse(client.is_alive())
        self.assertEqual(503, response["status"])
        self.assertEqual("FEED_STOPPING", response["body"]["error"]["code"])
        self.assertEqual(0, self.state.request_queue.qsize())
        self.assertEqual(0, self.state.request_queue.unfinished_tasks)
        self.assertEqual(0, len(self.state.download_tasks))
        self.assertEqual([], self.download_history_calls)

    def test_request_logs_correlate_download_without_raw_body(self):
        with patch.object(self.strategy, "log_message") as log:
            status, task = self.request_json(
                "/download_history_data", {"stockcode": "600000.SH", "period": "1d"}
            )
            self.context.callback(self.context)
        self.assertEqual(202, status)
        calls = log.call_args_list
        received = next(c for c in calls if c.args[1] == "Request received")
        response = next(c for c in calls if c.args[1] == "Response sent")
        started = next(c for c in calls if c.args[1] == "Download started")
        self.assertEqual(received.kwargs["request_id"], response.kwargs["request_id"])
        self.assertEqual(received.kwargs["request_id"], started.kwargs["request_id"])
        self.assertEqual(task["task_id"], response.kwargs["task_id"])
        self.assertEqual(202, response.kwargs["status"])
        self.assertGreater(response.kwargs["response_bytes"], 0)
        self.assertGreaterEqual(response.kwargs["elapsed_ms"], 0)

    def test_request_log_redacts_unknown_parameters_and_warns_on_failure(self):
        with patch.object(self.strategy, "log_message") as log:
            status, _ = self.request_json("/download_history_data", {
                "stockcode": "600000.SH", "period": "1d", "password": "secret-value"
            })
        self.assertEqual(400, status)
        self.assertNotIn("secret-value", str(log.call_args_list))
        response = next(c for c in log.call_args_list if c.args[1] == "Response sent")
        self.assertEqual("WARNING", response.args[0])
        self.assertEqual("INVALID_PARAMS", response.kwargs["error_code"])

    def test_successful_status_poll_logs_at_debug(self):
        _, task = self.request_json("/download_history_data", {
            "stockcode": "600000.SH", "period": "1d"
        })
        with patch.object(self.strategy, "log_message") as log:
            status, _ = self.request_json("/get_download_status?task_id=" + task["task_id"])
        self.assertEqual(200, status)
        self.assertEqual(["DEBUG", "DEBUG"], [c.args[0] for c in log.call_args_list])

    def test_download_history_data_get_is_rejected(self):
        status, body = self.request_json(
            "/download_history_data?stockcode=600000.SH&period=1d"
        )

        self.assertEqual(405, status)
        self.assertEqual("METHOD_NOT_ALLOWED", body["error"]["code"])
        self.assertEqual(0, self.state.request_queue.qsize())

    def test_download_status_rejects_unknown_task_without_qmt_dispatch(self):
        status, body = self.request_json(
            "/get_download_status?task_id=download-missing"
        )

        self.assertEqual(404, status)
        self.assertEqual("DOWNLOAD_TASK_NOT_FOUND", body["error"]["code"])
        self.assertEqual(0, self.state.request_queue.qsize())
        self.assertEqual([], self.download_history_calls)

    def test_download_history_data_rolls_back_task_when_queue_is_full(self):
        request_queue = self.state.request_queue
        for _ in range(self.strategy.QUEUE_MAX_SIZE):
            request_queue.put_nowait(
                self.strategy.RequestJob({"method": "account", "params": {}})
            )

        status, body = self.request_json(
            "/download_history_data",
            {
                "stockcode": "600000.SH",
                "period": "1d",
            },
        )

        self.assertEqual(429, status)
        self.assertEqual("QUEUE_FULL", body["error"]["code"])
        self.assertEqual(0, len(self.state.download_tasks))
        self.assertEqual([], self.download_history_calls)

    def test_get_full_tick_uses_current_schedule_context(self):
        body = {"stock_code": ["600000.SH", "000001.SZ"]}
        client_thread, response = self.start_request(
            "/get_full_tick",
            body,
        )
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {"method": "get_full_tick", "params": body},
            queued_job.request,
        )
        reset_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual(
            {
                stockcode: {
                    "time": 1782871200000,
                    "lastPrice": 10.1,
                    "lastClose": 10.0,
                    "askPrice": [10.2, 10.3],
                    "bidPrice": [10.0, 9.9],
                    "volume": 1234,
                    "amount": None,
                }
                for stockcode in ("600000.SH", "000001.SZ")
            },
            response["body"],
        )
        self.assertEqual(
            [
                (
                    schedule_thread_id,
                    ["600000.SH", "000001.SZ"],
                )
            ],
            reset_context.full_tick_calls,
        )
        self.assertEqual([], self.context.full_tick_calls)

    def test_get_full_tick_accepts_single_stock_code(self):
        client_thread, response = self.start_request(
            "/get_full_tick",
            {"stock_code": "600000.SH"},
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            [(threading.get_ident(), ["600000.SH"])],
            self.context.full_tick_calls,
        )

    def test_get_full_tick_rejects_invalid_params(self):
        invalid_params = (
            {},
            {"stock_code": "600000"},
            {"stock_code": []},
            {"stock_code": ["600000.SH"] * 21},
            {"stock_code": ["600000.SH"], "unknown": "value"},
        )
        for params in invalid_params:
            client_thread, response = self.start_request(
                "/get_full_tick",
                params,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.full_tick_calls)

    def test_get_full_tick_rejects_invalid_qmt_result(self):
        invalid_results = (
            [],
            {1: {}},
            {"600000.SH": []},
            {"000001.SZ": {}},
            {
                "600000.SH": {
                    "askPrice": FakeArray([1] * 50001),
                }
            },
        )
        for result in invalid_results:
            self.context.get_full_tick = lambda *args: result
            client_thread, response = self.start_request(
                "/get_full_tick",
                {"stock_code": ["600000.SH"]},
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(500, response["status"])
            self.assertEqual(
                "INVALID_QMT_RESULT",
                response["body"]["error"]["code"],
            )

    def test_get_full_tick_processes_at_most_one_job_per_tick(self):
        first_thread, first_response = self.start_request(
            "/get_full_tick",
            {"stock_code": ["600000.SH"]},
        )
        self.wait_for_queued_job()
        second_thread, second_response = self.start_request(
            "/get_full_tick",
            {"stock_code": ["000001.SZ"]},
        )
        deadline = time.time() + 2
        while self.state.request_queue.qsize() < 2 and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(2, self.state.request_queue.qsize())

        self.context.callback(self.context)
        first_thread.join(timeout=2)

        self.assertFalse(first_thread.is_alive())
        self.assertTrue(second_thread.is_alive())
        self.assertEqual(1, self.state.request_queue.qsize())
        self.assertEqual(1, len(self.context.full_tick_calls))

        self.context.callback(self.context)
        second_thread.join(timeout=2)

        self.assertFalse(second_thread.is_alive())
        self.assertEqual(200, first_response["status"])
        self.assertEqual(200, second_response["status"])
        self.assertEqual(2, len(self.context.full_tick_calls))

    def test_get_his_index_data_uses_current_schedule_context(self):
        client_thread, response = self.start_request(
            "/get_his_index_data?" + urlencode({"stockcode": "000300.SH"})
        )
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {
                "method": "get_his_index_data",
                "params": {"stockcode": "000300.SH"},
            },
            queued_job.request,
        )
        reset_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual(
            {
                "20260701": {"600000.SH": 0.0438},
                "20260702": {"600000.SH": None},
            },
            response["body"],
        )
        self.assertEqual(
            [(schedule_thread_id, "000300.SH")],
            reset_context.his_index_data_calls,
        )
        self.assertEqual([], self.context.his_index_data_calls)

    def test_get_his_index_data_rejects_invalid_params(self):
        invalid_params = (
            {},
            {"stockcode": "000300"},
            {"stockcode": []},
            {"stockcode": "000300.SH", "unknown": "value"},
        )
        for params in invalid_params:
            client_thread, response = self.start_request(
                "/get_his_index_data",
                params,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.his_index_data_calls)

    def test_get_his_index_data_rejects_invalid_qmt_result(self):
        invalid_results = (
            "unexpected scalar",
            [0] * 50001,
        )
        for result in invalid_results:
            self.context.get_his_index_data = lambda *args: result
            client_thread, response = self.start_request(
                "/get_his_index_data",
                {"stockcode": "000300.SH"},
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(500, response["status"])
            self.assertEqual(
                "INVALID_QMT_RESULT",
                response["body"]["error"]["code"],
            )

    def test_get_his_contract_list_uses_current_schedule_context(self):
        client_thread, response = self.start_request(
            "/get_his_contract_list?" + urlencode({"market": "sho"})
        )
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {
                "method": "get_his_contract_list",
                "params": {"market": "sho"},
            },
            queued_job.request,
        )
        reset_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        self.assertEqual(
            ["10000001.SHO", "10000002.SHO"],
            response["body"],
        )
        self.assertEqual(
            [(schedule_thread_id, "SHO")],
            reset_context.his_contract_list_calls,
        )
        self.assertEqual([], self.context.his_contract_list_calls)

    def test_get_his_contract_list_rejects_invalid_params(self):
        invalid_params = (
            {},
            {"market": []},
            {"market": ""},
            {"market": "SH.O"},
            {"market": "上海"},
            {"market": "SHO", "unknown": "value"},
        )
        for params in invalid_params:
            with self.subTest(params=params):
                client_thread, response = self.start_request(
                    "/get_his_contract_list",
                    params,
                )
                self.wait_for_queued_job()

                self.context.callback(self.context)
                client_thread.join(timeout=2)

                self.assertEqual(400, response["status"])
                self.assertEqual(
                    "INVALID_PARAMS",
                    response["body"]["error"]["code"],
                )
        self.assertEqual([], self.context.his_contract_list_calls)

    def test_get_his_contract_list_rejects_invalid_qmt_result(self):
        invalid_results = (
            None,
            "10000001.SHO",
            ["10000001"],
            ["10000001.SHO", 1],
            ["10000001.SHO"] * 50001,
        )
        for result in invalid_results:
            with self.subTest(result_type=type(result).__name__):
                self.context.get_his_contract_list = lambda *args: result
                client_thread, response = self.start_request(
                    "/get_his_contract_list",
                    {"market": "SHO"},
                )
                self.wait_for_queued_job()

                self.context.callback(self.context)
                client_thread.join(timeout=2)

                self.assertEqual(500, response["status"])
                self.assertEqual(
                    "INVALID_QMT_RESULT",
                    response["body"]["error"]["code"],
                )

    def test_get_longhubang_serializes_nested_booth_tables(self):
        body = {
            "stock_list": ["000002.SZ"],
            "startTime": "20260701",
            "endTime": "20260731",
        }
        client_thread, response = self.start_request(
            "/get_longhubang",
            body,
        )
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {"method": "get_longhubang", "params": body},
            queued_job.request,
        )
        reset_context = FakeContext()
        schedule_thread_id = threading.get_ident()
        self.context.callback(reset_context)
        client_thread.join(timeout=2)

        self.assertFalse(client_thread.is_alive())
        self.assertEqual(200, response["status"])
        result = response["body"]
        self.assertEqual(
            [
                "stockCode",
                "date",
                "close",
                "buyTraderBooth",
                "sellTraderBooth",
            ],
            result["columns"],
        )
        self.assertEqual("2026-07-01T00:00:00", result["data"][0][1])
        self.assertEqual(
            ["traderName", "buyAmount", "sellAmount", "rank"],
            result["data"][0][3]["columns"],
        )
        self.assertEqual(
            [["example buy booth", 1000000.0, 0.0, 1]],
            result["data"][0][3]["data"],
        )
        self.assertIsNone(result["data"][0][4]["data"][0][2])
        self.assertEqual(
            [
                (
                    schedule_thread_id,
                    ["000002.SZ"],
                    "20260701",
                    "20260731",
                )
            ],
            reset_context.longhubang_calls,
        )
        self.assertEqual([], self.context.longhubang_calls)

    def test_get_longhubang_rejects_invalid_params(self):
        invalid_params = (
            {},
            {
                "stock_list": [],
                "startTime": "20260701",
                "endTime": "20260731",
            },
            {
                "stock_list": ["000002"],
                "startTime": "20260701",
                "endTime": "20260731",
            },
            {
                "stock_list": ["000002.SZ"],
                "startTime": "20260732",
                "endTime": "20260731",
            },
            {
                "stock_list": ["000002.SZ"],
                "startTime": "20260801",
                "endTime": "20260731",
            },
            {
                "stock_list": ["000002.SZ"],
                "startTime": "20100101",
                "endTime": "20210101",
            },
            {
                "stock_list": ["000002.SZ"] * 20,
                "startTime": "20260101",
                "endTime": "20261231",
            },
            {
                "stock_list": ["000002.SZ"],
                "startTime": "20260701",
                "endTime": "20260731",
                "unknown": "value",
            },
        )
        for params in invalid_params:
            client_thread, response = self.start_request(
                "/get_longhubang",
                params,
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(400, response["status"])
            self.assertEqual(
                "INVALID_PARAMS",
                response["body"]["error"]["code"],
            )
        self.assertEqual([], self.context.longhubang_calls)

    def test_get_longhubang_rejects_invalid_qmt_result(self):
        invalid_results = (
            [],
            OversizedTable(1001),
            FakeTable(
                [0],
                ["buyTraderBooth"],
                [[FakeArray([0] * 50001)]],
            ),
        )
        for result in invalid_results:
            self.context.get_longhubang = lambda *args: result
            client_thread, response = self.start_request(
                "/get_longhubang",
                {
                    "stock_list": ["000002.SZ"],
                    "startTime": "20260701",
                    "endTime": "20260731",
                },
            )
            self.wait_for_queued_job()

            self.context.callback(self.context)
            client_thread.join(timeout=2)

            self.assertEqual(500, response["status"])
            self.assertEqual(
                "INVALID_QMT_RESULT",
                response["body"]["error"]["code"],
            )

    def test_new_history_methods_process_at_most_one_job_per_tick(self):
        cases = (
            (
                "/get_his_index_data",
                {"stockcode": "000300.SH"},
                "his_index_data_calls",
            ),
            (
                "/get_longhubang",
                {
                    "stock_list": ["000002.SZ"],
                    "startTime": "20260701",
                    "endTime": "20260731",
                },
                "longhubang_calls",
            ),
            (
                "/get_his_contract_list",
                {"market": "SHO"},
                "his_contract_list_calls",
            ),
        )
        for path, body, calls_name in cases:
            with self.subTest(path=path):
                first_thread, first_response = self.start_request(path, body)
                self.wait_for_queued_job()
                second_thread, second_response = self.start_request(path, body)
                deadline = time.time() + 2
                while (
                    self.state.request_queue.qsize() < 2
                    and time.time() < deadline
                ):
                    time.sleep(0.01)
                self.assertEqual(2, self.state.request_queue.qsize())

                self.context.callback(self.context)
                first_thread.join(timeout=2)

                self.assertFalse(first_thread.is_alive())
                self.assertTrue(second_thread.is_alive())
                self.assertEqual(1, self.state.request_queue.qsize())
                self.assertEqual(1, len(getattr(self.context, calls_name)))

                self.context.callback(self.context)
                second_thread.join(timeout=2)

                self.assertFalse(second_thread.is_alive())
                self.assertEqual(200, first_response["status"])
                self.assertEqual(200, second_response["status"])
                self.assertEqual(2, len(getattr(self.context, calls_name)))

    def test_sector_realtime_param_is_converted_to_millisecond_timestamp(self):
        client_thread, response = self.start_request(
            "/get_stock_list_in_sector",
            {
                "sectorname": "沪深300",
                "realtime": "1720000000000",
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            [(threading.get_ident(), "沪深300", 1720000000000)],
            self.context.sector_calls,
        )

    def test_sectorname_is_required(self):
        client_thread, response = self.start_request(
            "/get_stock_list_in_sector",
            {},
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(400, response["status"])
        self.assertEqual("INVALID_PARAMS", response["body"]["error"]["code"])
        self.assertEqual([], self.context.sector_calls)

    def test_fractional_sector_realtime_is_rejected(self):
        client_thread, response = self.start_request(
            "/get_stock_list_in_sector",
            {
                "sectorname": "沪深300",
                "realtime": 1720000000000.75,
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(400, response["status"])
        self.assertEqual("INVALID_PARAMS", response["body"]["error"]["code"])
        self.assertEqual([], self.context.sector_calls)

    def test_non_ascii_digit_sector_realtime_is_rejected(self):
        client_thread, response = self.start_request(
            "/get_stock_list_in_sector",
            {
                "sectorname": "沪深300",
                "realtime": "²",
            },
        )
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(400, response["status"])
        self.assertEqual("INVALID_PARAMS", response["body"]["error"]["code"])
        self.assertEqual([], self.context.sector_calls)

    def test_post_json_body_becomes_params(self):
        client_thread, response = self.start_request(
            "/account",
            {
                "accountId": "test-account",
                "accountType": "STOCK",
            },
        )
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {
                "method": "account",
                "params": {
                    "accountId": "test-account",
                    "accountType": "STOCK",
                },
            },
            queued_job.request,
        )
        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(200, response["status"])
        self.assertEqual(
            ("test-account", "STOCK", "account"),
            self.query_calls[0][1:],
        )

    def test_unknown_method_is_rejected_by_dispatch_not_http_layer(self):
        client_thread, response = self.start_request("/positions?code=600000.SH")
        queued_job = self.wait_for_queued_job()

        self.assertEqual(
            {
                "method": "positions",
                "params": {"code": "600000.SH"},
            },
            queued_job.request,
        )
        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(404, response["status"])
        self.assertEqual("METHOD_NOT_FOUND", response["body"]["error"]["code"])
        self.assertEqual([], self.query_calls)

    def test_account_field_failure_is_returned_as_qmt_error(self):
        self.strategy.get_trade_detail_data = (
            lambda account_id, account_type, detail_type: [BrokenAccount()]
        )
        client_thread, response = self.start_request("/account")
        self.wait_for_queued_job()

        self.context.callback(self.context)
        client_thread.join(timeout=2)

        self.assertEqual(500, response["status"])
        self.assertEqual("QMT_ERROR", response["body"]["error"]["code"])
        self.assertIn("m_broken", response["body"]["error"]["message"])

    def test_full_queue_returns_429_without_qmt_dispatch(self):
        request_queue = self.state.request_queue
        for _ in range(self.strategy.QUEUE_MAX_SIZE):
            request_queue.put_nowait(
                self.strategy.RequestJob({"method": "account", "params": {}})
            )

        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(self.base_url() + "/account", timeout=2)

        self.assertEqual(429, caught.exception.code)
        body = json.loads(caught.exception.read().decode("utf-8"))
        self.assertEqual("QUEUE_FULL", body["error"]["code"])
        self.assertEqual([], self.query_calls)

    def test_stop_only_signals_server_and_cancels_schedule(self):
        started = time.perf_counter()
        self.strategy.stop(self.context)
        elapsed = time.perf_counter() - started

        self.assertLess(elapsed, 0.05)
        self.assertEqual(self.context.timer_id, self.context.cancelled_timer_id)
        self.assertTrue(self.state.stop_event.is_set())


if __name__ == "__main__":
    unittest.main()
