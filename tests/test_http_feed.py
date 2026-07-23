import copy
import importlib.util
import json
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
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


class FakeContext:
    def __init__(self):
        self.account_id = None
        self.callback = None
        self.timer_id = "timer-1"
        self.cancelled_timer_id = None
        self.sector_calls = []

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


class HttpFeedTest(unittest.TestCase):
    def setUp(self):
        self.strategy = load_strategy()
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
