# -*- coding: utf-8 -*-
"""QMT 回调异常诊断；Last modified: 2026-09-26。"""
import queue
import unittest
from unittest.mock import Mock, patch

from order_bridge.common import qmt_exception_details
from order_bridge.qmt import QmtAdapter
from order_bridge.runtime import OrderRuntime


class CallbackDiagnosticTests(unittest.TestCase):
    def runtime(self):
        logger = Mock()
        runtime = OrderRuntime({}, object(), "account-1", repository=Mock(),
                               local_lock=Mock(), logger=logger)
        runtime.adapter = Mock()
        return runtime, logger

    def test_snapshot_failure_logs_original_exception_and_gap(self):
        runtime, logger = self.runtime()
        runtime.adapter.snapshot.side_effect = ValueError("native field unavailable")

        runtime.observe("order", object())

        self.assertTrue(runtime.observation_gap)
        self.assertEqual(runtime.background.overflows, 1)
        self.assertEqual(runtime.background.fact_generation, 1)
        self.assertTrue(runtime.observations.empty())
        fields = logger.call_args[1]
        self.assertEqual(fields["account_id"], "account-1")
        self.assertEqual(fields["kind"], "order")
        self.assertEqual(fields["source"], "qmt_callback")
        self.assertEqual(fields["phase"], "snapshot")
        self.assertEqual(fields["error_type"], "ValueError")
        self.assertEqual(fields["error_code"], "QMT_ERROR")
        self.assertEqual(fields["error_message"], "native field unavailable")
        self.assertIn("ValueError: native field unavailable", fields["traceback"])

    def test_queue_full_is_distinct_from_snapshot_failure(self):
        runtime, logger = self.runtime()
        runtime.adapter.snapshot.side_effect = lambda value: {"id": value}
        runtime.observations = queue.Queue(maxsize=1)
        runtime.observations.put_nowait(("order", {"id": "first"}))

        runtime.observe("deal", "second")

        self.assertEqual(runtime.adapter.snapshot.call_count, 1)
        self.assertEqual(runtime.observations.qsize(), 1)
        self.assertTrue(runtime.observation_gap)
        self.assertEqual(runtime.background.overflows, 1)
        self.assertEqual(runtime.background.fact_generation, 1)
        fields = logger.call_args[1]
        self.assertEqual(fields["kind"], "deal")
        self.assertEqual(fields["phase"], "enqueue")
        self.assertEqual(fields["error_code"], "OBSERVATION_QUEUE_FULL")
        self.assertEqual(fields["error_type"], "Full")

    def test_error_callback_conversion_failure_uses_same_diagnostics(self):
        runtime, logger = self.runtime()
        runtime.adapter.snapshot.side_effect = TypeError("invalid passorder")

        runtime.observe_error(object(), "rejected")

        self.assertTrue(runtime.observation_gap)
        self.assertEqual(runtime.background.overflows, 1)
        self.assertEqual(runtime.background.fact_generation, 1)
        fields = logger.call_args[1]
        self.assertEqual(fields["kind"], "error")
        self.assertEqual(fields["phase"], "snapshot")
        self.assertEqual(fields["error_type"], "TypeError")
        self.assertEqual(fields["error_message"], "invalid passorder")
        self.assertIn("TypeError: invalid passorder", fields["traceback"])

    def test_error_callback_message_conversion_failure_is_identified(self):
        runtime, logger = self.runtime()
        runtime.adapter.snapshot.side_effect = lambda value: dict(value)

        class BrokenMessage(object):
            def __str__(self):
                raise RuntimeError("message decode failed")

        runtime.observe_error({"id": "two"}, BrokenMessage())

        self.assertTrue(runtime.observation_gap)
        self.assertEqual(runtime.background.fact_generation, 1)
        self.assertEqual(logger.call_args[1]["phase"], "convert_error")
        self.assertEqual(logger.call_args[1]["error_message"], "message decode failed")
        self.assertTrue(runtime.observations.empty())

    def test_error_callback_success_and_ordinary_callback_preserve_queue(self):
        runtime, logger = self.runtime()
        runtime.adapter.snapshot.side_effect = lambda value: dict(value)

        runtime.observe("order", {"id": "one"})
        runtime.observe_error({"id": "two"}, "rejected")

        self.assertEqual(runtime.observations.get_nowait(), ("order", {"id": "one"}))
        self.assertEqual(runtime.observations.get_nowait(),
                         ("error", {"id": "two", "error_message": "rejected"}))
        self.assertFalse(runtime.observation_gap)
        self.assertEqual(runtime.background.overflows, 0)
        self.assertEqual(runtime.background.fact_generation, 0)
        logger.assert_not_called()

    def test_logger_failure_does_not_escape_callback(self):
        runtime, logger = self.runtime()
        runtime.adapter.snapshot.side_effect = ValueError("native failed")
        logger.side_effect = RuntimeError("logger unavailable")

        runtime.observe("order", object())

        self.assertEqual(runtime.background.fact_generation, 1)

    def test_exception_details_keep_chain_and_bound_text(self):
        try:
            try:
                raise ValueError("inner")
            except ValueError as cause:
                raise RuntimeError("outer " + "x" * 5000) from cause
        except RuntimeError as exc:
            details = qmt_exception_details(exc)

        self.assertEqual(details["code"], "QMT_ERROR")
        self.assertEqual(details["type"], "RuntimeError")
        self.assertLessEqual(len(details["message"]), 4096)
        self.assertLessEqual(len(details["traceback"]), 16384)
        self.assertIn("ValueError: inner", details["traceback"])
        self.assertIn("RuntimeError: outer", details["traceback"])

    def test_traceback_does_not_include_local_payload(self):
        def fail():
            secret_payload = "local-secret-marker"
            raise ValueError("safe error")

        try:
            fail()
        except ValueError as exc:
            details = qmt_exception_details(exc)

        self.assertNotIn("local-secret-marker", details["traceback"])

    def test_exception_details_never_read_source_files(self):
        def fail_here():
            raise LookupError("native read failed")

        try:
            fail_here()
        except LookupError as exc:
            with patch("linecache.getline", side_effect=AssertionError("source read")), \
                    patch("linecache.getlines", side_effect=AssertionError("source read")), \
                    patch("builtins.open", side_effect=AssertionError("file read")):
                details = qmt_exception_details(exc)

        self.assertIn("LookupError: native read failed", details["traceback"])
        self.assertIn("in fail_here", details["traceback"])
        self.assertIn("line ", details["traceback"])

    def test_native_snapshot_field_error_keeps_type_and_field_context(self):
        class BrokenNative(object):
            @property
            def m_strOrderID(self):
                raise UnicodeDecodeError("gbk", b"\xff", 0, 1, "invalid bytes")

        adapter = QmtAdapter({"get_trade_detail_data": lambda *args: [BrokenNative()]},
                             object(), account_id="account-1")
        with self.assertRaises(UnicodeDecodeError) as caught:
            adapter.query("order")

        details = qmt_exception_details(caught.exception)
        self.assertEqual(details["type"], "UnicodeDecodeError")
        self.assertEqual(details["phase"], "snapshot")
        self.assertEqual(details["field"], "m_strOrderID")
        self.assertEqual(details["object_type"], "BrokenNative")

    def test_native_query_error_keeps_type_and_query_phase(self):
        def fail_query(*args):
            raise RuntimeError("QMT query failed")

        adapter = QmtAdapter({"get_trade_detail_data": fail_query},
                             object(), account_id="account-1")
        with self.assertRaises(RuntimeError) as caught:
            adapter.query("deal")

        details = qmt_exception_details(caught.exception)
        self.assertEqual(details["type"], "RuntimeError")
        self.assertEqual(details["message"], "QMT query failed")
        self.assertEqual(details["phase"], "native_query")
        self.assertNotIn("field", details)

    def test_callback_log_includes_native_field_without_changing_phase(self):
        class BrokenNative(object):
            @property
            def m_strOrderID(self):
                raise UnicodeDecodeError("gbk", b"\xff", 0, 1, "invalid bytes")

        runtime, logger = self.runtime()
        runtime.adapter = QmtAdapter({}, object(), account_id="account-1")

        runtime.observe("order", BrokenNative())

        fields = logger.call_args[1]
        self.assertEqual(fields["phase"], "snapshot")
        self.assertEqual(fields["qmt_phase"], "snapshot")
        self.assertEqual(fields["error_field"], "m_strOrderID")
        self.assertEqual(fields["object_type"], "BrokenNative")
        self.assertEqual(fields["error_type"], "UnicodeDecodeError")

    def test_unprintable_native_exception_still_yields_diagnostics(self):
        class BrokenStringError(Exception):
            def __str__(self):
                raise RuntimeError("broken exception formatter")

        try:
            raise BrokenStringError()
        except BrokenStringError as exc:
            details = qmt_exception_details(exc)

        self.assertEqual(details["type"], "BrokenStringError")
        self.assertEqual(details["message"], "<exception message unavailable>")
        self.assertIn("BrokenStringError: <exception message unavailable>", details["traceback"])


if __name__ == "__main__":
    unittest.main()
