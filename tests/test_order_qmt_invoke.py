# -*- coding: utf-8 -*-
"""QMT 调用日志不改变原生行为；Last modified: 2026-09-26。"""
import json
import unittest
from unittest.mock import Mock

from order_bridge.common import qmt_invoke


class QmtInvokeTests(unittest.TestCase):
    def test_start_precedes_call_and_scalar_returns_are_exact(self):
        for value in (0, False, True, None, "return-code"):
            records = []
            params = {"orderId": "1", "accountId": "account"}
            def log(level, message, **fields):
                records.append((level, message, fields))
            def native(argument):
                self.assertEqual(records[0][1], "QMT call started")
                self.assertEqual(argument, "1")
                params["orderId"] = "changed"
                return value
            returned = qmt_invoke(log, "cancel", native, args=("1",), parameters=params,
                                  correlation={"attempt_id": "attempt", "cancel_request_id": "cancel"})
            self.assertIs(returned, value)
            self.assertEqual([r[1] for r in records], ["QMT call started", "QMT call returned"])
            self.assertTrue(all(r[0] == "INFO" for r in records))
            before, after = records[0][2], records[1][2]
            self.assertEqual(before["qmt_call_id"], after["qmt_call_id"])
            self.assertEqual(before["qmt_parameters"]["orderId"], "1")
            self.assertEqual(after["return_value"], value)
            self.assertEqual(after["return_type"], type(value).__name__)
            self.assertEqual(after["attempt_id"], "attempt")
            self.assertGreaterEqual(after["elapsed_ms"], 0)

    def test_native_objects_are_never_reflected_or_serialized(self):
        class Native(object):
            def __repr__(self):
                raise AssertionError("repr must not be called")
            def __getattribute__(self, name):
                raise AssertionError("native attribute must not be read")
        value = Native()
        log = Mock()
        returned = qmt_invoke(log, "get_trade_detail_data", lambda: [value],
                              parameters={"ContextInfo": value, "password": "secret-marker"})
        self.assertIs(returned[0], value)
        fields = log.call_args[1]
        self.assertEqual(fields["return_type"], "list")
        self.assertEqual(fields["return_count"], 1)
        self.assertNotIn("return_value", fields)
        text = json.dumps([call[1] for call in log.call_args_list])
        self.assertNotIn("secret-marker", text)

    def test_exception_preserves_identity_and_is_not_retried(self):
        error = TypeError("native failed")
        native = Mock(side_effect=error)
        log = Mock()
        with self.assertRaises(TypeError) as caught:
            qmt_invoke(log, "passorder", native)
        self.assertIs(caught.exception, error)
        native.assert_called_once()
        self.assertEqual(log.call_args[0], ("INFO", "QMT call failed"))
        self.assertEqual(log.call_args[1]["error_type"], "TypeError")
        self.assertIn("native failed", log.call_args[1]["error_message"])
        self.assertEqual(log.call_args_list[0][1]["qmt_call_id"], log.call_args[1]["qmt_call_id"])

    def test_logger_failure_does_not_prevent_native_call_or_return(self):
        native = Mock(return_value=0)
        returned = qmt_invoke(Mock(side_effect=RuntimeError("log failed")), "passorder", native)
        self.assertEqual(returned, 0)
        native.assert_called_once()

    def test_parameters_are_bounded(self):
        log = Mock()
        qmt_invoke(log, "set_basket", lambda: None, parameters={"stocks": ["x" * 10000] * 1000})
        fields = log.call_args_list[0][1]
        self.assertTrue(fields["parameters_truncated"])
        self.assertLess(len(json.dumps(fields)), 22000)


if __name__ == "__main__":
    unittest.main()
