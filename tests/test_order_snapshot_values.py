# -*- coding: utf-8 -*-
"""原生 QMT 字段读取失败时的有界返回诊断；Last modified: 2026-09-28。"""
import json
import unittest
from unittest.mock import Mock

from order_bridge.common import OrderError, qmt_exception_details
from order_bridge.qmt import QmtAdapter


class SnapshotValuesTests(unittest.TestCase):
    def adapter(self, rows, logger=None):
        return QmtAdapter({"get_trade_detail_data": lambda *args: rows},
                          object(), account_id="account-1", logger=logger)

    def test_xt_tag_getter_is_skipped_and_warned_once_with_actual_values(self):
        reads = {"m_a": 0, "m_xtTag": 0, "m_z": 0}

        class COrderDetail(object):
            @property
            def m_a(self):
                reads["m_a"] += 1
                return 123

            @property
            def m_xtTag(self):
                reads["m_xtTag"] += 1
                raise TypeError("No to_python converter for boost::shared_ptr<se::CXtOrderTag>")

            @property
            def m_z(self):
                reads["m_z"] += 1
                return "after failing field"

        logger = Mock()
        adapter = self.adapter([COrderDetail(), COrderDetail()], logger=logger)
        result = adapter.query("order")

        self.assertEqual(result, [{"m_a": 123, "m_z": "after failing field"}] * 2)
        self.assertEqual(reads, {"m_a": 2, "m_xtTag": 0, "m_z": 2})
        warnings = [call for call in logger.call_args_list if call[0][0] == "WARN"]
        self.assertEqual(len(warnings), 1)
        warning = warnings[0]
        self.assertEqual(warning[0], ("WARN", "QMT snapshot internal field excluded"))
        self.assertEqual(warning[1]["account_id"], "account-1")
        self.assertEqual(warning[1]["object_type"], "COrderDetail")
        self.assertEqual(warning[1]["skipped_field"], "m_xtTag")
        diagnostic = warning[1]["return_snapshot"]
        self.assertEqual(diagnostic["object_type"], "COrderDetail")
        self.assertEqual(diagnostic["object_module"], COrderDetail.__module__)
        self.assertEqual(diagnostic["fields_source"], "dir(object)")
        self.assertEqual(diagnostic["field_count"], 3)
        self.assertEqual(diagnostic["fields"], {"m_a": 123, "m_z": "after failing field"})
        self.assertEqual(diagnostic["field_errors"], {})
        skipped = diagnostic["skipped_fields"]["m_xtTag"]
        self.assertEqual(skipped["reason"], "QMT_INTERNAL_NATIVE_FIELD")
        self.assertTrue(skipped["declared_on"].endswith(".COrderDetail"))
        self.assertEqual(skipped["descriptor_type"], "property")
        self.assertFalse(diagnostic["truncated"])
        json.dumps(diagnostic, ensure_ascii=False)

    def test_large_values_are_bounded_without_native_repr(self):
        repr_calls = []

        class Evil(object):
            def __repr__(self):
                repr_calls.append(True)
                raise AssertionError("native repr forbidden")

        class COrderDetail(object):
            m_a_big = "x" * 50000
            m_b_list = list(range(100))

            @property
            def m_unreadableBusinessField(self):
                raise TypeError("business conversion failed")

            @property
            def m_z(self):
                return Evil()

        with self.assertRaises(TypeError) as caught:
            self.adapter([COrderDetail()]).query("order")

        diagnostic = qmt_exception_details(caught.exception)["return_snapshot"]
        self.assertTrue(diagnostic["truncated"])
        self.assertLessEqual(len(diagnostic["fields"]["m_a_big"]), 1024)
        self.assertLessEqual(len(diagnostic["fields"]["m_b_list"]), 16)
        self.assertIn("m_z", diagnostic["field_errors"])
        self.assertEqual(repr_calls, [])
        self.assertLess(len(json.dumps(diagnostic)), 20000)

    def test_current_query_skips_xt_tag_and_keeps_native_trading_day(self):
        reads = []

        class COrderDetail(object):
            m_strOrderID = "order-1"
            m_strTradingDay = "20260926"

            @property
            def m_xtTag(self):
                reads.append("m_xtTag")
                raise TypeError("converter unavailable")

        adapter = self.adapter([COrderDetail()])
        result = adapter.query("order")
        self.assertEqual(result, [{"m_strOrderID": "order-1", "m_strTradingDay": "20260926"}])
        self.assertEqual(reads, [])

    def test_other_native_getter_still_fails_with_bounded_diagnostics(self):
        class COrderDetail(object):
            m_a = 1

            @property
            def m_unreadableBusinessField(self):
                raise TypeError("business getter failed")

            m_z = 2

        with self.assertRaises(TypeError) as caught:
            self.adapter([COrderDetail()]).query("order")
        details = qmt_exception_details(caught.exception)
        self.assertEqual(details["field"], "m_unreadableBusinessField")
        self.assertEqual(details["return_snapshot"]["fields"], {"m_a": 1, "m_z": 2})
        self.assertEqual(details["return_snapshot"]["field_errors"]["m_unreadableBusinessField"]["type"], "TypeError")

    def test_dict_xt_tag_remains_regular_data(self):
        adapter = self.adapter([])
        self.assertEqual(adapter.snapshot({"m_xtTag": "plain value", "m_z": 2}),
                         {"m_xtTag": "plain value", "m_z": 2})

    def test_warning_logger_failure_does_not_break_snapshot(self):
        class COrderDetail(object):
            m_a = 1
            m_xtTag = object()

        logger = Mock(side_effect=RuntimeError("log queue unavailable"))
        adapter = self.adapter([COrderDetail()], logger=logger)
        self.assertEqual(adapter.query("order"), [{"m_a": 1}])
        self.assertEqual(len([call for call in logger.call_args_list if call[0][0] == "WARN"]), 1)

    def test_only_xt_tag_does_not_create_empty_successful_order(self):
        reads = []

        class COrderDetail(object):
            @property
            def m_xtTag(self):
                reads.append(True)
                raise AssertionError("excluded getter must not be called")

        logger = Mock()
        with self.assertRaises(OrderError) as caught:
            self.adapter([COrderDetail()], logger=logger).query("order")
        self.assertEqual(caught.exception.code, "INVALID_QMT_RESULT")
        self.assertEqual(reads, [])
        self.assertFalse(any(call[0][0] == "WARN" for call in logger.call_args_list))

    def test_normal_snapshot_result_is_unchanged(self):
        class COrderDetail(object):
            m_a = 123
            m_z = ["ok", 2]

        result = self.adapter([COrderDetail()]).query("order")
        self.assertEqual(result, [{"m_a": 123, "m_z": ["ok", 2]}])

    def test_many_field_errors_share_one_character_budget(self):
        def fail(self):
            raise ValueError("x" * 1000)

        fields = {"m_{0:02d}".format(index): property(fail) for index in range(40)}
        COrderDetail = type("COrderDetail", (object,), fields)
        with self.assertRaises(ValueError) as caught:
            self.adapter([COrderDetail()]).query("order")

        diagnostic = qmt_exception_details(caught.exception)["return_snapshot"]
        self.assertEqual(diagnostic["field_count"], 40)
        self.assertTrue(diagnostic["truncated"])
        self.assertLess(len(json.dumps(diagnostic)), 20000)

    def test_passorder_fields_are_read_once(self):
        reads = {"accountID": 0, "orderCode": 0, "strategyName": 0}

        class NativePassOrder(object):
            @property
            def accountID(self):
                reads["accountID"] += 1
                return "account-1"

            @property
            def orderCode(self):
                reads["orderCode"] += 1
                return "600000.SH"

            @property
            def strategyName(self):
                reads["strategyName"] += 1
                return "other-strategy"

        result = self.adapter([NativePassOrder()]).query("order")
        self.assertEqual(result[0]["account_id"], "account-1")
        self.assertEqual(reads, {"accountID": 1, "orderCode": 1, "strategyName": 1})


if __name__ == "__main__":
    unittest.main()
