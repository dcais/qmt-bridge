# -*- coding: utf-8 -*-
"""QMT 原生调用日志与参数恒等性测试。Last modified: 2026-09-28。"""
import copy
import unittest

from order_bridge.contracts import normalize_order
from order_bridge.qmt import QmtAdapter


class QmtCallLoggingTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.context = object()
        self.apis = {}
        self.adapter = QmtAdapter(self.apis, self.context, account_id="account-1",
                                  logger=self.log)
        self.adapter.log_context = {"request_id": "http-1", "task_id": "task-1"}

    def log(self, level, message, **fields):
        self.events.append((level, message, fields))

    def native(self, name, result):
        def call(*args):
            self.events.append(("native", name, args))
            return result
        self.apis[name] = call

    def pair(self, name, failed=False):
        entries = [item for item in self.events if item[1] in
                   ("QMT call started", "QMT call returned", "QMT call failed", name)]
        self.assertEqual(["QMT call started", name,
                          "QMT call failed" if failed else "QMT call returned"],
                         [item[1] for item in entries])
        start, native, finish = entries
        self.assertEqual("INFO", start[0])
        self.assertEqual("INFO", finish[0])
        self.assertEqual(name, start[2]["qmt_method"])
        self.assertEqual(start[2]["qmt_call_id"], finish[2]["qmt_call_id"])
        self.assertEqual("http-1", start[2]["request_id"])
        self.assertEqual("account-1", start[2]["account_id"])
        self.assertIn("elapsed_ms", finish[2])
        self.assertEqual(1, len([item for item in self.events if item[0] == "native"]))
        return start[2], native[2], finish[2]

    @staticmethod
    def frozen(function="passorder"):
        result = {"function": function, "opType": 23, "orderType": 1101,
                  "accountid": "account-1", "orderCode": "510300.SH", "prType": 5,
                  "price": 0.0, "volume": 37, "strategyName": "bridge",
                  "quickTrade": 2, "userOrderId": "remark-1"}
        if function == "algo_passorder":
            result["userOrderParam"] = {"OrderType": 1}
        if function == "smart_algo_passorder":
            result.update({"smartAlgoType": "VWAP", "startTime": "10:00:00",
                           "endTime": "14:00:00", "algoParam": {"m_dLimitOverRate": 0.2}})
        return result

    def test_submit_three_paths_log_frozen_parameters_and_raw_zero(self):
        for name in ("passorder", "algo_passorder", "smart_algo_passorder"):
            with self.subTest(name=name):
                self.events[:] = []
                self.native(name, 0)
                frozen = self.frozen(name)
                order = {"account_id": "account-1", "order_id": "order-1",
                         "client_order_id": "client-1", "remark": "remark-1",
                         "resolved_request": frozen}
                self.assertEqual(0, self.adapter.submit(order))
                started, args, returned = self.pair(name)
                self.assertIs(self.context, args[-1])
                self.assertEqual(tuple(frozen[key] for key in
                                       ("opType", "orderType", "accountid", "orderCode",
                                        "prType", "price", "volume", "strategyName",
                                        "quickTrade", "userOrderId")), args[:10])
                if name == "algo_passorder":
                    self.assertEqual(frozen["userOrderParam"], args[10])
                elif name == "smart_algo_passorder":
                    self.assertEqual(("VWAP", "10:00:00", "14:00:00"), args[10:13])
                    self.assertEqual(frozen["algoParam"], args[13])
                self.assertEqual("order-1", started["order_id"])
                self.assertEqual("client-1", started["client_order_id"])
                self.assertEqual({key: value for key, value in frozen.items() if key != "function"},
                                 started["qmt_parameters"])
                self.assertEqual(0, returned["return_value"])
                self.assertNotIn("ContextInfo", str(started))

    def test_cancel_logs_raw_false_and_true_before_bool_conversion(self):
        for result in (False, True):
            with self.subTest(result=result):
                self.events[:] = []
                self.native("cancel_task", result)
                action = {"kind": "CANCEL_TASK", "target_id": "native-task-9",
                          "cancel_request_id": "cancel-1"}
                self.assertIs(result, self.adapter.cancel_action(action))
                started, args, returned = self.pair("cancel_task")
                self.assertEqual(("native-task-9", "account-1", "STOCK", self.context), args)
                self.assertIs(result, returned["return_value"])
                self.assertEqual("cancel-1", started["cancel_request_id"])
                self.assertEqual("native-task-9", started["qmt_task_id"])
                self.assertEqual("native-task-9", started["qmt_parameters"]["target_id"])
        self.events[:] = []
        self.native("cancel", False)
        self.assertFalse(self.adapter.cancel_action({"kind": "CANCEL_ORDER", "target_id": 42}))
        started, args, returned = self.pair("cancel")
        self.assertEqual(42, args[0])
        self.assertIs(False, returned["return_value"])
        self.assertEqual(42, started["qmt_parameters"]["target_id"])
        self.assertEqual(42, started["qmt_order_id"])

    def test_failed_native_call_logs_and_reraises_same_exception(self):
        failure = RuntimeError("QMT unavailable")
        def fail(*args):
            self.events.append(("native", "cancel", args))
            raise failure
        self.apis["cancel"] = fail
        with self.assertRaises(RuntimeError) as caught:
            self.adapter.cancel_action({"kind": "CANCEL_ORDER", "target_id": "x"})
        self.assertIs(failure, caught.exception)
        started, args, failed = self.pair("cancel", failed=True)
        self.assertEqual("RuntimeError", failed["error_type"])
        self.assertEqual("x", started["qmt_parameters"]["target_id"])
        self.assertIs(self.context, args[-1])

    def test_smart_metadata_resolve_and_prepare_step_log_algolist(self):
        request = normalize_order({
            "client_order_id": "client-1", "account_id": "account-1",
            "order_type": "SINGLE", "sizing_type": "QUANTITY", "symbol": "510300.SH",
            "side": "BUY", "quantity": 37, "price_type": "MARKET",
            "execution": {"type": "SMART", "algorithm": "VWAP",
                          "start_at": "2026-09-28T10:00:00+08:00",
                          "end_at": "2026-09-28T14:00:00+08:00", "params": {}}}, "account-1")
        metadata = {"VWAP": [{"key": "m_dLimitOverRate", "dataType": "浮点数",
                              "defaultValue": "20", "unit": "%"}]}
        self.apis["smart_algo_passorder"] = lambda *args: None
        for mode in ("resolve", "prepare_step"):
            with self.subTest(mode=mode):
                self.events[:] = []
                self.native("get_smart_algo_param", metadata)
                if mode == "resolve":
                    self.adapter.resolve(request, "remark-1")
                else:
                    order = {"account_id": "account-1", "order_id": "order-1",
                             "remark": "remark-1", "order_type": "SINGLE", "request": request}
                    self.adapter.prepare_step(order, "SMART")
                started, args, returned = self.pair("get_smart_algo_param")
                self.assertEqual((["VWAP"],), args)
                self.assertEqual({"algoList": ["VWAP"]}, started["qmt_parameters"])
                self.assertEqual("dict", returned["return_type"])
                self.assertEqual(1, returned["return_count"])

    def test_basket_stage_and_legacy_paths_log_each_actual_call(self):
        basket = {"name": "basket-1", "stocks": [
            {"stock": "510300.SH", "weight": 0, "quantity": 37, "optType": 23}]}
        order = {"order_type": "BASKET", "basket_name": "basket-1",
                 "resolved_request": {"orderCode": "basket-1"},
                 "items": [{"symbol": "510300.SH", "quantity": 37, "side": "BUY"}]}
        for mode in ("stage", "legacy"):
            with self.subTest(mode=mode):
                self.events[:] = []
                stored = {}
                def get_basket(name):
                    self.events.append(("native", "get_basket", (name,)))
                    return copy.deepcopy(stored.get(name))
                def set_basket(value):
                    self.events.append(("native", "set_basket", (value,)))
                    stored[value["name"]] = copy.deepcopy(value)
                self.apis.update({"get_basket": get_basket, "set_basket": set_basket})
                if mode == "stage":
                    self.adapter.prepare_step(order, "BASKET_GET")
                    self.adapter.prepare_step(order, "BASKET_SET")
                    self.adapter.prepare_step(order, "BASKET_VERIFY")
                else:
                    self.assertTrue(self.adapter.prepare_basket(order))
                natives = [item for item in self.events if item[0] == "native"]
                self.assertEqual(["get_basket", "set_basket", "get_basket"],
                                 [item[1] for item in natives])
                self.assertEqual(["QMT call started", "get_basket", "QMT call returned",
                                  "QMT call started", "set_basket", "QMT call returned",
                                  "QMT call started", "get_basket", "QMT call returned"],
                                 [item[1] for item in self.events])
                logs = [item for item in self.events if item[0] == "INFO"]
                self.assertEqual(["QMT call started", "QMT call returned"] * 3,
                                 [item[1] for item in logs])
                self.assertEqual(basket, logs[2][2]["qmt_parameters"])
                self.assertEqual(basket, natives[1][2][0])

    def test_current_queries_log_kind_and_single_call(self):
        self.assertNotIn("get_history_trade_detail_data", self.apis)
        for kind in ("order", "deal", "task"):
            with self.subTest(kind=kind):
                self.events[:] = []
                self.native("get_trade_detail_data", [])
                self.assertEqual([], self.adapter.query(kind))
                started, actual, returned = self.pair("get_trade_detail_data")
                self.assertEqual(("account-1", "STOCK", kind), actual)
                self.assertEqual(kind, started["query_kind"])
                self.assertEqual({"accountID": "account-1", "accountType": "STOCK",
                                  "dataType": kind}, started["qmt_parameters"])
                self.assertEqual(0, returned["return_count"])


if __name__ == "__main__":
    unittest.main()
