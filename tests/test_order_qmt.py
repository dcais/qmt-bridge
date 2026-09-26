# -*- coding: utf-8 -*-
import copy
import json
import unittest

from order_bridge.common import OrderError
from order_bridge.contracts import normalize_order, capabilities
from order_bridge.qmt import QmtAdapter
from order_bridge.state import observation_identifiers


class QmtAdapterTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.context = object()
        self.apis = {}
        for name in ("passorder", "algo_passorder", "smart_algo_passorder", "cancel", "cancel_task"):
            def function(*args, **kwargs):
                self.calls.append((name, args, kwargs))
                return True
            self.apis[name] = self._recorder(name)
        self.adapter = QmtAdapter(self.apis, self.context, "123")

    def _recorder(self, name):
        def record(*args):
            self.calls.append((name, args))
            return True
        return record

    def order(self, request, remark="rb123", basket=False):
        return {"account_id": "123", "order_type": "BASKET" if basket else "SINGLE",
                "remark": remark, "basket_name": remark if basket else None,
                "items": request.get("items", []), "resolved_request": self.adapter.resolve(request, remark)}

    def direct_request(self, **updates):
        value = {"client_order_id": "c1", "account_id": "123", "order_type": "SINGLE",
                 "sizing_type": "QUANTITY",
                 "symbol": "510300.SH", "side": "BUY", "quantity": 37,
                 "price_type": "QUOTE", "quote_type": "LATEST", "execution": {"type": "DIRECT"}}
        value.update(updates)
        if value["price_type"] != "QUOTE":
            value.pop("quote_type", None)
        if "amount" in updates:
            value.pop("quantity", None)
            value["sizing_type"] = "AMOUNT"
        return normalize_order(value, "123")

    def phase_order(self, request, remark="rb123"):
        basket = request["order_type"] == "BASKET"
        return {"account_id": "123", "order_type": request["order_type"],
                "remark": remark, "basket_name": remark if basket else None,
                "items": request.get("items", []), "request": request}

    def phase(self, order, stage="RESOLVE"):
        before = len(self.calls)
        result = self.adapter.prepare_step(order, stage)
        self.assertLessEqual(len(self.calls) - before, 1)
        self.assertEqual(set(("stage", "updates")), set(result))
        json.dumps(result)
        order.update(result["updates"])
        return result["stage"]

    def test_prepare_direct_and_sliced_are_pure_and_frozen(self):
        direct = self.direct_request()
        order = self.phase_order(direct)
        self.assertIsNone(self.phase(order))
        self.assertEqual([], self.calls)
        self.assertEqual(self.adapter.resolve(direct, "rb123"), order["resolved_request"])
        self.adapter.submit(order)
        self.assertEqual("passorder", self.calls[-1][0])
        params = {key: 0 for key in capabilities()["executions"]["SLICED"]["required_params"]}
        params.update({"MaxOrderCount": 20, "PlaceOrderInterval": 5,
                       "VolumeType": 10, "VolumeRate": 0.2, "ValidTimeElapse": 60})
        sliced = self.direct_request(execution={"type": "SLICED", "mode": "RANDOM", "params": params})
        order = self.phase_order(sliced, "s1")
        self.calls[:] = []
        self.assertIsNone(self.phase(order))
        self.assertEqual([], self.calls)
        self.assertEqual(self.adapter.resolve(sliced, "s1"), order["resolved_request"])
        self.adapter.submit(order)
        self.assertEqual("algo_passorder", self.calls[-1][0])

    def test_prepare_smart_uses_one_metadata_call_and_frozen_values(self):
        def smart_metadata(names):
            self.calls.append(("get_smart_algo_param", names))
            return {"VWAP": [{"key": "m_dLimitOverRate", "dataType": "浮点数",
                              "valueRange": "0.00-100.00", "defaultValue": "20.00", "unit": "%"}]}
        self.apis["get_smart_algo_param"] = smart_metadata
        request = self.direct_request(price_type="MARKET", execution={
            "type": "SMART", "algorithm": "VWAP", "start_at": "2026-09-28T10:00:00+08:00",
            "end_at": "2026-09-28T14:00:00+08:00", "params": {}})
        order = self.phase_order(request)
        self.assertEqual("SMART", self.phase(order))
        self.assertEqual([], self.calls)
        self.assertNotIn("resolved_request", order)
        self.assertIsNone(self.phase(order, "SMART"))
        self.assertEqual(["get_smart_algo_param"], [call[0] for call in self.calls])
        self.assertEqual(0.2, order["resolved_request"]["algoParam"]["m_dLimitOverRate"])
        self.assertEqual("rb123", order["resolved_request"]["algoParam"]["m_strCmdRemark"])
        self.adapter.submit(order)
        self.assertEqual("smart_algo_passorder", self.calls[-1][0])

    def test_prepare_basket_created_reused_and_conflict(self):
        request = normalize_order({"client_order_id": "b1", "account_id": "123",
                                   "sizing_type": "QUANTITY", "order_type": "BASKET",
                                   "items": [{"item_id": "x", "symbol": "600000.SH",
                                              "side": "BUY", "quantity": 100}],
                                   "price_type": "QUOTE", "quote_type": "LATEST",
                                   "execution": {"type": "DIRECT"}}, "123")
        store = {}
        def get_basket(name):
            self.calls.append(("get_basket", name))
            return copy.deepcopy(store.get(name))
        def set_basket(basket):
            self.calls.append(("set_basket", basket))
            store[basket["name"]] = copy.deepcopy(basket)
        self.apis.update({"get_basket": get_basket, "set_basket": set_basket})
        order = self.phase_order(request)
        stage = self.phase(order)
        self.assertEqual("BASKET_GET", stage)
        stage = self.phase(order, stage)
        self.assertEqual("BASKET_SET", stage)
        stage = self.phase(order, stage)
        self.assertEqual("BASKET_VERIFY", stage)
        self.assertIsNone(self.phase(order, stage))
        self.assertEqual(["get_basket", "set_basket", "get_basket"], [call[0] for call in self.calls])
        self.calls[:] = []
        restarted = self.phase_order(request)
        self.assertEqual("BASKET_GET", self.phase(restarted))
        self.assertIsNone(self.phase(restarted, "BASKET_GET"))
        self.assertEqual(["get_basket"], [call[0] for call in self.calls])
        self.assertEqual(order["resolved_request"], restarted["resolved_request"])
        store["rb123"]["stocks"][0]["quantity"] = 999
        self.calls[:] = []
        wrong = self.phase_order(request)
        self.assertEqual("BASKET_GET", self.phase(wrong))
        with self.assertRaises(OrderError) as caught:
            self.adapter.prepare_step(wrong, "BASKET_GET")
        self.assertEqual("BASKET_READBACK_MISMATCH", caught.exception.code)
        self.assertEqual(["get_basket"], [call[0] for call in self.calls])

    def test_prepare_basket_verify_rejects_wrong_readback(self):
        request = normalize_order({"client_order_id": "b1", "account_id": "123",
                                   "sizing_type": "QUANTITY", "order_type": "BASKET",
                                   "items": [{"item_id": "x", "symbol": "600000.SH",
                                              "side": "BUY", "quantity": 100}],
                                   "price_type": "QUOTE", "quote_type": "LATEST",
                                   "execution": {"type": "DIRECT"}}, "123")
        order = self.phase_order(request)
        self.phase(order)
        self.apis["get_basket"] = lambda name: {"name": name, "stocks": []}
        with self.assertRaises(OrderError):
            self.adapter.prepare_step(order, "BASKET_VERIFY")

    def test_direct_frozen_arguments(self):
        request = self.direct_request()
        order = self.order(request)
        order["request"] = dict(request, quantity=900)
        self.adapter.submit(order)
        name, args = self.calls[-1]
        self.assertEqual("passorder", name)
        self.assertEqual((23, 1101, "123", "510300.SH", 5, 0.0, 37), args[:7])
        self.assertIs(self.context, args[-1])

    def test_amount_is_1102_and_float_cny(self):
        request = self.direct_request(symbol="600000.SH", amount="101.25")
        frozen = self.adapter.resolve(request, "r")
        self.assertEqual(1102, frozen["orderType"])
        self.assertEqual(101.25, frozen["volume"])

    def test_market_business_enum_maps_by_exchange(self):
        sh = self.direct_request(symbol="600000.SH", price_type="MARKET",
                                 market_type="BEST5_IOC", protection_price="10.50")
        self.assertEqual(42, self.adapter.resolve(sh, "sh1")["prType"])
        sz = self.direct_request(symbol="000001.SZ", price_type="MARKET", market_type="BEST5_IOC")
        self.assertEqual(47, self.adapter.resolve(sz, "sz1")["prType"])

    def test_missing_submission_api_rejected_at_resolve(self):
        self.apis.pop("passorder")
        with self.assertRaises(OrderError) as caught:
            self.adapter.resolve(self.direct_request(), "r1")
        self.assertEqual(501, caught.exception.status)

    def test_smart_defaults_percent_and_protected_remark(self):
        self.apis["get_smart_algo_param"] = lambda names: {"VWAP": [
            {"key": "m_dLimitOverRate", "dataType": "浮点数", "valueRange": "0.00-100.00",
             "defaultValue": "20.00", "unit": "%"},
            {"key": "m_nStopTradeForOwnHiLow", "dataType": "整数", "enumName": "无,控制",
             "enumValue": "0,1", "defaultValue": "控制"},
            {"key": "m_strCmdRemark", "dataType": "字符串", "defaultValue": ""}]}
        execution = {"type": "SMART", "algorithm": "VWAP",
                     "start_at": "2026-09-28T10:00:00+08:00",
                     "end_at": "2026-09-28T14:00:00+08:00", "params": {"m_dLimitOverRate": 0.25}}
        request = self.direct_request(price_type="MARKET", execution=execution)
        frozen = self.adapter.resolve(request, "r1")
        self.assertEqual(12, frozen["prType"])
        self.assertEqual(0.25, frozen["algoParam"]["m_dLimitOverRate"])
        self.assertEqual(1, frozen["algoParam"]["m_nStopTradeForOwnHiLow"])
        self.assertEqual("r1", frozen["algoParam"]["m_strCmdRemark"])
        self.assertEqual("10:00:00", frozen["startTime"])
        order = self.order(request, "r1")
        self.adapter.submit(order)
        name, args = self.calls[-1]
        self.assertEqual("smart_algo_passorder", name)
        self.assertEqual(12, args[4])
        self.assertEqual(0.25, args[-2]["m_dLimitOverRate"])

    def test_sliced_full_params_and_frozen_dispatch(self):
        params = {key: 0 for key in capabilities()["executions"]["SLICED"]["required_params"]}
        params.update({"MaxOrderCount": 20, "PlaceOrderInterval": 5,
                       "VolumeType": 10, "VolumeRate": 0.2, "ValidTimeElapse": 60})
        request = self.direct_request(execution={"type": "SLICED", "mode": "RANDOM", "params": params})
        frozen = self.adapter.resolve(request, "s1")
        self.assertEqual("algo_passorder", frozen["function"])
        self.assertEqual(2, frozen["userOrderParam"]["OrderType"])
        self.assertEqual(5, frozen["userOrderParam"]["PriceType"])
        order = self.order(request, "s1")
        self.adapter.submit(order)
        self.assertEqual("algo_passorder", self.calls[-1][0])
        self.assertEqual(37, self.calls[-1][1][6])
        self.assertEqual(2, self.calls[-1][1][-2]["OrderType"])

    def test_smart_native_basket_market(self):
        self.apis["get_smart_algo_param"] = lambda names: {"VWAP": [
            {"key": "m_dLimitOverRate", "dataType": "浮点数", "valueRange": "0.00-100.00",
             "defaultValue": "20.00", "unit": "%"}]}
        execution = {"type": "SMART", "algorithm": "VWAP",
                     "start_at": "2026-09-28T10:00:00+08:00",
                     "end_at": "2026-09-28T14:00:00+08:00", "params": {}}
        request = normalize_order({"client_order_id": "b1", "account_id": "123", "order_type": "BASKET",
                                   "sizing_type": "QUANTITY",
                                   "items": [{"item_id": "x", "symbol": "600000.SH", "side": "BUY", "quantity": 100}],
                                   "price_type": "MARKET", "execution": execution}, "123")
        order = self.order(request, basket=True)
        self.adapter.submit(order)
        self.assertEqual("smart_algo_passorder", self.calls[-1][0])
        self.assertEqual((35, 2101, "123", "rb123", 12, 0.0, 1), self.calls[-1][1][:7])
        self.assertEqual(0.2, self.calls[-1][1][-2]["m_dLimitOverRate"])

    def test_basket_readback_then_native_submit(self):
        rows = [{"item_id": "x", "symbol": "600000.SH", "side": "BUY", "quantity": 100},
                {"item_id": "y", "symbol": "600001.SH", "side": "SELL", "quantity": 200}]
        request = normalize_order({"client_order_id": "b1", "account_id": "123",
                                   "sizing_type": "QUANTITY",
                                   "order_type": "BASKET", "items": rows, "price_type": "QUOTE",
                                   "quote_type": "LATEST", "execution": {"type": "DIRECT"}}, "123")
        order = self.order(request, basket=True)
        store = {}
        self.apis["get_basket"] = lambda name: store.get(name)
        self.apis["set_basket"] = lambda basket: store.update({basket["name"]: copy.deepcopy(basket)})
        self.assertTrue(self.adapter.prepare_basket(order))
        self.assertFalse(self.adapter.prepare_basket(order))
        self.assertEqual([23, 24], [item["optType"] for item in store["rb123"]["stocks"]])
        self.adapter.submit(order)
        self.assertEqual((35, 2101, "123", "rb123", 5, 0.0, 1), self.calls[-1][1][:7])
        store["rb123"]["stocks"][0]["quantity"] = 999
        with self.assertRaises(OrderError):
            self.adapter.prepare_basket(order)

    def test_cancel_query_and_snapshot(self):
        self.assertTrue(self.adapter.cancel_action({"kind": "CANCEL_TASK", "target_id": "9",
                                                    "cancel_request_id": "c1", "attempt_id": "a1"}))
        self.assertEqual(("9", "123", "STOCK", self.context), self.calls[-1][1])
        self.assertTrue(self.adapter.cancel_action({"kind": "CANCEL_ORDER", "target_id": "o1"}))
        self.assertEqual("cancel", self.calls[-1][0])
        with self.assertRaises(OrderError):
            self.adapter.cancel_action({"kind": "task", "task_id": "9"})
        class Row(object):
            m_strRemark = "r1"
            m_nVolume = 10
        self.apis["get_trade_detail_data"] = lambda *args: [Row()]
        self.assertEqual("r1", self.adapter.query("order")[0]["m_strRemark"])
        with self.assertRaises(OrderError):
            self.adapter.query("task", "20260927", "20260928")
        self.apis["get_history_trade_detail_data"] = lambda *args: [("20260928", Row())]
        history = self.adapter.query("deal", "20260927", "20260928")[0]
        self.assertEqual("r1", history["m_strRemark"])
        self.assertEqual("20260928", history["m_strTradingDay"])
        self.apis["get_history_trade_detail_data"] = lambda *args: [("20260928", {
            "m_strTradingDay": "20260927", "m_strRemark": "r1"})]
        self.assertEqual("20260927", self.adapter.query("deal", "20260927", "20260928")[0]["m_strTradingDay"])
        self.apis["get_history_trade_detail_data"] = lambda *args: [("20260928", Row(), Row())]
        self.assertEqual(2, len(self.adapter.query("deal", "20260927", "20260928")))
        self.apis["get_history_trade_detail_data"] = lambda *args: [("20260928", [Row(), Row()])]
        self.assertEqual(2, len(self.adapter.query("deal", "20260927", "20260928")))
        self.apis["get_history_trade_detail_data"] = lambda *args: [("bad-date", Row())]
        with self.assertRaises(OrderError):
            self.adapter.query("deal", "20260927", "20260928")

    def test_passorder_error_callback_snapshot_correlates_remark(self):
        class PassorderArguments(object):
            accountID = "123"
            currentTime = 0
            formulaName = ""
            modelPrice = 0.0
            modelVolume = 100.0
            opType = 23
            orderCode = "SZ000001"
            orderType = 1101
            prType = 11
            strategyName = "qmt-bridge-order_&&&_rb123"
        raw = self.adapter.snapshot(PassorderArguments())
        self.assertEqual("123", raw["accountID"])
        self.assertEqual("123", raw["account_id"])
        self.assertEqual("rb123", raw["remark"])
        self.assertEqual("SZ000001", raw["orderCode"])
        self.assertEqual(100.0, raw["modelVolume"])
        ids = observation_identifiers("error", raw)
        self.assertEqual("rb123", ids["remark"])
        self.assertEqual("123", ids["account_id"])
        PassorderArguments.strategyName = "other_&&&_rb123"
        self.assertNotIn("remark", self.adapter.snapshot(PassorderArguments()))
        with self.assertRaises(OrderError):
            self.adapter.snapshot({"strategyName": "qmt-bridge-order_&&&_rb123"})


if __name__ == "__main__":
    unittest.main()
