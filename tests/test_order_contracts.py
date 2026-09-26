# -*- coding: utf-8 -*-
import unittest

from order_bridge.common import OrderError, fingerprint
from order_bridge.contracts import normalize_order, normalize_cancel, capabilities


class OrderContractTests(unittest.TestCase):
    def base(self, **overrides):
        value = {"client_order_id": "c-1", "account_id": "123", "order_type": "SINGLE",
                 "sizing_type": "QUANTITY",
                 "symbol": "600000.SH", "side": "BUY", "quantity": 100,
                 "price_type": "QUOTE", "quote_type": "LATEST", "execution": {"type": "DIRECT"}}
        value.update(overrides)
        return value

    def test_share_and_etf_units_unchanged(self):
        order = normalize_order(self.base(symbol="510300.SH", quantity=37), "123")
        self.assertEqual(37, order["quantity"])

    def test_stock_amount_decimal_and_etf_rejected(self):
        value = self.base()
        value.pop("quantity")
        value["amount"] = "100.25"
        value["sizing_type"] = "AMOUNT"
        self.assertEqual("100.25", normalize_order(value, "123")["amount"])
        value["symbol"] = "159919.SZ"
        with self.assertRaises(OrderError) as caught:
            normalize_order(value, "123")
        self.assertEqual(422, caught.exception.status)

    def test_basket_duplicate_unknown_and_price_rejected(self):
        member = {"item_id": "a", "symbol": "600000.SH", "side": "BUY", "quantity": 100}
        value = self.base(order_type="BASKET", items=[member, dict(member, item_id="b")])
        for key in ("symbol", "side", "quantity"):
            value.pop(key)
        with self.assertRaises(OrderError):
            normalize_order(value, "123")
        value["items"] = [dict(member, weight=0.5)]
        with self.assertRaises(OrderError):
            normalize_order(value, "123")
        value["items"] = [member]
        value["price_type"] = "LIMIT"
        value.pop("quote_type")
        value["limit_price"] = "10.20"
        with self.assertRaises(OrderError):
            normalize_order(value, "123")

    def test_approved_sizing_and_strategy_fields(self):
        value = self.base(sizing_type="QUANTITY", strategy_id="strategy-A")
        result = normalize_order(value, "123")
        self.assertEqual("strategy-A", result["strategy_id"])
        self.assertEqual("QUANTITY", result["sizing_type"])
        with self.assertRaises(OrderError):
            normalize_order(self.base(sizing_type="AMOUNT"), "123")
        with self.assertRaises(OrderError):
            normalize_order(self.base(strategy_id=" "), "123")
        with self.assertRaises(OrderError):
            normalize_order(self.base(sizing_type=None), "123")

    def test_decimal_double_bounds_and_canonical_fingerprint(self):
        def amount_order(amount):
            value = self.base(sizing_type="AMOUNT", amount=amount)
            value.pop("quantity")
            return normalize_order(value, "123")
        self.assertEqual("5000", amount_order("5000.00")["amount"])
        self.assertEqual(fingerprint(amount_order("5000.00")), fingerprint(amount_order("5000.0")))
        with self.assertRaises(OrderError):
            amount_order("1" + "0" * 1000)
        with self.assertRaises(OrderError):
            amount_order("0." + "0" * 400 + "1")
        value = self.base(price_type="LIMIT", limit_price="9" * 500)
        value.pop("quote_type")
        with self.assertRaises(OrderError):
            normalize_order(value, "123")

    def test_quantity_and_symbol_class_bounds(self):
        with self.assertRaises(OrderError):
            normalize_order(self.base(quantity=2147483648), "123")
        for symbol in ("123018.SZ", "204001.SH", "000300.SH", "399001.SZ", "689001.SH"):
            with self.subTest(symbol=symbol), self.assertRaises(OrderError) as caught:
                normalize_order(self.base(symbol=symbol), "123")
            self.assertEqual(422, caught.exception.status)
        for symbol in ("600000.SH", "688001.SH", "510300.SH", "589030.SH",
                       "000001.SZ", "301001.SZ", "159919.SZ", "920001.BJ"):
            with self.subTest(symbol=symbol):
                self.assertEqual(symbol, normalize_order(self.base(symbol=symbol), "123")["symbol"])

    def test_sliced_enums_and_active_time_trigger(self):
        schema = capabilities()["executions"]["SLICED"]["param_schema"]
        self.assertTrue(all(schema[key]["unit"] and schema[key]["range"] is not None
                            for key in schema))
        params = {key: 0 for key in schema}
        params.update({"MaxOrderCount": 10, "PlaceOrderInterval": 5, "ValidTimeElapse": 60,
                       "VolumeType": 10, "UndealtEntrustRule": 4})
        value = self.base(execution={"type": "SLICED", "mode": "ALGO", "params": params})
        self.assertEqual(10, normalize_order(value, "123")["execution"]["params"]["MaxOrderCount"])
        for key, bad in (("VolumeType", 13), ("UndealtEntrustRule", 41), ("TriggerType", 3)):
            with self.subTest(field=key):
                invalid = dict(params, **{key: bad})
                with self.assertRaises(OrderError):
                    normalize_order(self.base(execution={"type": "SLICED", "mode": "ALGO", "params": invalid}), "123")
        invalid = dict(params, ValidTimeType=1, ValidTimeElapse=0,
                       ValidTimeStart=1770000000, ValidTimeEnd=1760000000)
        with self.assertRaises(OrderError):
            normalize_order(self.base(execution={"type": "SLICED", "mode": "ALGO", "params": invalid}), "123")
        valid = dict(params, ValidTimeType=1, ValidTimeElapse=0,
                     ValidTimeStart=1760000000, ValidTimeEnd=1770000000,
                     UseTrigger=1, TriggerType=1, TriggerPrice=10.5)
        self.assertEqual(1, normalize_order(self.base(execution={"type": "SLICED", "mode": "ALGO", "params": valid}), "123")["execution"]["params"]["TriggerType"])

    def test_market_protection_and_smart_price_separation(self):
        value = self.base(price_type="MARKET", market_type="OPPONENT_BEST")
        value.pop("quote_type")
        with self.assertRaises(OrderError):
            normalize_order(value, "123")
        value["protection_price"] = "10.25"
        self.assertEqual("OPPONENT_BEST", normalize_order(value, "123")["market_type"])
        with self.assertRaises(OrderError):
            normalize_order(dict(value, protection_price="0"), "123")
        value["market_type"] = "IOC"
        with self.assertRaises(OrderError):
            normalize_order(value, "123")
        value["execution"] = {"type": "SMART", "algorithm": "VWAP",
                              "start_at": "2026-09-28T10:00:00+08:00",
                              "end_at": "2026-09-28T14:00:00+08:00", "params": {}}
        with self.assertRaises(OrderError):
            normalize_order(value, "123")
        value.pop("market_type")
        value.pop("protection_price")
        self.assertEqual("SMART", normalize_order(value, "123")["execution"]["type"])

    def test_account_timestamp_and_cancel(self):
        with self.assertRaises(OrderError):
            normalize_order(self.base(account_id="other"), "123")
        with self.assertRaises(OrderError):
            normalize_order(self.base(submit_before="2026-09-28T10:00:00"), "123")
        order = normalize_order(self.base(submit_before="2026-09-28T10:00:00+08:00"), "123")
        self.assertEqual("2026-09-28T02:00:00+00:00", order["submit_before"])
        self.assertEqual("cancel-1", normalize_cancel(
            {"cancel_request_id": "cancel-1", "account_id": "123", "client_order_id": "c-1"}, "123")["cancel_request_id"])
        with self.assertRaises(OrderError):
            normalize_cancel({"client_cancel_id": "old", "account_id": "123", "client_order_id": "c-1"}, "123")
        self.assertIn("required_params", capabilities()["executions"]["SLICED"])


if __name__ == "__main__":
    unittest.main()
