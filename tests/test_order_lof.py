"""Offline LOF contracts and exact native DIRECT/LIMIT quantity mapping."""
import importlib.util
import json
from pathlib import Path
import unittest

from order_bridge import contracts as legacy
from service.ipc_bridge.order import contracts as ipc
from service.ipc_bridge.qmt_runtime.order.adapter import QmtAdapter


ROOT = Path(__file__).resolve().parents[1]


def request(symbol="501018.SH", **updates):
    value = {"client_order_id": "lof-regression", "account_id": "123", "order_type": "SINGLE",
             "sizing_type": "QUANTITY", "symbol": symbol, "side": "BUY", "quantity": 10000,
             "price_type": "LIMIT", "limit_price": "2.302", "execution": {"type": "DIRECT"}}
    value.update(updates)
    return value


class LofOrderTests(unittest.TestCase):
    def test_complete_official_catalog_is_consistent_and_generated(self):
        snapshot = json.loads((ROOT / "docs/data/lof_catalog.json").read_text(encoding="utf-8"))
        symbols = {row["symbol"] for row in snapshot["records"]}
        self.assertEqual(symbols, ipc.LOF_SYMBOLS)
        self.assertEqual(symbols, legacy.LOF_SYMBOLS)
        self.assertFalse(snapshot["listing_status_verified"])
        for symbol in symbols:
            for contract in (ipc, legacy):
                with self.subTest(contract=contract.__name__, symbol=symbol):
                    self.assertEqual("LOF", contract._contracts_instrument_kind(symbol))
                    self.assertEqual(symbol, contract.normalize_order(request(symbol), "123")["symbol"])
        spec = importlib.util.spec_from_file_location("update_lof_catalog", ROOT / "tools/update_lof_catalog.py")
        tool = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tool)
        self.assertEqual(0, tool.main(["--check"]))

    def test_direct_limit_quantity_maps_to_native_passorder_without_conversion(self):
        calls = []
        adapter = QmtAdapter({"passorder": lambda *args: calls.append(args)}, object(), "123")
        for symbol in ("501018.SH", "161226.SZ"):
            for side, operation in (("BUY", 23), ("SELL", 24)):
                with self.subTest(symbol=symbol, side=side):
                    normalized = ipc.normalize_order(request(symbol, side=side), "123")
                    frozen = adapter.resolve(normalized, "lof-test")
                    self.assertEqual("passorder", frozen["function"])
                    self.assertEqual((operation, 1101, symbol, 11, 2.302, 10000),
                        tuple(frozen[k] for k in ("opType", "orderType", "orderCode", "prType", "price", "volume")))
                    adapter.submit({"account_id": "123", "order_type": "SINGLE", "remark": "lof-test",
                                    "resolved_request": frozen})
                    self.assertEqual((operation, 1101, "123", symbol, 11, 2.302, 10000), calls[-1][:7])

    def test_lof_units_and_basket_are_not_lots(self):
        for contract in (ipc, legacy):
            self.assertEqual(37, contract.normalize_order(request(quantity=37), "123")["quantity"])
            basket = request(order_type="BASKET", price_type="QUOTE", quote_type="LATEST",
                items=[{"item_id": "sh", "symbol": "501018.SH", "side": "BUY", "quantity": 10000},
                       {"item_id": "sz", "symbol": "161226.SZ", "side": "SELL", "quantity": 37}])
            for key in ("symbol", "side", "quantity", "limit_price"):
                basket.pop(key)
            result = contract.normalize_order(basket, "123")
            self.assertEqual([10000, 37], [item["quantity"] for item in result["items"]])

    def test_amount_unknown_non_lof_and_wrong_exchange_are_rejected(self):
        for contract in (ipc, legacy):
            for symbol in ("501018.SH", "161226.SZ"):
                value = request(symbol, sizing_type="AMOUNT", amount="23020")
                value.pop("quantity")
                with self.subTest(contract=contract.__name__, symbol=symbol), self.assertRaises(contract.OrderError) as caught:
                    contract.normalize_order(value, "123")
                self.assertEqual(422, caught.exception.status)
                self.assertIn("amount", str(caught.exception))
            for symbol in ("160000.SZ", "169999.SZ", "501999.SH", "501018.SZ", "161226.SH",
                           "508000.SH", "123018.SZ", "204001.SH", "000300.SH", "399001.SZ"):
                with self.subTest(contract=contract.__name__, symbol=symbol), self.assertRaises(contract.OrderError) as caught:
                    contract.normalize_order(request(symbol), "123")
                self.assertEqual(422, caught.exception.status)

    def test_lof_does_not_relax_limit_or_direct_contract(self):
        for contract in (ipc, legacy):
            for updates in ({"quote_type": "LATEST"}, {"execution": {"type": "DIRECT", "algorithm": "VWAP"}},
                            {"limit_price": "0"}, {"quantity": 0}):
                with self.subTest(contract=contract.__name__, updates=updates), self.assertRaises(contract.OrderError):
                    contract.normalize_order(request(**updates), "123")


if __name__ == "__main__":
    unittest.main()
