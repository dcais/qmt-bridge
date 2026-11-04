"""Read-only amount views on both order runtimes."""
import copy
from unittest.mock import Mock

import pytest

from order_bridge.runtime import OrderRuntime as StrategyRuntime
from service.ipc_bridge.order.runtime import OrderRuntime as ServiceRuntime
from test_order_state import document


@pytest.mark.parametrize("runtime_type", [ServiceRuntime, StrategyRuntime])
@pytest.mark.parametrize("stored, source", [
    (["306552.924", "98835.876"], "callback"),
    (["306552.92", "98835.88"], "query"),
])
def test_order_and_list_cent_view(runtime_type, stored, source):
    doc = document()
    doc["fills"] = [dict(item_id="single", amount=value, source=source, raw={"original": value})
                    for value in stored]
    original = copy.deepcopy(doc)
    repo = Mock()
    repo.get_order.return_value = doc
    repo.list_orders.side_effect = lambda **kwargs: {"orders": [doc]}
    options = {"adapter": Mock()} if runtime_type is ServiceRuntime else {}
    runtime = runtime_type({}, None, "acct", repository=repo, **options)
    for method, params in [("order", {"client_order_id": "client"}), ("orders", {})]:
        def read(**options):
            _, result = runtime.handle(method, dict(params, **options), "GET")
            return result if method == "order" else result["orders"][0]
        view = read(amount_view="cent")
        assert [f["amount"] for f in view["fills"]] == ["306552.92", "98835.88"]
        assert [f["raw"]["original"] for f in view["fills"]] == stored
        assert view["filled_amount"] == view["items"][0]["filled_amount"] == "405388.80"
        assert [f["amount"] for f in read()["fills"]] == stored
        with pytest.raises(Exception) as error:
            read(amount_view="unknown")
        assert error.value.code == "INVALID_PARAMS"
    assert doc == original


def test_basket_rounds_each_fill_before_summing():
    doc = document(basket=True)
    doc["fills"] = [dict(item_id=i, amount="1.005") for i in ["a", "a", "b"]]
    view = ServiceRuntime._query_order(doc, True)
    assert [r["filled_amount"] for r in view["items"]] == ["2.02", "1.01"]
    assert view["filled_amount"] == "3.03"
    doc["items"][1]["symbol"] = "830001.BJ"
    assert ServiceRuntime._query_order(doc, True) == ServiceRuntime._query_order(doc, False)
