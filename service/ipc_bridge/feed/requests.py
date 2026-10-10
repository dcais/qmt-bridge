"""Public Feed method whitelist and HTTP query normalization."""

from urllib.parse import parse_qs

from service.ipc_bridge.ipc_protocol.feed_contracts import FeedError


FEED_METHODS = frozenset({
    "get_stock_list_in_sector", "get_sector_list", "get_trading_dates",
    "get_instrument_detail", "get_instrument_details", "get_divid_factors",
    "get_divid_factors_batch", "get_weight_in_index", "get_weights_in_index",
    "get_full_tick", "get_his_index_data", "get_his_contract_list",
    "get_longhubang", "get_market_data_ex", "get_financial_data",
    "download_history_data", "get_download_status",
})


def parse_query_params(query):
    """Keep GET scalars as strings and repeated keys as lists, like HTTP Feed."""
    return {
        name: values[0] if len(values) == 1 else values
        for name, values in parse_qs(query, keep_blank_values=True).items()
    }


def normalize_request(method, params, verb="GET"):
    if not isinstance(method, str) or not isinstance(params, dict):
        raise FeedError(400, "INVALID_REQUEST", "method and params are required")
    if method not in FEED_METHODS:
        raise FeedError(404, "METHOD_NOT_FOUND", "unsupported method: {0}".format(method))
    if verb not in ("GET", "POST"):
        raise FeedError(405, "METHOD_NOT_ALLOWED", "Feed only accepts GET or POST")
    if method == "download_history_data" and verb != "POST":
        raise FeedError(405, "METHOD_NOT_ALLOWED", "download_history_data only accepts POST")
    return dict(params)
