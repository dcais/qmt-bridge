"""Pure Feed parameters, limits and errors shared by both processes.

This module is also inlined into the QMT 3.6 standalone strategy.
It performs no file, network or database I/O.
"""

import datetime as dt
import math


MAX_TRADING_DATES_COUNT = 10000


MAX_STOCKCODE_LENGTH = 64


MAX_DIVID_FACTOR_RECORDS = 1000


MAX_FINANCIAL_FIELDS = 16


MAX_FINANCIAL_STOCKS = 20


MAX_FINANCIAL_CELLS = 20000


MAX_FINANCIAL_NAME_LENGTH = 128


MAX_FINANCIAL_BARPOS = 10000000


FINANCIAL_REPORT_TYPES = {"announce_time", "report_time"}


MAX_MARKET_DATA_FIELDS = 32


MAX_MARKET_DATA_STOCKS = 20


MAX_MARKET_DATA_COUNT = 1000


MAX_MARKET_DATA_CELLS = 20000


MAX_MARKET_DATA_COLUMNS = 128


MAX_MARKET_DATA_JSON_VALUES = 50000


MAX_FULL_TICK_STOCKS = 20


MAX_FULL_TICK_JSON_VALUES = 50000


MAX_BATCH_STOCKS = 20


MAX_BATCH_INSTRUMENT_DETAIL_JSON_VALUES = 50000


MAX_BATCH_DIVID_FACTOR_RECORDS = (
    MAX_BATCH_STOCKS * MAX_DIVID_FACTOR_RECORDS
)


MAX_HIS_INDEX_JSON_VALUES = 50000


MAX_HIS_CONTRACTS = 50000


MAX_LONGHUBANG_STOCKS = 20


MAX_LONGHUBANG_DATE_DAYS = 3660


MAX_LONGHUBANG_STOCK_DAYS = 3660


MAX_LONGHUBANG_ROWS = 1000


MAX_LONGHUBANG_COLUMNS = 32


MAX_LONGHUBANG_CELLS = 20000


MAX_LONGHUBANG_JSON_VALUES = 50000


MAX_QMT_TABLE_ROWS = 1000


MAX_QMT_TABLE_COLUMNS = 64


MAX_QMT_TABLE_CELLS = 20000


MAX_DOWNLOAD_TASKS = 1000


DOWNLOAD_HISTORY_PERIODS = {"tick", "1m", "5m", "1d"}


MARKET_DATA_BAR_FIELDS = (
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "settle",
    "openInterest",
    "preClose",
    "suspendFlag",
)


MARKET_DATA_TICK_FIELDS = (
    "time",
    "lastPrice",
    "lastClose",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "settle",
    "openInterest",
    "stockStatus",
)


MARKET_DATA_BAR_PERIODS = {
    "1m",
    "3m",
    "5m",
    "10m",
    "15m",
    "30m",
    "60m",
    "1h",
    "2h",
    "3h",
    "4h",
    "1d",
    "2d",
    "3d",
    "5d",
    "1w",
    "1mon",
    "1q",
    "1hy",
    "1y",
}


MARKET_DATA_LEVEL2_FIELDS = {
    "l2quote": (
        "time",
        "lastPrice",
        "volume",
        "amount",
        "askPrice",
        "askVol",
        "bidPrice",
        "bidVol",
    ),
    "l2quoteaux": (
        "time",
        "avgBidPrice",
        "totalBidQuantity",
        "avgOffPrice",
        "totalOffQuantity",
    ),
    "l2order": (
        "time",
        "price",
        "volume",
        "entrustNo",
        "entrustType",
        "entrustDirection",
    ),
    "l2transaction": (
        "time",
        "price",
        "volume",
        "amount",
        "tradeIndex",
        "buyNo",
        "sellNo",
        "tradeType",
        "tradeFlag",
    ),
    "l2transactioncount": (
        "time",
        "bidNumber",
        "offNumber",
        "ddx",
        "ddy",
        "ddz",
        "netOrder",
        "netWithdraw",
    ),
    "l2orderqueue": (
        "time",
        "bidLevelPrice",
        "bidLevelVolume",
        "offerLevelPrice",
        "offerLevelVolume",
    ),
}


MARKET_DATA_DIVIDEND_TYPES = {
    "follow",
    "none",
    "front",
    "back",
    "front_ratio",
    "back_ratio",
}


ONE_JOB_PER_TICK_METHODS = {
    "download_history_data",
    "get_divid_factors",
    "get_full_tick",
    "get_his_index_data",
    "get_his_contract_list",
    "get_longhubang",
    "get_market_data_ex",
}


BATCH_METHODS = {
    "get_divid_factors_batch",
    "get_instrument_details",
    "get_weights_in_index",
}


TRADING_DATE_PERIODS = {
    "1d",
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "1w",
    "1mon",
    "1q",
    "1hy",
    "1y",
}


class FeedError(Exception):
    def __init__(self, status, code, message):
        Exception.__init__(self, message)
        self.status = status
        self.code = code
        self.message = message


def normalize_stockcode(value):
    if not isinstance(value, str):
        return None
    stockcode = value.strip()
    if (
        not stockcode
        or len(stockcode) > MAX_STOCKCODE_LENGTH
        or stockcode.count(".") != 1
        or any(not part for part in stockcode.split("."))
        or any(
            char.isspace() or not char.isprintable()
            for char in stockcode
        )
    ):
        return None
    return stockcode


def normalize_market(value):
    if not isinstance(value, str):
        return None
    market = value.strip().upper()
    if (
        not market
        or len(market) > 16
        or any(
            not ("A" <= char <= "Z" or "0" <= char <= "9")
            for char in market
        )
    ):
        return None
    return market


def normalize_boolean(value, name, default):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text == "true":
            return True
        if text == "false":
            return False
    raise FeedError(
        400,
        "INVALID_PARAMS",
        "{0} must be true or false".format(name),
    )


def normalize_download_history_time(value, name):
    if value is None:
        return "", None
    if not isinstance(value, str):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "{0} must be a string".format(name),
        )
    value = value.strip()
    if not value:
        return "", None
    if (
        len(value) not in (8, 14)
        or not all("0" <= char <= "9" for char in value)
    ):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "{0} must use YYYYMMDD or YYYYMMDDHHMMSS".format(name),
        )
    date_format = "%Y%m%d" if len(value) == 8 else "%Y%m%d%H%M%S"
    try:
        parsed = dt.datetime.strptime(value, date_format)
    except ValueError:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "{0} must be a valid date or datetime".format(name),
        )
    return value, parsed


def normalize_download_history_params(params):
    allowed_params = {
        "stockcode",
        "period",
        "startTime",
        "endTime",
        "incrementally",
    }
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported download history params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )

    stockcode = normalize_stockcode(params.get("stockcode"))
    if stockcode is None:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "stockcode must use stock.market format",
        )

    period = params.get("period")
    if not isinstance(period, str):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "period must be tick, 1m, 5m, or 1d",
        )
    period = period.strip()
    if period not in DOWNLOAD_HISTORY_PERIODS:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "period must be tick, 1m, 5m, or 1d",
        )

    start_time, start_value = normalize_download_history_time(
        params.get("startTime"),
        "startTime",
    )
    end_time, end_value = normalize_download_history_time(
        params.get("endTime"),
        "endTime",
    )
    if (
        start_value is not None
        and end_value is not None
        and start_value > end_value
    ):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "startTime must not be after endTime",
        )

    normalized = {
        "stockcode": stockcode,
        "period": period,
        "startTime": start_time,
        "endTime": end_time,
    }
    if "incrementally" in params:
        incrementally = params.get("incrementally")
        if incrementally is not None:
            incrementally = normalize_boolean(
                incrementally,
                "incrementally",
                False,
            )
        normalized["incrementally"] = incrementally
    return normalized


def normalize_download_task_id(params):
    allowed_params = {"task_id"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported download status params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )
    task_id = params.get("task_id")
    if not isinstance(task_id, str) or not task_id.strip():
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "task_id must be a non-empty string",
        )
    return task_id.strip()


def normalize_financial_name(value, name):
    if not isinstance(value, str):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "{0} must be a string".format(name),
        )
    value = value.strip()
    if (
        not value
        or len(value) > MAX_FINANCIAL_NAME_LENGTH
        or any(
            char.isspace() or not char.isprintable()
            for char in value
        )
    ):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "{0} contains invalid characters".format(name),
        )
    return value


def normalize_financial_list(value, name, max_size, stockcodes=False):
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        values = None
    if not values or len(values) > max_size:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "{0} must contain between 1 and {1} strings".format(
                name,
                max_size,
            ),
        )

    normalized = []
    for item in values:
        if stockcodes:
            item = normalize_stockcode(item)
            if item is None:
                raise FeedError(
                    400,
                    "INVALID_PARAMS",
                    "{0} must contain stock.market values".format(name),
                )
        else:
            item = normalize_financial_name(item, name)
        normalized.append(item)
    return normalized


def normalize_batch_stock_list(value, name="stock_code"):
    stocks = normalize_financial_list(
        value,
        name,
        MAX_BATCH_STOCKS,
        stockcodes=True,
    )
    if len(set(stocks)) != len(stocks):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "{0} must not contain duplicates".format(name),
        )
    return stocks


def normalize_financial_date(value, name):
    if (
        not isinstance(value, str)
        or len(value) != 8
        or not all("0" <= char <= "9" for char in value)
    ):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "{0} must use YYYYMMDD".format(name),
        )
    try:
        parsed = dt.datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "{0} must be a valid date".format(name),
        )
    return value, parsed


def normalize_financial_report_type(value, default):
    report_type = default if value is None else value
    if not isinstance(report_type, str):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "report_type must be a string",
        )
    report_type = report_type.strip()
    if report_type not in FINANCIAL_REPORT_TYPES:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "report_type must be announce_time or report_time",
        )
    return report_type


def normalize_financial_barpos(value):
    if isinstance(value, bool):
        barpos = None
    elif isinstance(value, int):
        barpos = value
    elif isinstance(value, str):
        text = value.strip()
        if (
            not text
            or len(text) > len(str(MAX_FINANCIAL_BARPOS))
            or not all("0" <= char <= "9" for char in text)
        ):
            barpos = None
        else:
            barpos = int(text)
    else:
        barpos = None
    if barpos is None or not 0 <= barpos <= MAX_FINANCIAL_BARPOS:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "barpos must be an integer between 0 and {0}".format(
                MAX_FINANCIAL_BARPOS
            ),
        )
    return barpos
