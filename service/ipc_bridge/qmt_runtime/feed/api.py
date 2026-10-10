"""QMT-thread-only Feed operations and bounded serializers."""
import datetime as dt
import math
from service.ipc_bridge.ipc_protocol.feed_contracts import (
    MAX_TRADING_DATES_COUNT,
    MAX_STOCKCODE_LENGTH,
    MAX_DIVID_FACTOR_RECORDS,
    MAX_FINANCIAL_FIELDS,
    MAX_FINANCIAL_STOCKS,
    MAX_FINANCIAL_CELLS,
    MAX_FINANCIAL_NAME_LENGTH,
    MAX_FINANCIAL_BARPOS,
    FINANCIAL_REPORT_TYPES,
    MAX_MARKET_DATA_FIELDS,
    MAX_MARKET_DATA_STOCKS,
    MAX_MARKET_DATA_COUNT,
    MAX_MARKET_DATA_CELLS,
    MAX_MARKET_DATA_COLUMNS,
    MAX_MARKET_DATA_JSON_VALUES,
    MAX_FULL_TICK_STOCKS,
    MAX_FULL_TICK_JSON_VALUES,
    MAX_BATCH_STOCKS,
    MAX_BATCH_INSTRUMENT_DETAIL_JSON_VALUES,
    MAX_BATCH_DIVID_FACTOR_RECORDS,
    MAX_HIS_INDEX_JSON_VALUES,
    MAX_HIS_CONTRACTS,
    MAX_LONGHUBANG_STOCKS,
    MAX_LONGHUBANG_DATE_DAYS,
    MAX_LONGHUBANG_STOCK_DAYS,
    MAX_LONGHUBANG_ROWS,
    MAX_LONGHUBANG_COLUMNS,
    MAX_LONGHUBANG_CELLS,
    MAX_LONGHUBANG_JSON_VALUES,
    MAX_QMT_TABLE_ROWS,
    MAX_QMT_TABLE_COLUMNS,
    MAX_QMT_TABLE_CELLS,
    MAX_DOWNLOAD_TASKS,
    DOWNLOAD_HISTORY_PERIODS,
    MARKET_DATA_BAR_FIELDS,
    MARKET_DATA_TICK_FIELDS,
    MARKET_DATA_BAR_PERIODS,
    MARKET_DATA_LEVEL2_FIELDS,
    MARKET_DATA_DIVIDEND_TYPES,
    ONE_JOB_PER_TICK_METHODS,
    BATCH_METHODS,
    TRADING_DATE_PERIODS,
    FeedError,
    normalize_stockcode,
    normalize_market,
    normalize_boolean,
    normalize_download_history_time,
    normalize_download_history_params,
    normalize_download_task_id,
    normalize_financial_name,
    normalize_financial_list,
    normalize_batch_stock_list,
    normalize_financial_date,
    normalize_financial_report_type,
    normalize_financial_barpos
)

_FEED_APIS = {}

def bind_feed_apis(apis):
    """Freeze the two QMT-injected module APIs for this strategy start."""
    global _FEED_APIS
    _FEED_APIS = {name: apis.get(name) for name in ("get_sector_list", "download_history_data")}

def feed_native_api(name):
    api = _FEED_APIS.get(name)
    if not callable(api):
        raise FeedError(503, "QMT_API_UNAVAILABLE", name + " is unavailable")
    return api

def qmt_table_to_json(
    value,
    budget=None,
    max_rows=MAX_QMT_TABLE_ROWS,
    max_columns=MAX_QMT_TABLE_COLUMNS,
    max_cells=MAX_QMT_TABLE_CELLS,
):
    if getattr(value, "ndim", None) != 2:
        raise ValueError("QMT result is not a two-dimensional table")
    try:
        row_count = len(value.index)
        column_count = len(value.columns)
    except (AttributeError, TypeError):
        raise ValueError("QMT table axes are unavailable")
    if row_count > max_rows:
        raise ValueError("QMT table row limit exceeded")
    if column_count > max_columns:
        raise ValueError("QMT table column limit exceeded")
    if row_count * column_count > max_cells:
        raise ValueError("QMT table cell limit exceeded")

    try:
        raw_data = value.values.tolist()
    except (AttributeError, TypeError, ValueError):
        raise ValueError("QMT table values are unavailable")
    if not isinstance(raw_data, list) or len(raw_data) != row_count:
        raise ValueError("QMT table row count is inconsistent")
    if any(
        not isinstance(row, (list, tuple)) or len(row) != column_count
        for row in raw_data
    ):
        raise ValueError("QMT table columns are inconsistent")

    return {
        "index": [qmt_json_value(item, budget) for item in list(value.index)],
        "columns": [
            qmt_json_value(item, budget) for item in list(value.columns)
        ],
        "data": qmt_json_value(raw_data, budget),
    }


def qmt_json_value(value, budget=None):
    if budget is not None:
        budget[0] -= 1
        if budget[0] < 0:
            raise ValueError("QMT JSON value limit exceeded")
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [qmt_json_value(item, budget) for item in value]
    if isinstance(value, dict):
        return {
            str(key): qmt_json_value(item, budget)
            for key, item in value.items()
        }
    if getattr(value, "ndim", None) == 2:
        return qmt_table_to_json(value, budget)

    tolist_method = getattr(value, "tolist", None)
    if callable(tolist_method):
        try:
            return qmt_json_value(tolist_method(), budget)
        except (TypeError, ValueError):
            if budget is not None and budget[0] < 0:
                raise

    item_method = getattr(value, "item", None)
    if callable(item_method):
        try:
            return qmt_json_value(item_method(), budget)
        except (TypeError, ValueError):
            if budget is not None and budget[0] < 0:
                raise
    return str(value)


def financial_axis_to_json(axis, budget=None):
    return [qmt_json_value(value, budget) for value in list(axis)]


def financial_table_to_json(result):
    ndim = getattr(result, "ndim", None)
    try:
        data = qmt_json_value(result.values.tolist())
        if ndim == 1:
            return {
                "type": "series",
                "index": financial_axis_to_json(result.index),
                "data": data,
            }
        if ndim == 2:
            return {
                "type": "dataframe",
                "index": financial_axis_to_json(result.index),
                "columns": financial_axis_to_json(result.columns),
                "data": data,
            }
        if ndim == 3:
            return {
                "type": "panel",
                "items": financial_axis_to_json(result.items),
                "major_axis": financial_axis_to_json(result.major_axis),
                "minor_axis": financial_axis_to_json(result.minor_axis),
                "data": data,
            }
    except (AttributeError, TypeError, ValueError):
        pass
    raise FeedError(
        500,
        "INVALID_QMT_RESULT",
        "get_financial_data returned an unsupported table result",
    )


def handle_get_stock_list_in_sector(ContextInfo, params):
    allowed_params = {"sectorname", "realtime"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported sector params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )

    sectorname = params.get("sectorname")
    if not isinstance(sectorname, str) or not sectorname.strip():
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "sectorname must be a non-empty string",
        )

    sectorname = sectorname.strip()
    if "realtime" not in params:
        stocks = ContextInfo.get_stock_list_in_sector(sectorname)
    else:
        realtime = params["realtime"]
        if isinstance(realtime, bool):
            raise FeedError(
                400,
                "INVALID_PARAMS",
                "realtime must be a millisecond timestamp",
            )
        if isinstance(realtime, int):
            pass
        elif (
            isinstance(realtime, str)
            and realtime.strip()
            and all("0" <= char <= "9" for char in realtime.strip())
        ):
            realtime = realtime.strip()
            realtime = int(realtime)
        else:
            raise FeedError(
                400,
                "INVALID_PARAMS",
                "realtime must be a millisecond timestamp",
            )
        if realtime < 0:
            raise FeedError(
                400,
                "INVALID_PARAMS",
                "realtime must be a millisecond timestamp",
            )
        stocks = ContextInfo.get_stock_list_in_sector(sectorname, realtime)

    if not isinstance(stocks, list):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_stock_list_in_sector did not return a list",
        )
    return stocks


def handle_get_sector_list(ContextInfo, params):
    allowed_params = {"node"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported get_sector_list params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )

    node = params.get("node", "")
    if not isinstance(node, str):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "node must be a string",
        )

    info_list = feed_native_api("get_sector_list")(node)
    if (
        not isinstance(info_list, list)
        or len(info_list) != 2
        or not all(isinstance(items, list) for items in info_list)
        or not all(
            isinstance(name, str)
            for items in info_list
            for name in items
        )
    ):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_sector_list did not return [sector_names, child_nodes]",
        )
    return info_list


def handle_get_trading_dates(ContextInfo, params):
    allowed_params = {
        "stockcode",
        "start_date",
        "end_date",
        "count",
        "period",
    }
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported get_trading_dates params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )

    stockcode = params.get("stockcode", "")
    if not isinstance(stockcode, str):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "stockcode must be a string",
        )
    stockcode = stockcode.strip()

    dates = {}
    for name in ("start_date", "end_date"):
        value = params.get(name, "")
        if not isinstance(value, str):
            raise FeedError(
                400,
                "INVALID_PARAMS",
                "{0} must be a string".format(name),
            )
        value = value.strip()
        if value and (
            len(value) not in (8, 14)
            or not all("0" <= char <= "9" for char in value)
        ):
            raise FeedError(
                400,
                "INVALID_PARAMS",
                "{0} must use YYYYMMDD or YYYYMMDDHHMMSS".format(name),
            )
        dates[name] = value

    count = params.get("count")
    if isinstance(count, bool):
        count = None
    elif isinstance(count, int):
        pass
    elif isinstance(count, str):
        count_text = count.strip()
        if (
            not count_text
            or len(count_text) > len(str(MAX_TRADING_DATES_COUNT))
            or not all("0" <= char <= "9" for char in count_text)
        ):
            count = None
        else:
            count = int(count_text)
    else:
        count = None
    if count is None or not 1 <= count <= MAX_TRADING_DATES_COUNT:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "count must be an integer between 1 and {0}".format(
                MAX_TRADING_DATES_COUNT
            ),
        )

    period = params.get("period", "1d")
    if not isinstance(period, str):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "period must be a string",
        )
    period = period.strip()
    if period not in TRADING_DATE_PERIODS:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported trading date period: {0}".format(period),
        )

    trading_dates = ContextInfo.get_trading_dates(
        stockcode,
        dates["start_date"],
        dates["end_date"],
        count,
        period,
    )
    if (
        not isinstance(trading_dates, list)
        or not all(isinstance(value, str) for value in trading_dates)
    ):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_trading_dates did not return a string list",
        )
    return trading_dates


def is_positional_argument_count_error(exc):
    message = str(exc)
    traceback = exc.__traceback__
    if traceback is None or traceback.tb_next is not None:
        return False
    return (
        "takes" in message
        and "positional argument" in message
        and "given" in message
    )


def call_get_instrument_detail(ContextInfo, stockcode, iscomplete):
    method = ContextInfo.get_instrument_detail
    try:
        return method(stockcode, iscomplete)
    except TypeError as exc:
        if not is_positional_argument_count_error(exc):
            raise
        return method(stockcode)


def handle_get_instrument_detail(ContextInfo, params):
    allowed_params = {"stockcode", "iscomplete"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported get_instrument_detail params: {0}".format(
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

    iscomplete = params.get("iscomplete", False)
    if isinstance(iscomplete, bool):
        pass
    elif isinstance(iscomplete, str):
        iscomplete_text = iscomplete.strip().lower()
        if iscomplete_text == "true":
            iscomplete = True
        elif iscomplete_text == "false":
            iscomplete = False
        else:
            raise FeedError(
                400,
                "INVALID_PARAMS",
                "iscomplete must be true or false",
            )
    else:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "iscomplete must be true or false",
        )

    detail = call_get_instrument_detail(ContextInfo, stockcode, iscomplete)
    if (
        not isinstance(detail, dict)
        or not all(isinstance(name, str) for name in detail)
    ):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_instrument_detail did not return a string-keyed dict",
        )
    return detail


def init_get_instrument_details_batch(params):
    allowed_params = {"stock_code", "iscomplete"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported instrument detail batch params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )
    return {
        "items": normalize_batch_stock_list(params.get("stock_code")),
        "position": 0,
        "results": {},
        "errors": {},
        "item_params": {
            "iscomplete": normalize_boolean(
                params.get("iscomplete"),
                "iscomplete",
                False,
            ),
        },
        "json_budget": [MAX_BATCH_INSTRUMENT_DETAIL_JSON_VALUES],
    }


def init_get_divid_factors_batch(params):
    allowed_params = {"stock_code"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported dividend factor batch params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )
    return {
        "items": normalize_batch_stock_list(params.get("stock_code")),
        "position": 0,
        "results": {},
        "errors": {},
        "item_params": {},
        "record_count": 0,
    }


def init_get_weights_in_index_batch(params):
    allowed_params = {"indexcode", "stock_code"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported index weight batch params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )
    indexcode = normalize_stockcode(params.get("indexcode"))
    if indexcode is None:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "indexcode must use stock.market format",
        )
    return {
        "items": normalize_batch_stock_list(params.get("stock_code")),
        "position": 0,
        "results": {},
        "errors": {},
        "item_params": {"indexcode": indexcode},
    }


def process_batch_request(ContextInfo, job):
    method = job.request.get("method")
    if job.batch_state is None:
        if method == "get_instrument_details":
            job.batch_state = init_get_instrument_details_batch(
                job.request.get("params")
            )
        elif method == "get_divid_factors_batch":
            job.batch_state = init_get_divid_factors_batch(
                job.request.get("params")
            )
        elif method == "get_weights_in_index":
            job.batch_state = init_get_weights_in_index_batch(
                job.request.get("params")
            )
        else:
            raise FeedError(
                404,
                "METHOD_NOT_FOUND",
                "unsupported batch method: {0}".format(method),
            )

    batch = job.batch_state
    stockcode = batch["items"][batch["position"]]
    try:
        if method == "get_instrument_details":
            params = {
                "stockcode": stockcode,
                "iscomplete": batch["item_params"]["iscomplete"],
            }
            result = handle_get_instrument_detail(
                ContextInfo,
                params,
            )
            try:
                result = qmt_json_value(result, batch["json_budget"])
            except ValueError as exc:
                if "JSON value limit exceeded" in str(exc):
                    raise FeedError(
                        500,
                        "RESULT_LIMIT_EXCEEDED",
                        "batch instrument details exceed the JSON value limit",
                    )
                raise
            batch["results"][stockcode] = result
        elif method == "get_divid_factors_batch":
            result = handle_get_divid_factors(
                ContextInfo,
                {"stockcode": stockcode},
            )
            next_record_count = batch["record_count"] + len(result)
            if next_record_count > MAX_BATCH_DIVID_FACTOR_RECORDS:
                raise FeedError(
                    500,
                    "RESULT_LIMIT_EXCEEDED",
                    "batch dividend factors exceed the record limit",
                )
            batch["record_count"] = next_record_count
            batch["results"][stockcode] = result
        elif method == "get_weights_in_index":
            batch["results"][stockcode] = handle_get_weight_in_index(
                ContextInfo,
                {
                    "indexcode": batch["item_params"]["indexcode"],
                    "stockcode": stockcode,
                },
            )
    except FeedError as exc:
        batch["errors"][stockcode] = {
            "code": exc.code,
            "message": exc.message,
        }
    except Exception as exc:
        batch["errors"][stockcode] = {
            "code": "QMT_ERROR",
            "message": str(exc),
        }

    batch["position"] += 1
    if batch["position"] < len(batch["items"]):
        return False
    job.result = {
        "results": batch["results"],
        "errors": batch["errors"],
    }
    return True


def handle_get_divid_factors(ContextInfo, params):
    allowed_params = {"stockcode"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported dividend factor params: {0}".format(
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

    result = ContextInfo.get_divid_factors(stockcode)
    if not isinstance(result, dict):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_divid_factors did not return a dict",
        )
    if len(result) > MAX_DIVID_FACTOR_RECORDS:
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_divid_factors returned too many records",
        )

    normalized = {}
    for raw_timestamp, raw_factors in result.items():
        timestamp = qmt_json_value(raw_timestamp)
        if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, int)
            or timestamp < 0
        ):
            raise FeedError(
                500,
                "INVALID_QMT_RESULT",
                "get_divid_factors returned an invalid timestamp",
            )
        if (
            not isinstance(raw_factors, (list, tuple))
            or len(raw_factors) != 7
        ):
            raise FeedError(
                500,
                "INVALID_QMT_RESULT",
                "get_divid_factors returned an invalid factor record",
            )

        factors = [qmt_json_value(value) for value in raw_factors]
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            for value in factors
        ):
            raise FeedError(
                500,
                "INVALID_QMT_RESULT",
                "get_divid_factors returned a non-numeric factor",
            )
        normalized[str(timestamp)] = factors
    return normalized


def handle_get_weight_in_index(ContextInfo, params):
    allowed_params = {"indexcode", "stockcode"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported index weight params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )

    indexcode = normalize_stockcode(params.get("indexcode"))
    if indexcode is None:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "indexcode must use stock.market format",
        )
    stockcode = normalize_stockcode(params.get("stockcode"))
    if stockcode is None:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "stockcode must use stock.market format",
        )

    weight = qmt_json_value(
        ContextInfo.get_weight_in_index(indexcode, stockcode)
    )
    if isinstance(weight, bool) or not isinstance(weight, (int, float)):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_weight_in_index did not return a number",
        )
    try:
        weight = float(weight)
    except OverflowError:
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_weight_in_index returned a non-finite number",
        )
    if not math.isfinite(weight):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_weight_in_index returned a non-finite number",
        )
    return weight


def handle_get_full_tick(ContextInfo, params):
    allowed_params = {"stock_code"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported full tick params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )

    stocks = normalize_financial_list(
        params.get("stock_code"),
        "stock_code",
        MAX_FULL_TICK_STOCKS,
        stockcodes=True,
    )
    result = ContextInfo.get_full_tick(stocks)
    if not isinstance(result, dict):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_full_tick did not return a dict",
        )
    if len(result) > len(stocks):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_full_tick returned too many stock results",
        )

    requested_stocks = set(stocks)
    normalized = {}
    json_budget = [MAX_FULL_TICK_JSON_VALUES]
    for stockcode, tick in result.items():
        if (
            not isinstance(stockcode, str)
            or stockcode not in requested_stocks
        ):
            raise FeedError(
                500,
                "INVALID_QMT_RESULT",
                "get_full_tick returned an unexpected stock code",
            )
        if (
            not isinstance(tick, dict)
            or not all(isinstance(field, str) for field in tick)
        ):
            raise FeedError(
                500,
                "INVALID_QMT_RESULT",
                "get_full_tick returned an invalid tick dict",
            )
        try:
            normalized[stockcode] = qmt_json_value(tick, json_budget)
        except (TypeError, ValueError):
            raise FeedError(
                500,
                "INVALID_QMT_RESULT",
                "get_full_tick result exceeds JSON limits",
            )
    return normalized


def handle_get_his_index_data(ContextInfo, params):
    allowed_params = {"stockcode"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported historical index params: {0}".format(
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

    result = ContextInfo.get_his_index_data(stockcode)
    if not isinstance(result, (dict, list, tuple)) and getattr(
        result,
        "ndim",
        None,
    ) != 2:
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_his_index_data did not return structured data",
        )
    try:
        return qmt_json_value(result, [MAX_HIS_INDEX_JSON_VALUES])
    except (AttributeError, TypeError, ValueError):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_his_index_data result exceeds JSON limits",
        )


def handle_get_his_contract_list(ContextInfo, params):
    allowed_params = {"market"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported historical contract params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )

    market = normalize_market(params.get("market"))
    if market is None:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "market must contain 1 to 16 ASCII letters or digits",
        )

    result = ContextInfo.get_his_contract_list(market)
    if (
        not isinstance(result, list)
        or len(result) > MAX_HIS_CONTRACTS
        or not all(normalize_stockcode(value) is not None for value in result)
    ):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_his_contract_list did not return a bounded stock code list",
        )
    return result


def handle_get_longhubang(ContextInfo, params):
    allowed_params = {"stock_list", "startTime", "endTime"}
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported longhubang params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )

    stock_list = normalize_financial_list(
        params.get("stock_list"),
        "stock_list",
        MAX_LONGHUBANG_STOCKS,
        stockcodes=True,
    )
    start_time, start_date = normalize_financial_date(
        params.get("startTime"),
        "startTime",
    )
    end_time, end_date = normalize_financial_date(
        params.get("endTime"),
        "endTime",
    )
    if start_date > end_date:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "startTime must not be later than endTime",
        )
    if (end_date - start_date).days > MAX_LONGHUBANG_DATE_DAYS:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "longhubang date range must not exceed {0} days".format(
                MAX_LONGHUBANG_DATE_DAYS
            ),
        )
    date_count = (end_date - start_date).days + 1
    if len(stock_list) * date_count > MAX_LONGHUBANG_STOCK_DAYS:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "stock count times date count must not exceed {0}".format(
                MAX_LONGHUBANG_STOCK_DAYS
            ),
        )

    result = ContextInfo.get_longhubang(
        stock_list,
        start_time,
        end_time,
    )
    try:
        return qmt_table_to_json(
            result,
            [MAX_LONGHUBANG_JSON_VALUES],
            MAX_LONGHUBANG_ROWS,
            MAX_LONGHUBANG_COLUMNS,
            MAX_LONGHUBANG_CELLS,
        )
    except (AttributeError, TypeError, ValueError):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_longhubang returned an invalid or oversized table",
        )


def resolve_market_data_profile_period(ContextInfo, period):
    normalized_period = period.lower()
    if normalized_period != "follow":
        return normalized_period

    context_period = getattr(ContextInfo, "period", None)
    if not isinstance(context_period, str) or not context_period.strip():
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "empty fields with period=follow require ContextInfo.period",
        )
    return context_period.strip().lower()


def default_market_data_fields(period):
    normalized_period = period.lower()
    if normalized_period == "tick":
        return list(MARKET_DATA_TICK_FIELDS)
    if normalized_period in MARKET_DATA_BAR_PERIODS:
        return list(MARKET_DATA_BAR_FIELDS)
    if normalized_period in MARKET_DATA_LEVEL2_FIELDS:
        return list(MARKET_DATA_LEVEL2_FIELDS[normalized_period])
    raise FeedError(
        400,
        "INVALID_PARAMS",
        "empty fields have no FEED profile for period: {0}".format(
            period
        ),
    )


def normalize_market_data_fields(value, ContextInfo, period):
    if value is None or value == []:
        profile_period = resolve_market_data_profile_period(
            ContextInfo,
            period,
        )
        return default_market_data_fields(profile_period)
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list):
        values = value
    else:
        values = None
    if (
        not values
        or len(values) > MAX_MARKET_DATA_FIELDS
    ):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "fields must contain between 1 and {0} strings".format(
                MAX_MARKET_DATA_FIELDS
            ),
        )
    return [
        normalize_financial_name(item, "fields")
        for item in values
    ]


def normalize_market_data_count(value):
    if isinstance(value, bool):
        count = None
    elif isinstance(value, int):
        count = value
    elif isinstance(value, str):
        text = value.strip()
        if (
            not text
            or len(text) > len(str(MAX_MARKET_DATA_COUNT))
            or not all("0" <= char <= "9" for char in text)
        ):
            count = None
        else:
            count = int(text)
    else:
        count = None
    if count is None or not 1 <= count <= MAX_MARKET_DATA_COUNT:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "count must be an integer between 1 and {0}".format(
                MAX_MARKET_DATA_COUNT
            ),
        )
    return count


def normalize_market_data_time(value, name):
    if value is None:
        value = ""
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
    is_date_only = len(value) == 8
    date_format = "%Y%m%d" if is_date_only else "%Y%m%d%H%M%S"
    try:
        parsed = dt.datetime.strptime(value, date_format)
    except ValueError:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "{0} must be a valid date or datetime".format(name),
        )
    if is_date_only and name == "end_time":
        parsed += dt.timedelta(days=1) - dt.timedelta(seconds=1)
    return value, parsed


def market_data_frame_to_json(
    frame,
    requested_count,
    remaining_cells,
    budget,
):
    if getattr(frame, "ndim", None) != 2:
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_market_data_ex values must be DataFrames",
        )
    try:
        row_count = len(frame.index)
        column_count = len(frame.columns)
    except (AttributeError, TypeError, ValueError):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_market_data_ex returned an invalid DataFrame",
        )
    cell_count = row_count * max(1, column_count)
    if (
        row_count > requested_count
        or column_count > MAX_MARKET_DATA_COLUMNS
        or cell_count > MAX_MARKET_DATA_CELLS
        or cell_count > remaining_cells
    ):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_market_data_ex DataFrame exceeds response limits",
        )
    try:
        index = financial_axis_to_json(frame.index, budget)
        columns = financial_axis_to_json(frame.columns, budget)
        data = qmt_json_value(frame.values.tolist(), budget)
    except (AttributeError, TypeError, ValueError):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_market_data_ex returned an invalid DataFrame",
        )
    if (
        not isinstance(data, list)
        or len(data) != len(index)
        or any(
            not isinstance(row, list) or len(row) != len(columns)
            for row in data
        )
    ):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_market_data_ex DataFrame exceeds response limits",
        )
    return (
        {
            "index": index,
            "columns": columns,
            "data": data,
        },
        cell_count,
    )


def handle_get_market_data_ex(ContextInfo, params):
    allowed_params = {
        "fields",
        "stock_code",
        "period",
        "start_time",
        "end_time",
        "count",
        "dividend_type",
        "fill_data",
        "subscribe",
    }
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported market data params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )

    period = normalize_financial_name(
        params.get("period", "follow"),
        "period",
    )
    fields = normalize_market_data_fields(
        params.get("fields"),
        ContextInfo,
        period,
    )
    stocks = normalize_financial_list(
        params.get("stock_code"),
        "stock_code",
        MAX_MARKET_DATA_STOCKS,
        stockcodes=True,
    )
    start_time, start_value = normalize_market_data_time(
        params.get("start_time"),
        "start_time",
    )
    end_time, end_value = normalize_market_data_time(
        params.get("end_time"),
        "end_time",
    )
    if (
        start_value is not None
        and end_value is not None
        and start_value > end_value
    ):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "start_time must not be after end_time",
        )

    count = normalize_market_data_count(params.get("count", 1))
    dividend_type = params.get("dividend_type", "follow")
    if (
        not isinstance(dividend_type, str)
        or dividend_type.strip() not in MARKET_DATA_DIVIDEND_TYPES
    ):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "dividend_type is not supported",
        )
    dividend_type = dividend_type.strip()
    fill_data = normalize_boolean(
        params.get("fill_data"),
        "fill_data",
        False,
    )
    subscribe = normalize_boolean(
        params.get("subscribe"),
        "subscribe",
        False,
    )
    if subscribe:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "subscribe=true is not supported by the HTTP feed",
        )

    estimated_cells = count * len(stocks) * len(fields)
    if estimated_cells > MAX_MARKET_DATA_CELLS:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "market data request exceeds {0} estimated cells".format(
                MAX_MARKET_DATA_CELLS
            ),
        )

    result = ContextInfo.get_market_data_ex(
        fields,
        stocks,
        period=period,
        start_time=start_time,
        end_time=end_time,
        count=count,
        dividend_type=dividend_type,
        fill_data=fill_data,
        subscribe=subscribe,
    )
    if not isinstance(result, dict):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_market_data_ex did not return a dict",
        )
    if len(result) > MAX_MARKET_DATA_STOCKS:
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_market_data_ex returned too many stock results",
        )

    normalized = {}
    result_cells = 0
    json_budget = [MAX_MARKET_DATA_JSON_VALUES]
    for stockcode, frame in result.items():
        if not isinstance(stockcode, str):
            raise FeedError(
                500,
                "INVALID_QMT_RESULT",
                "get_market_data_ex returned a non-string stock code",
            )
        table, table_cells = market_data_frame_to_json(
            frame,
            count,
            MAX_MARKET_DATA_CELLS - result_cells,
            json_budget,
        )
        result_cells += table_cells
        normalized[stockcode] = table
    return normalized


def handle_get_financial_data_range(ContextInfo, params):
    allowed_params = {
        "mode",
        "fieldList",
        "stockList",
        "startDate",
        "endDate",
        "report_type",
    }
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported financial range params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )

    fields = normalize_financial_list(
        params.get("fieldList"),
        "fieldList",
        MAX_FINANCIAL_FIELDS,
    )
    stocks = normalize_financial_list(
        params.get("stockList"),
        "stockList",
        MAX_FINANCIAL_STOCKS,
        stockcodes=True,
    )
    start_date, start_value = normalize_financial_date(
        params.get("startDate"),
        "startDate",
    )
    end_date, end_value = normalize_financial_date(
        params.get("endDate"),
        "endDate",
    )
    if start_value > end_value:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "startDate must not be after endDate",
        )

    calendar_days = (end_value - start_value).days + 1
    estimated_cells = calendar_days * len(fields) * len(stocks)
    if estimated_cells > MAX_FINANCIAL_CELLS:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "financial range exceeds {0} estimated cells".format(
                MAX_FINANCIAL_CELLS
            ),
        )

    report_type = normalize_financial_report_type(
        params.get("report_type"),
        "announce_time",
    )
    result = ContextInfo.get_financial_data(
        fields,
        stocks,
        start_date,
        end_date,
        report_type,
    )
    return financial_table_to_json(result)


def handle_get_financial_data_bar(ContextInfo, params):
    allowed_params = {
        "mode",
        "tabname",
        "colname",
        "market",
        "code",
        "barpos",
        "report_type",
    }
    unknown_params = set(params) - allowed_params
    if unknown_params:
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "unsupported financial bar params: {0}".format(
                ",".join(sorted(unknown_params))
            ),
        )

    tabname = normalize_financial_name(params.get("tabname"), "tabname")
    colname = normalize_financial_name(params.get("colname"), "colname")
    market = normalize_financial_name(params.get("market"), "market")
    code = normalize_financial_name(params.get("code"), "code")
    barpos = normalize_financial_barpos(params.get("barpos"))

    if "report_type" in params:
        report_type = normalize_financial_report_type(
            params.get("report_type"),
            "report_time",
        )
        result = ContextInfo.get_financial_data(
            tabname,
            colname,
            market,
            code,
            barpos,
            report_type=report_type,
        )
    else:
        result = ContextInfo.get_financial_data(
            tabname,
            colname,
            market,
            code,
            barpos,
        )

    result = qmt_json_value(result)
    if (
        result is not None
        and (
            isinstance(result, bool)
            or not isinstance(result, (int, float))
        )
    ):
        raise FeedError(
            500,
            "INVALID_QMT_RESULT",
            "get_financial_data bar mode did not return a number",
        )
    return result


def handle_get_financial_data(ContextInfo, params):
    mode = params.get("mode", "range")
    if not isinstance(mode, str):
        raise FeedError(
            400,
            "INVALID_PARAMS",
            "financial mode must be range or bar",
        )
    mode = mode.strip().lower()
    if mode == "range":
        return handle_get_financial_data_range(ContextInfo, params)
    if mode == "bar":
        return handle_get_financial_data_bar(ContextInfo, params)
    raise FeedError(
        400,
        "INVALID_PARAMS",
        "financial mode must be range or bar",
    )


class BatchJob(object):
    """Minimal batch state; runtime may call process_batch_request once per tick."""

    def __init__(self, request):
        self.request = request
        self.batch_state = None
        self.result = None


def handle_download_history_data(ContextInfo, params):
    params = normalize_download_history_params(params)
    kwargs = {}
    if "incrementally" in params:
        kwargs["incrementally"] = params["incrementally"]
    feed_native_api("download_history_data")(
        params["stockcode"], params["period"], params["startTime"],
        params["endTime"], **kwargs
    )
    return {"message": "download_history_data returned; data coverage was not verified"}


def dispatch_request(ContextInfo, request):
    method = request.get("method")
    params = request.get("params")
    if not isinstance(method, str) or not isinstance(params, dict):
        raise FeedError(400, "INVALID_REQUEST", "method and params are required")

    if method in BATCH_METHODS:
        job = BatchJob(request)
        while not process_batch_request(ContextInfo, job):
            pass
        return job.result
    if method == "download_history_data":
        return handle_download_history_data(ContextInfo, params)
    if method == "get_stock_list_in_sector":
        return handle_get_stock_list_in_sector(ContextInfo, params)
    if method == "get_sector_list":
        return handle_get_sector_list(ContextInfo, params)
    if method == "get_trading_dates":
        return handle_get_trading_dates(ContextInfo, params)
    if method == "get_instrument_detail":
        return handle_get_instrument_detail(ContextInfo, params)
    if method == "get_divid_factors":
        return handle_get_divid_factors(ContextInfo, params)
    if method == "get_weight_in_index":
        return handle_get_weight_in_index(ContextInfo, params)
    if method == "get_full_tick":
        return handle_get_full_tick(ContextInfo, params)
    if method == "get_his_index_data":
        return handle_get_his_index_data(ContextInfo, params)
    if method == "get_his_contract_list":
        return handle_get_his_contract_list(ContextInfo, params)
    if method == "get_longhubang":
        return handle_get_longhubang(ContextInfo, params)
    if method == "get_market_data_ex":
        return handle_get_market_data_ex(ContextInfo, params)
    if method == "get_financial_data":
        return handle_get_financial_data(ContextInfo, params)

    raise FeedError(
        404,
        "METHOD_NOT_FOUND",
        "unsupported method: {0}".format(method),
    )
