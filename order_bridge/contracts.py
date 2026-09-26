# -*- coding: utf-8 -*-
"""HTTP ORDER 输入合同；不在此层调用 QMT。"""
import re
import math
import sys
from decimal import Decimal, InvalidOperation

from .common import OrderError, parse_timestamp


_contracts_symbol = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$")
_contracts_quote = {"LATEST": 5, "OWN_BEST": 13, "OPPONENT_BEST": 14, "FAR_LIMIT": 12}
_contracts_market = {"SH": ("BEST5_IOC", "BEST5_TO_LIMIT", "OPPONENT_BEST", "OWN_BEST"),
                     "SZ": ("BEST5_IOC", "OPPONENT_BEST", "OWN_BEST", "IOC", "FOK"),
                     "BJ": ("BEST5_IOC", "BEST5_TO_LIMIT", "OPPONENT_BEST", "OWN_BEST")}
_contracts_stock_prefixes = {"SH": ("600", "601", "603", "605", "688"),
                             "SZ": ("000", "001", "002", "003", "300", "301"),
                             "BJ": ("430", "830", "831", "832", "833", "834", "835",
                                    "836", "837", "838", "839", "870", "871", "872",
                                    "873", "874", "875", "876", "877", "878", "879",
                                    "880", "920")}
_contracts_etf_prefixes = {"SH": ("510", "511", "512", "513", "514", "515",
                                   "516", "517", "518", "519", "520", "521",
                                   "522", "523", "524", "525", "526", "527",
                                   "528", "529", "560", "561", "562", "563",
                                   "588", "589"), "SZ": ("159",), "BJ": ()}
_contracts_max_qmt_int = 2147483647
_contracts_max_qmt_float = Decimal(str(sys.float_info.max))
_contracts_sliced_fields = (
    "MaxOrderCount", "SinglePriceRange", "PriceRangeType", "PriceRangeValue",
    "PriceRangeRate", "SuperPriceType", "SuperPriceRate", "SuperPriceValue",
    "VolumeType", "VolumeRate", "SingleNumMin", "SingleNumMax",
    "ValidTimeType", "ValidTimeElapse", "ValidTimeStart", "ValidTimeEnd",
    "UndealtEntrustRule", "PlaceOrderInterval", "UseTrigger", "TriggerType",
    "TriggerPrice", "SuperPriceEnable")
_contracts_sliced_int = set(_contracts_sliced_fields) - {
    "PriceRangeValue", "PriceRangeRate", "SuperPriceRate", "SuperPriceValue",
    "VolumeRate", "SingleNumMin", "SingleNumMax", "TriggerPrice"}
_contracts_sliced_descriptions = {
    "MaxOrderCount": "最大下单次数", "SinglePriceRange": "波动区间是否单向(0/1)",
    "PriceRangeType": "波动区间类型(0比例/1数值)", "PriceRangeValue": "波动区间数值",
    "PriceRangeRate": "波动区间比例(0-1)", "SuperPriceType": "单笔超价类型(0比例/1数值)",
    "SuperPriceRate": "单笔超价比例(0-1)", "SuperPriceValue": "单笔超价数值",
    "VolumeType": "单笔基准量类型", "VolumeRate": "单笔下单比率(0-1)",
    "SingleNumMin": "单笔下单量最小值", "SingleNumMax": "单笔下单量最大值",
    "ValidTimeType": "有效时间类型(0持续/1区间)", "ValidTimeElapse": "有效持续时间",
    "ValidTimeStart": "有效开始时间偏移", "ValidTimeEnd": "有效结束时间偏移",
    "UndealtEntrustRule": "未成委托处理报价类型", "PlaceOrderInterval": "下撤单时间间隔",
    "UseTrigger": "是否触价(0/1)", "TriggerType": "触价类型",
    "TriggerPrice": "触价价格", "SuperPriceEnable": "超价启用笔数"}
_contracts_sliced_enum = {
    "SinglePriceRange": (0, 1), "PriceRangeType": (0, 1),
    "SuperPriceType": (0, 1), "VolumeType": tuple(range(13)),
    "ValidTimeType": (0, 1), "UseTrigger": (0, 1),
    "TriggerType": (0, 1, 2),
    "UndealtEntrustRule": tuple(range(15)) + (42, 43, 44, 45, 46, 47, 48)}
_contracts_sliced_units = {
    "MaxOrderCount": "orders", "SinglePriceRange": "boolean 0/1",
    "PriceRangeType": "enum", "PriceRangeValue": "CNY", "PriceRangeRate": "ratio",
    "SuperPriceType": "enum", "SuperPriceRate": "ratio", "SuperPriceValue": "CNY",
    "VolumeType": "enum", "VolumeRate": "ratio", "SingleNumMin": "shares or ETF units",
    "SingleNumMax": "shares or ETF units", "ValidTimeType": "enum",
    "ValidTimeElapse": "QMT duration integer; official unit unspecified",
    "ValidTimeStart": "Unix epoch seconds", "ValidTimeEnd": "Unix epoch seconds",
    "UndealtEntrustRule": "QMT prType enum", "PlaceOrderInterval": "seconds",
    "UseTrigger": "boolean 0/1", "TriggerType": "enum",
    "TriggerPrice": "CNY", "SuperPriceEnable": "orders"}
_contracts_sliced_ranges = {
    "MaxOrderCount": [1, _contracts_max_qmt_int],
    "SinglePriceRange": [0, 1], "PriceRangeType": [0, 1],
    "PriceRangeValue": [0, str(_contracts_max_qmt_float)], "PriceRangeRate": [0, 1],
    "SuperPriceType": [0, 1], "SuperPriceRate": [0, 1],
    "SuperPriceValue": [0, str(_contracts_max_qmt_float)],
    "VolumeType": [0, 12], "VolumeRate": [0, 1],
    "SingleNumMin": [0, _contracts_max_qmt_int],
    "SingleNumMax": [0, _contracts_max_qmt_int],
    "ValidTimeType": [0, 1], "ValidTimeElapse": [0, _contracts_max_qmt_int],
    "ValidTimeStart": [0, _contracts_max_qmt_int], "ValidTimeEnd": [0, _contracts_max_qmt_int],
    "UndealtEntrustRule": [0, 48], "PlaceOrderInterval": [1, _contracts_max_qmt_int],
    "UseTrigger": [0, 1], "TriggerType": [0, 2],
    "TriggerPrice": [0, str(_contracts_max_qmt_float)],
    "SuperPriceEnable": [0, _contracts_max_qmt_int]}


def _contracts_error(field, message="invalid value", status=400):
    raise OrderError(status, "INVALID_ORDER", field + ": " + message)


def _contracts_object(value, field):
    if not isinstance(value, dict):
        _contracts_error(field, "must be an object")
    return value


def _contracts_keys(value, allowed, required, field):
    unknown = set(value) - set(allowed)
    missing = set(required) - set(value)
    if unknown:
        _contracts_error(field, "unknown fields: " + ", ".join(sorted(unknown)))
    if missing:
        _contracts_error(field, "missing fields: " + ", ".join(sorted(missing)))


def _contracts_string(value, field):
    if not isinstance(value, str) or not value.strip():
        _contracts_error(field, "must be a nonempty string")
    return value.strip()


def _contracts_positive_int(value, field):
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= _contracts_max_qmt_int:
        _contracts_error(field, "must be a positive signed 32-bit integer")
    return value


def _contracts_decimal(value, field, allow_zero=False):
    if isinstance(value, bool) or not isinstance(value, str) or not re.fullmatch(r"\d+(?:\.\d+)?", value):
        _contracts_error(field, "must be a decimal string")
    try:
        number = Decimal(value)
    except InvalidOperation:
        _contracts_error(field, "must be a decimal string")
    if not number.is_finite() or number < 0 or (not allow_zero and number == 0):
        _contracts_error(field, "must be nonnegative and finite" if allow_zero else "must be positive and finite")
    if number > _contracts_max_qmt_float or (number and float(number) == 0):
        _contracts_error(field, "outside finite QMT double range", 422)
    canonical = format(number, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    return canonical


def _contracts_instrument_kind(symbol):
    code, exchange = symbol.split(".")
    if code == "000000":
        return None
    if code.startswith(_contracts_stock_prefixes[exchange]):
        return "STOCK"
    if code.startswith(_contracts_etf_prefixes[exchange]):
        return "ETF"
    return None


def _contracts_symbol_value(value, field):
    if not isinstance(value, str) or not _contracts_symbol.match(value):
        _contracts_error(field, "use six digits and .SH/.SZ/.BJ")
    if _contracts_instrument_kind(value) is None:
        _contracts_error(field, "outside supported A-share/ETF code ranges", 422)
    return value


def _contracts_enum(value, values, field):
    try:
        valid = value in values
    except TypeError:
        valid = False
    if not valid:
        _contracts_error(field, "unsupported value")
    return value


def _contracts_number(value, field, integer=False):
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        _contracts_error(field, "must be numeric")
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        _contracts_error(field, "must be finite")
    if not number.is_finite() or number > _contracts_max_qmt_float or number < -_contracts_max_qmt_float or (integer and number != int(number)):
        _contracts_error(field, "must be finite" + (" integer" if integer else ""))
    if integer and not -_contracts_max_qmt_int <= number <= _contracts_max_qmt_int:
        _contracts_error(field, "outside signed 32-bit range", 422)
    converted = int(number) if integer else float(number)
    if not integer and not math.isfinite(converted):
        _contracts_error(field, "outside finite QMT double range", 422)
    return converted


def _contracts_sliced(value):
    _contracts_keys(value, ("type", "mode", "params"), ("type", "mode", "params"), "execution")
    _contracts_enum(value["mode"], ("ALGO", "RANDOM"), "execution.mode")
    params = _contracts_object(value["params"], "execution.params")
    # QMT 文档说全部参数可选，但缺失会读取交易面板设置；故明确要求每个字段。
    _contracts_keys(params, _contracts_sliced_fields, _contracts_sliced_fields, "sliced.params")
    result = {}
    for key in _contracts_sliced_fields:
        result[key] = _contracts_number(params[key], "sliced.params." + key,
                                        key in _contracts_sliced_int)
    for key in ("PriceRangeRate", "SuperPriceRate", "VolumeRate"):
        if not 0 <= result[key] <= 1:
            _contracts_error("sliced.params." + key, "must be in [0, 1]")
    for key in ("SinglePriceRange", "PriceRangeType", "SuperPriceType", "ValidTimeType", "UseTrigger"):
        if result[key] not in (0, 1):
            _contracts_error("sliced.params." + key, "must be 0 or 1")
    for key in ("VolumeType", "UndealtEntrustRule", "TriggerType"):
        if result[key] not in _contracts_sliced_enum[key]:
            _contracts_error("sliced.params." + key, "outside documented QMT enumeration", 422)
    if result["MaxOrderCount"] <= 0 or result["PlaceOrderInterval"] <= 0:
        _contracts_error("sliced.params", "count and interval must be positive")
    if result["SuperPriceEnable"] < 0:
        _contracts_error("sliced.params.SuperPriceEnable", "must be nonnegative")
    if (result["SingleNumMin"] < 0 or result["SingleNumMax"] < result["SingleNumMin"] or
            result["SingleNumMax"] > _contracts_max_qmt_int):
        _contracts_error("sliced.params", "invalid single order quantity range")
    for key in ("PriceRangeValue", "SuperPriceValue", "TriggerPrice"):
        if result[key] < 0:
            _contracts_error("sliced.params." + key, "must be nonnegative")
    if result["ValidTimeType"] == 0:
        if result["ValidTimeElapse"] <= 0 or result["ValidTimeStart"] != 0 or result["ValidTimeEnd"] != 0:
            _contracts_error("sliced.params", "duration mode requires positive ValidTimeElapse and zero start/end")
    elif (result["ValidTimeElapse"] != 0 or result["ValidTimeStart"] <= 0 or
          result["ValidTimeEnd"] <= result["ValidTimeStart"]):
        _contracts_error("sliced.params", "time-window mode requires ordered Unix seconds and zero elapse")
    if result["UseTrigger"] == 0:
        if result["TriggerType"] != 0 or result["TriggerPrice"] != 0:
            _contracts_error("sliced.params", "disabled trigger requires zero type and price")
    elif result["TriggerType"] not in (1, 2) or result["TriggerPrice"] <= 0:
        _contracts_error("sliced.params", "enabled trigger requires type 1/2 and positive price")
    return {"type": "SLICED", "mode": value["mode"], "params": result}


def _contracts_smart(value):
    _contracts_keys(value, ("type", "algorithm", "start_at", "end_at", "params"),
                    ("type", "algorithm", "start_at", "end_at", "params"), "execution")
    algorithm = _contracts_string(value["algorithm"], "execution.algorithm")
    start = parse_timestamp(value["start_at"])
    end = parse_timestamp(value["end_at"])
    if start >= end:
        _contracts_error("execution", "start_at must precede end_at")
    params = _contracts_object(value["params"], "execution.params")
    if "m_strCmdRemark" in params:
        _contracts_error("smart.params.m_strCmdRemark", "reserved")
    for key, param in params.items():
        if not isinstance(key, str) or not key.startswith("m_") or isinstance(param, (dict, list, bool)) or param is None:
            _contracts_error("smart.params", "parameters must be scalar m_ fields")
    return {"type": "SMART", "algorithm": algorithm, "start_at": value["start_at"],
            "end_at": value["end_at"], "params": dict(params)}


def _contracts_validate_sliced_order(result):
    if result["execution"]["type"] != "SLICED":
        return
    rule = result["execution"]["params"]["UndealtEntrustRule"]
    if result["order_type"] == "BASKET":
        if rule in tuple(range(7, 12)) + tuple(range(42, 49)):
            _contracts_error("execution.params.UndealtEntrustRule", "unsupported for basket", 422)
    elif 42 <= rule <= 48:
        market = result["symbol"].rsplit(".", 1)[1]
        allowed = {"SH": (42, 43, 44, 45), "SZ": (44, 45, 46, 47, 48),
                   "BJ": (42, 43, 44, 45)}[market]
        if rule not in allowed:
            _contracts_error("execution.params.UndealtEntrustRule", "unsupported for exchange", 422)


def normalize_order(payload, account_id):
    payload = _contracts_object(payload, "order")
    common = ("client_order_id", "account_id", "order_type", "sizing_type", "strategy_id",
              "execution", "price_type", "limit_price", "quote_type", "market_type",
              "protection_price", "submit_before")
    _contracts_keys(payload, common + ("symbol", "side", "quantity", "amount", "items"),
                    ("client_order_id", "account_id", "order_type", "sizing_type",
                     "execution", "price_type"), "order")
    execution = _contracts_object(payload["execution"], "execution")
    mode = _contracts_enum(execution.get("type"), ("DIRECT", "SLICED", "SMART"), "execution.type")
    if mode == "DIRECT":
        _contracts_keys(execution, ("type",), ("type",), "execution")
        execution = {"type": "DIRECT"}
    elif mode == "SLICED":
        execution = _contracts_sliced(execution)
    else:
        execution = _contracts_smart(execution)
    result = {"client_order_id": _contracts_string(payload["client_order_id"], "client_order_id"),
              "account_id": _contracts_string(payload["account_id"], "account_id"),
              "order_type": _contracts_enum(payload["order_type"], ("SINGLE", "BASKET"), "order_type"),
              "sizing_type": _contracts_enum(payload["sizing_type"], ("QUANTITY", "AMOUNT"), "sizing_type"),
              "execution": execution,
              "price_type": _contracts_enum(payload["price_type"], ("LIMIT", "QUOTE", "MARKET"), "price_type")}
    if result["account_id"] != account_id:
        raise OrderError(403, "ACCOUNT_MISMATCH", "account_id does not match configured account")
    if "submit_before" in payload:
        result["submit_before"] = parse_timestamp(payload["submit_before"]).isoformat()
    if "strategy_id" in payload:
        result["strategy_id"] = _contracts_string(payload["strategy_id"], "strategy_id")
    if result["order_type"] == "SINGLE":
        _contracts_keys(payload, tuple(set(common) | {"symbol", "side", "quantity", "amount"}),
                        ("symbol", "side"), "single order")
        result["symbol"] = _contracts_symbol_value(payload["symbol"], "symbol")
        result["side"] = _contracts_enum(payload["side"], ("BUY", "SELL"), "side")
        if ("quantity" in payload) == ("amount" in payload):
            _contracts_error("single order", "specify exactly one of quantity or amount")
        if (result["sizing_type"] == "QUANTITY") != ("quantity" in payload):
            _contracts_error("sizing_type", "must match quantity or amount")
        if "quantity" in payload:
            result["quantity"] = _contracts_positive_int(payload["quantity"], "quantity")
        else:
            if _contracts_instrument_kind(result["symbol"]) == "ETF":
                _contracts_error("amount", "ETF amount orders are unsupported", 422)
            result["amount"] = _contracts_decimal(payload["amount"], "amount")
    else:
        _contracts_keys(payload, tuple(set(common) | {"items"}), ("items",), "basket order")
        if result["sizing_type"] != "QUANTITY":
            _contracts_error("sizing_type", "basket supports QUANTITY only", 422)
        if not isinstance(payload["items"], list) or not payload["items"]:
            _contracts_error("items", "must be a nonempty list")
        seen_ids, seen_pairs = set(), set()
        result["items"] = []
        for item in payload["items"]:
            item = _contracts_object(item, "item")
            _contracts_keys(item, ("item_id", "symbol", "side", "quantity"),
                            ("item_id", "symbol", "side", "quantity"), "item")
            member = {"item_id": _contracts_string(item["item_id"], "item_id"),
                      "symbol": _contracts_symbol_value(item["symbol"], "symbol"),
                      "side": _contracts_enum(item["side"], ("BUY", "SELL"), "side"),
                      "quantity": _contracts_positive_int(item["quantity"], "quantity")}
            pair = (member["symbol"], member["side"])
            if member["item_id"] in seen_ids or pair in seen_pairs:
                _contracts_error("items", "duplicate item_id or symbol/side")
            seen_ids.add(member["item_id"])
            seen_pairs.add(pair)
            result["items"].append(member)
    price_type = result["price_type"]
    if price_type == "LIMIT":
        if result["order_type"] == "BASKET":
            _contracts_error("price_type", "basket LIMIT is unsupported", 422)
        if any(key in payload for key in ("quote_type", "market_type", "protection_price")):
            _contracts_error("price_type", "LIMIT has no quote or market fields")
        if "limit_price" not in payload:
            _contracts_error("limit_price", "required")
        result["limit_price"] = _contracts_decimal(payload["limit_price"], "limit_price")
    elif price_type == "QUOTE":
        if any(key in payload for key in ("limit_price", "market_type", "protection_price")):
            _contracts_error("price_type", "QUOTE has no limit or market fields")
        if mode == "SMART":
            _contracts_error("price_type", "SMART does not support QUOTE", 422)
        result["quote_type"] = _contracts_enum(payload.get("quote_type"), _contracts_quote, "quote_type")
    else:
        if any(key in payload for key in ("limit_price", "quote_type")):
            _contracts_error("price_type", "MARKET has no quote or limit fields")
        if mode == "SMART":
            if "market_type" in payload or "protection_price" in payload:
                _contracts_error("price_type", "SMART MARKET has no exchange fields", 422)
        else:
            if result["order_type"] == "BASKET":
                _contracts_error("price_type", "basket market instruction is unsupported", 422)
            market = result["symbol"].split(".")[1]
            kind = payload.get("market_type")
            if kind not in _contracts_market[market]:
                _contracts_error("market_type", "unsupported for exchange", 422)
            result["market_type"] = kind
            if market in ("SH", "BJ"):
                if "protection_price" not in payload:
                    _contracts_error("protection_price", "required for this exchange")
                result["protection_price"] = _contracts_decimal(payload["protection_price"], "protection_price")
                if Decimal(result["protection_price"]) > 9999:
                    _contracts_error("protection_price", "must be at most 9999", 422)
            elif "protection_price" in payload:
                _contracts_error("protection_price", "unsupported for this exchange", 422)
    _contracts_validate_sliced_order(result)
    return result


def normalize_cancel(payload, account_id):
    payload = _contracts_object(payload, "cancel")
    _contracts_keys(payload, ("cancel_request_id", "account_id", "client_order_id"),
                    ("cancel_request_id", "account_id", "client_order_id"), "cancel")
    result = {key: _contracts_string(payload[key], key) for key in payload}
    if result["account_id"] != account_id:
        raise OrderError(403, "ACCOUNT_MISMATCH", "account_id does not match configured account")
    return result


def capabilities():
    return {"order_types": ["SINGLE", "BASKET"], "sides": ["BUY", "SELL"],
            "sizing_types": {"SINGLE": ["QUANTITY", "AMOUNT"], "BASKET": ["QUANTITY"]},
            "optional_order_fields": ["strategy_id", "submit_before"],
            "supported_symbol_prefixes": {"stock": {key: list(value) for key, value in _contracts_stock_prefixes.items()},
                                          "etf": {key: list(value) for key, value in _contracts_etf_prefixes.items()}},
            "symbol_prefixes_do_not_prove_listing": True,
            "quantity_range": [1, _contracts_max_qmt_int],
            "executions": {"DIRECT": {"required": ["type"]},
                           "SLICED": {"required": ["type", "mode", "params"],
                                      "modes": ["ALGO", "RANDOM"],
                                      "required_params": list(_contracts_sliced_fields),
                                      "conditional_rules": [
                                          "ValidTimeType=0: positive ValidTimeElapse, zero start/end",
                                          "ValidTimeType=1: ordered positive Unix-second start/end, zero elapse",
                                          "UseTrigger=0: zero TriggerType/TriggerPrice",
                                          "UseTrigger=1: TriggerType 1/2 and positive TriggerPrice",
                                          "UndealtEntrustRule uses documented prType values; basket and exchange limits apply"],
                                      "param_schema": {key: {"required": True,
                                                            "type": "integer" if key in _contracts_sliced_int else "number",
                                                            "description": _contracts_sliced_descriptions[key],
                                                            "unit": _contracts_sliced_units[key],
                                                            "range": list(_contracts_sliced_ranges[key]),
                                                            "enum": list(_contracts_sliced_enum[key]) if key in _contracts_sliced_enum else None}
                                                       for key in _contracts_sliced_fields}},
                           "SMART": {"required": ["type", "algorithm", "start_at", "end_at", "params"],
                                     "reserved_params": ["m_strCmdRemark"]}},
            "price_types": {"LIMIT": {"required": ["limit_price"], "basket": False},
                            "QUOTE": {"required": ["quote_type"], "values": sorted(_contracts_quote)},
                            "MARKET": {"required": ["market_type"], "smart_required": [],
                                       "market_types": {key: list(value) for key, value in _contracts_market.items()},
                                       "protection_price_required": ["SH", "BJ"], "basket": "SMART only"}},
            "sliced": {"modes": ["ALGO", "RANDOM"], "required_params": list(_contracts_sliced_fields),
                       "percent_params": ["PriceRangeRate", "SuperPriceRate", "VolumeRate"]},
            "smart": {"required": ["algorithm", "start_at", "end_at", "params"],
                      "percent_params": "QMT unit % fields use decimal [0,1]", "reserved": ["m_strCmdRemark"]},
            "quantity_unit": "shares or ETF units; no multiplication", "amount_unit": "CNY, stocks only"}
