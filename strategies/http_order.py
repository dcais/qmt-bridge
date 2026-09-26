# -*- coding: gbk -*-
# Last modified (Asia/Shanghai): 2026-09-26 09:50:50

# ---- order_bridge/common.py ----
# QMT ORDER 运行参数（策略编辑器右侧“参数设置”，修改后停止并重新运行策略）
# 参数名       默认值           用途
# account_id   "66027616"       绑定的股票账户；建议使用字符串，保留账户号前导零。
# http_port    8888             HTTP 监听端口，整数 1..65535；模拟盘和实盘可用不同端口。
# pg_host      "127.0.0.1"      PostgreSQL 服务器地址。
# pg_port      5432             PostgreSQL 端口，整数 1..65535。
# pg_database  未配置           数据库名称；启用交易必填，模拟盘/实盘分别连接不同数据库。
# pg_user      未配置           数据库用户名；启用交易接口时必填。
# pg_password  未配置           数据库密码；启用交易接口时必填，不打印或通过 HTTP 返回。
# 上述参数均优先读取小写名称，也兼容同名全大写参数；小写值非法时不回退。
# pg_database、pg_user、pg_password 全部未配置或为空时，仅开放查询模式；启用交易须完整填写。
# 每个数据库内部固定使用 qmt_order schema，不接受 pg_schema 运行参数。
# DDL 独立存放在 sql/order_v1.sql；tools/order_admin.py schema init 显式读取并安装。
# 策略启动检查关键表、字段和版本，再按 account_id 自动补齐账户运行记录；已有记录不重置。
# 不自动建表或升级，无需在部署前手工注册账户。
# 不同数据库隔离订单与幂等记录，http_port 不参与幂等身份。
# HTTP_HOST 固定为 127.0.0.1，交易账户类型固定为 STOCK；日志和队列设置是代码常量。
#
"""订单模块共用类型。此处不连接数据库，也不调用 QMT。"""
import copy
import datetime as dt
import hashlib
import json
import re
import uuid
from decimal import Decimal


class OrderError(Exception):
    def __init__(self, status, code, message):
        super(OrderError, self).__init__(message)
        self.status = status
        self.code = code
        self.message = message


def utc_now():
    return dt.datetime.now(dt.timezone.utc)


def iso_datetime(value=None):
    return (value or utc_now()).astimezone(dt.timezone.utc).isoformat()


def parse_timestamp(value):
    if not isinstance(value, str):
        raise OrderError(400, "INVALID_TIMESTAMP", "timestamp must include a timezone")
    match = re.match(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d{1,6})?(Z|[+-]\d\d:\d\d)$", value)
    if not match:
        raise OrderError(400, "INVALID_TIMESTAMP", "use ISO 8601 with timezone")
    zone = "+0000" if match.group(3) == "Z" else match.group(3).replace(":", "")
    text = match.group(1) + (match.group(2) or "") + zone
    try:
        return dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%S" + (".%f" if match.group(2) else "") + "%z").astimezone(dt.timezone.utc)
    except ValueError:
        raise OrderError(400, "INVALID_TIMESTAMP", "invalid timestamp")


def json_text(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True,
                      separators=(",", ":"), default=lambda x: str(x) if isinstance(x, Decimal) else (_ for _ in ()).throw(TypeError("not JSON serializable")))


def fingerprint(value):
    return hashlib.sha256(json_text(value).encode("utf-8")).hexdigest()


def copy_json(value):
    return copy.deepcopy(value)


def new_order_document(request):
    order_id = str(uuid.uuid4())
    token = "qb" + uuid.uuid4().hex[:20]
    now = iso_datetime()
    single = request["order_type"] == "SINGLE"
    members = [{"item_id": "single", "symbol": request["symbol"], "side": request["side"],
                "quantity": request.get("quantity"), "amount": request.get("amount")}] if single else copy_json(request["items"])
    for member in members:
        member.update({"requested_quantity": member.get("quantity"), "requested_amount": member.get("amount"),
                       "filled_quantity": 0, "filled_amount": "0", "open_quantity": 0,
                       "cancelled_quantity": 0, "execution_status": "NOT_STARTED", "error": None})
    return {"order_id": order_id, "client_order_id": request["client_order_id"],
            "account_id": request["account_id"], "account_type": "STOCK", "order_type": request["order_type"],
            "request": copy_json(request), "request_hash": fingerprint(request), "contract_version": 1,
            "resolved_request": None, "remark": token, "basket_name": None if single else token,
            "basket_state": "NONE" if single else "PENDING", "basket_cleanup_eligible": False,
            "submission_status": "QUEUED", "execution_status": "NOT_STARTED", "cancel_status": "NONE",
            "cancel_requested": False, "active_cancel_request_id": None, "version": 1,
            "created_at": now, "updated_at": now, "submit_before": request.get("submit_before"),
            "requested_quantity": request.get("quantity") if single else None,
            "requested_amount": request.get("amount") if single else None,
            "filled_quantity": 0 if single else None, "filled_amount": "0" if single else None,
            "open_quantity": 0 if single else None, "cancelled_quantity": 0 if single else None,
            "items": members, "qmt_tasks": [], "qmt_orders": [], "fills": [], "attempts": [],
            "cancel_requests": [], "sync_status": "PENDING", "last_reconciled_at": None,
            "reconcile_requested": False, "error": None}


def public_order(document, replayed=None):
    result = copy_json(document)
    for key in ("request_hash", "remark", "contract_version", "attempts", "reconcile_requested", "manual_resolutions"):
        result.pop(key, None)
    if replayed is not None:
        result["replayed"] = replayed
    return result

# ---- order_bridge/contracts.py ----
"""HTTP ORDER 输入合同；不在此层调用 QMT。"""
import re
import math
import sys
from decimal import Decimal, InvalidOperation



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

# ---- order_bridge/state.py ----
"""纯订单事实归并；所有调用副作用由持久库和运行时负责。"""
from decimal import Decimal, InvalidOperation


state_ORDER_STATUS = {48: 'WORKING', 49: 'WORKING', 50: 'WORKING',
                      51: 'WORKING', 52: 'PARTIALLY_FILLED', 53: 'PARTIALLY_CANCELLED',
                      54: 'CANCELLED', 55: 'PARTIALLY_FILLED', 56: 'FILLED', 57: 'REJECTED'}
state_TASK_STATUS = {0: 'UNKNOWN', 1: 'WAITING', 2: 'SUBMITTING', 3: 'RUNNING',
                     4: 'PAUSED', 5: 'CANCELLING', 6: 'CANCELLING', 7: 'COMPLETED',
                     8: 'CANCELLED', 9: 'REJECTED', 10: 'STOPPED', 11: 'DROPPED', 12: 'STOPPED'}
state_TERMINAL = {'FILLED', 'CANCELLED', 'PARTIALLY_CANCELLED', 'REJECTED'}
state_TASK_TERMINAL = {'COMPLETED', 'CANCELLED', 'REJECTED', 'STOPPED', 'DROPPED'}
state_CANCEL_ACTIVE = {'REQUESTED', 'WAITING_QMT_ID', 'PENDING', 'UNKNOWN'}


def state_value(raw, *keys):
    for key in keys:
        if raw.get(key) not in (None, ''):
            return raw[key]
    return None


def state_id(value):
    return str(value) if value not in (None, '', 0, '0', -1, '-1') else None


def state_number(raw, *keys):
    value = state_value(raw, *keys)
    try:
        return max(0, int(value or 0))
    except (ValueError, TypeError):
        return 0


def state_status(value, mapping):
    try:
        return mapping.get(int(value), 'UNKNOWN')
    except (TypeError, ValueError):
        return str(value).upper() if value else 'UNKNOWN'


def observation_identifiers(kind, raw):
    market = state_value(raw, 'market', 'm_strExchangeID')
    market = {'SSE': 'SH', 'SZSE': 'SZ', 'SHSE': 'SH'}.get(str(market).upper(), market)
    symbol = state_value(raw, 'symbol', 'm_strInstrumentID', 'm_stockCode')
    if symbol and '.' in str(symbol):
        market = market or str(symbol).rsplit('.', 1)[1]
    elif symbol and market:
        symbol = str(symbol) + '.' + str(market)
    side = state_value(raw, 'side', 'm_nOffsetFlag')
    side = {48: 'BUY', 49: 'SELL', 23: 'BUY', 24: 'SELL', '48': 'BUY', '49': 'SELL',
            '23': 'BUY', '24': 'SELL'}.get(side, str(side).upper() if side is not None else None)
    if side is None:
        # CTaskDetail 使用 EOperationType，其18/19与passorder的23/24不同。
        side = {18: 'BUY', 19: 'SELL', '18': 'BUY', '19': 'SELL'}.get(raw.get('m_eOperationType'))
    day = state_value(raw, 'trading_day', 'm_strTradingDay', 'm_strTradeDate', 'm_strInsertDate')
    return {'remark': state_value(raw, 'remark', 'm_strRemark', 'userOrderId'),
            'qmt_order_id': state_id(state_value(raw, 'qmt_order_id', 'm_strOrderSysID')),
            'qmt_task_id': state_id(state_value(raw, 'qmt_task_id', 'm_nTaskId', 'm_nTaskID')),
            'trade_id': state_id(state_value(raw, 'trade_id', 'm_strTradeID')),
            'trading_day': str(day) if day is not None else None,
            'market': market, 'symbol': symbol, 'side': side,
            'account_id': state_value(raw, 'account_id', 'm_strAccountID')}


def state_item(doc, identifiers):
    candidates = [item for item in doc['items'] if item['symbol'] == identifiers.get('symbol')
                  and item['side'] == identifiers.get('side')]
    return candidates[0] if len(candidates) == 1 else None


def state_evidence(doc, kind, raw, source, reason, observed_at):
    evidence = doc.setdefault('unassociated_evidence', [])
    key = fingerprint({'kind': kind, 'raw': raw, 'reason': reason})
    if not any(row['evidence_id'] == key for row in evidence):
        evidence.append({'evidence_id': key, 'kind': kind, 'raw': copy_json(raw),
                         'source': source, 'reason': reason, 'observed_at': observed_at})
    doc['reconciliation_complete'] = False
    doc['sync_status'] = 'INCOMPLETE'


def apply_observation(doc, kind, raw, source, observed_at=None):
    before = copy_json(doc)
    stamp = observed_at or iso_datetime()
    ids = observation_identifiers(kind, raw)
    if kind == 'error':
        # 错误回调不能证明先前不确定调用未进入交易系统。
        doc['error'] = {'raw': copy_json(raw), 'source': source, 'observed_at': stamp}
        if doc['submission_status'] in ('QUEUED', 'SUBMITTING') and not doc['qmt_orders'] and not doc['qmt_tasks']:
            doc['submission_status'] = 'REJECTED'
    elif kind == 'deal':
        key = [ids['trading_day'], ids['market'], ids['trade_id']]
        item = state_item(doc, ids)
        if not all(key) or item is None:
            state_evidence(doc, kind, raw, source, 'MISSING_FILL_IDENTITY_OR_ITEM', stamp)
        else:
            previous = next((row for row in doc['fills'] if row['fill_key'] == key), None)
            quantity = state_number(raw, 'quantity', 'filled_quantity', 'm_nVolume')
            amount = state_value(raw, 'amount', 'filled_amount', 'm_dTradeAmount')
            try:
                amount = Decimal(str(amount))
                valid_amount = amount.is_finite() and amount >= 0
            except (InvalidOperation, ValueError):
                valid_amount = False
            if not valid_amount or quantity <= 0:
                state_evidence(doc, kind, raw, source, 'MISSING_FILL_ECONOMICS', stamp)
            elif previous is None:
                record = dict(ids, fill_key=key, item_id=item['item_id'], quantity=quantity,
                              amount=str(Decimal(str(amount))), raw=copy_json(raw), source=source, observed_at=stamp)
                doc['fills'].append(record)
                doc['submission_status'] = 'CONFIRMED'
                doc['reconciliation_complete'] = False
            elif previous['quantity'] != quantity or Decimal(previous['amount']) != Decimal(str(amount)) or previous['item_id'] != item['item_id']:
                state_evidence(doc, kind, raw, source, 'CONFLICTING_FILL_IDENTITY', stamp)
    elif kind in ('order', 'task'):
        id_name = 'qmt_order_id' if kind == 'order' else 'qmt_task_id'
        collection = doc['qmt_orders'] if kind == 'order' else doc['qmt_tasks']
        if not ids[id_name]:
            state_evidence(doc, kind, raw, source, 'MISSING_QMT_ID', stamp)
        else:
            # 委托号可能跨交易日复用；缺日期的已关联记录仅在唯一时补齐。
            matching = [row for row in collection if row[id_name] == ids[id_name]
                        and (not ids['trading_day'] or not row.get('trading_day') or row['trading_day'] == ids['trading_day'])
                        and (not ids['market'] or not row.get('market') or row['market'] == ids['market'])]
            if len(matching) > 1:
                state_evidence(doc, kind, raw, source, 'AMBIGUOUS_QMT_ID', stamp)
            else:
                record = matching[0] if matching else dict(ids)
                if not matching:
                    collection.append(record)
                old_status = record.get('status', 'UNKNOWN')
                status = state_status(state_value(raw, 'status', 'execution_status', 'm_nOrderStatus') if kind == 'order'
                                      else state_value(raw, 'status', 'task_status', 'm_eStatus', 'm_nTaskStatus'),
                                      state_ORDER_STATUS if kind == 'order' else state_TASK_STATUS)
                terminal = state_TERMINAL if kind == 'order' else state_TASK_TERMINAL
                # 终态可由真实成交修正，旧的活动快照不能令已撤委托复活。
                if old_status in terminal and status not in terminal:
                    status = old_status
                if old_status == 'FILLED':
                    status = 'FILLED'
                previous_record = copy_json(record)
                record.update({key: value for key, value in ids.items() if value is not None})
                record.update(status=status, terminal=status in terminal)
                if kind == 'order':
                    record['quantity'] = max(record.get('quantity', 0), state_number(raw, 'quantity', 'm_nVolumeTotalOriginal'))
                    record['filled_quantity'] = max(record.get('filled_quantity', 0), state_number(raw, 'filled_quantity', 'm_nVolumeTraded'))
                    item = state_item(doc, record)
                    record['item_id'] = item['item_id'] if item else None
                    if not item:
                        state_evidence(doc, kind, raw, source, 'MISSING_ITEM_MAPPING', stamp)
                    if str(raw.get('m_nOrderSubmitStatus')) == '53' and not record['terminal']:
                        for attempt in doc['attempts']:
                            if (attempt.get('kind') == 'CANCEL_ORDER'
                                    and str(attempt.get('target_id')) == ids['qmt_order_id']
                                    and attempt.get('cancel_request_id') == doc.get('active_cancel_request_id')):
                                attempt['status'] = 'REJECTED'
                                attempt['rejection_evidence'] = copy_json(raw)
                semantic_changed = previous_record != record
                if semantic_changed:
                    record.update(raw=copy_json(raw), source=source, observed_at=stamp)
                    doc['reconciliation_complete'] = False
                doc['submission_status'] = 'CONFIRMED'
    else:
        raise ValueError('unsupported observation kind: ' + str(kind))
    recompute_order(doc)
    return doc != before


def state_algorithm(doc):
    request = doc.get('request', {})
    return bool(doc.get('qmt_tasks') or request.get('execution', {}).get('type') in ('SLICED', 'SMART'))


def state_execution_finished(doc):
    # 未达目标可以是完整的结束事实；同步缺口则绝不能据此清理。
    if (doc.get('submission_status') != 'CONFIRMED' or not doc.get('reconciliation_complete')
            or doc.get('sync_status') != 'COMPLETE' or doc.get('unassociated_evidence')):
        return False
    tasks, orders = doc['qmt_tasks'], doc['qmt_orders']
    if not (tasks or orders) or (state_algorithm(doc) and not tasks):
        return False
    if any(not row.get('terminal') for row in tasks + orders):
        return False
    return doc['execution_status'] in state_TERMINAL or doc['execution_status'] == 'INCOMPLETE'


def recompute_order(doc, now=None, reconciled=False):
    before = copy_json(doc)
    if reconciled:
        doc['reconciliation_complete'] = True
    if doc['submission_status'] == 'QUEUED' and doc.get('submit_before'):
        current = parse_timestamp(now) if isinstance(now, str) else (now or utc_now())
        if current >= parse_timestamp(doc['submit_before']):
            doc['submission_status'] = 'EXPIRED'
    identity_complete = all(row.get('trading_day') and row.get('market') and row.get('item_id')
                            for row in doc['qmt_orders'])
    complete = bool(doc.get('reconciliation_complete')) and not doc.get('unassociated_evidence') and identity_complete
    tasks = doc['qmt_tasks']
    tasks_stopped = bool(tasks) and all(row.get('terminal') for row in tasks)
    algorithm = state_algorithm(doc)
    producer_stopped = tasks_stopped if algorithm else True
    local = doc['submission_status'] in ('CANCELLED_LOCAL', 'EXPIRED', 'REJECTED') and not doc['qmt_orders'] and not doc['fills']
    fill_gap = False
    for item in doc['items']:
        orders = [row for row in doc['qmt_orders'] if row.get('item_id') == item['item_id']]
        fills = [row for row in doc['fills'] if row.get('item_id') == item['item_id']]
        item['filled_quantity'] = sum(row['quantity'] for row in fills)
        item['filled_amount'] = str(sum((Decimal(row['amount']) for row in fills), Decimal(0)))
        open_qty, cancelled_qty = 0, 0
        all_orders_filled = bool(orders)
        for row in orders:
            actual = sum(fill['quantity'] for fill in fills if fill.get('qmt_order_id') == row['qmt_order_id']
                         and (not row.get('trading_day') or fill['trading_day'] == row['trading_day'])
                         and (not row.get('market') or fill['market'] == row['market']))
            traded = max(row.get('filled_quantity', 0), actual)
            fill_gap = (fill_gap or traded > actual
                        or (row.get('status') == 'FILLED' and (not row.get('quantity') or actual < row['quantity'])))
            if row.get('quantity') and actual >= row['quantity']:
                row.update(status='FILLED', terminal=True)
            all_orders_filled = (all_orders_filled and row.get('status') == 'FILLED'
                                 and row.get('quantity', 0) > 0 and actual >= row['quantity'])
            remaining = max(0, row.get('quantity', 0) - traded)
            if row['status'] in ('CANCELLED', 'PARTIALLY_CANCELLED'):
                cancelled_qty += remaining
            elif not row.get('terminal'):
                open_qty += remaining
        item['open_quantity'] = open_qty
        item['cancelled_quantity'] = cancelled_qty
        qty = item.get('requested_quantity')
        filled = item['filled_quantity']
        terminal_orders = bool(orders) and all(row.get('terminal') for row in orders)
        # 达到目标不意味着算法停止，也不意味着超量/迟到子委托已无风险。
        if qty is not None and filled >= qty and producer_stopped and not any(not row.get('terminal') for row in orders):
            status = 'FILLED'
        elif (qty is None and item.get('requested_amount') is not None and not algorithm
              and doc.get('request', {}).get('execution', {}).get('type') == 'DIRECT'
              and complete and all_orders_filled):
            # 金额单的整手余款不要求成交额等于预算；须由每笔委托及稳定成交共同证明已成。
            status = 'FILLED'
        elif local:
            status = 'REJECTED' if doc['submission_status'] == 'REJECTED' else 'NOT_STARTED'
        elif terminal_orders and producer_stopped and complete:
            if cancelled_qty:
                status = 'PARTIALLY_CANCELLED' if filled else 'CANCELLED'
            elif all(row['status'] == 'REJECTED' for row in orders):
                status = 'REJECTED'
            else:
                status = 'INCOMPLETE'
        elif open_qty:
            status = 'PARTIALLY_FILLED' if filled else 'WORKING'
        elif terminal_orders or tasks_stopped:
            status = 'INCOMPLETE'
        elif filled:
            status = 'PARTIALLY_FILLED'
        elif orders or tasks:
            status = 'WORKING' if any(row.get('status') != 'UNKNOWN' for row in orders + tasks) else 'UNKNOWN'
        else:
            status = 'UNKNOWN' if doc['submission_status'] in ('UNKNOWN', 'CONFIRMED') else 'NOT_STARTED'
        item['execution_status'] = status
    if fill_gap:
        complete = False
    statuses = [item['execution_status'] for item in doc['items']]
    if all(status == 'FILLED' for status in statuses):
        execution = 'FILLED'
    elif len(statuses) == 1:
        execution = statuses[0]
    elif all(status in state_TERMINAL for status in statuses):
        execution = 'PARTIALLY_CANCELLED' if any(item['filled_quantity'] for item in doc['items']) else ('REJECTED' if all(s == 'REJECTED' for s in statuses) else 'CANCELLED')
    elif 'WORKING' in statuses or 'PARTIALLY_FILLED' in statuses:
        execution = 'PARTIALLY_FILLED' if any(item['filled_quantity'] for item in doc['items']) else 'WORKING'
    elif 'INCOMPLETE' in statuses:
        execution = 'INCOMPLETE'
    else:
        execution = 'UNKNOWN' if 'UNKNOWN' in statuses else 'NOT_STARTED'
    if fill_gap or doc.get('unassociated_evidence'):
        execution = 'INCOMPLETE'
    doc['execution_status'] = execution
    if doc['order_type'] == 'SINGLE':
        for field in ('filled_quantity', 'filled_amount', 'open_quantity', 'cancelled_quantity'):
            doc[field] = doc['items'][0][field]
    if doc.get('cancel_requested'):
        attempts = [row for row in doc['attempts'] if row.get('kind') in ('CANCEL_ORDER', 'CANCEL_TASK')
                    and row.get('cancel_request_id') == doc.get('active_cancel_request_id')]
        unresolved_attempts = [attempt for attempt in attempts if any(
            not row.get('terminal') and str(row.get('qmt_order_id' if attempt['kind'] == 'CANCEL_ORDER' else 'qmt_task_id')) == str(attempt.get('target_id'))
            for row in (doc['qmt_orders'] if attempt['kind'] == 'CANCEL_ORDER' else tasks))]
        live = any(not row.get('terminal') for row in doc['qmt_orders'])
        if doc['submission_status'] == 'CANCELLED_LOCAL':
            cancel = 'CONFIRMED'
        elif local:
            cancel = 'NOT_NEEDED'
        elif execution == 'FILLED' and producer_stopped and not live and complete:
            cancel = 'NOT_NEEDED'
        elif producer_stopped and not live and complete and (doc['qmt_orders'] or tasks):
            cancel = 'CONFIRMED'
        elif any(row.get('status') == 'UNKNOWN' for row in unresolved_attempts):
            cancel = 'UNKNOWN'
        elif unresolved_attempts and all(row.get('status') == 'REJECTED' for row in unresolved_attempts):
            cancel = 'REJECTED'
        elif not doc['qmt_orders'] and not tasks:
            cancel = 'WAITING_QMT_ID'
        else:
            cancel = 'PENDING' if attempts else 'REQUESTED'
        doc['cancel_status'] = cancel
        for request in doc['cancel_requests']:
            if request['canonical_cancel_request_id'] == doc.get('active_cancel_request_id'):
                request['status'] = cancel
    doc['sync_status'] = 'COMPLETE' if complete else ('PENDING' if doc['submission_status'] == 'QUEUED' and not doc.get('unassociated_evidence') else 'INCOMPLETE')
    doc['basket_cleanup_eligible'] = doc['order_type'] != 'SINGLE' and state_execution_finished(doc)
    return before != doc


def apply_cancel_request(doc, request):
    cancel_id = request['cancel_request_id']
    digest = fingerprint(request)
    previous = next((row for row in doc['cancel_requests'] if row['cancel_request_id'] == cancel_id), None)
    if previous:
        if previous['request_hash'] != digest:
            raise OrderError(409, 'CANCEL_ID_CONFLICT', 'cancel_request_id already has a different request')
        return copy_json(previous), 200
    recompute_order(doc)
    canonical = doc.get('active_cancel_request_id') if doc['cancel_status'] in state_CANCEL_ACTIVE else cancel_id
    record = {'cancel_request_id': cancel_id, 'canonical_cancel_request_id': canonical,
              'request_hash': digest, 'request': copy_json(request), 'created_at': iso_datetime(), 'status': 'REQUESTED'}
    doc['cancel_requests'].append(record)
    doc.update(cancel_requested=True, active_cancel_request_id=canonical)
    if doc['submission_status'] == 'QUEUED':
        doc['submission_status'] = 'CANCELLED_LOCAL'
    recompute_order(doc)
    return copy_json(record), 200 if record['status'] in ('CONFIRMED', 'NOT_NEEDED') else 202


def cancel_response(doc, cancel_request_id, replayed=False):
    record = next((row for row in doc['cancel_requests'] if row['cancel_request_id'] == cancel_request_id), None)
    if record is None:
        raise OrderError(404, 'CANCEL_NOT_FOUND', 'cancel request does not exist')
    result = copy_json(record)
    result.pop('request_hash', None)
    result.update(order_id=doc['order_id'], client_order_id=doc['client_order_id'],
                  account_id=doc['account_id'], submission_status=doc['submission_status'],
                  execution_status=doc['execution_status'], cancel_status=record['status'], replayed=replayed)
    return result


def pending_cancellations(doc):
    if not doc.get('cancel_requested') or doc['submission_status'] == 'CANCELLED_LOCAL':
        return []
    request_id = doc.get('active_cancel_request_id')
    tasks = doc['qmt_tasks']
    if state_algorithm(doc) and not tasks:
        return []
    live_tasks = [row for row in tasks if not row.get('terminal')]
    targets = [('CANCEL_TASK', row['qmt_task_id'], row) for row in live_tasks]
    if not live_tasks:
        targets = [('CANCEL_ORDER', row['qmt_order_id'], row) for row in doc['qmt_orders'] if not row.get('terminal')]
    actions = []
    for kind, target, row in targets:
        attempts = [attempt for attempt in doc['attempts'] if attempt.get('kind') == kind and str(attempt.get('target_id')) == str(target)]
        # 返回成功只是调用确认；等待真实回报，绝不对UNKNOWN盲重试。
        if any(attempt.get('status') in ('CALLING', 'RETURNED', 'UNKNOWN', 'CONFIRMED') for attempt in attempts):
            continue
        # 停止门闩证明未进入QMT的调用可以续派；明确失败仍要求新撤单ID。
        if any(attempt.get('cancel_request_id') == request_id and attempt.get('status') != 'ABORTED_NO_CALL'
               for attempt in attempts):
            continue
        actions.append({'kind': kind, 'target_id': target, 'cancel_request_id': request_id,
                        'market': row.get('market'), 'trading_day': row.get('trading_day')})
    return actions


def is_order_active(doc):
    if doc['submission_status'] == 'QUEUED':
        return True
    if doc['submission_status'] in ('CANCELLED_LOCAL', 'EXPIRED', 'REJECTED') and not doc['qmt_orders'] and not doc['qmt_tasks']:
        return False
    return not state_execution_finished(doc)


def mark_reconciled(doc, complete=True, now=None):
    before = copy_json(doc)
    doc['reconciliation_complete'] = bool(complete)
    doc['last_reconciled_at'] = now if isinstance(now, str) else iso_datetime(now)
    doc['reconcile_requested'] = False
    recompute_order(doc, now=now)
    return before != doc

# ---- order_bridge/repository.py ----
"""PostgreSQL 订单事实库；事务头锁同时保护投影和连续事件游标。"""
import hashlib
import json
import re
import threading
import uuid



repo_SCHEMA_VERSION = 1
repo_CHILD_TABLES = {"order_items": "items", "execution_attempts": "attempts",
                     "cancel_requests": "cancel_requests", "qmt_tasks": "qmt_tasks",
                     "qmt_orders": "qmt_orders", "fills": "fills"}


def repo_schema(value):
    if not isinstance(value, str) or not re.match(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$", value):
        raise OrderError(503, "INVALID_PERSISTENCE_CONFIG", "invalid PostgreSQL schema identifier")
    return '"' + value + '"'


def repo_json(value):
    return json.loads(value) if isinstance(value, str) else copy_json(value)


def repo_error(code="PERSISTENCE_UNAVAILABLE"):
    # 驱动异常可能包含连接串和口令，不向调用方透出原始错误。
    return OrderError(503, code, "PostgreSQL persistence unavailable" if code != "PERSISTENCE_OUTCOME_UNKNOWN"
                      else "transaction commit outcome unknown; reconcile using original request identifier")


class PostgresRepository(object):
    def __init__(self, config, account_id, account_type="STOCK", connect_factory=None):
        self.config = dict(config)
        self.account_id = str(account_id)
        self.account_type = str(account_type)
        self.repo_scope = (self.account_type, self.account_id)
        self.repo_s = repo_schema(config.get("pg_schema", "qmt_order"))
        self.repo_connect_factory = connect_factory
        self.repo_executor = None
        self.repo_executor_epoch = None
        self.repo_executor_instance = None
        self.repo_executor_lost = False
        self.repo_executor_mutex = threading.RLock()
        # PostgreSQL advisory lock 本身已隔离数据库，不把可别名的连接参数混入锁键。
        lock_scope = [config.get("pg_schema", "qmt_order"),
                      self.account_type, self.account_id]
        self.repo_lock_key = int.from_bytes(hashlib.sha256(json_text(lock_scope).encode("utf-8")).digest()[:8],
                                           byteorder="big", signed=True)

    def repo_connect(self):
        try:
            factory = self.repo_connect_factory
            if factory is None:
                from pg8000 import dbapi
                factory = dbapi.connect
            conn = factory(host=self.config.get("pg_host", "127.0.0.1"),
                           port=int(self.config.get("pg_port", 5432)),
                           database=self.config.get("pg_database", "postgres"),
                           user=self.config.get("pg_user", "postgres"),
                           password=self.config.get("pg_password", ""),
                           timeout=float(self.config.get("pg_connect_timeout", 3)))
            cur = conn.cursor()
            cur.execute("SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)",
                        (str(int(self.config.get("pg_statement_timeout_ms", 2000))),
                         str(int(self.config.get("pg_lock_timeout_ms", 1000)))))
            conn.commit()
            cur.close()
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            raise repo_error() from None

    def repo_run(self, callback, mutation=False):
        conn = self.repo_connect()
        try:
            cur = conn.cursor()
            if mutation:
                cur.execute("SELECT event_seq FROM " + self.repo_s +
                            ".account_runtime WHERE account_type=%s AND account_id=%s FOR UPDATE", self.repo_scope)
                if cur.fetchone() is None:
                    raise OrderError(503, "SCHEMA_NOT_READY", "account runtime record is missing; restart the bridge")
            result = callback(cur)
            try:
                conn.commit()
            except Exception:
                raise repo_error("PERSISTENCE_OUTCOME_UNKNOWN") from None
            return result
        except OrderError:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise repo_error() from None
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def check_schema(self):
        """只检查既有表结构和版本；账户记录由策略启动时自动补齐。"""
        def check(cur):
            required = {
                "schema_version": ("version",),
                "account_runtime": ("account_type", "account_id", "event_seq", "executor_host", "executor_instance", "executor_epoch"),
                "orders": ("account_type", "account_id", "order_id", "client_order_id", "request_hash", "remark", "active", "document"),
                "order_events": ("account_type", "account_id", "event_seq", "order_id", "event_type", "occurred_at", "document"),
                "qmt_observations": ("observation_id", "account_type", "account_id", "kind", "source", "observed_at", "raw", "order_id", "applied", "observation_hash"),
            }
            required.update({table: ("account_type", "account_id", "order_id", "record_id", "document")
                             for table in repo_CHILD_TABLES})
            cur.execute("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema=%s",
                        (self.config.get("pg_schema", "qmt_order"),))
            existing = {}
            for table, column in cur.fetchall():
                existing.setdefault(table, set()).add(column)
            for table, columns in required.items():
                if table not in existing:
                    raise OrderError(503, "SCHEMA_NOT_READY", "missing required table: " + table)
                missing = sorted(set(columns) - existing[table])
                if missing:
                    raise OrderError(503, "SCHEMA_NOT_READY", "missing required columns in " + table + ": " + ", ".join(missing))
            cur.execute("SELECT version FROM " + self.repo_s + ".schema_version")
            if [row[0] for row in cur.fetchall()] != [repo_SCHEMA_VERSION]:
                raise OrderError(503, "SCHEMA_VERSION_MISMATCH", "unsupported order schema version")
            return {"schema_version": repo_SCHEMA_VERSION, "ready": True}
        return self.repo_run(check)

    def ensure_account_runtime(self):
        """按启动账户幂等插入运行记录；冲突时保留事件序号、主机和执行代次。"""
        def ensure(cur):
            cur.execute("INSERT INTO " + self.repo_s +
                        ".account_runtime(account_type,account_id) VALUES(%s,%s) "
                        "ON CONFLICT(account_type,account_id) DO NOTHING RETURNING account_id", self.repo_scope)
            return cur.fetchone() is not None
        # 首次运行尚无可锁的账户行，依靠主键和 ON CONFLICT 处理并发创建。
        return self.repo_run(ensure)

    def health(self):
        result = self.check_schema()
        result["executor"] = self.check_executor() if self.repo_executor is not None else False
        def unknown_count(cur):
            cur.execute("SELECT count(*) FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "AND document->>'submission_status'='UNKNOWN'", self.repo_scope)
            return cur.fetchone()[0]
        result["unknown_order_count"] = self.repo_run(unknown_count)
        return result

    def acquire_executor(self, instance_id, host_id):
        with self.repo_executor_mutex:
            if self.repo_executor_lost:
                raise OrderError(503, "EXECUTOR_LOCK_LOST", "executor lock was lost; restart required")
            if self.repo_executor is not None:
                self.check_executor()
                return {"epoch": self.repo_executor_epoch, "instance_id": self.repo_executor_instance}
            conn = self.repo_connect()
            try:
                cur = conn.cursor()
                cur.execute("SELECT pg_try_advisory_lock(%s)", (self.repo_lock_key,))
                if not cur.fetchone()[0]:
                    raise OrderError(503, "EXECUTOR_ALREADY_RUNNING", "another executor holds this account")
                cur.execute("SELECT executor_host,executor_epoch FROM " + self.repo_s +
                            ".account_runtime WHERE account_type=%s AND account_id=%s FOR UPDATE", self.repo_scope)
                row = cur.fetchone()
                if row is None:
                    raise OrderError(503, "SCHEMA_NOT_READY", "account runtime has not been initialized")
                if row[0] and row[0] != host_id:
                    raise OrderError(503, "EXECUTOR_HOST_MISMATCH", "account is bound to another QMT host")
                epoch = row[1] + 1
                cur.execute("UPDATE " + self.repo_s + ".account_runtime SET executor_host=%s,executor_instance=%s,"
                            "executor_epoch=%s WHERE account_type=%s AND account_id=%s",
                            (host_id, instance_id, epoch) + self.repo_scope)
                try:
                    conn.commit()
                except Exception:
                    raise repo_error("PERSISTENCE_OUTCOME_UNKNOWN") from None
                self.repo_executor = conn
                self.repo_executor_epoch = epoch
                self.repo_executor_instance = instance_id
                return {"epoch": epoch, "instance_id": instance_id, "host_id": host_id}
            except Exception as exc:
                try:
                    conn.close()
                except Exception:
                    pass
                if isinstance(exc, OrderError):
                    raise
                raise repo_error() from None

    def check_executor(self):
        with self.repo_executor_mutex:
            if self.repo_executor is None or self.repo_executor_lost:
                raise OrderError(503, "EXECUTOR_LOCK_LOST", "executor session is not available")
            try:
                cur = self.repo_executor.cursor()
                # 不再次获取 advisory lock，避免重入计数掩盖意外 unlock。
                key = self.repo_lock_key & ((1 << 64) - 1)
                cur.execute("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' "
                            "AND pid=pg_backend_pid() AND classid=%s::oid AND objid=%s::oid "
                            "AND objsubid=1 AND granted)", (key >> 32, key & 0xffffffff))
                held = cur.fetchone()[0]
                self.repo_executor.commit()
                cur.close()
                if not held:
                    raise RuntimeError("lock absent")
                return True
            except Exception:
                self.repo_executor_lost = True
                raise OrderError(503, "EXECUTOR_LOCK_LOST", "executor lock lost; automatic takeover disabled") from None

    def release_executor(self):
        with self.repo_executor_mutex:
            conn, self.repo_executor = self.repo_executor, None
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def close(self):
        self.release_executor()

    def repo_load(self, cur, value, by_client=False, lock=False):
        column = "client_order_id" if by_client else "order_id"
        cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s AND " +
                    column + "=%s" + (" FOR UPDATE" if lock else ""), self.repo_scope + (value,))
        row = cur.fetchone()
        return repo_json(row[0]) if row else None

    def repo_require(self, doc):
        if doc is None:
            raise OrderError(404, "ORDER_NOT_FOUND", "order not found")
        return doc

    def repo_save(self, cur, doc, event_type, fresh=False):
        if not fresh:
            doc["version"] = int(doc.get("version", 0)) + 1
        doc["updated_at"] = iso_datetime()
        cur.execute("INSERT INTO " + self.repo_s + ".orders(account_type,account_id,order_id,client_order_id,request_hash,"
                    "remark,active,document) VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb) "
                    "ON CONFLICT(account_type,account_id,order_id) DO UPDATE SET active=EXCLUDED.active,document=EXCLUDED.document",
                    self.repo_scope + (doc["order_id"], doc["client_order_id"], doc["request_hash"], doc["remark"],
                                       is_order_active(doc), json_text(doc)))
        for table, field in repo_CHILD_TABLES.items():
            cur.execute("DELETE FROM " + self.repo_s + "." + table +
                        " WHERE account_type=%s AND account_id=%s AND order_id=%s", self.repo_scope + (doc["order_id"],))
            for index, record in enumerate(doc.get(field, [])):
                keys = {"items": "item_id", "attempts": "attempt_id", "cancel_requests": "cancel_request_id",
                        "qmt_tasks": "qmt_task_id", "qmt_orders": "qmt_order_id", "fills": "trade_id"}
                record_id = str(record.get(keys[field]) or record.get("id") or index)
                if field in ("qmt_tasks", "qmt_orders", "fills"):
                    record_id = fingerprint([record.get("trading_day"), record.get("market"), record_id])
                cur.execute("INSERT INTO " + self.repo_s + "." + table +
                            "(account_type,account_id,order_id,record_id,document) VALUES(%s,%s,%s,%s,%s::jsonb)",
                            self.repo_scope + (doc["order_id"], record_id, json_text(record)))
        cur.execute("UPDATE " + self.repo_s + ".account_runtime SET event_seq=event_seq+1 "
                    "WHERE account_type=%s AND account_id=%s RETURNING event_seq", self.repo_scope)
        seq = cur.fetchone()[0]
        cur.execute("INSERT INTO " + self.repo_s + ".order_events(account_type,account_id,event_seq,order_id,event_type,"
                    "occurred_at,document) VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb)",
                    self.repo_scope + (seq, doc["order_id"], event_type, doc["updated_at"], json_text(doc)))
        return doc

    def accept_order(self, normalized):
        if str(normalized.get("account_id")) != self.account_id:
            raise OrderError(400, "ACCOUNT_MISMATCH", "request does not belong to this account")
        def accept(cur):
            doc = self.repo_load(cur, normalized["client_order_id"], by_client=True, lock=True)
            if doc:
                if doc["request_hash"] != fingerprint(normalized):
                    raise OrderError(409, "IDEMPOTENCY_CONFLICT", "client_order_id already has different request content")
                return False, doc
            doc = new_order_document(normalized)
            doc["account_type"] = self.account_type
            return True, self.repo_save(cur, doc, "ORDER_ACCEPTED", fresh=True)
        return self.repo_run(accept, mutation=True)

    def get_order(self, client_order_id):
        return self.repo_run(lambda cur: self.repo_require(self.repo_load(cur, client_order_id, by_client=True)))

    def get_by_id(self, order_id):
        return self.repo_run(lambda cur: self.repo_require(self.repo_load(cur, order_id)))

    def update_order(self, order_id, event_type, mutator):
        def update(cur):
            doc = self.repo_require(self.repo_load(cur, order_id, lock=True))
            before = json_text(doc)
            mutator(doc)
            return self.repo_save(cur, doc, event_type) if json_text(doc) != before else doc
        return self.repo_run(update, mutation=True)

    def claim_submission(self, order_id):
        self.check_executor()
        def claim(cur):
            self.check_executor()
            cur.execute("SELECT executor_instance,executor_epoch FROM " + self.repo_s +
                        ".account_runtime WHERE account_type=%s AND account_id=%s", self.repo_scope)
            if tuple(cur.fetchone()) != (self.repo_executor_instance, self.repo_executor_epoch):
                raise OrderError(503, "EXECUTOR_LOCK_LOST", "executor epoch changed")
            doc = self.repo_require(self.repo_load(cur, order_id, lock=True))
            if doc["submission_status"] != "QUEUED" or doc.get("cancel_requested"):
                return False, doc
            if doc.get("submit_before") and parse_timestamp(doc["submit_before"]) <= utc_now():
                doc["submission_status"] = "EXPIRED"
                return False, self.repo_save(cur, doc, "SUBMISSION_EXPIRED")
            doc["submission_status"] = "SUBMITTING"
            doc["attempts"].append({"attempt_id": str(uuid.uuid4()), "kind": "SUBMIT", "remark": doc["remark"],
                                    "status": "CALLING", "created_at": iso_datetime(),
                                    "executor_epoch": self.repo_executor_epoch})
            return True, self.repo_save(cur, doc, "SUBMISSION_CLAIMED")
        return self.repo_run(claim, mutation=True)

    def request_cancel(self, normalized):
        if normalized.get("account_id") is not None and str(normalized["account_id"]) != self.account_id:
            raise OrderError(400, "ACCOUNT_MISMATCH", "request does not belong to this account")
        def cancel(cur):
            cancel_id = normalized["cancel_request_id"]
            cur.execute("SELECT order_id,document FROM " + self.repo_s + ".cancel_requests "
                        "WHERE account_type=%s AND account_id=%s AND record_id=%s", self.repo_scope + (cancel_id,))
            row = cur.fetchone()
            if row:
                record = repo_json(row[1])
                if record.get("request_hash") != fingerprint(normalized):
                    raise OrderError(409, "IDEMPOTENCY_CONFLICT", "cancel_request_id already has different request content")
                doc = self.repo_require(self.repo_load(cur, row[0], lock=True))
                return record.get("http_status", 200), cancel_response(doc, cancel_id, replayed=True)
            doc = self.repo_require(self.repo_load(cur, normalized["client_order_id"], by_client=True, lock=True))
            record, status = apply_cancel_request(doc, normalized)
            record = next(row for row in doc["cancel_requests"] if row["cancel_request_id"] == cancel_id)
            record["request_hash"] = fingerprint(normalized)
            record["http_status"] = status
            self.repo_save(cur, doc, "CANCEL_REQUESTED")
            return status, cancel_response(doc, cancel_id)
        return self.repo_run(cancel, mutation=True)

    def list_orders(self, active=True, limit=100, cursor=None):
        limit = max(1, min(int(limit), 1000))
        def listing(cur):
            params = self.repo_scope
            sql = "SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s"
            if active:
                sql += " AND active=true"
            if cursor:
                sql += " AND order_id>%s"
                params += (str(cursor),)
            cur.execute(sql + " ORDER BY order_id LIMIT %s", params + (limit + 1,))
            docs = [repo_json(row[0]) for row in cur.fetchall()]
            return {"orders": docs[:limit], "has_more": len(docs) > limit,
                    "next_cursor": docs[limit - 1]["order_id"] if len(docs) > limit else None}
        return self.repo_run(listing)

    def events(self, after=0, limit=100):
        limit = max(1, min(int(limit), 1000))
        def listing(cur):
            cur.execute("SELECT event_seq,order_id,event_type,occurred_at,document FROM " + self.repo_s +
                        ".order_events WHERE account_type=%s AND account_id=%s AND event_seq>%s "
                        "ORDER BY event_seq LIMIT %s", self.repo_scope + (int(after), limit + 1))
            rows = cur.fetchall()
            events = []
            for row in rows[:limit]:
                doc = repo_json(row[4])
                events.append({"event_id": row[0], "order_id": row[1], "order_version": doc["version"],
                               "type": row[2], "recorded_at": row[3], "data": doc,
                               "event_seq": row[0], "event_type": row[2], "occurred_at": row[3], "order": doc})
            return {"events": events, "next_after": events[-1]["event_seq"] if events else int(after),
                    "has_more": len(rows) > limit}
        return self.repo_run(listing)

    def work_orders(self, limit=100):
        return self.list_orders(active=True, limit=limit)["orders"]

    def queued_orders(self, limit=10):
        def listing(cur):
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "AND document->>'submission_status'='QUEUED' ORDER BY document->>'created_at',order_id LIMIT %s",
                        self.repo_scope + (max(1, int(limit)),))
            return [repo_json(row[0]) for row in cur.fetchall()]
        return self.repo_run(listing)

    def cancellation_orders(self, limit=100):
        def listing(cur):
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "AND document->>'cancel_requested'='true' AND document->>'cancel_status' "
                        "NOT IN ('CONFIRMED','NOT_NEEDED','REJECTED') "
                        "ORDER BY CASE document->>'cancel_status' WHEN 'REQUESTED' THEN 0 "
                        "WHEN 'UNKNOWN' THEN 2 ELSE 1 END,"
                        "COALESCE(document->>'last_reconciled_at',''),order_id LIMIT %s",
                        self.repo_scope + (max(1, int(limit)),))
            return [repo_json(row[0]) for row in cur.fetchall()]
        return self.repo_run(listing)

    def reconcile_orders(self, limit=1000):
        # 历史终态也可能迟到成交；按最后核对时间轮转避免首页永久饥饿。
        def listing(cur):
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "ORDER BY COALESCE(document->>'last_reconciled_at',''),order_id LIMIT %s",
                        self.repo_scope + (max(1, int(limit)),))
            return [repo_json(row[0]) for row in cur.fetchall()]
        return self.repo_run(listing)

    def repo_match(self, cur, kind, raw):
        identifiers = observation_identifiers(kind, raw)
        if identifiers.get("account_id") and str(identifiers["account_id"]) != self.account_id:
            return None
        remark = identifiers.get("remark")
        if remark:
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s AND remark=%s FOR UPDATE",
                        self.repo_scope + (str(remark),))
            row = cur.fetchone()
            if row:
                return repo_json(row[0])
        matches = {}
        for field, table in (("qmt_task_id", "qmt_tasks"), ("qmt_order_id", "qmt_orders")):
            value = identifiers.get(field)
            if value is None or str(value) in ("", "0", "-1"):
                continue
            sql = "SELECT DISTINCT order_id FROM " + self.repo_s + "." + table + \
                  " WHERE account_type=%s AND account_id=%s AND document->>%s=%s"
            params = self.repo_scope + (field, str(value))
            for dimension in ("trading_day", "market"):
                if identifiers.get(dimension):
                    sql += " AND (document->>%s IS NULL OR document->>%s=%s)"
                    params += (dimension, dimension, str(identifiers[dimension]))
            cur.execute(sql, params)
            for row in cur.fetchall():
                matches[row[0]] = True
        if len(matches) == 1:
            doc = self.repo_load(cur, next(iter(matches)), lock=True)
            if remark and str(remark) != doc["remark"]:
                return None
            return doc
        return None

    def repo_apply_pending(self, cur):
        cur.execute("SELECT observation_id,kind,raw,source,observed_at FROM " + self.repo_s +
                    ".qmt_observations WHERE account_type=%s AND account_id=%s AND applied=false ORDER BY observation_id",
                    self.repo_scope)
        rows = cur.fetchall()
        # 新父记录可能建立关联，重复扫到固定点后停止。
        while rows:
            remaining = []
            for row in rows:
                doc = self.repo_match(cur, row[1], repo_json(row[2]))
                if doc is None:
                    remaining.append(row)
                    continue
                before = json_text(doc)
                apply_observation(doc, row[1], repo_json(row[2]), row[3], observed_at=row[4])
                if json_text(doc) != before:
                    self.repo_save(cur, doc, "QMT_OBSERVATION")
                cur.execute("UPDATE " + self.repo_s + ".qmt_observations SET order_id=%s,applied=true WHERE observation_id=%s",
                            (doc["order_id"], row[0]))
            if len(remaining) == len(rows):
                break
            rows = remaining

    def ingest_observation(self, kind, raw, source="callback"):
        def ingest(cur):
            digest = fingerprint({"kind": kind, "source": source, "raw": raw})
            cur.execute("SELECT order_id FROM " + self.repo_s + ".qmt_observations "
                        "WHERE account_type=%s AND account_id=%s AND observation_hash=%s", self.repo_scope + (digest,))
            previous = cur.fetchone()
            if previous is not None:
                return self.repo_load(cur, previous[0]) if previous[0] else None
            stamp = iso_datetime()
            cur.execute("INSERT INTO " + self.repo_s + ".qmt_observations(account_type,account_id,kind,source,observed_at,raw,observation_hash) "
                        "VALUES(%s,%s,%s,%s,%s,%s::jsonb,%s) RETURNING observation_id",
                        self.repo_scope + (kind, source, stamp, json_text(raw), digest))
            obs_id = cur.fetchone()[0]
            doc = self.repo_match(cur, kind, raw)
            if doc is None:
                return None
            before = json_text(doc)
            identities_before = self.repo_qmt_identities(doc)
            apply_observation(doc, kind, raw, source, observed_at=stamp)
            if json_text(doc) != before:
                self.repo_save(cur, doc, "QMT_OBSERVATION")
            cur.execute("UPDATE " + self.repo_s + ".qmt_observations SET order_id=%s,applied=true WHERE observation_id=%s",
                        (doc["order_id"], obs_id))
            # 外部回报不全扫历史积压；仅新增可关联身份时重放先到的子回报。
            if self.repo_qmt_identities(doc) != identities_before:
                self.repo_apply_pending(cur)
            return self.repo_load(cur, doc["order_id"])
        return self.repo_run(ingest, mutation=True)

    def repo_qmt_identities(self, doc):
        return {(field, str(row.get(field)), str(row.get("trading_day")), str(row.get("market")))
                for collection, field in (("qmt_orders", "qmt_order_id"), ("qmt_tasks", "qmt_task_id"))
                for row in doc.get(collection, [])}

    def recover(self):
        self.check_executor()
        def recovery(cur):
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "ORDER BY order_id FOR UPDATE", self.repo_scope)
            docs = [repo_json(row[0]) for row in cur.fetchall()]
            count = 0
            for doc in docs:
                before = json_text(doc)
                submitting = doc["submission_status"] == "SUBMITTING"
                if submitting:
                    doc["submission_status"] = "UNKNOWN"
                    doc["sync_status"] = "PENDING"
                for attempt in doc.get("attempts", []):
                    is_cancel = attempt.get("kind") in ("CANCEL", "CANCEL_ORDER", "CANCEL_TASK")
                    if (is_cancel or submitting) and attempt.get("status") in ("CALLING", "RETURNED"):
                        attempt["status"] = "UNKNOWN"
                        if is_cancel:
                            doc["cancel_status"] = "UNKNOWN"
                if json_text(doc) != before:
                    recompute_order(doc)
                    self.repo_save(cur, doc, "EXECUTOR_RECOVERY")
                    count += 1
            self.repo_apply_pending(cur)
            return {"recovered": count}
        return self.repo_run(recovery, mutation=True)

    def lookup_observations(self, order_id, include_unassociated=False):
        def lookup(cur):
            doc = self.repo_require(self.repo_load(cur, order_id))
            predicate = "(order_id=%s OR order_id IS NULL)" if include_unassociated else "order_id=%s"
            cur.execute("SELECT observation_id,kind,raw,source,observed_at,applied,order_id FROM " + self.repo_s +
                        ".qmt_observations WHERE account_type=%s AND account_id=%s AND " + predicate + " ORDER BY observation_id",
                        self.repo_scope + (order_id,))
            result = []
            members = {(item["symbol"], item["side"]) for item in doc["items"]}
            for row in cur.fetchall():
                raw = repo_json(row[2])
                ids = observation_identifiers(row[1], raw)
                if row[6] is None:
                    if (ids.get("symbol"), ids.get("side")) not in members:
                        continue
                    if ids.get("remark") and str(ids["remark"]) != doc["remark"]:
                        continue
                    if ids.get("account_id") and str(ids["account_id"]) != self.account_id:
                        continue
                result.append({"observation_id": row[0], "kind": row[1], "raw": raw, "source": row[3],
                               "observed_at": row[4], "applied": row[5], "order_id": row[6]})
            return result
        return self.repo_run(lookup)

    def manual_associate(self, order_id, observation_ids, expected_version, audit):
        if not isinstance(audit, dict) or any(not audit.get(key) for key in ("reason", "operator", "evidence")):
            raise OrderError(400, "MANUAL_AUDIT_REQUIRED", "reason, operator and positive evidence are required")
        if not isinstance(observation_ids, (list, tuple)) or not observation_ids:
            raise OrderError(400, "INVALID_OBSERVATIONS", "select persisted QMT observations")
        if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in observation_ids):
            raise OrderError(400, "INVALID_OBSERVATIONS", "observation IDs must be positive integers")
        selected = sorted(set(observation_ids))
        if len(selected) != len(observation_ids):
            raise OrderError(400, "INVALID_OBSERVATIONS", "observation IDs must be unique")
        def associate(cur):
            doc = self.repo_require(self.repo_load(cur, order_id, lock=True))
            if doc["version"] != expected_version:
                raise OrderError(409, "ORDER_VERSION_CONFLICT", "order changed; inspect current facts")
            if doc["submission_status"] != "UNKNOWN":
                raise OrderError(409, "ORDER_NOT_UNKNOWN", "manual association requires UNKNOWN submission")
            members = {(row["symbol"], row["side"]) for row in doc["items"]}
            observations, qmt_orders, qmt_tasks = [], set(), set()
            for observation_id in selected:
                cur.execute("SELECT kind,raw,source,observed_at,order_id FROM " + self.repo_s +
                            ".qmt_observations WHERE account_type=%s AND account_id=%s AND observation_id=%s FOR UPDATE",
                            self.repo_scope + (observation_id,))
                row = cur.fetchone()
                if row is None:
                    raise OrderError(404, "OBSERVATION_NOT_FOUND", "observation is not in this account")
                kind, raw, source, observed_at, associated_order = row
                raw = repo_json(raw)
                ids = observation_identifiers(kind, raw)
                if associated_order is not None and associated_order != order_id:
                    raise OrderError(409, "OBSERVATION_CONFLICT", "observation already belongs to another order")
                if ids.get("account_id") and str(ids["account_id"]) != self.account_id:
                    raise OrderError(409, "OBSERVATION_CONFLICT", "observation account differs")
                if ids.get("remark") and str(ids["remark"]) != doc["remark"]:
                    raise OrderError(409, "OBSERVATION_CONFLICT", "observation remark contradicts target order")
                if not ids.get("remark"):
                    evidence = audit.get("evidence")
                    attribution = evidence.get("manual_attribution") if isinstance(evidence, dict) else None
                    attributed_ids = attribution.get("observation_ids") if isinstance(attribution, dict) else None
                    if (not isinstance(attribution, dict) or attribution.get("order_id") != order_id
                            or attribution.get("account_id") != self.account_id
                            or not isinstance(attributed_ids, list)
                            or any(type(value) is not int for value in attributed_ids)
                            or sorted(attributed_ids) != selected
                            or not isinstance(attribution.get("basis"), str) or len(attribution["basis"].strip()) < 20):
                        raise OrderError(409, "MANUAL_ATTRIBUTION_REQUIRED", "missing remark requires explicit order/account/observations and attribution basis")
                if (ids.get("symbol"), ids.get("side")) not in members:
                    raise OrderError(409, "OBSERVATION_CONFLICT", "observation symbol and side do not match an order item")
                if kind not in ("order", "task", "deal") or not (ids.get("qmt_order_id") or ids.get("qmt_task_id")):
                    raise OrderError(409, "OBSERVATION_CONFLICT", "observation has no positive QMT identity")
                for field, table, found in (("qmt_order_id", "qmt_orders", qmt_orders),
                                            ("qmt_task_id", "qmt_tasks", qmt_tasks)):
                    if not ids.get(field):
                        continue
                    found.add(str(ids[field]))
                    sql = "SELECT 1 FROM " + self.repo_s + "." + table + \
                          " WHERE account_type=%s AND account_id=%s AND order_id<>%s AND document->>%s=%s"
                    params = self.repo_scope + (order_id, field, str(ids[field]))
                    for dimension in ("trading_day", "market"):
                        if ids.get(dimension):
                            sql += " AND (document->>%s IS NULL OR document->>%s=%s)"
                            params += (dimension, dimension, str(ids[dimension]))
                    cur.execute(sql + " LIMIT 1", params)
                    if cur.fetchone() is not None:
                        raise OrderError(409, "QMT_ID_CONFLICT", "QMT identity already belongs to another order")
                observations.append((observation_id, kind, raw, source, observed_at))
            for key, actual in (("qmt_order_ids", qmt_orders), ("qmt_task_ids", qmt_tasks)):
                if key in audit and {str(value) for value in audit[key]} != actual:
                    raise OrderError(409, "QMT_ID_CONFLICT", "supplied QMT identities differ from selected observations")
            # 核验全部证据后才归并；原始记录保持不变，人工凭据单独追加审计。
            for observation_id, kind, raw, source, observed_at in observations:
                apply_observation(doc, kind, raw, source, observed_at=observed_at)
                cur.execute("UPDATE " + self.repo_s + ".qmt_observations SET order_id=%s,applied=true WHERE observation_id=%s",
                            (order_id, observation_id))
            if doc["submission_status"] != "CONFIRMED":
                raise OrderError(409, "INSUFFICIENT_QMT_EVIDENCE", "selected facts do not confirm submission")
            entry = copy_json(audit)
            entry.update(expected_version=expected_version, resolution="observed", observation_ids=selected,
                         qmt_order_ids=sorted(qmt_orders), qmt_task_ids=sorted(qmt_tasks),
                         before_submission_status="UNKNOWN", after_submission_status=doc["submission_status"], at=iso_datetime())
            doc.setdefault("manual_resolutions", []).append(entry)
            doc["resolution"] = "OBSERVED"
            doc["reconcile_requested"] = True
            self.repo_save(cur, doc, "MANUAL_UNKNOWN_RESOLUTION")
            self.repo_apply_pending(cur)
            return self.repo_load(cur, order_id)
        return self.repo_run(associate, mutation=True)

# ---- order_bridge/qmt.py ----
"""QMT 函数适配层。调用者必须处于策略调度回调线程。"""
import datetime as dt
import math
import re
from decimal import Decimal, InvalidOperation



_qmt_quote = {"LATEST": 5, "OWN_BEST": 13, "OPPONENT_BEST": 14, "FAR_LIMIT": 12}
_qmt_market = {"SH": {"BEST5_IOC": 42, "BEST5_TO_LIMIT": 43,
                       "OPPONENT_BEST": 44, "OWN_BEST": 45},
               "SZ": {"BEST5_IOC": 47, "OPPONENT_BEST": 44,
                       "OWN_BEST": 45, "IOC": 46, "FOK": 48},
               "BJ": {"BEST5_IOC": 42, "BEST5_TO_LIMIT": 43,
                       "OPPONENT_BEST": 44, "OWN_BEST": 45}}
_qmt_strategy_name = "qmt-bridge-order"
_qmt_passorder_fields = ("accountID", "currentTime", "formulaName", "modelPrice",
                         "modelVolume", "opType", "orderCode", "orderType", "prType", "strategyName")
_qmt_history_date = re.compile(r"^\d{8}$")
_qmt_range = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*$")


def _qmt_error(code, message, status=422):
    raise OrderError(status, code, message)


def _qmt_number(value, field):
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        _qmt_error("INVALID_SMART_PARAM", field + " must be numeric")
    if not number.is_finite():
        _qmt_error("INVALID_SMART_PARAM", field + " must be finite")
    return number


def _qmt_smart_default(meta):
    value = meta.get("defaultValueByName") or meta.get("defaultValue")
    if value is None or str(value) == "":
        _qmt_error("SMART_DEFAULT_UNAVAILABLE", "missing default for " + str(meta.get("key")))
    return value


def _qmt_smart_field(meta, raw, from_default):
    key = meta["key"]
    enums = [part.strip() for part in str(meta.get("enumValue") or "").split(",") if part.strip()]
    names = [part.strip() for part in str(meta.get("enumName") or "").split(",") if part.strip()]
    if enums:
        if str(raw) in names and len(names) == len(enums):
            raw = enums[names.index(str(raw))]
        if str(raw) not in enums:
            _qmt_error("INVALID_SMART_PARAM", key + " is outside QMT enumeration")
        try:
            return int(raw)
        except ValueError:
            return str(raw)
    dtype = str(meta.get("dataType") or "")
    if "整数" in dtype or "浮点" in dtype or isinstance(raw, (int, float, Decimal)):
        number = _qmt_number(raw, key)
        percent = meta.get("unit") == "%"
        bounds = _qmt_range.match(str(meta.get("valueRangeByName") or meta.get("valueRange") or ""))
        if percent:
            if from_default:
                number /= 100
            if bounds:
                low = Decimal(bounds.group(1)) / 100
                high = Decimal(bounds.group(2)) / 100
                if not low <= number <= high:
                    _qmt_error("INVALID_SMART_PARAM", key + " is outside QMT percent range")
            elif not 0 <= number <= 1:
                _qmt_error("INVALID_SMART_PARAM", key + " must be decimal in [0,1]")
        elif bounds and not Decimal(bounds.group(1)) <= number <= Decimal(bounds.group(2)):
            _qmt_error("INVALID_SMART_PARAM", key + " is outside QMT range")
        if "整数" in dtype:
            if number != int(number):
                _qmt_error("INVALID_SMART_PARAM", key + " must be integer")
            return int(number)
        return float(number)
    if not isinstance(raw, str):
        _qmt_error("INVALID_SMART_PARAM", key + " must be string")
    return raw


def _qmt_passorder_snapshot(raw):
    """保留 QMT 原字段，并仅从本 bridge 的策略名提取可信备注。"""
    if not all(key in raw for key in ("accountID", "orderCode", "strategyName")):
        _qmt_error("INVALID_QMT_RESULT", "passorder callback is missing identity fields", 502)
    account = raw["accountID"]
    if not isinstance(account, str) or not account:
        _qmt_error("INVALID_QMT_RESULT", "passorder callback has invalid account", 502)
    result = dict(raw)
    result["account_id"] = account
    strategy = raw["strategyName"]
    prefix = _qmt_strategy_name + "_&&&_"
    if isinstance(strategy, str) and strategy.startswith(prefix) and len(strategy) > len(prefix):
        result["remark"] = strategy[len(prefix):]
    return result


class QmtAdapter(object):
    def __init__(self, apis, context, account_id=None, account_type="STOCK"):
        self.apis = apis
        self.context = context
        self.account_id = account_id or getattr(context, "account_id", None)
        self.account_type = account_type

    def available(self, name):
        return callable(self.apis.get(name))

    def _require(self, name):
        function = self.apis.get(name)
        if not callable(function):
            _qmt_error("API_UNAVAILABLE", name + " is unavailable", 501)
        return function

    def resolve(self, request, remark):
        if not isinstance(request, dict):
            _qmt_error("INVALID_ORDER", "normalized request must be an object", 400)
        account = request.get("account_id")
        if not account or (self.account_id is not None and account != self.account_id):
            _qmt_error("ACCOUNT_MISMATCH", "order account does not match adapter", 403)
        basket = request["order_type"] == "BASKET"
        execution = request["execution"]["type"]
        if execution == "SMART":
            pr_type = 11 if request["price_type"] == "LIMIT" else 12
        elif request["price_type"] == "LIMIT":
            pr_type = 11
        elif request["price_type"] == "QUOTE":
            pr_type = _qmt_quote[request["quote_type"]]
        else:
            market = request["symbol"].rsplit(".", 1)[1]
            try:
                pr_type = _qmt_market[market][request["market_type"]]
            except (KeyError, TypeError):
                _qmt_error("INVALID_MARKET_TYPE", "market instruction is unsupported for exchange")
        result = {"function": {"DIRECT": "passorder", "SLICED": "algo_passorder",
                               "SMART": "smart_algo_passorder"}[execution],
                  "opType": 35 if basket else (23 if request["side"] == "BUY" else 24),
                  "orderType": 2101 if basket else (1102 if "amount" in request else 1101),
                  "accountid": account,
                  "orderCode": remark if basket else request["symbol"],
                  "prType": pr_type,
                  "price": float(request["limit_price"]) if request["price_type"] == "LIMIT" else
                           float(request["protection_price"]) if "protection_price" in request else 0.0,
                  "volume": 1 if basket else float(request["amount"]) if "amount" in request else request["quantity"],
                  "strategyName": _qmt_strategy_name, "quickTrade": 2, "userOrderId": remark}
        # 认领持久任务前确认目标函数存在，避免已知不可执行任务进入提交不明状态。
        self._require(result["function"])
        if execution == "SLICED":
            params = copy_json(request["execution"]["params"])
            params["OrderType"] = 1 if request["execution"]["mode"] == "ALGO" else 2
            params["PriceType"] = pr_type
            result["userOrderParam"] = params
        elif execution == "SMART":
            smart = request["execution"]
            data = self._require("get_smart_algo_param")([smart["algorithm"]])
            if not isinstance(data, dict) or smart["algorithm"] not in data or not isinstance(data[smart["algorithm"]], list):
                _qmt_error("SMART_ALGORITHM_UNAVAILABLE", "QMT did not return algorithm metadata")
            definitions = data[smart["algorithm"]]
            lookup = {}
            for item in definitions:
                if not isinstance(item, dict) or not isinstance(item.get("key"), str) or item["key"] in lookup:
                    _qmt_error("INVALID_SMART_METADATA", "QMT returned invalid parameter definition")
                lookup[item["key"]] = item
            custom = smart["params"]
            unknown = set(custom) - set(lookup)
            if unknown or "m_strCmdRemark" in custom:
                _qmt_error("INVALID_SMART_PARAM", "unknown or reserved fields: " + ", ".join(sorted(unknown | ({"m_strCmdRemark"} & set(custom)))))
            expanded = {}
            for key, item in lookup.items():
                if key == "m_strCmdRemark":
                    continue
                is_default = key not in custom
                expanded[key] = _qmt_smart_field(item, _qmt_smart_default(item) if is_default else custom[key], is_default)
            expanded["m_strCmdRemark"] = remark
            qmt_zone = dt.timezone(dt.timedelta(hours=8))
            start = parse_timestamp(smart["start_at"]).astimezone(qmt_zone)
            end = parse_timestamp(smart["end_at"]).astimezone(qmt_zone)
            if start.date() != end.date():
                _qmt_error("INVALID_SMART_TIME", "smart interval crosses Shanghai calendar day")
            result.update({"smartAlgoType": smart["algorithm"],
                           "startTime": start.strftime("%H:%M:%S"),
                           "endTime": end.strftime("%H:%M:%S"), "algoParam": expanded})
        return result

    def prepare_basket(self, order):
        if order["order_type"] != "BASKET":
            return False
        name = order["basket_name"]
        if not name or order.get("resolved_request", {}).get("orderCode") != name:
            _qmt_error("BASKET_NAME_MISMATCH", "basket name differs from frozen request")
        get_basket = self._require("get_basket")
        set_basket = self._require("set_basket")
        expected = {"name": name, "stocks": [
            {"stock": item["symbol"], "weight": 0, "quantity": item["quantity"],
             "optType": 23 if item["side"] == "BUY" else 24}
            for item in order["items"]]}
        existing = get_basket(name)
        if existing:
            self._check_basket(existing, expected)
            return False
        set_basket(expected)
        self._check_basket(get_basket(name), expected)
        return True

    def _check_basket(self, actual, expected):
        if not isinstance(actual, dict) or actual.get("name") != expected["name"] or not isinstance(actual.get("stocks"), list):
            _qmt_error("BASKET_READBACK_MISMATCH", "QMT basket could not be verified")
        def canonical(stocks):
            return sorted((item.get("stock"), item.get("optType"), item.get("quantity")) for item in stocks)
        try:
            if canonical(actual["stocks"]) != canonical(expected["stocks"]):
                _qmt_error("BASKET_READBACK_MISMATCH", "QMT basket content differs")
        except (TypeError, AttributeError):
            _qmt_error("BASKET_READBACK_MISMATCH", "QMT basket content is invalid")

    def submit(self, order):
        frozen = order.get("resolved_request")
        if not isinstance(frozen, dict):
            _qmt_error("MISSING_RESOLVED_REQUEST", "order has no frozen QMT request")
        if frozen.get("accountid") != order.get("account_id") or frozen.get("userOrderId") != order.get("remark"):
            _qmt_error("RESOLVED_REQUEST_MISMATCH", "frozen request identity differs")
        name = frozen.get("function")
        if name not in ("passorder", "algo_passorder", "smart_algo_passorder"):
            _qmt_error("INVALID_RESOLVED_REQUEST", "unsupported QMT function")
        base = [frozen[key] for key in ("opType", "orderType", "accountid", "orderCode", "prType",
                                        "price", "volume", "strategyName", "quickTrade", "userOrderId")]
        if name == "algo_passorder":
            base.append(copy_json(frozen["userOrderParam"]))
        elif name == "smart_algo_passorder":
            base.extend([frozen["smartAlgoType"], frozen["startTime"], frozen["endTime"],
                         copy_json(frozen["algoParam"])])
        base.append(self.context)
        return self._require(name)(*base)

    def cancel_action(self, action):
        if not isinstance(action, dict):
            _qmt_error("INVALID_CANCEL_ACTION", "action must be an object", 400)
        kind = action.get("kind")
        if kind not in ("CANCEL_ORDER", "CANCEL_TASK"):
            _qmt_error("INVALID_CANCEL_ACTION", "kind must be CANCEL_ORDER or CANCEL_TASK", 400)
        identifier = action.get("target_id")
        account = action.get("account_id", self.account_id)
        if (isinstance(identifier, bool) or not isinstance(identifier, (str, int)) or
                not str(identifier).strip() or not account or
                (self.account_id is not None and account != self.account_id)):
            _qmt_error("INVALID_CANCEL_ACTION", "identifier/account mismatch", 400)
        return bool(self._require("cancel" if kind == "CANCEL_ORDER" else "cancel_task")(
            identifier, account, self.account_type, self.context))

    def query(self, kind, start_date=None, end_date=None):
        if kind not in ("order", "deal", "task"):
            _qmt_error("INVALID_QUERY", "kind must be order, deal or task", 400)
        if not self.account_id:
            _qmt_error("ACCOUNT_UNAVAILABLE", "adapter has no bound account", 500)
        if (start_date is None) != (end_date is None):
            _qmt_error("INVALID_QUERY", "both history dates are required", 400)
        if start_date is not None:
            if kind == "task":
                _qmt_error("HISTORY_UNAVAILABLE", "QMT history does not document task data", 501)
            if not _qmt_history_date.match(start_date) or not _qmt_history_date.match(end_date) or start_date > end_date:
                _qmt_error("INVALID_QUERY", "history dates must be ordered YYYYMMDD", 400)
            rows = self._require("get_history_trade_detail_data")(
                self.account_id, self.account_type, kind.upper(), start_date, end_date)
            if not isinstance(rows, (list, tuple)):
                _qmt_error("INVALID_QMT_RESULT", "history query returned invalid collection", 502)
            result = []
            for row in rows:
                if not isinstance(row, (list, tuple)) or len(row) < 2:
                    _qmt_error("INVALID_QMT_RESULT", "history query returned invalid tuple", 502)
                for raw_item in row[1:]:
                    group = raw_item if isinstance(raw_item, (list, tuple)) else [raw_item]
                    for member in group:
                        item = self.snapshot(member)
                        if not isinstance(item, dict):
                            _qmt_error("INVALID_QMT_RESULT", "history entry has no QMT fields", 502)
                        if not item.get("m_strTradingDay"):
                            trading_day = str(row[0])
                            if not _qmt_history_date.match(trading_day):
                                _qmt_error("INVALID_QMT_RESULT", "history entry has no valid trading day", 502)
                            item["m_strTradingDay"] = trading_day
                        result.append(item)
            return result
        rows = self._require("get_trade_detail_data")(self.account_id, self.account_type, kind)
        if not isinstance(rows, (list, tuple)):
            _qmt_error("INVALID_QMT_RESULT", "query returned invalid collection", 502)
        return [self.snapshot(row) for row in rows]

    def snapshot(self, value):
        if value is None or isinstance(value, (str, bool, int)):
            return value
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, Decimal):
            return float(value) if value.is_finite() else None
        if isinstance(value, (list, tuple)):
            return [self.snapshot(item) for item in value]
        if isinstance(value, dict):
            raw = {str(key): self.snapshot(item) for key, item in value.items()}
            if any(key in raw for key in _qmt_passorder_fields):
                return _qmt_passorder_snapshot(raw)
            return raw
        names = [name for name in dir(value) if name.startswith("m_")]
        names.extend(name for name in _qmt_passorder_fields if name not in names and hasattr(value, name))
        if not names:
            _qmt_error("INVALID_QMT_RESULT", "object has no recognized QMT fields", 502)
        raw = {name: self.snapshot(getattr(value, name)) for name in names}
        if any(key in raw for key in _qmt_passorder_fields):
            return _qmt_passorder_snapshot(raw)
        return raw

# ---- order_bridge/runtime.py ----
"""持久命令与 QMT 调度器；HTTP 路径不会调用 QMT。"""
import datetime as dt
import os
import queue
import socket
import sys
import threading
import time
import uuid



def read_pg_config(values):
    """QMT 面板小写优先；完全未配置时仍提供原有三个查询。"""
    if "pg_schema" in values or "PG_SCHEMA" in values:
        raise ValueError("pg_schema is fixed to qmt_order; remove pg_schema and use pg_database for isolation")
    config = {}
    defaults = {"pg_host": "127.0.0.1", "pg_port": 5432,
                "pg_database": None, "pg_user": None, "pg_password": None}
    for key, default in defaults.items():
        config[key] = values.get(key, values.get(key.upper(), default))
    # schema 是内部固定布局；模拟盘和实盘由数据库名隔离。
    config["pg_schema"] = "qmt_order"
    if all(config[key] in (None, "") for key in ("pg_database", "pg_user", "pg_password")):
        return None
    for key in ("pg_host", "pg_database", "pg_user", "pg_password", "pg_schema"):
        if not isinstance(config[key], str) or not config[key]:
            raise ValueError(key + " must be a nonempty string")
    port = config["pg_port"]
    if isinstance(port, bool):
        raise ValueError("pg_port must be an integer in 1..65535")
    try:
        parsed = int(port)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("pg_port must be an integer in 1..65535")
    if parsed < 1 or parsed > 65535 or (isinstance(port, float) and parsed != port):
        raise ValueError("pg_port must be an integer in 1..65535")
    config["pg_port"] = parsed
    # 驱动安装在 bridge 私有目录，不改变 QMT 自带 site-packages。
    vendor = os.path.join(os.path.expanduser("~"), "qmt-bridge", "vendor")
    if os.path.isdir(vendor) and vendor not in sys.path:
        sys.path.insert(0, vendor)
    return config


class LocalExecutorLock:
    """整个执行器生命周期持有 OS 文件独占锁，进程退出由 OS 回收。"""
    def __init__(self, config, account_id):
        identity = {key: config[key] for key in ("pg_host", "pg_port", "pg_database", "pg_schema")}
        identity.update(account_id=account_id, account_type="STOCK")
        directory = os.path.join(os.path.expanduser("~"), "qmt-bridge", "runtime")
        self.path = os.path.join(directory, fingerprint(identity) + ".lock")
        self.handle = None
        self.file = None
        self.kernel = None

    def acquire(self):
        if self.handle is not None or self.file is not None:
            return
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if os.name == "nt":
            import ctypes
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.CreateFileW.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32,
                                          ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p]
            kernel.CreateFileW.restype = ctypes.c_void_p
            kernel.CloseHandle.argtypes = [ctypes.c_void_p]
            kernel.CloseHandle.restype = ctypes.c_int
            handle = kernel.CreateFileW(self.path, 0xC0000000, 0, None, 4, 0x80, None)
            if handle == ctypes.c_void_p(-1).value:
                raise OrderError(503, "EXECUTOR_ALREADY_RUNNING", "another local executor holds this account")
            self.handle, self.kernel = handle, kernel
        else:
            import fcntl
            stream = open(self.path, "a+b")
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                stream.close()
                raise OrderError(503, "EXECUTOR_ALREADY_RUNNING", "another local executor holds this account")
            self.file = stream

    def release(self):
        if self.handle is not None:
            self.kernel.CloseHandle(self.handle)
            self.handle = None
        if self.file is not None:
            self.file.close()
            self.file = None


class OrderRuntime:
    def __init__(self, apis, context, account_id, pg_config=None, logger=None,
                 repository=None, local_lock=None, clock=None):
        self.apis = apis
        self.account_id = account_id
        self.config = pg_config
        self.adapter = QmtAdapter(apis, context, account_id=account_id, account_type="STOCK")
        self.repo = repository or (PostgresRepository(pg_config, account_id) if pg_config else None)
        self.local_lock = local_lock or (LocalExecutorLock(pg_config, account_id) if pg_config else None)
        self.logger = logger or (lambda *args, **kwargs: None)
        self.clock = clock or time.monotonic
        self.instance_id = uuid.uuid4().hex
        self.host_id = socket.gethostname().lower()
        self.stop_event = threading.Event()
        self.tick_lock = threading.Lock()
        self.admission_lock = threading.RLock()
        self.observations = queue.Queue(maxsize=4096)
        self.observation_gap = False
        self.initialized = False
        self.recovery_complete = False
        self.last_error = None
        self.last_tick = None
        self.last_reconciled_at = None
        self.history_coverage_complete = False
        self.next_reconcile = 0
        self.next_initialize = 0
        self.confirmation_timeout = 30

    def initialize(self):
        if self.repo is None or self.stop_event.is_set():
            return
        try:
            if self.local_lock:
                self.local_lock.acquire()
            schema = self.repo.check_schema()
            self.repo.ensure_account_runtime()  # 使用启动参数账户；已有运行状态不重置。
            self.repo.acquire_executor(self.instance_id, self.host_id)
            self.repo.recover()
            self.initialized = True
            self.recovery_complete = False
            self.last_error = None
            self.logger("INFO", "Order executor recovering", account_id=self.account_id,
                        database=(self.config or {}).get("pg_database"), schema=(self.config or {}).get("pg_schema"),
                        schema_version=schema.get("schema_version"))
        except Exception as exc:
            self.initialized = False
            self.recovery_complete = False
            self.last_error = getattr(exc, "code", "PERSISTENCE_UNAVAILABLE")
            self.next_initialize = self.clock() + 5
            try:
                self.repo.release_executor()
            except Exception:
                pass
            # 错误仅记录稳定代码；PG 异常文本可能包含连接参数。
            self.logger("ERROR", "Order executor unavailable", error_code=self.last_error)

    def _runtime_require_store(self):
        if self.repo is None:
            raise OrderError(503, "TRADING_NOT_CONFIGURED", "PostgreSQL is not configured; query-only mode")

    def _runtime_require_ready(self):
        self._runtime_require_store()
        if self.stop_event.is_set():
            raise OrderError(503, "ORDER_STOPPING", "order executor is stopping")
        if not self.initialized or not self.recovery_complete:
            raise OrderError(503, "EXECUTOR_NOT_READY", "executor has not completed recovery")
        self.repo.check_executor()

    def handle(self, method, params, verb):
        writes = ("submit_order", "cancel_order")
        reads = ("order", "orders", "order_events", "capabilities", "health")
        if method in writes and verb != "POST" or method in reads and verb != "GET":
            raise OrderError(405, "METHOD_NOT_ALLOWED", "commands require POST; order queries require GET")
        if not isinstance(params, dict):
            raise OrderError(400, "INVALID_PARAMS", "parameters must be an object")
        if method == "health":
            self._runtime_keys(params, ())
            return 200, self.health()
        if method == "capabilities":
            self._runtime_keys(params, ())
            result = capabilities()
            result["qmt_functions"] = {name: self.adapter.available(name) for name in
                ("passorder", "algo_passorder", "smart_algo_passorder", "cancel", "cancel_task", "set_basket", "get_basket")}
            result["verification_status"] = "UNVERIFIED"
            result["trading_configured"] = self.repo is not None
            result["paths"] = [{"order_type": kind, "execution": execution, "implemented": True,
                                 "qmt_function": function, "function_available": self.adapter.available(function)
                                 and (kind != "BASKET" or (self.adapter.available("set_basket") and self.adapter.available("get_basket"))),
                                 "locally_verified": False}
                                for kind in ("SINGLE", "BASKET")
                                for execution, function in (("DIRECT", "passorder"), ("SLICED", "algo_passorder"), ("SMART", "smart_algo_passorder"))]
            return 200, result
        self._runtime_require_store()
        if method == "submit_order":
            request = normalize_order(params, self.account_id)
            # 已有订单允许在恢复期间查询重放；不能先因执行器未就绪而丢掉幂等结果。
            try:
                existing = self.repo.get_order(request["client_order_id"])
            except OrderError as exc:
                if exc.status != 404:
                    raise
                existing = None
            if existing is not None:
                if existing["request_hash"] != fingerprint(request):
                    raise OrderError(409, "IDEMPOTENCY_CONFLICT", "client_order_id has a different request")
                return 200, public_order(existing, True)
            with self.admission_lock:
                self._runtime_require_ready()
                self._runtime_smart_window(request)
                created, document = self.repo.accept_order(request)
            self.logger("INFO", "Order accepted" if created else "Order replayed",
                        client_order_id=request["client_order_id"], order_id=document["order_id"])
            return (202 if created else 200), public_order(document, not created)
        if method == "cancel_order":
            request = normalize_cancel(params, self.account_id)
            # 恢复期间仍可保存撤单意图；新 QMT 副作用由调度器恢复门闩控制。
            with self.admission_lock:
                if self.stop_event.is_set():
                    raise OrderError(503, "ORDER_STOPPING", "order executor is stopping")
                status, result = self.repo.request_cancel(request)
            self.logger("INFO", "Cancel request saved", client_order_id=request["client_order_id"],
                        cancel_request_id=request["cancel_request_id"], cancel_status=result.get("cancel_status"))
            return status, result
        if method == "order":
            self._runtime_keys(params, ("client_order_id",))
            value = params.get("client_order_id")
            if not isinstance(value, str) or not value:
                raise OrderError(400, "INVALID_PARAMS", "client_order_id is required")
            return 200, public_order(self.repo.get_order(value))
        if method == "orders":
            self._runtime_keys(params, ("active", "limit", "cursor"))
            active = params.get("active", "true")
            if active not in ("true", "false", True, False):
                raise OrderError(400, "INVALID_PARAMS", "active must be true or false")
            result = self.repo.list_orders(active=active in ("true", True),
                                          limit=self._runtime_limit(params.get("limit", 100)), cursor=params.get("cursor"))
            result["orders"] = [public_order(row) for row in result["orders"]]
            return 200, result
        if method == "order_events":
            self._runtime_keys(params, ("after", "limit"))
            after = self._runtime_integer(params.get("after", 0), "after", 0, 9223372036854775807)
            result = self.repo.events(after=after, limit=self._runtime_limit(params.get("limit", 100)))
            events = []
            for event in result["events"]:
                document = event.get("data", event.get("order", {}))
                events.append({"event_id": event.get("event_id", event.get("event_seq")),
                               "order_id": document.get("order_id"),
                               "client_order_id": document.get("client_order_id"),
                               "order_version": document.get("version"),
                               "type": event.get("type", event.get("event_type")),
                               "recorded_at": event.get("recorded_at", event.get("occurred_at")),
                               "data": public_order(document)})
            return 200, {"events": events, "next_after": result["next_after"], "has_more": result["has_more"]}
        raise OrderError(404, "METHOD_NOT_FOUND", "unknown method")

    @staticmethod
    def _runtime_keys(params, allowed):
        if set(params) - set(allowed):
            raise OrderError(400, "INVALID_PARAMS", "unexpected parameters")

    @staticmethod
    def _runtime_integer(value, name, minimum, maximum):
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise OrderError(400, "INVALID_PARAMS", name + " must be an integer")
        try:
            number = int(value)
        except ValueError:
            raise OrderError(400, "INVALID_PARAMS", name + " must be an integer")
        if number < minimum or number > maximum:
            raise OrderError(400, "INVALID_PARAMS", name + " is out of range")
        return number

    def _runtime_limit(self, value):
        return self._runtime_integer(value, "limit", 1, 1000)

    def health(self):
        database = False
        executor = False
        unknown_count = None
        schema_version = None
        if self.repo:
            try:
                health = self.repo.health()
                database = bool(health.get("ready"))
                executor = bool(health.get("executor"))
                unknown_count = health.get("unknown_order_count")
                schema_version = health.get("schema_version")
            except Exception:
                pass
        alive = self.last_tick is not None and self.clock() - self.last_tick < 5
        return {"http_running": not self.stop_event.is_set(), "database_available": database,
                "trading_configured": self.repo is not None, "scheduler_alive": alive,
                "recovery_complete": self.recovery_complete,
                "history_coverage_complete": self.history_coverage_complete,
                "accepting_orders": bool(database and executor and alive and self.initialized and self.recovery_complete and not self.stop_event.is_set()),
                "executor_owned": executor, "schema_version": schema_version,
                "unknown_order_count": unknown_count,
                "account_id": self.account_id, "last_reconciled_at": self.last_reconciled_at,
                "error_code": self.last_error, "observation_gap": self.observation_gap}

    def observe(self, kind, value):
        """只在 QMT 回报上下文展开对象；队列满标记缺口，随后靠完整查询补齐。"""
        if self.repo is None:
            return
        try:
            raw = self.adapter.snapshot(value)
            self.observations.put_nowait((kind, raw))
        except Exception:
            self.observation_gap = True
            self.next_reconcile = 0
            self.logger("ERROR", "QMT observation could not be buffered", kind=kind)

    def tick(self):
        self.last_tick = self.clock()
        if self.repo is None or self.stop_event.is_set() or not self.tick_lock.acquire(False):
            return
        try:
            if not self.initialized:
                if self.clock() >= self.next_initialize:
                    self.initialize()
                if not self.initialized:
                    return
            self.repo.check_executor()
            for unused in range(100):
                try:
                    kind, raw = self.observations.get_nowait()
                except queue.Empty:
                    break
                try:
                    document = self.repo.ingest_observation(kind, raw, "callback")
                    identifiers = observation_identifiers(kind, raw)
                    self.logger("INFO", "QMT callback recorded", kind=kind,
                                order_id=document.get("order_id") if document else None,
                                client_order_id=document.get("client_order_id") if document else None,
                                order_version=document.get("version") if document else None,
                                qmt_order_id=identifiers.get("qmt_order_id"), qmt_task_id=identifiers.get("qmt_task_id"))
                except Exception:
                    self.observation_gap = True
                    raise
                finally:
                    self.observations.task_done()
            if self.recovery_complete and self._runtime_dispatch_cancel():
                return
            if self.clock() >= self.next_reconcile or not self.recovery_complete:
                self._runtime_reconcile()
            if not self.recovery_complete or self.stop_event.is_set():
                return
            # 每轮最多一种交易副作用；撤单优先。DB 中的等待意图不会占住新单队列。
            if self._runtime_dispatch_cancel():
                return
            queued = self.repo.queued_orders(limit=1)
            if queued:
                self._runtime_submit(queued[0])
        except Exception as exc:
            self.initialized = False
            self.recovery_complete = False
            self.last_error = getattr(exc, "code", "EXECUTOR_ERROR")
            self.next_initialize = self.clock() + 5
            try:
                self.repo.release_executor()
            except Exception:
                pass
            self.logger("ERROR", "Order executor suspended", error_code=self.last_error)
        finally:
            self.tick_lock.release()

    def _runtime_dispatch_cancel(self):
        for document in self.repo.cancellation_orders(limit=100):
            actions = pending_cancellations(document)
            if actions:
                self._runtime_cancel(document, actions[0])
                return True
        return False

    def _runtime_reconcile(self):
        self.next_reconcile = self.clock() + 1
        documents = self.repo.reconcile_orders(limit=1000)
        # 任务先查，再查委托和成交，防止把任务结束前的一次子单快照当终态。
        complete = True
        for kind in ("task", "order", "deal"):
            try:
                for raw in self.adapter.query(kind):
                    self.repo.ingest_observation(kind, raw, "query")
            except OrderError as exc:
                complete = False
                self.logger("WARNING", "QMT reconciliation incomplete", kind=kind, error_code=exc.code)
        today = utc_now().astimezone(dt.timezone(dt.timedelta(hours=8))).date()
        past = [parse_timestamp(row["created_at"]).astimezone(dt.timezone(dt.timedelta(hours=8))).date()
                for row in documents]
        earliest = min(past) if past else today
        history_complete = True
        if earliest < today:
            for kind in ("order", "deal"):
                try:
                    for raw in self.adapter.query(kind, earliest.strftime("%Y%m%d"), (today - dt.timedelta(days=1)).strftime("%Y%m%d")):
                        self.repo.ingest_observation(kind, raw, "history")
                except OrderError as exc:
                    history_complete = False
                    self.logger("WARNING", "Historical reconciliation incomplete", kind=kind, error_code=exc.code)
        stamp = iso_datetime()
        for old in documents:
            def reconcile(document):
                is_past = parse_timestamp(document["created_at"]).astimezone(dt.timezone(dt.timedelta(hours=8))).date() < today
                mark_reconciled(document, complete=complete and (not is_past or history_complete), now=stamp)
                document["reconcile_requested"] = False
                if document["submission_status"] == "SUBMITTING":
                    attempts = [row for row in document["attempts"] if row.get("kind") == "SUBMIT"]
                    started = attempts[-1].get("created_at", document["updated_at"]) if attempts else document["updated_at"]
                    if (parse_timestamp(stamp) - parse_timestamp(started)).total_seconds() >= self.confirmation_timeout:
                        document["submission_status"] = "UNKNOWN"
                        document["execution_status"] = "UNKNOWN"
                        document["error"] = {"code": "SUBMISSION_OUTCOME_UNKNOWN", "message": "QMT acknowledgement is not yet associated"}
            self.repo.update_order(old["order_id"], "RECONCILED", reconcile)
        self.history_coverage_complete = history_complete
        # 历史缺口只冻结相关订单的终态判断；旧 SUBMITTING 已在恢复时转 UNKNOWN。
        # 不因一笔历史 UNKNOWN 阻塞其他确定未提交的 QUEUED。
        if complete:
            self.recovery_complete = True
            self.last_reconciled_at = stamp
            self.last_error = None
            self.observation_gap = False

    @staticmethod
    def _runtime_smart_window(request):
        execution = request["execution"]
        if execution["type"] != "SMART":
            return
        zone = dt.timezone(dt.timedelta(hours=8))
        now = utc_now().astimezone(zone)
        start = parse_timestamp(execution["start_at"]).astimezone(zone)
        end = parse_timestamp(execution["end_at"]).astimezone(zone)
        if end <= now:
            raise OrderError(422, "SMART_WINDOW_EXPIRED", "SMART execution window has ended")
        if start.date() != now.date() or end.date() != now.date():
            raise OrderError(422, "SMART_DATE_UNSUPPORTED", "SMART times must use the current Shanghai calendar day")

    def _runtime_submit(self, document):
        order_id = document["order_id"]
        try:
            # 冻结的 QMT 参数只有时分秒，重启后必须重新核验原始带日期窗口。
            self._runtime_smart_window(document["request"])
            if document.get("submit_before") and utc_now() >= parse_timestamp(document["submit_before"]):
                self.repo.update_order(order_id, "ORDER_EXPIRED", lambda row: recompute_order(row))
                return
            if document.get("resolved_request") is None:
                resolved = self.adapter.resolve(document["request"], document["remark"])
                def freeze(row):
                    if row["submission_status"] == "QUEUED" and row.get("resolved_request") is None:
                        row["resolved_request"] = copy_json(resolved)
                document = self.repo.update_order(order_id, "PARAMETERS_RESOLVED", freeze)
            if document["submission_status"] != "QUEUED" or self.stop_event.is_set():
                return
            if document["order_type"] == "BASKET":
                self.repo.check_executor()
                self.adapter.prepare_basket(document)
                document = self.repo.update_order(order_id, "BASKET_VERIFIED", lambda row: row.update(basket_state="VERIFIED"))
        except OrderError as exc:
            if exc.status >= 500 and exc.code.startswith(("PERSISTENCE", "EXECUTOR", "DATABASE")):
                raise
            def reject(row):
                if row["submission_status"] == "QUEUED":
                    row["submission_status"] = "EXPIRED" if exc.code == "SMART_WINDOW_EXPIRED" else "REJECTED"
                    row["error"] = {"code": exc.code, "message": exc.message}
                    recompute_order(row)
            self.repo.update_order(order_id, "ORDER_REJECTED", reject)
            return
        claimed, document = self.repo.claim_submission(order_id)
        if not claimed:
            return
        if self.stop_event.is_set():
            def abort(row):
                row["submission_status"] = "CANCELLED_LOCAL" if row.get("cancel_requested") else "QUEUED"
                for attempt in row["attempts"]:
                    if attempt.get("kind") == "SUBMIT" and attempt.get("status") == "CALLING":
                        attempt["status"] = "ABORTED_NO_CALL"
                recompute_order(row)
            self.repo.update_order(order_id, "DISPATCH_STOPPED_BEFORE_CALL", abort)
            return
        try:
            self.repo.check_executor()
            call_result = self.adapter.snapshot(self.adapter.submit(document))
        except Exception as exc:
            # 进入调用路径后异常一律按可能产生副作用处理，绝不自动回 QUEUED。
            def unknown(row):
                if row["submission_status"] in ("SUBMITTING", "UNKNOWN"):
                    row["submission_status"] = "UNKNOWN"
                    row["execution_status"] = "UNKNOWN"
                    row["error"] = {"code": "SUBMISSION_OUTCOME_UNKNOWN", "message": "QMT submission requires reconciliation"}
                for attempt in row["attempts"]:
                    if attempt.get("kind") == "SUBMIT" and attempt.get("status") == "CALLING":
                        attempt["status"] = "UNKNOWN"
                        attempt["error"] = {"code": getattr(exc, "code", "QMT_ERROR"),
                                            "type": type(exc).__name__, "message": str(exc)[:4096]}
            self.repo.update_order(order_id, "SUBMISSION_UNKNOWN", unknown)
            self.logger("ERROR", "QMT submit outcome unknown", order_id=order_id,
                        client_order_id=document["client_order_id"], error_code=getattr(exc, "code", "QMT_ERROR"))
            return
        def returned(row):
            for attempt in row["attempts"]:
                if attempt.get("kind") == "SUBMIT" and attempt.get("status") == "CALLING":
                    attempt.update(status="RETURNED", returned_at=iso_datetime(), return_value=copy_json(call_result))
        self.repo.update_order(order_id, "SUBMIT_CALL_RETURNED", returned)
        self.logger("INFO", "QMT submit call returned", order_id=order_id, client_order_id=document["client_order_id"],
                    attempt_id=document["attempts"][-1]["attempt_id"])

    def _runtime_cancel(self, document, action):
        attempt_id = str(uuid.uuid4())
        chosen = dict(action, attempt_id=attempt_id, status="CALLING", created_at=iso_datetime(),
                      cancel_request_id=document["active_cancel_request_id"])
        claimed = [False]
        def claim(row):
            candidates = pending_cancellations(row)
            if any(item["kind"] == action["kind"] and str(item["target_id"]) == str(action["target_id"]) for item in candidates):
                row["attempts"].append(copy_json(chosen))
                row["cancel_status"] = "PENDING"
                claimed[0] = True
        document = self.repo.update_order(document["order_id"], "CANCEL_DISPATCHING", claim)
        if not claimed[0]:
            return
        outcome = "UNKNOWN"
        call_error = None
        emitted = None
        try:
            if not self.stop_event.is_set():
                self.repo.check_executor()
                emitted = self.adapter.cancel_action(chosen)
                outcome = "RETURNED" if emitted is True else "REJECTED"
            else:
                outcome = "ABORTED_NO_CALL"
        except Exception as exc:
            call_error = {"code": getattr(exc, "code", "QMT_ERROR"),
                          "type": type(exc).__name__, "message": str(exc)[:4096]}
        def finish(row):
            for attempt in row["attempts"]:
                if attempt["attempt_id"] == attempt_id:
                    attempt.update(status=outcome, returned_at=iso_datetime(), return_value=emitted, error=call_error)
            recompute_order(row)
        self.repo.update_order(document["order_id"], "CANCEL_CALL_" + outcome, finish)
        self.logger("INFO" if outcome == "RETURNED" else "WARNING", "QMT cancel call finished",
                    order_id=document["order_id"], cancel_request_id=chosen["cancel_request_id"],
                    attempt_id=attempt_id, outcome=outcome)

    def stop(self):
        with self.admission_lock:
            self.stop_event.set()
        # 等待正在执行的 QMT 调用退出后才释放本机锁；不自动撤单。
        with self.tick_lock:
            try:
                if self.repo:
                    try:
                        self.repo.release_executor()
                    finally:
                        self.repo.close()
            finally:
                if self.local_lock:
                    self.local_lock.release()
                self.initialized = False
                self.recovery_complete = False

# ---- order_bridge/http.py ----
import datetime as dt
import json
import math
import os
import queue
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, unquote, urlsplit


# QMT ORDER：原有查询保持同步队列，交易命令先入 PostgreSQL 再由定时器派发。
# 此源码由构建器与内部模块合并成单份 GBK 策略，不要直接导入本源码片段。
# 参数面板推荐填写 account_id、http_port，小写优先，兼容大写参数。
# 面板变量可能在脚本执行前或 init 前注入，不能用默认值覆盖已有变量。
# 未传运行参数时使用下面的默认值；账户号按字符串处理以保留前导零。
DEFAULT_ACCOUNT_ID = "66027616"
DEFAULT_HTTP_PORT = 8888

try:
    ACCOUNT_ID
except NameError:
    ACCOUNT_ID = DEFAULT_ACCOUNT_ID
ACCOUNT_TYPE = "STOCK"
# 只监听本机回环地址；模拟盘和实盘通过各自参数配置不同的端口。
HTTP_HOST = "127.0.0.1"
try:
    HTTP_PORT
except NameError:
    HTTP_PORT = DEFAULT_HTTP_PORT
LOG_DIRECTORY = os.path.join(os.path.expanduser("~"), "qmt-bridge", "logs")
# 队列满立即返回 429；请求体上限为字节数；等待超时返回 504。
QUEUE_MAX_SIZE = 64
MAX_BODY_BYTES = 1024 * 1024
REQUEST_TIMEOUT_SECONDS = 10
# 限制每轮启动的任务数量和耗时，单次已开始的 QMT 调用不能被强制中断。
MAX_JOBS_PER_TICK = 10
SCHEDULE_BUDGET_MILLISECONDS = 50
SCHEDULE_INTERVAL = dt.timedelta(milliseconds=10)
ACCOUNT_TYPES = frozenset((
    "STOCK", "CREDIT", "FUTURE", "HUGANGTONG", "SHENGANGTONG", "STOCK_OPTION",
))
METHODS = frozenset(("account", "positions", "get_smart_algo_param"))

_LOG_LOCK = threading.Lock()
_ORDER_STATE = None
_ORDER_TIMER_ID = None


# 日志使用上海时区，线程锁保护 console 和每日 UTF-8 文件的双写。
# 日志失败不影响查询；只记录请求元信息，启动日志按需包含完整账户号。
def log_message(level, message, **fields):
    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8)))
    record = "{0} [{1}] {2}".format(
        now.strftime("%Y-%m-%d %H:%M:%S"), level, message
    )
    if fields:
        record += " " + json.dumps(fields, ensure_ascii=False, default=str)
    record = record.replace("\r", "\\r").replace("\n", "\\n")
    with _LOG_LOCK:
        try:
            print(record, flush=True)
        except Exception:
            pass
        try:
            os.makedirs(LOG_DIRECTORY, exist_ok=True)
            path = os.path.join(LOG_DIRECTORY, "order-" + now.strftime("%Y-%m-%d") + ".log")
            with open(path, "a", encoding="utf-8") as output:
                output.write(record + "\n")
        except OSError:
            try:
                print("{0} [ERROR] order log file write failed".format(
                    now.strftime("%Y-%m-%d %H:%M:%S")
                ), flush=True)
            except Exception:
                pass


# 可预期的接口错误，统一携带 HTTP 状态、业务错误码和说明。
class RequestJob:
    def __init__(self, request):
        self.request = request
        self.done = threading.Event()
        self.result = None
        self.error = None
        self.error_status = 500
        self.deadline = time.monotonic() + REQUEST_TIMEOUT_SECONDS
        self.expired = False
        self.started = False
        self.lock = threading.Lock()
        self.request_id = None

    def set_error(self, status, code, message):
        self.error_status = status
        self.error = {"code": code, "message": message}

    # 超时与开始执行共用锁，已在队列中过期的请求不再调用 QMT。
    def try_start(self):
        with self.lock:
            if self.expired or time.monotonic() >= self.deadline:
                self.expired = True
                return False
            self.started = True
            return True

    def expire(self):
        with self.lock:
            self.expired = True


# 本次策略实例的队列、监听服务和启动配置；停止后整体释放。
class OrderState:
    def __init__(self):
        self.request_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)
        self.stop_event = threading.Event()
        self.lifecycle_lock = threading.Lock()
        self.command_slots = threading.BoundedSemaphore(QUEUE_MAX_SIZE)
        self.server = None
        self.server_thread = None
        self.account_id = None
        self.http_port = None
        self.runtime = None


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def _reject_unknown(params, allowed, method):
    if not isinstance(params, dict):
        raise OrderError(400, "INVALID_PARAMS", "params must be an object")
    unknown = set(params) - allowed
    if unknown:
        raise OrderError(
            400, "INVALID_PARAMS",
            "unsupported {0} params: {1}".format(method, ",".join(sorted(unknown))),
        )


# 面板数值可能是浮点数：整数值转成不带 .0 的账户号。
# 大浮点数可能已损失精度，因此拒绝；字符串账户保留前导零。
def runtime_account_id(value):
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, bool):
        raise ValueError("ACCOUNT_ID must be a nonempty string or positive integer")
    if isinstance(value, int) and value > 0:
        return str(value)
    if (isinstance(value, float) and math.isfinite(value)
            and value.is_integer() and 0 < value < 2 ** 53):
        return str(int(value))
    raise ValueError("ACCOUNT_ID must be a nonempty string or exact positive integer")


# 端口必须明确且可监听：接受整数、整数浮点值、十进制整数字符串。
# 禁止 0，避免生产服务随机选择端口；bool 虽是 int 子类，也必须拒绝。
def runtime_http_port(value):
    if isinstance(value, bool):
        raise ValueError("HTTP_PORT must be an integer between 1 and 65535")
    if isinstance(value, str):
        value = value.strip()
        if not value or any(char < "0" or char > "9" for char in value):
            raise ValueError("HTTP_PORT must be an integer between 1 and 65535")
        value = int(value)
    if isinstance(value, float) and math.isfinite(value) and value.is_integer():
        value = int(value)
    if not isinstance(value, int) or not 1 <= value <= 65535:
        raise ValueError("HTTP_PORT must be an integer between 1 and 65535")
    return value


# HTTP accountId 显式值优先，否则使用启动时固定的账户号。
# 参数面板用 account_id，HTTP 接口沿用 accountId，二者名称不要混淆。
def _normalize_account_params(params, method):
    _reject_unknown(params, {"accountId", "accountType"}, method)
    state = _ORDER_STATE
    default_account = (state.account_id if state is not None and state.account_id is not None
                       else runtime_account_id(globals().get("account_id", ACCOUNT_ID)))
    account_id = params.get("accountId", default_account)
    account_type = params.get("accountType", ACCOUNT_TYPE)
    if not isinstance(account_id, str) or not account_id.strip():
        raise OrderError(400, "INVALID_PARAMS", "accountId must be a nonempty string")
    if not isinstance(account_type, str):
        raise OrderError(400, "INVALID_PARAMS", "accountType must be a string")
    account_type = account_type.strip().upper()
    if account_type not in ACCOUNT_TYPES:
        raise OrderError(400, "INVALID_PARAMS", "unsupported accountType")
    return {"accountId": account_id, "accountType": account_type}


# GET 支持单个或重复 algoList 参数；POST 使用字符串数组。
# 空列表交给 QMT 表示查询全部有权限算法配置，不启动任何算法任务。
def _normalize_algo_params(params, allow_string):
    _reject_unknown(params, {"algoList"}, "get_smart_algo_param")
    algo_list = params.get("algoList", [])
    if algo_list is None or algo_list == "":
        algo_list = []
    elif isinstance(algo_list, str) and allow_string:
        algo_list = [algo_list]
    if allow_string and isinstance(algo_list, list):
        algo_list = [item for item in algo_list if item != ""]
    if not isinstance(algo_list, list) or not all(
        isinstance(item, str) and item != "" for item in algo_list
    ):
        raise OrderError(400, "INVALID_PARAMS", "algoList must be a list of strings")
    return {"algoList": algo_list}


def normalize_request(method, params, allow_algo_string=True):
    if method not in METHODS:
        raise OrderError(404, "METHOD_NOT_FOUND", "unsupported method")
    if method in ("account", "positions"):
        params = _normalize_account_params(params, method)
    else:
        params = _normalize_algo_params(params, allow_algo_string)
    return {"method": method, "params": params}


# 在 QMT 调度回调中递归转换返回数据，避免 HTTP 线程访问 QMT 原生对象。
# 非有限浮点数转 null；兼容提供 tolist/item 的数组和标量，无需引入 numpy。
def qmt_json_value(value):
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [qmt_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): qmt_json_value(item) for key, item in value.items()}
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        try:
            return qmt_json_value(tolist())
        except (TypeError, ValueError):
            pass
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return qmt_json_value(item())
        except (TypeError, ValueError):
            pass
    return str(value)


def qmt_fields_to_dict(value):
    result = {}
    for name in dir(value):
        if name.startswith("m_"):
            # 保留全部 m_ 字段；读取失败必须报错，不能静默遗漏账户或持仓数据。
            result[name] = qmt_json_value(getattr(value, name))
    return result


# 业务分发只由 QMT 定时回调执行，HTTP 线程不得直接调用此函数。
def dispatch_request(ContextInfo, request):
    if not isinstance(request, dict):
        raise OrderError(400, "INVALID_REQUEST", "request must be an object")
    method = request.get("method")
    params = request.get("params")
    if not isinstance(method, str) or not isinstance(params, dict):
        raise OrderError(400, "INVALID_REQUEST", "method and params are required")
    normalized = normalize_request(method, params)
    params = normalized["params"]
    if method in ("account", "positions"):
        detail_type = "account" if method == "account" else "position"
        rows = get_trade_detail_data(
            params["accountId"], params["accountType"], detail_type
        )
        # None 或异常类型不能伪装成空持仓，只有有效空列表表示无记录。
        if not isinstance(rows, (list, tuple)):
            raise OrderError(
                500, "INVALID_QMT_RESULT", "get_trade_detail_data must return a list"
            )
        if method == "account":
            if not rows:
                raise OrderError(404, "ACCOUNT_NOT_FOUND", "account was not found")
            return qmt_fields_to_dict(rows[0])
        return [qmt_fields_to_dict(row) for row in rows]
    # 部分客户端不提供此全局 API，明确返回 501，避免把缺失能力当作空配置。
    api = globals().get("get_smart_algo_param")
    if not callable(api):
        raise OrderError(501, "API_UNAVAILABLE", "get_smart_algo_param is unavailable")
    result = api(params["algoList"])
    if not isinstance(result, dict):
        raise OrderError(500, "INVALID_QMT_RESULT", "get_smart_algo_param must return a dict")
    return qmt_json_value(result)


# schedule_run 入口：按本轮预算拉取队列，查询完成或异常都要释放等待方。
def process_http_requests(ContextInfo):
    state = _ORDER_STATE
    if state is None or state.stop_event.is_set():
        return
    if state.runtime is not None:
        state.runtime.tick()
    started_at = time.monotonic()
    processed = 0
    while processed < MAX_JOBS_PER_TICK:
        if (time.monotonic() - started_at) * 1000 >= SCHEDULE_BUDGET_MILLISECONDS:
            break
        try:
            job = state.request_queue.get_nowait()
        except queue.Empty:
            break
        try:
            with state.lifecycle_lock:
                if state.stop_event.is_set():
                    job.set_error(503, "ORDER_STOPPING", "HTTP order is stopping")
                elif not job.try_start():
                    job.set_error(504, "REQUEST_EXPIRED", "request expired before QMT processing")
            if job.error is None:
                job.result = dispatch_request(ContextInfo, job.request)
        except OrderError as exc:
            job.set_error(exc.status, exc.code, exc.message)
        except Exception:
            job.set_error(500, "QMT_ERROR", "QMT request failed")
        finally:
            if job.error is not None:
                log_message(
                    "ERROR" if job.error_status >= 500 else "WARNING",
                    "QMT request failed",
                    request_id=job.request_id,
                    method=job.request.get("method"),
                    status=job.error_status,
                    error_code=job.error["code"],
                )
            job.done.set()
            state.request_queue.task_done()
            processed += 1


# HTTP 层仅做解析、校验、入队和返回 JSON；QMT 查询留给调度回调。
class OrderRequestHandler(BaseHTTPRequestHandler):
    server_version = "QMTHttpOrder/1.0"

    def _begin_request(self):
        self.request_id = uuid.uuid4().hex
        self.request_started = time.monotonic()
        parsed = urlsplit(self.path)
        parts = [unquote(part) for part in parsed.path.split("/") if part]
        self.request_method = parts[0] if len(parts) == 1 else None
        return parsed

    def do_GET(self):
        parsed = self._begin_request()
        try:
            params = {}
            for name, values in parse_qs(parsed.query, keep_blank_values=True).items():
                params[name] = values[0] if len(values) == 1 else values
            if self.request_method in ("submit_order", "cancel_order", "order", "orders", "order_events", "capabilities", "health"):
                self._bridge_submit(params)
            else:
                self._submit(normalize_request(self.request_method, params, True))
        except OrderError as exc:
            self._send_error(exc.status, exc.code, exc.message)

    def do_POST(self):
        parsed = self._begin_request()
        try:
            if parsed.query:
                raise OrderError(400, "INVALID_PARAMS", "POST query params are unsupported")
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                raise OrderError(400, "INVALID_CONTENT_LENGTH", "invalid Content-Length")
            if content_length < 0:
                raise OrderError(400, "INVALID_CONTENT_LENGTH", "invalid Content-Length")
            if content_length > MAX_BODY_BYTES:
                raise OrderError(413, "BODY_TOO_LARGE", "JSON body is too large")
            raw_body = self.rfile.read(content_length)
            try:
                params = json.loads(
                    raw_body.decode("utf-8"),
                    parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
                ) if raw_body else {}
            except (UnicodeDecodeError, ValueError):
                raise OrderError(400, "INVALID_JSON", "request body must be UTF-8 JSON")
            if not isinstance(params, dict):
                raise OrderError(400, "INVALID_PARAMS", "JSON body must be an object")
            if self.request_method in ("submit_order", "cancel_order", "order", "orders", "order_events", "capabilities", "health"):
                self._bridge_submit(params)
            else:
                self._submit(normalize_request(self.request_method, params, False))
        except OrderError as exc:
            self._send_error(exc.status, exc.code, exc.message)

    def _unsupported_verb(self):
        self._begin_request()
        self._send_error(405, "METHOD_NOT_ALLOWED", "only GET and POST are supported")

    do_PUT = _unsupported_verb
    do_PATCH = _unsupported_verb
    do_DELETE = _unsupported_verb
    do_HEAD = _unsupported_verb
    do_OPTIONS = _unsupported_verb
    do_TRACE = _unsupported_verb
    do_CONNECT = _unsupported_verb

    def __getattr__(self, name):
        if name.startswith("do_"):
            return self._unsupported_verb
        raise AttributeError(name)

    def _submit(self, request):
        state = self.server.order_state
        job = RequestJob(request)
        job.request_id = self.request_id
        with state.lifecycle_lock:
            if state.stop_event.is_set():
                raise OrderError(503, "ORDER_STOPPING", "HTTP order is stopping")
            try:
                state.request_queue.put_nowait(job)
            except queue.Full:
                raise OrderError(429, "QUEUE_FULL", "request queue is full")
        log_message(
            "INFO", "Request received", request_id=self.request_id,
            method=request["method"], http_method=self.command,
            queue_size=state.request_queue.qsize(),
        )
        # HTTP 超时只停止等待；已进入 QMT 的查询可能仍会执行完毕。
        remaining = max(0.0, job.deadline - time.monotonic())
        if not job.done.wait(remaining):
            job.expire()
            self._send_error(504, "QMT_TIMEOUT", "QMT did not finish in time")
        elif job.error is not None:
            self._send_json(job.error_status, {"error": job.error})
        else:
            self._send_json(200, job.result)

    def _bridge_submit(self, params):
        state = self.server.order_state
        if state.stop_event.is_set():
            raise OrderError(503, "ORDER_STOPPING", "HTTP order is stopping")
        if state.runtime is None:
            raise OrderError(503, "EXECUTOR_NOT_READY", "order runtime is not initialized")
        log_message("INFO", "Request received", request_id=self.request_id,
                    method=self.request_method, http_method=self.command)
        command = self.request_method in ("submit_order", "cancel_order") and self.command == "POST"
        if command and not state.command_slots.acquire(False):
            raise OrderError(429, "COMMAND_CAPACITY_EXCEEDED", "command capacity reached; retry with the same id")
        try:
            status, result = state.runtime.handle(self.request_method, params, self.command)
        except OrderError:
            raise
        except Exception:
            # 不把驱动异常/连接字符串放入 HTTP 正文或日志。
            raise OrderError(503, "PERSISTENCE_UNAVAILABLE", "order persistence is unavailable; retry with the same id")
        finally:
            if command:
                state.command_slots.release()
        self._send_json(status, result)

    def _send_error(self, status, code, message):
        self._send_json(status, {"error": {"code": code, "message": message}})

    def _send_json(self, status, payload):
        body = json.dumps(
            payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        fields = {
            "request_id": self.request_id,
            "method": self.request_method,
            "status": status,
            "response_bytes": len(body),
            "elapsed_ms": round((time.monotonic() - self.request_started) * 1000, 2),
        }
        if isinstance(payload, dict):
            fields.update({key: payload[key] for key in ("client_order_id", "order_id", "cancel_request_id") if key in payload})
        if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
            fields["error_code"] = payload["error"].get("code")
        log_message("INFO" if status < 400 else "WARNING", "Response sent", **fields)

    def log_message(self, format, *args):
        pass


def serve_http(state):
    try:
        while not state.stop_event.is_set():
            state.server.handle_request()
    finally:
        if state.server is not None:
            state.server.server_close()


# QMT 启动入口：先校验参数，再绑定账户、监听端口并注册定时任务。
# 有效配置固定在 OrderState 中，运行期间修改面板不会热切换账户或端口。
def init(ContextInfo):
    global _ORDER_STATE, _ORDER_TIMER_ID
    if _ORDER_STATE is not None:
        raise RuntimeError("HTTP order is already initialized")
    # 显式的小写参数优先；小写值非法时直接失败，不静默回退到其他账户。
    account_id = runtime_account_id(globals().get("account_id", ACCOUNT_ID))
    http_port = runtime_http_port(globals().get("http_port", HTTP_PORT))
    ContextInfo.set_account(account_id)
    state = OrderState()
    state.account_id = account_id
    state.http_port = http_port
    pg_config = read_pg_config(globals())
    state.runtime = OrderRuntime(globals(), ContextInfo, account_id, pg_config=pg_config, logger=log_message)
    server = ThreadingHTTPServer((HTTP_HOST, http_port), OrderRequestHandler)
    server.timeout = 0.2
    server.order_state = state
    state.server = server
    state.server_thread = threading.Thread(
        target=serve_http, args=(state,), name="qmt-http-order", daemon=True
    )
    _ORDER_STATE = state
    try:
        state.runtime.initialize()
        _ORDER_TIMER_ID = ContextInfo.schedule_run(
            process_http_requests, "20200101000000", -1,
            SCHEDULE_INTERVAL, "http_order_timer",
        )
        state.server_thread.start()
    except Exception:
        # 启动中途失败时撤销定时器并关闭端口，便于修正配置后重新启动。
        state.stop_event.set()
        timer_id = _ORDER_TIMER_ID
        _ORDER_TIMER_ID = None
        if timer_id is not None:
            try:
                ContextInfo.cancel_schedule_run(timer_id)
            except Exception:
                pass
        server.server_close()
        state.runtime.stop()
        _ORDER_STATE = None
        raise
    log_message(
        "INFO", "QMT HTTP order listening",
        host=HTTP_HOST, port=server.server_address[1], account_id=state.account_id,
    )
    if pg_config:
        log_message("INFO", "ORDER persistence configured", account_id=account_id,
                    database=pg_config["pg_database"], schema=pg_config["pg_schema"],
                    executor_initialized=state.runtime.initialized)


# 服务由定时器驱动，不依赖行情 tick 到达，因此 handlebar 无需业务逻辑。
def handlebar(ContextInfo):
    pass


# QMT 停止入口：阻止新请求、撤销定时器、以 503 唤醒排队请求，再关闭服务。
# 即使取消定时器抛出异常，finally 仍会完成队列和端口清理。
def stop(ContextInfo):
    global _ORDER_STATE, _ORDER_TIMER_ID
    state = _ORDER_STATE
    if state is None:
        return
    with state.lifecycle_lock:
        state.stop_event.set()
        if state.runtime is not None:
            state.runtime.stop_event.set()
    timer_id = _ORDER_TIMER_ID
    _ORDER_TIMER_ID = None
    try:
        if timer_id is not None:
            ContextInfo.cancel_schedule_run(timer_id)
    finally:
        if state.runtime is not None:
            try:
                state.runtime.stop()
            except Exception:
                log_message("ERROR", "Order executor cleanup failed", error_code="EXECUTOR_CLOSE_ERROR")
        count = 0
        while True:
            try:
                job = state.request_queue.get_nowait()
            except queue.Empty:
                break
            job.set_error(503, "ORDER_STOPPING", "HTTP order is stopping")
            job.done.set()
            state.request_queue.task_done()
            count += 1
        if state.server_thread is not None and state.server_thread.is_alive():
            state.server_thread.join(timeout=1.0)
        if state.server is not None:
            state.server.server_close()
        _ORDER_STATE = None
        log_message("INFO", "HTTP order stopped", pending_released=count)


# QMT 回报只把当前上下文中的对象转为普通字典；数据库更新交给调度器。
def order_callback(ContextInfo, orderInfo):
    state = _ORDER_STATE
    if state is not None and state.runtime is not None:
        state.runtime.observe("order", orderInfo)


def deal_callback(ContextInfo, dealInfo):
    state = _ORDER_STATE
    if state is not None and state.runtime is not None:
        state.runtime.observe("deal", dealInfo)


def task_callback(ContextInfo, taskInfo):
    state = _ORDER_STATE
    if state is not None and state.runtime is not None:
        state.runtime.observe("task", taskInfo)


def orderError_callback(ContextInfo, passOrderInfo, msg):
    state = _ORDER_STATE
    if state is not None and state.runtime is not None:
        try:
            raw = state.runtime.adapter.snapshot(passOrderInfo)
            raw["error_message"] = str(msg)
            state.runtime.observe("error", raw)
        except Exception:
            state.runtime.observation_gap = True
