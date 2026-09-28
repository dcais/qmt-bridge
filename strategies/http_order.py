# -*- coding: gbk -*-
# Last modified (Asia/Shanghai): 2026-09-28 15:31:30

# ---- order_bridge/common.py ----
# QMT ORDER 运行参数（策略编辑器右侧“参数设置”，修改后停止并重新运行策略）
# 参数名       默认值           用途
# account_id   "66027616"       绑定的股票账户；建议使用字符串，保留账户号前导零。
# http_port    8888             HTTP 监听端口，整数 1..65535；模拟盘和实盘可用不同端口。
# submit_batch_size    10       每轮实际派发业务订单上限；篮子算一笔。
# cancel_batch_size    10       每轮实际 QMT 撤单动作上限；任务和子委托分别计数。
# reconcile_batch_size 100      每批对账业务订单数；同一轮快照复用，剩余批次跨 tick 推进。
# reconcile_interval_seconds 30  对账间隔秒数；每轮成功或失败后等待此间隔，也用于逐单 due_at。
# schedule_budget_ms  50        整个调度回调的毫秒预算；已开始的同步 QMT 调用不可强制中断。
# 以上批量、预算和间隔参数均为正整数，支持面板整数浮点值；与账户、端口一起在启动时冻结。
# pg_host      "127.0.0.1"      PostgreSQL 服务器地址。
# pg_port      5432             PostgreSQL 端口，整数 1..65535。
# pg_database  未配置           数据库名称；启用交易必填，模拟盘/实盘分别连接不同数据库。
# pg_user      未配置           数据库用户名；启用交易接口时必填。
# pg_password  未配置           数据库密码；启用交易接口时必填，不打印或通过 HTTP 返回。
# 上述参数均优先读取小写名称，也兼容同名全大写参数；小写值非法时不回退。
# pg_database、pg_user、pg_password 全部未配置或为空时，仅开放查询模式；启用交易须完整填写。
# 每个数据库内部固定使用 qmt_order schema，不接受 pg_schema 运行参数。
# DDL 统一存放在 sql/order_init.sql；仅维护当前初始化结构，不提供历史版本升级。
# 策略启动检查关键表、字段和版本，再按 account_id 自动补齐账户运行记录；已有记录不重置。
# 不自动建表或升级，无需在部署前手工注册账户。
# 不同数据库隔离订单与幂等记录，http_port 不参与幂等身份。
# HTTP_HOST 固定为 127.0.0.1，交易账户类型固定为 STOCK；日志和队列设置是代码常量。
#
"""订单共用类型及调用诊断；导入时不连接数据库或调用 QMT。Last modified: 2026-09-28。"""
import copy
import datetime as dt
import hashlib
import json
import math as order_call_math
import re
import time as order_call_time
import uuid
from decimal import Decimal


class OrderError(Exception):
    def __init__(self, status, code, message):
        super(OrderError, self).__init__(message)
        self.status = status
        self.code = code
        self.message = message


def qmt_exception_details(exc, default_code="QMT_ERROR"):
    """只读 traceback 元数据；QMT 回调线程不查源码、文件或局部变量。"""
    def message(current):
        try:
            return str(current)[:4096]
        except Exception:
            return "<exception message unavailable>"

    def render(current, seen):
        if id(current) in seen or len(seen) >= 8:
            return ""
        seen.add(id(current))
        prior = current.__cause__
        separator = "\nThe above exception was the direct cause of the following exception:\n\n"
        if prior is None and not current.__suppress_context__:
            prior = current.__context__
            separator = "\nDuring handling of the above exception, another exception occurred:\n\n"
        result = render(prior, seen) + separator if prior is not None else ""
        result += "Traceback (most recent call last):\n"
        frames = []
        item = current.__traceback__
        while item is not None and len(frames) < 64:
            code = item.tb_frame.f_code
            frames.append('  File "{0}", line {1}, in {2}\n'.format(
                code.co_filename[:1024], item.tb_lineno, code.co_name[:256]))
            item = item.tb_next
        result += "".join(frames)
        result += "{0}: {1}\n".format(type(current).__name__, message(current))
        return result

    try:
        code = getattr(exc, "code", default_code)
    except Exception:
        code = default_code
    if not isinstance(code, str) or not code:
        code = default_code
    trace = render(exc, set())
    details = {"code": code[:128], "message": message(exc),
               "type": type(exc).__name__[:128], "traceback": trace[:16384]}
    for name, attribute, limit in (("phase", "qmt_phase", 64),
                                   ("field", "qmt_field", 256),
                                   ("object_type", "qmt_object_type", 128),
                                   ("return_type", "qmt_return_type", 128)):
        try:
            value = getattr(exc, attribute, None)
        except Exception:
            continue
        if isinstance(value, str) and value:
            details[name] = value[:limit]
    for name, attribute in (("return_count", "qmt_return_count"),
                            ("row_index", "qmt_row_index")):
        try:
            value = getattr(exc, attribute, None)
        except Exception:
            continue
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            details[name] = value
    try:
        snapshot = getattr(exc, "qmt_return_snapshot", None)
    except Exception:
        snapshot = None
    if isinstance(snapshot, dict):
        details["return_snapshot"] = snapshot
    return details


def _qmt_trace_parameters(parameters):
    """只复制有界内置值；不读取 ContextInfo/原生对象，不调用 repr。"""
    budget = {"nodes": 512, "chars": 16384, "truncated": False}

    def plain(value, depth=0):
        budget["nodes"] -= 1
        if budget["nodes"] < 0 or depth > 5:
            budget["truncated"] = True
            return "<truncated>"
        kind = type(value)
        if value is None or kind is bool:
            return value
        if kind is int:
            if value.bit_length() > 2048:
                budget["truncated"] = True
                return "<large integer>"
            return value
        if kind is float:
            return value if order_call_math.isfinite(value) else None
        if kind is str:
            limit = min(1024, max(0, budget["chars"]))
            budget["truncated"] |= len(value) > limit
            budget["chars"] -= min(len(value), limit)
            return value[:limit]
        if kind is dict:
            result = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= 64 or budget["nodes"] <= 0 or budget["chars"] <= 0:
                    budget["truncated"] = True
                    break
                if type(key) is not str:
                    budget["truncated"] = True
                    continue
                name = plain(key, depth + 1)
                # 调用参数白名单由适配层提供，再防御性屏蔽凭据字段。
                if any(part in key.lower() for part in ("password", "secret", "token", "authorization")):
                    result[name] = "<redacted>"
                else:
                    result[name] = plain(item, depth + 1)
            return result
        if kind in (list, tuple):
            result = []
            for index, item in enumerate(value):
                if index >= 32 or budget["nodes"] <= 0 or budget["chars"] <= 0:
                    budget["truncated"] = True
                    break
                result.append(plain(item, depth + 1))
            return result
        return "<" + kind.__name__[:128] + ">"

    return plain(parameters), budget


def qmt_invoke(logger, qmt_method, function, args=(), kwargs=None, parameters=None, correlation=None):
    """记录真实 QMT 调用边界；不重试、不转换返回值，日志失效不影响调用。"""
    if logger is None:
        return function(*args, **(kwargs or {}))
    fields = {"qmt_method": qmt_method, "qmt_call_id": uuid.uuid4().hex}
    try:
        if type(correlation) is dict:
            for key in ("request_id", "account_id", "account_type", "client_order_id", "order_id",
                        "cancel_request_id", "attempt_id", "round_id", "stage", "query_kind",
                        "qmt_task_id", "qmt_order_id", "directive_id"):
                value = correlation.get(key)
                if type(value) in (str, int, bool):
                    fields[key] = value[:1024] if type(value) is str else value
        fields["qmt_parameters"], bounds = _qmt_trace_parameters(parameters or {})
        fields["parameters_truncated"] = bounds["truncated"]
    except Exception:
        fields.update(qmt_parameters={}, parameters_truncated=True)

    def emit(message, extra=None):
        try:
            record = dict(fields)
            record.update(extra or {})
            logger("INFO", message, **record)
        except Exception:
            # 日志故障不能使已经受理的调用被跳过，也不能引发自动重发。
            pass

    emit("QMT call started")
    started = order_call_time.monotonic()
    try:
        result = function(*args, **(kwargs or {}))
    except Exception as exc:
        try:
            details = qmt_exception_details(exc)
            emit("QMT call failed", {
                "elapsed_ms": round((order_call_time.monotonic() - started) * 1000, 3),
                "error_code": details["code"], "error_type": details["type"],
                "error_message": details["message"], "traceback": details["traceback"]})
        except Exception:
            pass
        raise
    try:
        kind = type(result)
        summary = {"elapsed_ms": round((order_call_time.monotonic() - started) * 1000, 3),
                   "return_type": kind.__name__[:128]}
        if result is None or kind in (str, bool, int, float):
            summary["return_value"], bounds = _qmt_trace_parameters(result)
            if bounds["truncated"]:
                summary["return_truncated"] = True
        elif kind in (list, tuple, dict):
            # 查询返回仅记录容器条数；不为日志再次读取原生业务属性。
            summary["return_count"] = len(result)
        emit("QMT call returned", summary)
    except Exception:
        pass
    return result


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
    for key in ("request_hash", "remark", "contract_version", "attempts", "reconcile_requested",
                "manual_resolutions", "terminal_facts_fingerprint"):
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
"""纯订单事实归并；所有调用副作用由持久库和运行时负责。Last modified: 2026-09-28。"""
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
    if any(row.get('evidence_id') == key for row in doc.get('resolved_evidence', [])):
        return
    if not any(row['evidence_id'] == key for row in evidence):
        evidence.append({'evidence_id': key, 'kind': kind, 'raw': copy_json(raw),
                         'source': source, 'reason': reason, 'observed_at': observed_at})
    doc['reconciliation_complete'] = False
    doc['sync_status'] = 'INCOMPLETE'


def resolve_missing_order_evidence(doc):
    """仅以同账户、同日、同市场的双重 QMT 原生引用消解缺委托号回报。"""
    active = doc.get('unassociated_evidence', [])
    remaining = []
    changed = False
    for evidence in active:
        if evidence.get('kind') != 'order' or evidence.get('reason') != 'MISSING_QMT_ID':
            remaining.append(evidence)
            continue
        raw = evidence.get('raw') or {}
        ids = observation_identifiers('order', raw)
        scope = (ids['account_id'], ids['trading_day'], ids['market'])
        references = (state_id(raw.get('m_nRef')), state_id(raw.get('m_strOrderRef')))
        if not all(scope) or ids['account_id'] != doc.get('account_id') or not all(references):
            remaining.append(evidence)
            continue
        candidates = []
        conflict = False
        for row in doc['qmt_orders']:
            known = observation_identifiers('order', row.get('raw') or {})
            for key in ('account_id', 'trading_day', 'market', 'symbol', 'side',
                        'remark', 'qmt_task_id'):
                known[key] = known[key] or row.get(key)
            known_scope = (known['account_id'], known['trading_day'], known['market'])
            if known_scope != scope or not all(known_scope) or not row.get('qmt_order_id'):
                continue
            native = (state_id(row.get('native_ref') or (row.get('raw') or {}).get('m_nRef')),
                      state_id(row.get('native_order_ref') or (row.get('raw') or {}).get('m_strOrderRef')))
            if not any(left == right for left, right in zip(references, native) if right):
                continue
            # 共享任一引用却在另一引用或业务字段上冲突时，不猜测子委托归属。
            if native != references or any(ids[key] and known[key] and ids[key] != known[key]
                                           for key in ('symbol', 'side', 'remark', 'qmt_task_id')):
                conflict = True
                continue
            candidates.append(row)
        if conflict or len(candidates) != 1:
            remaining.append(evidence)
            continue
        row = candidates[0]
        # 旧回报可能带有查询尚未覆盖的累计成交；不能静默丢失该事实。
        cumulative = max(state_number(raw, 'm_nVolumeTraded'), state_number(raw, 'filled_quantity'))
        if cumulative > row.get('filled_quantity', 0):
            remaining.append(evidence)
            continue
        resolved = copy_json(evidence)
        resolved['qmt_order_id'] = row['qmt_order_id']
        resolved['identity_basis'] = {'account_id': scope[0], 'trading_day': scope[1],
                                      'market': scope[2], 'm_nRef': references[0],
                                      'm_strOrderRef': references[1]}
        previous = doc.setdefault('resolved_evidence', [])
        if not any(item.get('evidence_id') == evidence.get('evidence_id') for item in previous):
            previous.append(resolved)
        changed = True
    if changed:
        doc['unassociated_evidence'] = remaining
        # 身份消解不是一次完整查询；由 mark_reconciled 明确释放对账门闩。
        doc['reconciliation_complete'] = False
    return changed


def apply_observation(doc, kind, raw, source, observed_at=None):
    before = copy_json(doc)
    stamp = observed_at or iso_datetime()
    ids = observation_identifiers(kind, raw)
    if kind == 'error':
        # 错误回调不能证明先前不确定调用未进入交易系统。
        doc['error'] = {'raw': copy_json(raw), 'source': source, 'observed_at': stamp}
        if doc['error'] != before.get('error'):
            doc['reconciliation_complete'] = False
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
                    # 成对保存引用，避免两个不完整快照拼出虚假的委托身份。
                    native_ref = state_id(raw.get('m_nRef'))
                    native_order_ref = state_id(raw.get('m_strOrderRef'))
                    previous_raw = previous_record.get('raw') or {}
                    previous_refs = (state_id(previous_record.get('native_ref') or previous_raw.get('m_nRef')),
                                     state_id(previous_record.get('native_order_ref') or previous_raw.get('m_strOrderRef')))
                    reference_conflict = any(old and new and old != new for old, new in
                                             zip(previous_refs, (native_ref, native_order_ref)))
                    if reference_conflict:
                        state_evidence(doc, kind, raw, source, 'CONFLICTING_ORDER_REFERENCE', stamp)
                    elif native_ref and native_order_ref:
                        record['native_ref'] = native_ref
                        record['native_order_ref'] = native_order_ref
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
    if doc.get('cancel_requested') and doc.get('cancel_status') in state_CANCEL_ACTIVE:
        return False
    return doc['execution_status'] in state_TERMINAL or doc['execution_status'] == 'INCOMPLETE'


def reconcile_pending(doc):
    """持久对账门闩：仅完整证据能使已进入 QMT 的订单退出。"""
    if doc.get('reconcile_requested') or doc.get('unassociated_evidence'):
        return True
    status = doc.get('submission_status')
    if status == 'QUEUED':
        return False
    entered = bool(doc.get('qmt_orders') or doc.get('qmt_tasks') or doc.get('fills'))
    if status in ('CANCELLED_LOCAL', 'EXPIRED', 'REJECTED') and not entered:
        return False
    if status in ('SUBMITTING', 'UNKNOWN'):
        return True
    return not state_execution_finished(doc)


def state_terminal_facts(doc):
    """记录已确认终态所依赖的事实，排除对账检查点与派生状态。"""
    derived = {'filled_quantity', 'filled_amount', 'open_quantity',
               'cancelled_quantity', 'execution_status'}
    return fingerprint({'submission_status': doc['submission_status'],
                        'request': doc.get('request'),
                        'items': [{key: value for key, value in item.items() if key not in derived}
                                  for item in doc['items']],
                        'qmt_orders': doc['qmt_orders'], 'qmt_tasks': doc['qmt_tasks'],
                        'fills': doc['fills'], 'error': doc.get('error'),
                        'unassociated_evidence': doc.get('unassociated_evidence'),
                        'resolved_evidence': doc.get('resolved_evidence')})


def recompute_order(doc, now=None, reconciled=False):
    before = copy_json(doc)
    resolve_missing_order_evidence(doc)
    if reconciled:
        doc['reconciliation_complete'] = True
    if doc['submission_status'] == 'QUEUED' and doc.get('submit_before'):
        current = parse_timestamp(now) if isinstance(now, str) else (now or utc_now())
        if current >= parse_timestamp(doc['submit_before']):
            doc['submission_status'] = 'EXPIRED'
    identity_complete = all(row.get('trading_day') and row.get('market') and row.get('item_id')
                            for row in doc['qmt_orders'])
    complete = bool(doc.get('reconciliation_complete')) and not doc.get('unassociated_evidence') and identity_complete
    terminal_facts = state_terminal_facts(doc)
    # 查询失败只改变覆盖状态；已有完整对账确认的相同事实仍保留执行终态。
    preserve_terminal = (not complete and bool(doc.get('last_reconciled_at'))
                         and doc.get('terminal_facts_fingerprint') == terminal_facts)
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
              and (complete or preserve_terminal) and all_orders_filled):
            # 金额单的整手余款不要求成交额等于预算；须由每笔委托及稳定成交共同证明已成。
            status = 'FILLED'
        elif local:
            status = 'REJECTED' if doc['submission_status'] == 'REJECTED' else 'NOT_STARTED'
        elif terminal_orders and producer_stopped and (complete or preserve_terminal):
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
    if complete:
        if any(status in state_TERMINAL for status in statuses):
            doc['terminal_facts_fingerprint'] = state_terminal_facts(doc)
        else:
            doc.pop('terminal_facts_fingerprint', None)
    elif not preserve_terminal:
        doc.pop('terminal_facts_fingerprint', None)
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
    doc['reconcile_pending'] = reconcile_pending(doc)
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
    # 旧文档尚无事实指纹时，先保存已经完整确认的终态，再记录失败检查点。
    if (not complete and not doc.get('terminal_facts_fingerprint')
            and doc.get('reconciliation_complete') and doc.get('sync_status') == 'COMPLETE'
            and doc.get('last_reconciled_at')
            and any(item.get('execution_status') in state_TERMINAL for item in doc['items'])):
        doc['terminal_facts_fingerprint'] = state_terminal_facts(doc)
    resolve_missing_order_evidence(doc)
    doc['reconciliation_complete'] = bool(complete)
    if complete:
        doc['last_reconciled_at'] = now if isinstance(now, str) else iso_datetime(now)
        doc['reconcile_requested'] = False
    recompute_order(doc, now=now)
    return before != doc

# ---- order_bridge/repository.py ----
"""PostgreSQL 订单事实库；事务头锁同时保护投影和连续事件游标。Last modified: 2026-09-28。"""
import hashlib
import json
import math
import re
import threading
import uuid
from datetime import timedelta



repo_SCHEMA_VERSION = 2
repo_EPOCH = "1970-01-01T00:00:00Z"
repo_CHILD_TABLES = {"order_items": "items", "execution_attempts": "attempts",
                     "cancel_requests": "cancel_requests", "qmt_tasks": "qmt_tasks",
                     "qmt_orders": "qmt_orders", "fills": "fills"}
repo_RECONCILE_CHECKPOINT_FIELDS = frozenset((
    "version", "updated_at", "last_reconcile_round", "reconcile_round_fact_version",
    "last_reconcile_attempt_at", "last_reconciled_at", "reconcile_due_at",
    "reconcile_priority", "reconcile_requested"))


def repo_reconcile_state(doc):
    """排除轮次和时间检查点，保留同步、终态及子记录的业务变化。"""
    return {key: value for key, value in doc.items()
            if key not in repo_RECONCILE_CHECKPOINT_FIELDS}


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
        self.repo_pending_cursor = 0
        # PostgreSQL advisory lock 本身已隔离数据库，不把可别名的连接参数混入锁键。
        lock_scope = [config.get("pg_schema", "qmt_order"),
                      self.account_type, self.account_id]
        self.repo_lock_key = int.from_bytes(hashlib.sha256(json_text(lock_scope).encode("utf-8")).digest()[:8],
                                           byteorder="big", signed=True)

    def repo_connect(self):
        conn = None
        cur = None
        try:
            factory = self.repo_connect_factory
            if factory is None:
                import psycopg2
                factory = psycopg2.connect
            timeout = float(self.config.get("pg_connect_timeout", 3))
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValueError("pg_connect_timeout must be finite and positive")
            # libpq accepts whole seconds and treats zero as an unlimited wait.
            connect_timeout = max(2, int(math.ceil(timeout)))
            conn = factory(host=self.config.get("pg_host", "127.0.0.1"),
                           port=int(self.config.get("pg_port", 5432)),
                           dbname=self.config.get("pg_database", "postgres"),
                           user=self.config.get("pg_user", "postgres"),
                           password=self.config.get("pg_password", ""),
                           connect_timeout=connect_timeout)
            cur = conn.cursor()
            cur.execute("SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)",
                        (str(int(self.config.get("pg_statement_timeout_ms", 2000))),
                         str(int(self.config.get("pg_lock_timeout_ms", 1000)))))
            conn.commit()
            cur.close()
            return conn
        except Exception:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            try:
                if conn is not None:
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
                "orders": ("account_type", "account_id", "order_id", "client_order_id", "request_hash", "remark", "active", "document",
                           "submission_status", "cancel_ready", "reconcile_pending", "reconcile_priority", "reconcile_due_at",
                           "last_reconcile_attempt_at", "last_reconciled_at", "fact_version", "created_at"),
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
        # 健康请求可能从 HTTP/DB 线程发起；专用 advisory 会话仅由后台所有者检查。
        result["executor"] = self.repo_executor is not None and not self.repo_executor_lost
        def unknown_count(cur):
            cur.execute("SELECT count(*) FILTER (WHERE submission_status='UNKNOWN'),"
                        "count(*) FILTER (WHERE reconcile_pending) FROM " + self.repo_s +
                        ".orders WHERE account_type=%s AND account_id=%s", self.repo_scope)
            return cur.fetchone()
        result["unknown_order_count"], result["pending_count"] = self.repo_run(unknown_count)
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

    def repo_save(self, cur, doc, event_type, fresh=False, fact_source=None, child_fields=None):
        doc.setdefault("fact_version", 0)
        doc.setdefault("last_reconcile_attempt_at", None)
        doc.setdefault("reconcile_due_at", repo_EPOCH)
        doc.setdefault("reconcile_priority", False)
        doc.setdefault("reconcile_requested", False)
        doc["reconcile_pending"] = reconcile_pending(doc)
        if event_type in ("QMT_OBSERVATION", "MANUAL_UNKNOWN_RESOLUTION", "RECONCILE_GAP"):
            # fact_version 是外部异步事实代次；本轮 QMT 查询归并只改变普通文档版本。
            if event_type != "QMT_OBSERVATION" or fact_source not in ("query", "history"):
                doc["fact_version"] += 1
            doc["reconcile_requested"] = True
            doc["reconcile_pending"] = True
            doc["reconcile_priority"] = True
            doc["reconcile_due_at"] = repo_EPOCH
        if not fresh:
            doc["version"] = int(doc.get("version", 0)) + 1
        doc["updated_at"] = iso_datetime()
        cancel_ready = bool(pending_cancellations(doc))
        cur.execute("INSERT INTO " + self.repo_s + ".orders(account_type,account_id,order_id,client_order_id,request_hash,"
                    "remark,active,document,submission_status,cancel_ready,reconcile_pending,reconcile_priority,reconcile_due_at,"
                    "last_reconcile_attempt_at,last_reconciled_at,fact_version,created_at) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(account_type,account_id,order_id) DO UPDATE SET active=EXCLUDED.active,document=EXCLUDED.document,"
                    "submission_status=EXCLUDED.submission_status,cancel_ready=EXCLUDED.cancel_ready,"
                    "reconcile_pending=EXCLUDED.reconcile_pending,reconcile_priority=EXCLUDED.reconcile_priority,"
                    "reconcile_due_at=EXCLUDED.reconcile_due_at,last_reconcile_attempt_at=EXCLUDED.last_reconcile_attempt_at,"
                    "last_reconciled_at=EXCLUDED.last_reconciled_at,fact_version=EXCLUDED.fact_version",
                    self.repo_scope + (doc["order_id"], doc["client_order_id"], doc["request_hash"], doc["remark"],
                                       is_order_active(doc), json_text(doc), doc["submission_status"], cancel_ready,
                                       bool(doc["reconcile_pending"]), bool(doc["reconcile_priority"]), doc["reconcile_due_at"],
                                       doc["last_reconcile_attempt_at"], doc.get("last_reconciled_at"), doc["fact_version"],
                                       doc["created_at"]))
        for table, field in repo_CHILD_TABLES.items():
            if child_fields is not None and field not in child_fields:
                continue
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
        if event_type is not None:
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

    def claim_submission(self, order_id, authority=None):
        if authority is None:
            # 仅供后台执行权所有者沿用；跨线程必须显式传入冻结授权。
            self.check_executor()
            authority = (self.repo_executor_instance, self.repo_executor_epoch)
        elif isinstance(authority, dict):
            authority = (authority.get("instance_id"), authority.get("epoch"))
        authority = tuple(authority)
        def claim(cur):
            cur.execute("SELECT executor_instance,executor_epoch FROM " + self.repo_s +
                        ".account_runtime WHERE account_type=%s AND account_id=%s", self.repo_scope)
            if tuple(cur.fetchone()) != authority:
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
                                    "executor_epoch": authority[1]})
            return True, self.repo_save(cur, doc, "SUBMISSION_CLAIMED")
        return self.repo_run(claim, mutation=True)

    def claim_cancel(self, order_id, action, authority):
        """持久化一次撤单调用意图，同时核验执行代次和仍可执行的目标。"""
        if isinstance(authority, dict):
            authority = (authority.get("instance_id"), authority.get("epoch"))
        authority = tuple(authority)
        def claim(cur):
            cur.execute("SELECT executor_instance,executor_epoch FROM " + self.repo_s +
                        ".account_runtime WHERE account_type=%s AND account_id=%s", self.repo_scope)
            if tuple(cur.fetchone()) != authority:
                raise OrderError(503, "EXECUTOR_LOCK_LOST", "executor epoch changed")
            doc = self.repo_require(self.repo_load(cur, order_id, lock=True))
            keys = ("kind", "target_id", "cancel_request_id")
            candidate = next((row for row in pending_cancellations(doc)
                              if all(str(row.get(key)) == str(action.get(key)) for key in keys)), None)
            if candidate is None:
                return False, doc
            attempt = dict(candidate, attempt_id=str(action.get("attempt_id") or uuid.uuid4()),
                           status="CALLING", created_at=iso_datetime(), executor_epoch=authority[1])
            doc["attempts"].append(attempt)
            return True, self.repo_save(cur, doc, "CANCEL_CLAIMED")
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

    def queued_orders(self, limit=10, cursor=None):
        def listing(cur):
            params = self.repo_scope
            after = ""
            if cursor:
                after = " AND order_id>%s"
                params += (str(cursor),)
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "AND submission_status='QUEUED'" + after + " ORDER BY order_id LIMIT %s",
                        params + (max(1, min(int(limit), 1000)),))
            return [repo_json(row[0]) for row in cur.fetchall()]
        return self.repo_run(listing)

    def cancellation_orders(self, limit=100, cursor=None):
        def listing(cur):
            params = self.repo_scope
            after = ""
            if cursor:
                after = " AND order_id>%s"
                params += (str(cursor),)
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "AND cancel_ready=true" + after + " ORDER BY order_id LIMIT %s",
                        params + (max(1, min(int(limit), 1000)),))
            return [repo_json(row[0]) for row in cur.fetchall()]
        return self.repo_run(listing)

    def reconcile_orders(self, limit=100, cursor=None, due_before=None):
        """候选只读页；cursor 为 (优先标志,最近尝试时间,order_id)。"""
        def listing(cur):
            params = self.repo_scope + (due_before or iso_datetime(),)
            after = ""
            if cursor:
                after = " AND (NOT reconcile_priority,COALESCE(last_reconcile_attempt_at,'epoch'::timestamptz),order_id)>(%s,%s::timestamptz,%s)"
                params += (not cursor[0], cursor[1] or repo_EPOCH, cursor[2])
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "AND reconcile_pending=true AND reconcile_due_at<=%s" + after +
                        " ORDER BY reconcile_priority DESC,COALESCE(last_reconcile_attempt_at,'epoch'::timestamptz),order_id LIMIT %s",
                        params + (max(1, min(int(limit), 1000)),))
            return [repo_json(row[0]) for row in cur.fetchall()]
        return self.repo_run(listing)

    def reconcile_history_start(self):
        def read(cur):
            cur.execute("SELECT min(created_at) FROM " + self.repo_s + ".orders "
                        "WHERE account_type=%s AND account_id=%s AND reconcile_pending=true", self.repo_scope)
            value = cur.fetchone()[0]
            return iso_datetime(value) if value else None
        return self.repo_run(read)

    def begin_reconcile_batch(self, limit=100, round_id=None, cursor=None, interval_seconds=30):
        """同一轮只选一次订单，逐单记录尝试；后续批次共用外部 QMT 快照。"""
        if not round_id:
            raise ValueError("round_id is required")
        if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, int) or not 1 <= interval_seconds <= 2147483647:
            raise ValueError("reconcile interval must be a positive integer")
        limit = max(1, min(int(limit), 1000))
        def begin(cur):
            params = self.repo_scope + (iso_datetime(), str(round_id))
            after = ""
            if cursor:
                after = " AND (NOT reconcile_priority,COALESCE(last_reconcile_attempt_at,'epoch'::timestamptz),order_id)>(%s,%s::timestamptz,%s)"
                params += (not cursor[0], cursor[1] or repo_EPOCH, cursor[2])
            cur.execute("SELECT order_id,document,last_reconcile_attempt_at,reconcile_priority FROM " + self.repo_s +
                        ".orders WHERE account_type=%s AND account_id=%s AND reconcile_pending=true "
                        "AND reconcile_due_at<=%s AND document->>'last_reconcile_round' IS DISTINCT FROM %s" + after +
                        " ORDER BY reconcile_priority DESC,COALESCE(last_reconcile_attempt_at,'epoch'::timestamptz),order_id LIMIT %s FOR UPDATE",
                        params + (limit + 1,))
            rows = cur.fetchall()
            selected = rows[:limit]
            stamp = iso_datetime()
            docs = []
            for order_id, raw, unused, priority in selected:
                doc = repo_json(raw)
                doc["last_reconcile_round"] = str(round_id)
                doc["reconcile_round_fact_version"] = int(doc.get("fact_version", 0))
                doc["last_reconcile_attempt_at"] = stamp
                # 选中的订单至少在本轮归并结束前不会再次进入首页。
                doc["reconcile_due_at"] = iso_datetime(utc_now() + timedelta(seconds=interval_seconds))
                doc["reconcile_priority"] = False
                self.repo_save(cur, doc, None, child_fields=())
                docs.append(doc)
            next_cursor = None
            if len(rows) > limit and selected:
                last = selected[-1]
                next_cursor = [last[3], iso_datetime(last[2]) if last[2] else None, last[0]]
            return {"orders": docs, "next_cursor": next_cursor, "has_more": len(rows) > limit}
        return self.repo_run(begin, mutation=True)

    def reconcile_round_batch(self, round_id, limit=100, cursor=None):
        """查询后按冻结轮次有界取单，携带查询前的逐单事实代次。"""
        limit = max(1, min(int(limit), 1000))
        def listing(cur):
            params = self.repo_scope + (str(round_id),)
            after = ""
            if cursor:
                after = " AND order_id>%s"
                params += (str(cursor),)
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "AND document->>'last_reconcile_round'=%s" + after + " ORDER BY order_id LIMIT %s",
                        params + (limit + 1,))
            docs = [repo_json(row[0]) for row in cur.fetchall()]
            return {"orders": docs[:limit], "next_cursor": docs[limit - 1]["order_id"] if len(docs) > limit else None,
                    "has_more": len(docs) > limit}
        return self.repo_run(listing)

    def finish_reconcile(self, order_id, expected_fact_version, complete, stamp=None, interval_seconds=30):
        """旧快照只能记录尝试，不能清除其后回报/缺口设下的门闩。"""
        if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, int) or not 1 <= interval_seconds <= 2147483647:
            raise ValueError("reconcile interval must be a positive integer")
        def finish(cur):
            doc = self.repo_require(self.repo_load(cur, order_id, lock=True))
            if int(doc.get("fact_version", 0)) != int(expected_fact_version):
                return False
            before = copy_json(doc)
            mark_reconciled(doc, complete=complete, now=stamp)
            doc["reconcile_due_at"] = iso_datetime(utc_now() + timedelta(seconds=interval_seconds))
            doc["reconcile_priority"] = False
            changed_children = tuple(field for field in repo_CHILD_TABLES.values()
                                     if before.get(field) != doc.get(field))
            event_type = ("RECONCILE_STATE_CHANGED" if repo_reconcile_state(before) != repo_reconcile_state(doc)
                          else None)
            self.repo_save(cur, doc, event_type, child_fields=changed_children)
            return True
        return self.repo_run(finish, mutation=True)

    def mark_reconcile_gap(self, since, limit=100, cursor=None, startup=False):
        """按 order_id 有界重开进入过 QMT 的记录；启动时保留已完整对账的终态。"""
        since = since if isinstance(since, str) else iso_datetime(since)
        limit = max(1, min(int(limit), 1000))
        def mark(cur):
            params = self.repo_scope
            after = ""
            if cursor:
                after = " AND order_id>%s"
                params += (str(cursor),)
            cur.execute("SELECT order_id,document FROM " + self.repo_s +
                        ".orders WHERE account_type=%s AND account_id=%s "
                        "AND (submission_status NOT IN ('QUEUED','CANCELLED_LOCAL','EXPIRED','REJECTED') "
                        "OR document->'qmt_orders'<>'[]'::jsonb OR document->'qmt_tasks'<>'[]'::jsonb "
                        "OR document->'fills'<>'[]'::jsonb)" + after + " ORDER BY order_id LIMIT %s FOR UPDATE",
                        params + (limit + 1,))
            rows = cur.fetchall()
            marked = 0
            for order_id, raw in rows[:limit]:
                doc = repo_json(raw)
                if doc.get("last_reconcile_gap_since") == since:
                    continue
                if (startup and state_execution_finished(doc) and doc.get("last_reconciled_at")
                        and not doc.get("reconcile_pending") and not doc.get("reconcile_requested")
                        and not doc.get("unassociated_evidence")):
                    continue
                doc["last_reconcile_gap_since"] = since
                doc["reconcile_requested"] = True
                self.repo_save(cur, doc, "RECONCILE_GAP")
                marked += 1
            has_more = len(rows) > limit
            return {"marked": marked, "next_cursor": rows[limit - 1][0] if has_more else None,
                    "has_more": has_more}
        return self.repo_run(mark, mutation=True)

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

    def repo_apply_pending(self, cur, limit=100):
        limit = max(1, min(int(limit), 1000))
        cur.execute("SELECT observation_id,kind,raw,source,observed_at FROM " + self.repo_s +
                    ".qmt_observations WHERE account_type=%s AND account_id=%s AND applied=false "
                    "AND observation_id>%s ORDER BY observation_id LIMIT %s",
                    self.repo_scope + (self.repo_pending_cursor, limit))
        rows = cur.fetchall()
        if not rows and self.repo_pending_cursor:
            self.repo_pending_cursor = 0
            cur.execute("SELECT observation_id,kind,raw,source,observed_at FROM " + self.repo_s +
                        ".qmt_observations WHERE account_type=%s AND account_id=%s AND applied=false "
                        "ORDER BY observation_id LIMIT %s", self.repo_scope + (limit,))
            rows = cur.fetchall()
        for row in rows:
            doc = self.repo_match(cur, row[1], repo_json(row[2]))
            if doc is None:
                continue
            before = json_text(doc)
            apply_observation(doc, row[1], repo_json(row[2]), row[3], observed_at=row[4])
            if json_text(doc) != before:
                self.repo_save(cur, doc, "QMT_OBSERVATION", fact_source=row[3])
            cur.execute("UPDATE " + self.repo_s + ".qmt_observations SET order_id=%s,applied=true WHERE observation_id=%s",
                        (doc["order_id"], row[0]))
        if rows:
            self.repo_pending_cursor = rows[-1][0]
        return len(rows)

    def replay_pending_observations(self, limit=100):
        return self.repo_run(lambda cur: self.repo_apply_pending(cur, limit), mutation=True)

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
                self.repo_save(cur, doc, "QMT_OBSERVATION", fact_source=source)
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

    def recover(self, limit=100, cursor=None, authority=None):
        if authority is None:
            self.check_executor()
        limit = max(1, min(int(limit), 1000))
        def recovery(cur):
            if authority is not None:
                expected = (authority.get("instance_id"), authority.get("epoch")) if isinstance(authority, dict) else tuple(authority)
                cur.execute("SELECT executor_instance,executor_epoch FROM " + self.repo_s +
                            ".account_runtime WHERE account_type=%s AND account_id=%s", self.repo_scope)
                if tuple(cur.fetchone()) != expected:
                    raise OrderError(503, "EXECUTOR_LOCK_LOST", "executor epoch changed")
            params = self.repo_scope
            after = ""
            if cursor:
                after = " AND order_id>%s"
                params += (str(cursor),)
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        + after + " ORDER BY order_id LIMIT %s FOR UPDATE", params + (limit + 1,))
            docs = [repo_json(row[0]) for row in cur.fetchall()]
            count = 0
            for doc in docs[:limit]:
                before = json_text(doc)
                submitting = doc["submission_status"] == "SUBMITTING"
                if (doc["submission_status"] == "QUEUED" and doc.get("order_type") == "BASKET"
                        and doc.get("resolved_request")):
                    # 崩溃可能发生在 set_basket 成功与阶段落库之间；先读回，再决定是否需要设置。
                    doc["preparation_stage"] = "BASKET_GET"
                    doc["preparation_complete"] = False
                    doc["basket_state"] = "PENDING"
                if submitting:
                    doc["submission_status"] = "UNKNOWN"
                    doc["sync_status"] = "PENDING"
                for attempt in doc.get("attempts", []):
                    is_cancel = attempt.get("kind") in ("CANCEL", "CANCEL_ORDER", "CANCEL_TASK")
                    if (is_cancel or submitting) and attempt.get("status") in ("CALLING", "RETURNED"):
                        attempt["status"] = "UNKNOWN"
                        if is_cancel:
                            doc["cancel_status"] = "UNKNOWN"
                recompute_order(doc)
                if json_text(doc) != before:
                    self.repo_save(cur, doc, "EXECUTOR_RECOVERY")
                    count += 1
            self.repo_apply_pending(cur)
            has_more = len(docs) > limit
            return {"recovered": count, "next_cursor": docs[limit - 1]["order_id"] if has_more else None,
                    "has_more": has_more}
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
"""QMT 函数适配层。调用者必须处于策略调度回调线程。Last modified: 2026-09-28。"""
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
_qmt_range = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*$")
_qmt_diagnostic_missing = object()


def _qmt_error(code, message, status=422):
    raise OrderError(status, code, message)


def _qmt_exception_context(exc, phase, field=None, value=None):
    """尽力给原异常附加纯字符串位置，不改变异常类型；Last modified: 2026-09-26。"""
    for name, text in (("qmt_phase", phase), ("qmt_field", field),
                       ("qmt_object_type", type(value).__name__ if value is not None else None)):
        if text is None:
            continue
        try:
            if getattr(exc, name, None) is None:
                setattr(exc, name, text)
        except Exception:
            pass


def _qmt_query_result_context(exc, rows, index):
    for name, item in (("qmt_return_type", type(rows).__name__),
                       ("qmt_return_count", len(rows)), ("qmt_row_index", index)):
        try:
            setattr(exc, name, item)
        except Exception:
            pass


def _qmt_declared_field(value, name):
    """静态查描述符定义，不调用原生 getter。"""
    try:
        for cls in type(value).__mro__:
            fields = vars(cls)
            if name in fields:
                return cls.__module__ + "." + cls.__name__, type(fields[name]).__name__
    except Exception:
        pass
    return None, None


def _qmt_field_error(value, name, exc):
    declared_on, descriptor_type = _qmt_declared_field(value, name)
    try:
        message = str(exc)[:1024]
    except Exception:
        message = "<exception message unavailable>"
    return {"type": type(exc).__name__[:128], "message": message,
            "declared_on": declared_on, "descriptor_type": descriptor_type}


def _qmt_diagnostic_plain(value, budget, depth=0):
    """仅裁剪已经快照化的普通值；绝不 repr 原生对象。"""
    if budget["nodes"] <= 0:
        budget["truncated"] = True
        return _qmt_diagnostic_missing
    budget["nodes"] -= 1
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        length = min(len(value), 1024, budget["chars"])
        budget["chars"] -= length
        if length < len(value):
            budget["truncated"] = True
            if length == 0:
                return _qmt_diagnostic_missing
        return value[:length]
    if isinstance(value, (int, float)):
        try:
            length = len(str(value))
        except Exception:
            budget["truncated"] = True
            return _qmt_diagnostic_missing
        if length > budget["chars"] or length > 1024:
            budget["truncated"] = True
            return _qmt_diagnostic_missing
        budget["chars"] -= length
        return value if not isinstance(value, float) or math.isfinite(value) else None
    if isinstance(value, (list, tuple, dict)):
        if depth >= 4:
            budget["truncated"] = True
            return _qmt_diagnostic_missing
        if isinstance(value, (list, tuple)):
            result = []
            if len(value) > 16:
                budget["truncated"] = True
            for member in value[:16]:
                item = _qmt_diagnostic_plain(member, budget, depth + 1)
                if item is not _qmt_diagnostic_missing:
                    result.append(item)
            return result
        result = {}
        if len(value) > 16:
            budget["truncated"] = True
        for index, (key, member) in enumerate(value.items()):
            if index >= 16:
                break
            if not isinstance(key, str):
                budget["truncated"] = True
                continue
            bounded_key = _qmt_diagnostic_plain(key, budget, depth + 1)
            bounded_member = _qmt_diagnostic_plain(member, budget, depth + 1)
            if bounded_key is not _qmt_diagnostic_missing and bounded_member is not _qmt_diagnostic_missing:
                result[bounded_key] = bounded_member
        return result
    budget["truncated"] = True
    return _qmt_diagnostic_missing


def _qmt_return_diagnostic(value, names, raw, errors, stopped_early):
    budget = {"nodes": 512, "chars": 16384, "truncated": stopped_early}
    selected = names[:128]
    first_error = next((name for name in names if name in errors), None)
    if first_error is not None and first_error not in selected:
        selected = names[:127] + [first_error]
    if len(names) > len(selected):
        budget["truncated"] = True
    fields, field_errors = {}, {}
    # 错误字段优先保留；所有文字共用同一个字符预算。
    for name in selected:
        if name not in errors:
            continue
        bounded_name = _qmt_diagnostic_plain(name[:256], budget)
        bounded_error = _qmt_diagnostic_plain(errors[name], budget)
        if bounded_name is not _qmt_diagnostic_missing and bounded_error is not _qmt_diagnostic_missing:
            field_errors[bounded_name] = bounded_error
    for name in selected:
        if name in raw:
            bounded_name = _qmt_diagnostic_plain(name[:256], budget)
            plain = _qmt_diagnostic_plain(raw[name], budget)
            if bounded_name is not _qmt_diagnostic_missing and plain is not _qmt_diagnostic_missing:
                fields[bounded_name] = plain
    return {"object_type": type(value).__name__[:128],
            "object_module": type(value).__module__[:256],
            "fields_source": "dir(object)", "field_count": len(names),
            "fields": fields, "field_errors": field_errors,
            "truncated": budget["truncated"]}


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
    def __init__(self, apis, context, account_id=None, account_type="STOCK", logger=None):
        self.apis = apis
        self.context = context
        self.account_id = account_id or getattr(context, "account_id", None)
        self.account_type = account_type
        self.logger = logger
        self.log_context = {}
        self._xt_tag_warning_logged = False

    def _warn_excluded_xt_tag(self, value, names, raw):
        if self._xt_tag_warning_logged:
            return
        self._xt_tag_warning_logged = True
        if self.logger is None:
            return
        try:
            declared_on, descriptor_type = _qmt_declared_field(value, "m_xtTag")
            diagnostic = _qmt_return_diagnostic(value, names, raw, {}, False)
            diagnostic["skipped_fields"] = {"m_xtTag": {
                "reason": "QMT_INTERNAL_NATIVE_FIELD", "declared_on": declared_on,
                "descriptor_type": descriptor_type}}
            self.logger("WARN", "QMT snapshot internal field excluded",
                        account_id=self.account_id, object_type=type(value).__name__[:128],
                        skipped_field="m_xtTag", return_snapshot=diagnostic)
        except Exception:
            # 日志不可用不影响已成功转换的业务字段。
            pass

    def available(self, name):
        return callable(self.apis.get(name))

    def _require(self, name):
        function = self.apis.get(name)
        if not callable(function):
            _qmt_error("API_UNAVAILABLE", name + " is unavailable", 501)
        return function

    def _call(self, name, args, parameters, correlation=None):
        """仅包装一次实际原生调用；上下文不包含 ContextInfo。Last modified: 2026-09-26。"""
        function = self._require(name)
        context = dict(self.log_context)
        context.update(correlation or {})
        context["account_id"] = self.account_id
        return qmt_invoke(self.logger, name, function, args=args,
                          parameters=parameters, correlation=context)

    def resolve(self, request, remark):
        result = self._resolve_base(request, remark)
        if request["execution"]["type"] == "SMART":
            smart = request["execution"]
            algorithms = [smart["algorithm"]]
            data = self.snapshot(self._call("get_smart_algo_param", (algorithms,),
                                            {"algoList": algorithms}))
            self._resolve_smart(result, smart, remark, data)
        return result

    def _resolve_base(self, request, remark):
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
        return result

    def _resolve_smart(self, result, smart, remark, data):
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

    def prepare_step(self, order, stage="RESOLVE", payload=None):
        """一次阶段最多调用一个 QMT API；调用方持久化 updates 和后续 stage。"""
        if not isinstance(order, dict):
            _qmt_error("INVALID_ORDER", "order must be an object", 400)
        if stage == "RESOLVE":
            request = order.get("request")
            result = self._resolve_base(request, order.get("remark"))
            if request["execution"]["type"] == "SMART":
                return {"stage": "SMART", "updates": {}}
            return {"stage": "BASKET_GET" if order.get("order_type") == "BASKET" else None,
                    "updates": {"resolved_request": result}}
        if stage == "SMART":
            request = order.get("request")
            if not isinstance(request, dict) or request.get("execution", {}).get("type") != "SMART":
                _qmt_error("INVALID_PREPARE_STAGE", "SMART stage needs SMART request", 400)
            result = self._resolve_base(request, order.get("remark"))
            smart = request["execution"]
            algorithms = [smart["algorithm"]]
            data = self.snapshot(self._call("get_smart_algo_param", (algorithms,),
                                            {"algoList": algorithms},
                                            {"order_id": order.get("order_id"),
                                             "client_order_id": request.get("client_order_id")}))
            self._resolve_smart(result, smart, order.get("remark"), data)
            return {"stage": "BASKET_GET" if order.get("order_type") == "BASKET" else None,
                    "updates": {"resolved_request": result}}
        if stage not in ("BASKET_GET", "BASKET_SET", "BASKET_VERIFY"):
            _qmt_error("INVALID_PREPARE_STAGE", "unknown preparation stage", 400)
        expected = self._expected_basket(order)
        correlation = {"order_id": order.get("order_id"),
                       "client_order_id": order.get("client_order_id")}
        if stage == "BASKET_SET":
            basket = copy_json(expected)
            self._call("set_basket", (basket,), basket, correlation)
            return {"stage": "BASKET_VERIFY", "updates": {}}
        actual = self.snapshot(self._call("get_basket", (expected["name"],),
                                          {"name": expected["name"]}, correlation))
        if stage == "BASKET_GET" and not actual:
            return {"stage": "BASKET_SET", "updates": {}}
        self._check_basket(actual, expected)
        return {"stage": None, "updates": {}}

    def _expected_basket(self, order):
        if order.get("order_type") != "BASKET":
            _qmt_error("INVALID_PREPARE_STAGE", "basket stage needs basket order", 400)
        name = order.get("basket_name")
        if not name or order.get("resolved_request", {}).get("orderCode") != name:
            _qmt_error("BASKET_NAME_MISMATCH", "basket name differs from frozen request")
        return {"name": name, "stocks": [
            {"stock": item["symbol"], "weight": 0, "quantity": item["quantity"],
             "optType": 23 if item["side"] == "BUY" else 24}
            for item in order["items"]]}

    def prepare_basket(self, order):
        if order["order_type"] != "BASKET":
            return False
        expected = self._expected_basket(order)
        name = expected["name"]
        self._require("get_basket")
        self._require("set_basket")
        correlation = {"order_id": order.get("order_id"),
                       "client_order_id": order.get("client_order_id")}
        existing = self._call("get_basket", (name,), {"name": name}, correlation)
        if existing:
            self._check_basket(existing, expected)
            return False
        self._call("set_basket", (expected,), expected, correlation)
        self._check_basket(self._call("get_basket", (name,), {"name": name}, correlation), expected)
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
        parameters = {key: value for key, value in frozen.items() if key != "function"}
        correlation = {"order_id": order.get("order_id"),
                       "client_order_id": order.get("client_order_id")}
        return self._call(name, tuple(base), parameters, correlation)

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
        name = "cancel" if kind == "CANCEL_ORDER" else "cancel_task"
        result = self._call(name, (identifier, account, self.account_type, self.context),
                            {"target_id": identifier, "accountID": account,
                             "accountType": self.account_type},
                            {"cancel_request_id": action.get("cancel_request_id"),
                             "qmt_order_id" if kind == "CANCEL_ORDER" else "qmt_task_id": identifier})
        return bool(result)

    def query(self, kind):
        if kind not in ("order", "deal", "task"):
            _qmt_error("INVALID_QUERY", "kind must be order, deal or task", 400)
        if not self.account_id:
            _qmt_error("ACCOUNT_UNAVAILABLE", "adapter has no bound account", 500)
        try:
            rows = self._call("get_trade_detail_data", (self.account_id, self.account_type, kind),
                              {"accountID": self.account_id, "accountType": self.account_type,
                               "dataType": kind}, {"query_kind": kind})
        except Exception as exc:
            _qmt_exception_context(exc, "native_query")
            raise
        if not isinstance(rows, (list, tuple)):
            _qmt_error("INVALID_QMT_RESULT", "query returned invalid collection", 502)
        result = []
        for row_index, row in enumerate(rows):
            try:
                result.append(self.snapshot(row))
            except Exception as exc:
                _qmt_exception_context(exc, "snapshot", value=row)
                _qmt_query_result_context(exc, rows, row_index)
                raise
        return result

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
        try:
            names = [name for name in dir(value) if name.startswith("m_")]
        except Exception as exc:
            _qmt_exception_context(exc, "snapshot", value=value)
            raise
        preloaded = {}
        field_errors = {}
        first_error = None
        for name in _qmt_passorder_fields:
            if name in names:
                continue
            try:
                member = getattr(value, name, _qmt_diagnostic_missing)
                if member is not _qmt_diagnostic_missing:
                    names.append(name)
                    preloaded[name] = member
            except Exception as exc:
                _qmt_exception_context(exc, "snapshot", field=name, value=value)
                if first_error is None:
                    first_error = exc
                names.append(name)
                field_errors[name] = _qmt_field_error(value, name, exc)
        if not names:
            _qmt_error("INVALID_QMT_RESULT", "object has no recognized QMT fields", 502)
        raw = {}
        stopped_early = False
        for index, name in enumerate(names):
            if first_error is not None and index >= 128:
                stopped_early = True
                break
            if name == "m_xtTag":
                # QMT 内部标签的 native getter 缺少 Python 转换器；不参与业务委托字段。
                continue
            if name in field_errors:
                continue
            try:
                member = preloaded[name] if name in preloaded else getattr(value, name)
                raw[name] = self.snapshot(member)
            except Exception as exc:
                _qmt_exception_context(exc, "snapshot", field=name, value=value)
                if first_error is None:
                    first_error = exc
                field_errors[name] = _qmt_field_error(value, name, exc)
        if first_error is not None:
            try:
                first_error.qmt_return_snapshot = _qmt_return_diagnostic(
                    value, names, raw, field_errors, stopped_early)
            except Exception:
                pass
            raise first_error
        if not raw and "m_xtTag" in names:
            _qmt_error("INVALID_QMT_RESULT", "native object has no readable QMT fields after excluding m_xtTag", 502)
        if any(key in raw for key in _qmt_passorder_fields):
            result = _qmt_passorder_snapshot(raw)
        else:
            result = raw
        if "m_xtTag" in names:
            self._warn_excluded_xt_tag(value, names, raw)
        return result

# ---- order_bridge/async_log.py ----
"""ORDER 有界异步日志；调用线程只提交普通数据快照。Last modified: 2026-09-26。"""
import datetime as order_log_datetime
import json as order_log_json
import math as order_log_math
import os as order_log_os
import queue as order_log_queue
import threading as order_log_threading
import time as order_log_time


class AsyncOrderLogger(object):
    """后台写日志。sink(record) 仅在工作线程调用，并替代默认双写。"""

    _ZONE = order_log_datetime.timezone(order_log_datetime.timedelta(hours=8))
    _MAX_FIELDS = 32
    _MAX_TEXT = 1024
    _DIAGNOSTIC_LIMITS = {"traceback": 16384, "error_message": 4096}

    def __init__(self, log_directory, capacity=2048, sink=None):
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError("log capacity must be a positive integer")
        self.log_directory = log_directory
        self._queue = order_log_queue.Queue(maxsize=capacity)
        self._sink = sink
        self._state_lock = order_log_threading.Lock()
        self._stop_event = order_log_threading.Event()
        self._thread = None
        self._running = False
        self._stopped = False
        self._dropped = 0
        self._write_errors = 0
        self._sampled_at = None

    @classmethod
    def _plain(cls, value, limit=None):
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, str):
            return value[:cls._MAX_TEXT if limit is None else limit]
        return "<unsupported>"

    @classmethod
    def _return_snapshot(cls, value):
        """只复制有界普通诊断数据；不读取原生属性或调用 repr。"""
        budget = {"nodes": 2048, "chars": 65536, "truncated": False}

        def plain(item, depth=0):
            budget["nodes"] -= 1
            if budget["nodes"] < 0 or depth > 8:
                budget["truncated"] = True
                return "<truncated>"
            if item is None or isinstance(item, (bool, int)):
                return item
            if isinstance(item, float):
                return item if order_log_math.isfinite(item) else None
            if isinstance(item, str):
                limit = min(4096, max(0, budget["chars"]))
                budget["truncated"] |= len(item) > limit
                budget["chars"] -= min(len(item), limit)
                return item[:limit]
            if type(item) is dict:
                result = {}
                for index, (key, member) in enumerate(item.items()):
                    if index >= 128 or budget["nodes"] <= 0 or budget["chars"] <= 0:
                        budget["truncated"] = True
                        break
                    if not isinstance(key, str):
                        budget["truncated"] = True
                        continue
                    bounded_key = key[:256]
                    budget["truncated"] |= len(key) > 256
                    budget["chars"] -= len(bounded_key)
                    result[bounded_key] = plain(member, depth + 1)
                return result
            if type(item) in (list, tuple):
                result = []
                for index, member in enumerate(item):
                    if index >= 128 or budget["nodes"] <= 0 or budget["chars"] <= 0:
                        budget["truncated"] = True
                        break
                    result.append(plain(member, depth + 1))
                return result
            budget["truncated"] = True
            return "<unsupported>"

        result = plain(value)
        if type(result) is dict and budget["truncated"]:
            result["truncated"] = True
        return result

    def __call__(self, level, message, **fields):
        # 不在 QMT 调用线程格式化、打印、写文件或遍历原生对象。
        snapshot = {}
        for index, (key, value) in enumerate(fields.items()):
            if index >= self._MAX_FIELDS:
                break
            # 异常堆栈单独限长，避免普通字段的 1 KiB 截断掉底部根因。
            snapshot[key[:64]] = (self._return_snapshot(value) if key in ("return_snapshot", "qmt_parameters") else
                                  self._plain(value, self._DIAGNOSTIC_LIMITS.get(key)))
        record = (order_log_time.time(), self._plain(level), self._plain(message), snapshot)
        with self._state_lock:
            if self._stop_event.is_set():
                self._dropped += 1
                return False
            try:
                self._queue.put_nowait(record)
            except order_log_queue.Full:
                self._dropped += 1
                return False
        return True

    def start(self):
        with self._state_lock:
            if self._stop_event.is_set():
                return False
            if self._thread is not None:
                return True
            thread = order_log_threading.Thread(target=self._run, name="order-log-writer")
            thread.daemon = True
            self._thread = thread
            self._running = True
            try:
                thread.start()
            except Exception:
                self._thread = None
                self._running = False
                raise
        return True

    def request_stop(self):
        # stop 回调不等待写入；队列在后台自然排空。
        with self._state_lock:
            self._stop_event.set()
            if self._thread is None:
                self._stopped = True

    def join(self, timeout=None):
        with self._state_lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout)
        with self._state_lock:
            return self._stopped

    def health(self):
        with self._state_lock:
            return {"running": self._running, "queue_size": self._queue.qsize(),
                    "dropped": self._dropped, "write_errors": self._write_errors,
                    "stopping": self._stop_event.is_set(), "stopped": self._stopped,
                    "sampled_at": self._sampled_at}

    def _sample(self):
        now = order_log_datetime.datetime.now(self._ZONE).isoformat()
        with self._state_lock:
            self._sampled_at = now

    def _write_default(self, record):
        line = "{0} [{1}] {2}".format(record["timestamp"], record["level"], record["message"])
        if record["fields"]:
            line += " " + order_log_json.dumps(record["fields"], ensure_ascii=False, default=str)
        line = line.replace("\r", "\\r").replace("\n", "\\n")
        try:
            print(line, flush=True)
        except Exception:
            self._write_failed()
        try:
            order_log_os.makedirs(self.log_directory, exist_ok=True)
            path = order_log_os.path.join(self.log_directory, "order-" + record["day"] + ".log")
            with open(path, "a", encoding="utf-8") as output:
                output.write(line + "\n")
        except Exception:
            self._write_failed()

    def _write_failed(self):
        with self._state_lock:
            self._write_errors += 1

    def _run(self):
        try:
            while True:
                try:
                    item = self._queue.get(timeout=0.1)
                except order_log_queue.Empty:
                    self._sample()
                    if self._stop_event.is_set():
                        break
                    continue
                try:
                    when = order_log_datetime.datetime.fromtimestamp(item[0], self._ZONE)
                    record = {"timestamp": when.strftime("%Y-%m-%d %H:%M:%S"),
                              "day": when.strftime("%Y-%m-%d"), "level": item[1],
                              "message": item[2], "fields": item[3]}
                    if self._sink is None:
                        self._write_default(record)
                    else:
                        try:
                            self._sink(record)
                        except Exception:
                            self._write_failed()
                except Exception:
                    self._write_failed()
                finally:
                    self._queue.task_done()
                    self._sample()
        finally:
            with self._state_lock:
                self._running = False
                self._stopped = True
                self._sampled_at = order_log_datetime.datetime.now(self._ZONE).isoformat()

# ---- order_bridge/background.py ----
"""ORDER 后台事务与短期授权；Last modified: 2026-09-28。

队列只承载普通快照。容量令牌覆盖排队、QMT 在途和待持久结果整个生命期。
"""
import datetime as dt
import queue
import threading
import time
import uuid



def _background_current_day_covered(document, day):
    """按提交时间核对当日覆盖；委托、成交缺日期或已有旧事实时保留缺口。"""
    facts = [row for field in ('qmt_tasks', 'qmt_orders', 'fills')
             for row in document.get(field, [])]
    if document['submission_status'] in ('QUEUED', 'CANCELLED_LOCAL', 'EXPIRED', 'REJECTED') and not facts:
        return True
    attempts = [row for row in document.get('attempts', [])
                if row.get('kind') == 'SUBMIT' and row.get('status') != 'ABORTED_NO_CALL']
    stamps = [row.get('created_at') for row in attempts] or [document.get('created_at')]
    zone = dt.timezone(dt.timedelta(hours=8))
    try:
        if any(parse_timestamp(stamp).astimezone(zone).date() != day for stamp in stamps):
            return False
    except OrderError:
        return False
    # CTaskDetail 在本机不带交易日，以持久提交时间为界；委托和成交仍须明确日期。
    dated = [row for field in ('qmt_orders', 'fills') for row in document.get(field, [])]
    if any(not row.get('trading_day') for row in dated):
        return False
    return all(not row.get('trading_day') or str(row['trading_day']).replace('-', '') == day.strftime('%Y%m%d')
               for row in facts)


class OrderBackground(object):
    def __init__(self, runtime):
        self.r = runtime
        self.queues = {kind: queue.Queue(128) for kind in ('cancel', 'submit', 'prepare', 'query')}
        self.results = queue.Queue(128)
        self.capacity = threading.BoundedSemaphore(128)
        self.grant = None
        self.db_ready = False
        self.schema_ready = threading.Event()
        self.authority_stop = threading.Event()
        self.authority_done = threading.Event()
        self.started = False
        self.stopped = False
        self.pending = set()
        self.consumed = set()
        self.fact_generation = 0
        self.overflows = 0
        self.rotation = 0
        self.sample = {}
        self.sampled_at = None
        self.tick_ms = 0
        self.qmt_ms = 0
        self.round = None
        self.next_round = 0
        self.last_reconcile_attempt_at = None
        self.last_reconcile_finished_at = None
        self.next_reconcile_at = None
        self.last_reconcile_error = None
        self.cursors = {'submit': None, 'cancel': None}
        self.last_sample = 0
        self.gap_cursor = None
        self.gap_stamp = None
        self.gap_generation = -1
        self.handled_gap_generation = 0
        self.db_thread = None
        self.idle = threading.Event()
        self.idle.set()
        self.logged_versions = {}
        self.busy = False

    def _log(self, level, message, **fields):
        try:
            self.r.logger(level, message, **fields)
        except Exception:
            pass

    def start(self):
        if self.started or self.r.repo is None:
            if self.r.stop_event.is_set() and self.r.repo is None:
                self.stopped = self.r.stopped = True
                self.r.stopped_event.set()
            return
        self.started = True
        self.r.stopped = False
        self.db_thread = threading.Thread(target=self._work, name='order-database', daemon=True)
        self.db_thread.start()

    def authorized(self, directive=None, require_database=True):
        grant = self.grant
        return bool(grant and (not require_database or (self.db_ready and self.sample.get('ready', True))) and not self.r.stop_event.is_set()
                    and self.r.clock() < grant[2]
                    and (directive is None or directive.get('authority') == grant[:2]))

    def _authority(self):
        r = self.r
        try:
            while not self.authority_stop.is_set():
                if not self.schema_ready.wait(.05):
                    continue
                try:
                    if self.grant is None:
                        result = r.repo.acquire_executor(r.instance_id, r.host_id)
                        identity = (r.instance_id, result['epoch'])
                    # 发放期限从检查开始算；阻塞 SQL 不得延长旧授权。
                    began = r.clock()
                    r.repo.check_executor()
                    self.grant = identity + (began + .75,)
                except Exception as exc:
                    self.grant = None
                    r.last_error = getattr(exc, 'code', 'EXECUTOR_LOCK_LOST')
                    self._log('ERROR', 'Order authority lost', error_code=r.last_error)
                    break
                self.authority_stop.wait(.15)
        finally:
            self.grant = None
            try:
                r.repo.close()
            except Exception:
                r.last_error = 'PERSISTENCE_CLOSE_ERROR'
            finally:
                self.authority_done.set()

    def _enqueue(self, kind, **data):
        if not self.capacity.acquire(False):
            return False
        data.update(kind=kind, token=uuid.uuid4().hex,
                    authority=self.grant[:2] if self.grant else None)
        try:
            self.queues[kind].put_nowait(data)
            return True
        except queue.Full:
            self.capacity.release()
            return False

    def tick(self, deadline=None, budget=None, max_actions=None):
        r = self.r
        began = r.clock()
        r.last_tick = began
        if r.repo is None or r.stop_event.is_set() or not r.tick_lock.acquire(False):
            return 0
        try:
            if self.busy or r.stop_event.is_set():
                return 0
            self.busy = True
            self.idle.clear()
        finally:
            r.tick_lock.release()
        if deadline is None:
            deadline = began + r.settings['schedule_budget_ms'] / 1000.0
        budget = budget if budget is not None else {'submit': 0, 'cancel': 0}
        count = 0
        try:
            empty = 0
            kinds = ('cancel', 'submit', 'prepare', 'query')
            while r.clock() < deadline and (max_actions is None or count < max_actions):
                kind = kinds[self.rotation]
                self.rotation = (self.rotation + 1) % len(kinds)
                if kind != 'query' and not self.db_ready:
                    empty += 1
                    if empty >= 4:
                        break
                    continue
                if kind in ('cancel', 'submit') and budget.get(kind, 0) >= r.settings[kind + '_batch_size']:
                    empty += 1
                    if empty >= 4:
                        break
                    continue
                try:
                    directive = self.queues[kind].get_nowait()
                except queue.Empty:
                    empty += 1
                    if empty >= 4:
                        break
                    continue
                empty = 0
                result = dict(directive, status='ABORTED_NO_CALL')
                token = directive['token']
                if token in self.consumed:
                    # 指令去重由 attempt 生命周期控制；重复令牌不产生第二个结果。
                    continue
                self.consumed.add(token)
                call_start = r.clock()
                # 一次调度指令内可能有多个 QMT 调用；用持久尝试/轮次串联，不泄露到下一笔。
                document = directive.get('document') or {}
                action = directive.get('action') or {}
                previous_log_context = getattr(r.adapter, 'log_context', {})
                r.adapter.log_context = dict(
                    order_id=document.get('order_id'), client_order_id=document.get('client_order_id'),
                    cancel_request_id=action.get('cancel_request_id'),
                    attempt_id=token if kind in ('submit', 'cancel') else None,
                    directive_id=token, round_id=directive.get('round_id'), stage=kind,
                    query_kind=directive.get('query_kind'))
                try:
                    if not r.stop_event.is_set() and (kind == 'query' or self.authorized(directive, require_database=False)):
                        if kind != 'query' and not self.db_ready:
                            self.consumed.discard(token)
                            self.queues[kind].put_nowait(directive)
                            break
                        if kind == 'submit':
                            document = directive['document']
                            expired = False
                            try:
                                if document.get('request'):
                                    r._runtime_smart_window(document['request'])
                                expired = bool(document.get('submit_before') and utc_now() >= parse_timestamp(document['submit_before']))
                            except Exception:
                                expired = True
                            if expired:
                                result.update(status='ABORTED_NO_CALL', error={'code': 'ORDER_EXPIRED', 'message': 'Order expired before QMT call'})
                                self.results.put_nowait(result)
                                count += 1
                                continue
                            budget[kind] = budget.get(kind, 0) + 1
                            result['value'] = r.adapter.snapshot(r.adapter.submit(directive['document']))
                        elif kind == 'cancel':
                            budget[kind] = budget.get(kind, 0) + 1
                            result['value'] = r.adapter.cancel_action(directive['action'])
                        elif kind == 'prepare':
                            result['value'] = r.adapter.prepare_step(directive['document'], directive['stage'])
                        else:
                            result['value'] = r.adapter.query(directive['query_kind'])
                        result['status'] = 'RETURNED'
                except Exception as exc:
                    if kind == 'query':
                        result.update(status='UNKNOWN', error=qmt_exception_details(exc))
                    else:
                        result.update(status='UNKNOWN', error={'code': getattr(exc, 'code', 'QMT_ERROR'),
                                      'message': str(exc)[:4096], 'type': type(exc).__name__})
                finally:
                    r.adapter.log_context = previous_log_context
                self.qmt_ms = (r.clock() - call_start) * 1000
                # 预留令牌保证队列不会满，QMT 线程从不等待消费者。
                self.results.put_nowait(result)
                count += 1
            return count
        finally:
            self.tick_ms = (r.clock() - began) * 1000
            self.busy = False
            self.idle.set()

    def _work(self):
        r = self.r
        authority = threading.Thread(target=self._authority, name='order-authority', daemon=True)
        authority.start()
        retained = None
        initialized = False
        recovery_cursor = None
        setup_done = False
        recovery_done = False
        startup_gap_cursor = None
        startup_stamp = iso_datetime()
        try:
            while True:
                if r.stop_event.is_set():
                    # 仅后台等待已进入的 QMT 调用；本机锁继续持有。
                    with r.admission_lock:
                        pass
                    self.idle.wait()
                    external = getattr(r, 'external_idle', None)
                    if external is not None:
                        external.wait()
                    for source in self.queues.values():
                        while True:
                            try:
                                self.results.put_nowait(dict(source.get_nowait(), status='ABORTED_NO_CALL'))
                            except queue.Empty:
                                break
                    if retained is None and self.results.empty() and r.observations.empty():
                        break
                try:
                    if not initialized and not r.stop_event.is_set():
                        if not setup_done:
                            if r.local_lock:
                                r.local_lock.acquire()
                            self.sample = r.repo.check_schema()
                            r.repo.ensure_account_runtime()
                            self.schema_ready.set()
                            setup_done = True
                        if self.grant is None:
                            r.stop_event.wait(.02)
                            continue
                        if not recovery_done:
                            page = r.repo.recover(limit=r.settings['reconcile_batch_size'],
                                                  cursor=recovery_cursor, authority=self.grant[:2])
                            recovery_cursor = page['next_cursor']
                            if page['has_more']:
                                continue
                            recovery_done = True
                        gap = r.repo.mark_reconcile_gap(startup_stamp, limit=r.settings['reconcile_batch_size'],
                                                        cursor=startup_gap_cursor, startup=True)
                        startup_gap_cursor = gap['next_cursor']
                        if gap['has_more']:
                            continue
                        initialized = r.initialized = True
                        self.db_ready = True
                        self._log('INFO', 'Order executor recovering', account_id=r.account_id, schema_version=self.sample.get('schema_version'))
                    if retained is None:
                        try:
                            retained = ('result', self.results.get_nowait())
                        except queue.Empty:
                            try:
                                retained = ('observation', r.observations.get_nowait())
                            except queue.Empty:
                                pass
                    if retained:
                        if retained[0] == 'result':
                            self.db_ready = False
                            self._merge(retained[1])
                            self.capacity.release()
                            self.consumed.discard(retained[1]['token'])
                        else:
                            self.db_ready = False
                            document = r.repo.ingest_observation(retained[1][0], retained[1][1], 'callback')
                            if document and self.logged_versions.get(document['order_id']) != document.get('version'):
                                self.logged_versions[document['order_id']] = document.get('version')
                                if len(self.logged_versions) > 1024:
                                    self.logged_versions.pop(next(iter(self.logged_versions)))
                                identifiers = observation_identifiers(retained[1][0], retained[1][1])
                                self._log('INFO', 'QMT callback recorded', order_id=document['order_id'],
                                          client_order_id=document.get('client_order_id'), order_version=document.get('version'),
                                          qmt_order_id=identifiers.get('qmt_order_id'), qmt_task_id=identifiers.get('qmt_task_id'))
                        was_result = retained[0] == 'result'
                        retained = None
                        self.db_ready = initialized
                        if was_result:
                            continue
                    if not r.stop_event.is_set() and initialized:
                        self._service()
                    r.stop_event.wait(.005)
                except _ContinueMerge:
                    self.db_ready = True
                    if not r.stop_event.is_set():
                        try:
                            self._service()
                        except Exception as exc:
                            self.db_ready = False
                            r.last_error = getattr(exc, 'code', 'PERSISTENCE_UNAVAILABLE')
                    continue
                except Exception as exc:
                    self.db_ready = False
                    code = getattr(exc, 'code', 'PERSISTENCE_UNAVAILABLE')
                    if r.last_error != code:
                        self._log('ERROR', 'Order persistence suspended', error_code=code)
                    r.last_error = code
                    # retained 不移除；提交结果不明可幂等重写结果，认领异常则没有指令。
                    time.sleep(.05)
        finally:
            self.db_ready = False
            self.authority_stop.set()
            self.authority_done.wait()
            if r.local_lock:
                r.local_lock.release()
            r.initialized = r.recovery_complete = False
            self.stopped = r.stopped = True
            r.stopped_event.set()

    def _service(self):
        """每个有界归并批次后提供一次后台派发/对账服务机会。"""
        r = self.r
        r.repo.replay_pending_observations(limit=r.settings['reconcile_batch_size'])
        if (r.observation_gap or self.fact_generation != self.handled_gap_generation) and self.gap_stamp is None:
            self.gap_stamp = iso_datetime()
            self.gap_generation = self.fact_generation
        if self.gap_stamp is not None:
            gap = r.repo.mark_reconcile_gap(self.gap_stamp, limit=r.settings['reconcile_batch_size'], cursor=self.gap_cursor)
            self.gap_cursor = gap['next_cursor']
            if not gap['has_more']:
                self.gap_stamp = None
                self.handled_gap_generation = self.gap_generation
                r.observation_gap = self.handled_gap_generation != self.fact_generation
            else:
                return
        self._reconcile_step()
        if r.recovery_complete and self.authorized():
            self._dispatch()
        if r.clock() - self.last_sample >= 1:
            self.sample = r.repo.health()
            self.db_ready = True
            self.sampled_at = iso_datetime()
            self.last_sample = r.clock()

    def _dispatch(self):
        r = self.r
        # 同时只认领一笔提交；预取/准备不把整批订单变成 SUBMITTING。
        if not any(key[0] == 'submit' for key in self.pending):
            rows = r.repo.queued_orders(limit=r.settings['submit_batch_size'], cursor=self.cursors['submit'])
            self.cursors['submit'] = rows[-1]['order_id'] if rows else None
            for document in rows:
                key = ('submit', document['order_id'])
                if key in self.pending:
                    continue
                try:
                    r._runtime_smart_window(document['request'])
                except Exception as exc:
                    def reject(row):
                        if row['submission_status'] == 'QUEUED':
                            row.update(submission_status='EXPIRED', error={'code': exc.code, 'message': exc.message})
                            recompute_order(row)
                    r.repo.update_order(document['order_id'], 'ORDER_EXPIRED', reject)
                    continue
                stage = document.get('preparation_stage', 'RESOLVE')
                if not document.get('preparation_complete'):
                    if self._enqueue('prepare', document=document, stage=stage, pending_key=key):
                        self.pending.add(key)
                    break
                if not self.capacity.acquire(False):
                    break
                try:
                    grant = self.grant
                    claimed, prepared = r.repo.claim_submission(document['order_id'], authority=grant[:2])
                    if claimed:
                        self.pending.add(key)
                        attempt = prepared['attempts'][-1]
                        self.queues['submit'].put_nowait(dict(kind='submit', token=attempt['attempt_id'],
                            authority=grant[:2], document=prepared, pending_key=key))
                    else:
                        self.capacity.release()
                except Exception:
                    self.capacity.release()
                    raise
                break
        rows = r.repo.cancellation_orders(limit=r.settings['cancel_batch_size'], cursor=self.cursors['cancel'])
        self.cursors['cancel'] = rows[-1]['order_id'] if rows else None
        for document in rows:
            key = ('cancel', document['order_id'])
            if key in self.pending:
                continue
            actions = pending_cancellations(document)
            if not actions or not self.capacity.acquire(False):
                continue
            try:
                grant = self.grant
                action = dict(actions[0], cancel_request_id=document['active_cancel_request_id'])
                claimed, prepared = r.repo.claim_cancel(document['order_id'], action, authority=grant[:2])
                if claimed:
                    action = prepared['attempts'][-1]
                    self.pending.add(key)
                    self.queues['cancel'].put_nowait(dict(kind='cancel', token=action['attempt_id'],
                        authority=grant[:2], document=prepared, action=action, pending_key=key))
                else:
                    self.capacity.release()
            except Exception:
                self.capacity.release()
                raise

    def _merge(self, result):
        r = self.r
        kind = result['kind']
        if kind == 'query':
            if result['status'] == 'RETURNED':
                # 每条原始事实独立事务，游标保留于结果上以便故障重试。
                index = result.get('merge_index', 0)
                values = result['value']
                for raw in values[index:index + r.settings['reconcile_batch_size']]:
                    r.repo.ingest_observation(result['query_kind'], raw, 'query')
                    index += 1
                    result['merge_index'] = index
                if index < len(values):
                    raise _ContinueMerge()
            else:
                self.round['complete'] = False
                error = result.get('error') or {}
                summary = {key: error[key] for key in ('code', 'message', 'type', 'phase', 'field', 'object_type')
                           if key in error}
                summary.setdefault('code', result['status'])
                self.last_reconcile_error = summary
                self._log('ERROR', 'QMT reconciliation incomplete', account_id=r.account_id,
                          kind=result['query_kind'], query_kind=result['query_kind'],
                          round_id=result.get('round_id') or self.round['id'],
                          query_scope='CURRENT_DAY',
                          error_code=summary['code'],
                          error_message=error.get('message'), error_type=error.get('type'),
                          error_phase=error.get('phase'), error_field=error.get('field'),
                          object_type=error.get('object_type'),
                          return_type=error.get('return_type'), return_count=error.get('return_count'),
                          row_index=error.get('row_index'), return_snapshot=error.get('return_snapshot'),
                          traceback=error.get('traceback'))
                self.round['live_complete'] = False
            self.round['waiting'] = False
            self.round['stage'] += 1
            if self.round['stage'] == len(self.round['queries']):
                self.round['query_finished_at'] = iso_datetime()
            return
        doc = result['document']
        status = result['status']
        def update(row):
            if kind == 'prepare':
                if row['submission_status'] != 'QUEUED':
                    return
                if status == 'RETURNED':
                    value = result['value']
                    row.update(copy_json(value['updates']))
                    row['preparation_stage'] = value['stage']
                    row['preparation_complete'] = value['stage'] is None
                    if value['stage'] is None and row.get('order_type') == 'BASKET':
                        row['basket_state'] = 'VERIFIED'
                elif status == 'UNKNOWN':
                    row.update(submission_status='REJECTED', error=result.get('error'))
                    recompute_order(row)
                return
            for attempt in row['attempts']:
                if attempt.get('attempt_id') == result['token']:
                    attempt.update(status=status, returned_at=iso_datetime(), return_value=result.get('value'), error=result.get('error'))
            if kind == 'submit' and status != 'RETURNED':
                # 认领后不回 QUEUED；无调用证明也保守留给恢复/对账。
                if row['submission_status'] == 'SUBMITTING':
                    if status == 'ABORTED_NO_CALL' and (result.get('error') or {}).get('code') == 'ORDER_EXPIRED':
                        row.update(submission_status='EXPIRED', error=result['error'])
                    else:
                        row.update(submission_status='UNKNOWN', execution_status='UNKNOWN')
            if kind == 'cancel' and status == 'RETURNED' and result.get('value') is not True:
                for attempt in row['attempts']:
                    if attempt.get('attempt_id') == result['token']:
                        attempt['status'] = 'REJECTED'
            recompute_order(row)
        r.repo.update_order(doc['order_id'], kind.upper() + '_CALL_' + status, update)
        self.pending.discard(result['pending_key'])
        self._log('INFO' if status == 'RETURNED' else 'WARNING', 'QMT execution stage persisted',
                  stage=kind, outcome=status, order_id=doc['order_id'], client_order_id=doc.get('client_order_id'),
                  attempt_id=result['token'], cancel_request_id=(result.get('action') or {}).get('cancel_request_id'),
                  error_code=(result.get('error') or {}).get('code'))

    def _reconcile_step(self):
        r = self.r
        interval = r.settings['reconcile_interval_seconds']
        if self.round is None:
            if r.clock() < self.next_round:
                return
            self.round = dict(id=uuid.uuid4().hex, stage=0, waiting=False, complete=True,
                              generation=self.fact_generation, cursor=None, frozen=False, live_complete=True)
            self.last_reconcile_attempt_at = iso_datetime()
            self.next_reconcile_at = None
        current = self.round
        if not current['frozen']:
            batch = r.repo.begin_reconcile_batch(limit=r.settings['reconcile_batch_size'],
                                                 round_id=current['id'], cursor=current['cursor'],
                                                 interval_seconds=interval)
            # begin 返回的逐单尝试已经落库；只在此时发异步诊断日志。
            for document in batch['orders']:
                self._log('INFO', 'RECONCILE_ATTEMPT', account_id=r.account_id,
                          order_id=document['order_id'], client_order_id=document.get('client_order_id'),
                          round_id=current['id'], version=document.get('version'),
                          fact_version=document.get('reconcile_round_fact_version'),
                          last_reconcile_attempt_at=document.get('last_reconcile_attempt_at'),
                          reconcile_due_at=document.get('reconcile_due_at'))
            current['cursor'] = batch['next_cursor']
            if batch['has_more']:
                return
            current['frozen'] = True
            current['cursor'] = None
            today = utc_now().astimezone(dt.timezone(dt.timedelta(hours=8))).date()
            earliest = r.repo.reconcile_history_start()
            current['query_day'] = today
            # get_trade_detail_data 不接受日期范围；旧单只影响历史覆盖标记。
            current['history_complete'] = not earliest or parse_timestamp(earliest).astimezone(
                dt.timezone(dt.timedelta(hours=8))).date() >= today
            current['queries'] = ['task', 'order', 'deal']
        if current['waiting']:
            return
        if current['stage'] < len(current['queries']):
            kind = current['queries'][current['stage']]
            if self._enqueue('query', query_kind=kind, round_id=current['id']):
                current['waiting'] = True
            return
        # 覆盖日期以 QMT 三份快照读完时为准；后续数据库分页不改变同一份快照的日期。
        current.setdefault('query_finished_at', iso_datetime())
        batch = r.repo.reconcile_round_batch(current['id'], limit=r.settings['reconcile_batch_size'],
                                             cursor=current['cursor'])
        stamp = iso_datetime()
        today = parse_timestamp(current['query_finished_at']).astimezone(dt.timezone(dt.timedelta(hours=8))).date()
        # 查询跨越上海零点时，本轮三份快照可能来自不同日期，不能宣告完整。
        same_day = current['query_day'] == today
        complete = current['complete'] and current['generation'] == self.fact_generation and same_day
        for document in batch['orders']:
            if document['submission_status'] == 'SUBMITTING':
                attempts = [a for a in document['attempts'] if a.get('kind') == 'SUBMIT']
                started = attempts[-1].get('created_at', document['updated_at']) if attempts else document['updated_at']
                if (parse_timestamp(stamp) - parse_timestamp(started)).total_seconds() >= r.confirmation_timeout:
                    def unknown(row):
                        if row['submission_status'] == 'SUBMITTING':
                            row.update(submission_status='UNKNOWN', execution_status='UNKNOWN',
                                       error={'code': 'SUBMISSION_OUTCOME_UNKNOWN', 'message': 'QMT acknowledgement is not yet associated'})
                    document = r.repo.update_order(document['order_id'], 'SUBMISSION_UNKNOWN', unknown)
            covered = _background_current_day_covered(document, current['query_day'])
            if not covered:
                current['history_complete'] = False
            order_complete = complete and covered
            checkpoint_applied = r.repo.finish_reconcile(
                document['order_id'], document.get('reconcile_round_fact_version', -1),
                order_complete, stamp, interval_seconds=interval)
            # False 表示查询期间事实已更新，旧快照未写入；不得记录为对账完成。
            outcome = ('STALE_FACT_VERSION' if not checkpoint_applied else
                       'COMPLETE' if order_complete else 'INCOMPLETE')
            self._log('INFO', 'RECONCILE_FINISHED', account_id=r.account_id,
                      order_id=document['order_id'], client_order_id=document.get('client_order_id'),
                      round_id=current['id'], source_version=document.get('version'),
                      fact_version=document.get('reconcile_round_fact_version'),
                      checkpoint_applied=bool(checkpoint_applied),
                      complete=bool(order_complete) if checkpoint_applied else None,
                      query_scope='CURRENT_DAY', query_day=current['query_day'].isoformat(),
                      coverage_gap=None if covered else 'HISTORY_NOT_COVERED',
                      outcome=outcome)
        current['cursor'] = batch['next_cursor']
        if batch['has_more']:
            return
        if current['live_complete'] and same_day:
            r.recovery_complete = True
        if complete:
            r.last_reconciled_at = stamp
            self.last_reconcile_error = None
        r.history_coverage_complete = complete and current['history_complete']
        self.round = None
        self.last_reconcile_finished_at = stamp
        self.next_round = r.clock() + interval
        self.next_reconcile_at = iso_datetime(utc_now() + dt.timedelta(seconds=interval))

    def health(self):
        r = self.r
        grant = self.grant
        alive = r.last_tick is not None and r.clock() - r.last_tick < 5
        return dict(http_running=not r.stop_event.is_set(), database_available=self.db_ready and self.sample.get('ready', True),
                    trading_configured=r.repo is not None, scheduler_alive=alive,
                    recovery_complete=r.recovery_complete, history_coverage_complete=r.history_coverage_complete,
                    reconcile_query_scope='CURRENT_DAY', history_query_available=False,
                    accepting_orders=bool(self.authorized() and r.recovery_complete and alive),
                    executor_owned=bool(grant and r.clock() < grant[2]), schema_version=self.sample.get('schema_version'),
                    unknown_order_count=self.sample.get('unknown_order_count'), pending_count=self.sample.get('pending_count'),
                    account_id=r.account_id, last_reconciled_at=r.last_reconciled_at,
                    last_reconcile_attempt_at=self.last_reconcile_attempt_at,
                    last_reconcile_finished_at=self.last_reconcile_finished_at,
                    next_reconcile_at=self.next_reconcile_at,
                    last_reconcile_error=self.last_reconcile_error, error_code=r.last_error,
                    observation_gap=r.observation_gap, sampled_at=self.sampled_at,
                    lifecycle=('STOPPED' if self.stopped else 'STOPPING' if r.stop_event.is_set() else 'RUNNING' if r.recovery_complete else 'RECOVERING' if r.initialized else 'STARTING'),
                    state='STOPPED' if self.stopped else ('STOPPING' if r.stop_event.is_set() else 'RUNNING'),
                    queues=dict([(k, v.qsize()) for k, v in self.queues.items()] + [('results', self.results.qsize()), ('observations', r.observations.qsize())]),
                    overflow_count=self.overflows, tick_ms=self.tick_ms, qmt_ms=self.qmt_ms,
                    authority_remaining_ms=max(0, (grant[2] - r.clock()) * 1000) if grant else 0)


class _ContinueMerge(Exception):
    """保留快照游标并向其他后台工作让出，不丢弃未合并记录。"""

# ---- order_bridge/runtime.py ----
"""持久命令与 QMT 调度器；Last modified: 2026-09-26。"""
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
                 repository=None, local_lock=None, clock=None, settings=None):
        self.apis = apis
        self.account_id = account_id
        self.config = pg_config
        self.logger = logger or (lambda *args, **kwargs: None)
        self.adapter = QmtAdapter(apis, context, account_id=account_id, account_type="STOCK",
                                  logger=self.logger)
        self.repo = repository or (PostgresRepository(pg_config, account_id) if pg_config else None)
        self.local_lock = local_lock or (LocalExecutorLock(pg_config, account_id) if pg_config else None)
        self.clock = clock or time.monotonic
        self.instance_id = uuid.uuid4().hex
        self.host_id = socket.gethostname().lower()
        self.stop_event = threading.Event()
        self.stopped_event = threading.Event()
        self.stopped = False
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
        self.settings = dict(submit_batch_size=10, cancel_batch_size=10, reconcile_batch_size=100,
                             schedule_budget_ms=50, reconcile_interval_seconds=30)
        self.settings.update(settings or {})
        self.background = OrderBackground(self)

    def initialize(self):
        """仅启动后台；连接、恢复和执行权检查不占用 QMT 回调。"""
        self.background.start()

    def _runtime_require_store(self):
        if self.repo is None:
            raise OrderError(503, "TRADING_NOT_CONFIGURED", "PostgreSQL is not configured; query-only mode")

    def _runtime_require_ready(self):
        self._runtime_require_store()
        if self.stop_event.is_set():
            raise OrderError(503, "ORDER_STOPPING", "order executor is stopping")
        if not self.initialized or not self.recovery_complete:
            raise OrderError(503, "EXECUTOR_NOT_READY", "executor has not completed recovery")
        if not self.background.authorized():
            raise OrderError(503, "EXECUTOR_NOT_READY", "executor authority is unavailable")

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
        return self.background.health()

    def observe(self, kind, value):
        if self.repo is None:
            return
        try:
            snapshot = self.adapter.snapshot(value)
        except Exception as exc:
            self._observation_failed(kind, "snapshot", exc)
            return
        try:
            self.observations.put_nowait((kind, snapshot))
        except queue.Full as exc:
            self._observation_failed(kind, "enqueue", exc, "OBSERVATION_QUEUE_FULL")
        except Exception as exc:
            self._observation_failed(kind, "enqueue", exc)

    def observe_error(self, pass_order_info, message):
        """在 QMT 回调线程构造错误事实，再交给普通观察队列。"""
        if self.repo is None:
            return
        try:
            snapshot = self.adapter.snapshot(pass_order_info)
        except Exception as exc:
            self._observation_failed("error", "snapshot", exc)
            return
        try:
            snapshot["error_message"] = str(message)
        except Exception as exc:
            self._observation_failed("error", "convert_error", exc)
            return
        self.observe("error", snapshot)

    def _observation_failed(self, kind, phase, exc, default_code="QMT_ERROR"):
        # 保留既有缺口代数与 overflow_count 语义，日志只进入异步队列。
        self.observation_gap = True
        self.background.overflows += 1
        self.background.fact_generation += 1
        try:
            details = qmt_exception_details(exc, default_code)
            self.logger("ERROR", "QMT observation failed", account_id=self.account_id,
                        kind=kind, source="qmt_callback", phase=phase,
                        error_code=details["code"], error_type=details["type"],
                        error_message=details["message"] or (
                            "observation queue is full" if isinstance(exc, queue.Full) else ""),
                        error_field=details.get("field"), object_type=details.get("object_type"),
                        return_snapshot=details.get("return_snapshot"),
                        qmt_phase=details.get("phase"), traceback=details["traceback"])
        except Exception:
            # 诊断通道失效不能中断 QMT 回调。
            pass

    def tick(self, deadline=None, budget=None, max_actions=None):
        """只消费内存指令；budget 可在一个外层回调的多次单步间共享。"""
        return self.background.tick(deadline, budget, max_actions)

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

    def stop(self):
        """停止受理立即返回；后台在 QMT 调用及结果收尾后释放本机锁。"""
        self.stop_event.set()
        self.background.start()

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

_ORDER_LOGGER = None
_ORDER_STATE = None
_ORDER_TIMER_ID = None


# 调度回调只把普通日志字段放入有界队列；格式化和双写由日志后台负责。
def log_message(level, message, **fields):
    logger = _ORDER_LOGGER
    if logger is not None:
        return logger(level, message, **fields)
    return False


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
        self.settings = None
        self.logger = None
        self.cleanup_started = False
        self.pending_released = 0
        self.cleanup_done = threading.Event()
        self.http_closed = threading.Event()
        self.callback_idle = threading.Event()
        self.callback_idle.set()
        self.callback_guard = threading.Lock()
        self.callback_running = False
        self.scheduler_turn = 0
        self.scheduler_metrics = {"sampled_at": None, "last_tick_ms": 0.0,
                                  "max_tick_ms": 0.0, "last_query_ms": 0.0}


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


def runtime_schedule_settings(values):
    """面板整数浮点值可接受；小写优先，启动后复制冻结，不读取热修改。"""
    defaults = {"submit_batch_size": 10, "cancel_batch_size": 10,
                "reconcile_batch_size": 100, "schedule_budget_ms": SCHEDULE_BUDGET_MILLISECONDS,
                "reconcile_interval_seconds": 30}
    result = {}
    for name, default in defaults.items():
        value = values.get(name, values.get(name.upper(), default))
        if isinstance(value, str):
            value = value.strip()
            if not value or any(char < "0" or char > "9" for char in value):
                raise ValueError(name + " must be a positive integer")
            value = int(value)
        if isinstance(value, float) and math.isfinite(value) and value.is_integer():
            value = int(value)
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 2147483647:
            raise ValueError(name + " must be a positive integer")
        result[name] = value
    return result


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
def dispatch_request(ContextInfo, request, logger=None, correlation=None):
    if not isinstance(request, dict):
        raise OrderError(400, "INVALID_REQUEST", "request must be an object")
    method = request.get("method")
    params = request.get("params")
    if not isinstance(method, str) or not isinstance(params, dict):
        raise OrderError(400, "INVALID_REQUEST", "method and params are required")
    normalized = normalize_request(method, params)
    params = normalized["params"]
    if logger is None:
        state = _ORDER_STATE
        logger = state.logger if state is not None and state.logger is not None else log_message
    correlation = dict(correlation or {})
    if method in ("account", "positions"):
        detail_type = "account" if method == "account" else "position"
        correlation.update(account_id=params["accountId"], query_kind=detail_type)
        rows = qmt_invoke(
            logger, "get_trade_detail_data", get_trade_detail_data,
            args=(params["accountId"], params["accountType"], detail_type),
            parameters={"account_id": params["accountId"],
                        "account_type": params["accountType"], "query_kind": detail_type},
            correlation=correlation,
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
    correlation["query_kind"] = method
    state = _ORDER_STATE
    if state is not None and state.account_id is not None:
        correlation["account_id"] = state.account_id
    result = qmt_invoke(logger, "get_smart_algo_param", api,
                        args=(params["algoList"],),
                        parameters={"algo_list": params["algoList"]},
                        correlation=correlation)
    if not isinstance(result, dict):
        raise OrderError(500, "INVALID_QMT_RESULT", "get_smart_algo_param must return a dict")
    return qmt_json_value(result)


# 预算从回调入口开始；交易阶段与旧查询交替，轮转位置跨 tick 保留。
def process_http_requests(ContextInfo):
    started_at = time.monotonic()
    state = _ORDER_STATE
    if state is None or state.stop_event.is_set():
        return
    # 短锁仅更新内存门闩；QMT I/O 期间不持有它。
    with state.callback_guard:
        if state.callback_running or state.stop_event.is_set():
            return
        state.callback_running = True
        state.callback_idle.clear()
    settings = state.settings or runtime_schedule_settings({})
    deadline = started_at + settings["schedule_budget_ms"] / 1000.0
    budget = {"submit": 0, "cancel": 0}
    queries, idle_turns = 0, 0
    try:
        while time.monotonic() < deadline and not state.stop_event.is_set():
            service = state.scheduler_turn
            state.scheduler_turn = 1 - service
            progressed = False
            if service == 0 and state.runtime is not None:
                progressed = bool(state.runtime.tick(deadline=deadline, budget=budget, max_actions=1))
            elif service == 1 and queries < MAX_JOBS_PER_TICK:
                try:
                    job = state.request_queue.get_nowait()
                except queue.Empty:
                    job = None
                if job is not None:
                    query_started = time.monotonic()
                    try:
                        with state.lifecycle_lock:
                            if state.stop_event.is_set():
                                job.set_error(503, "ORDER_STOPPING", "HTTP order is stopping")
                            elif not job.try_start():
                                job.set_error(504, "REQUEST_EXPIRED", "request expired before QMT processing")
                        if job.error is None:
                            job.result = dispatch_request(
                                ContextInfo, job.request, logger=state.logger,
                                correlation={"request_id": job.request_id},
                            )
                    except OrderError as exc:
                        job.set_error(exc.status, exc.code, exc.message)
                    except Exception:
                        job.set_error(500, "QMT_ERROR", "QMT request failed")
                    finally:
                        if job.error is not None:
                            log_message("ERROR" if job.error_status >= 500 else "WARNING", "QMT request failed",
                                        request_id=job.request_id, method=job.request.get("method"),
                                        status=job.error_status, error_code=job.error["code"])
                        job.done.set()
                        state.request_queue.task_done()
                        state.scheduler_metrics["last_query_ms"] = (time.monotonic() - query_started) * 1000
                    queries += 1
                    progressed = True
            idle_turns = 0 if progressed else idle_turns + 1
            if idle_turns >= 2:
                break
    finally:
        elapsed_ms = (time.monotonic() - started_at) * 1000
        state.scheduler_metrics.update(sampled_at=time.time(), last_tick_ms=elapsed_ms,
                                       max_tick_ms=max(state.scheduler_metrics["max_tick_ms"], elapsed_ms))
        with state.callback_guard:
            state.callback_running = False
            state.callback_idle.set()


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
        if state.stop_event.is_set() and self.request_method != "health":
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
            if self.request_method == "health":
                result = dict(result)
                result["http_running"] = not state.http_closed.is_set()
                result["http_lifecycle"] = "STOPPING" if state.stop_event.is_set() else "RUNNING"
                result["schedule_settings"] = dict(state.settings or runtime_schedule_settings({}))
                result["scheduler"] = dict(state.scheduler_metrics)
                result["logging"] = state.logger.health() if state.logger is not None else None
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
        # 请求始终写入所属实例，旧 HTTP 线程不能把响应日志写到重启后的实例。
        logger = self.server.order_state.logger
        if logger is not None:
            logger("INFO" if status < 400 else "WARNING", "Response sent", **fields)

    def log_message(self, format, *args):
        pass


def serve_http(state):
    try:
        while not state.http_closed.is_set():
            state.server.handle_request()
    finally:
        if state.server is not None:
            state.server.server_close()


# QMT 启动入口：先校验参数和启动异步日志，再绑定账户、监听端口并注册定时任务。
# 有效配置固定在 OrderState 中，运行期间修改面板不会热切换账户或端口。
def init(ContextInfo):
    global _ORDER_STATE, _ORDER_TIMER_ID, _ORDER_LOGGER
    if _ORDER_STATE is not None:
        raise RuntimeError("HTTP order is already initialized")
    # 显式的小写参数优先；小写值非法时直接失败，不静默回退到其他账户。
    account_id = runtime_account_id(globals().get("account_id", ACCOUNT_ID))
    http_port = runtime_http_port(globals().get("http_port", HTTP_PORT))
    settings = runtime_schedule_settings(globals())
    pg_config = read_pg_config(globals())
    state = OrderState()
    state.account_id = account_id
    state.http_port = http_port
    state.settings = dict(settings)
    state.logger = AsyncOrderLogger(LOG_DIRECTORY)
    try:
        state.logger.start()
        qmt_invoke(state.logger, "ContextInfo.set_account", ContextInfo.set_account,
                   args=(account_id,), parameters={"account_id": account_id},
                   correlation={"account_id": account_id})
        state.runtime = OrderRuntime(globals(), ContextInfo, account_id, pg_config=pg_config,
                                     logger=state.logger, settings=settings)
        state.runtime.external_idle = state.callback_idle
        server = ThreadingHTTPServer((HTTP_HOST, http_port), OrderRequestHandler)
        server.timeout = 0.2
        server.order_state = state
        state.server = server
        state.server_thread = threading.Thread(
            target=serve_http, args=(state,), name="qmt-http-order", daemon=True
        )
        _ORDER_STATE = state
        _ORDER_LOGGER = state.logger
        _ORDER_TIMER_ID = qmt_invoke(
            state.logger, "ContextInfo.schedule_run", ContextInfo.schedule_run,
            args=(process_http_requests, "20200101000000", -1,
                  SCHEDULE_INTERVAL, "http_order_timer"),
            parameters={"callback": "process_http_requests", "start_time": "20200101000000",
                        "repeat_times": -1,
                        "interval_ms": SCHEDULE_INTERVAL.total_seconds() * 1000,
                        "timer_name": "http_order_timer"},
            correlation={"account_id": account_id},
        )
        state.runtime.initialize()
        state.server_thread.start()
    except Exception:
        # 启动中途失败时撤销定时器并关闭端口；账户绑定失败也保留失败日志。
        state.stop_event.set()
        state.http_closed.set()
        timer_id = _ORDER_TIMER_ID
        _ORDER_TIMER_ID = None
        if timer_id is not None:
            try:
                qmt_invoke(state.logger, "ContextInfo.cancel_schedule_run",
                           ContextInfo.cancel_schedule_run, args=(timer_id,),
                           parameters={"timer_id": timer_id if type(timer_id) in (str, int) else None},
                           correlation={"account_id": account_id, "stage": "init_failure"})
            except Exception:
                pass
        if state.server is not None:
            state.server.server_close()
        if _ORDER_STATE is state:
            try:
                state.runtime.stop()
            finally:
                _start_order_cleanup(state)
        else:
            state.logger.request_stop()
            state.logger.join(0.2)
        raise
    log_message(
        "INFO", "QMT HTTP order listening",
        host=HTTP_HOST, port=state.server.server_address[1], account_id=state.account_id, **settings
    )
    if pg_config:
        log_message("INFO", "ORDER persistence configured", account_id=account_id,
                    database=pg_config["pg_database"], schema=pg_config["pg_schema"],
                    executor_initialized=state.runtime.initialized)


# 服务由定时器驱动，不依赖行情 tick 到达，因此 handlebar 无需业务逻辑。
def handlebar(ContextInfo):
    pass


# 后台清理等待在途 QMT、数据库和日志退出；QMT stop 回调只发信号。
def _finish_order_cleanup(state):
    global _ORDER_STATE, _ORDER_LOGGER
    try:
        state.callback_idle.wait()
        if state.runtime is not None:
            state.runtime.stopped_event.wait()
        # 慢日志收尾期间仍开放 /health，客户端可以看到 STOPPING。
        if state.logger is not None:
            state.logger("INFO", "HTTP order stopped", pending_released=state.pending_released)
            state.logger.request_stop()
            state.logger.join()
        state.http_closed.set()
        if state.server_thread is not None and state.server_thread.is_alive():
            state.server_thread.join()
        if state.server is not None:
            state.server.server_close()
    finally:
        state.cleanup_done.set()
        if _ORDER_STATE is state:
            _ORDER_STATE = None
            _ORDER_LOGGER = None


def _start_order_cleanup(state):
    with state.lifecycle_lock:
        if state.cleanup_started:
            return
        state.cleanup_started = True
    worker = threading.Thread(target=_finish_order_cleanup, args=(state,),
                              name="qmt-order-cleanup", daemon=True)
    worker.start()


# QMT 停止入口：不等待后台线程，清理完成前仍保留 STOPPING 状态。
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
            qmt_invoke(state.logger, "ContextInfo.cancel_schedule_run",
                       ContextInfo.cancel_schedule_run, args=(timer_id,),
                       parameters={"timer_id": timer_id if type(timer_id) in (str, int) else None},
                       correlation={"account_id": state.account_id, "stage": "stop"})
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
        state.pending_released = count
        log_message("INFO", "HTTP order stopping", pending_released=count)
        _start_order_cleanup(state)


# QMT 回报只把当前上下文中的对象转为普通字典；数据库更新交给后台工作线程。
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
        state.runtime.observe_error(passOrderInfo, msg)
