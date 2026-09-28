# -*- coding: utf-8 -*-
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


def new_record_metadata(created_at=None):
    """Assign immutable identity and creation time when a child record is born."""
    return {"record_id": str(uuid.uuid4()), "created_at": created_at or iso_datetime()}


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
        member.update(new_record_metadata(now))
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
    for field in ("items", "attempts", "cancel_requests", "qmt_tasks", "qmt_orders", "fills"):
        for record in result.get(field, []):
            record.pop("record_id", None)
            if field in ("items", "qmt_tasks", "qmt_orders", "fills"):
                record.pop("created_at", None)
    if replayed is not None:
        result["replayed"] = replayed
    return result
