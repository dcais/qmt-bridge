# -*- coding: utf-8 -*-
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
# 策略启动只检查关键表、字段和版本，不自动建表或升级。
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
