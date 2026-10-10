import datetime as dt
import math
from service.ipc_bridge.ipc_protocol.order_common import OrderError, qmt_invoke
from .adapter import QmtAdapter
from service.ipc_bridge.ipc_protocol.order_queries import qmt_history_capabilities
from service.ipc_bridge.ipc_protocol.order_queries import normalize_request

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
def dispatch_request(ContextInfo, request, apis, account_id, logger=None, correlation=None):
    if not isinstance(request, dict):
        raise OrderError(400, "INVALID_REQUEST", "request must be an object")
    method = request.get("method")
    params = request.get("params")
    if not isinstance(method, str) or not isinstance(params, dict):
        raise OrderError(400, "INVALID_REQUEST", "method and params are required")
    normalized = normalize_request(method, params, account_id=account_id)
    params = normalized["params"]
    correlation = dict(correlation or {})
    if method == "history_capabilities":
        return qmt_history_capabilities(apis)
    if method == "get_history_trade_detail_data":
        adapter = QmtAdapter(apis, ContextInfo, params["accountId"], params["accountType"], logger)
        adapter.log_context = correlation
        return adapter.history_query(params)
    if method in ("account", "positions"):
        detail_type = "account" if method == "account" else "position"
        correlation.update(account_id=params["accountId"], query_kind=detail_type)
        rows = qmt_invoke(
            logger, "get_trade_detail_data", apis["get_trade_detail_data"],
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
    api = apis.get("get_smart_algo_param")
    if not callable(api):
        raise OrderError(501, "API_UNAVAILABLE", "get_smart_algo_param is unavailable")
    correlation["query_kind"] = method
    correlation["account_id"] = account_id
    result = qmt_invoke(logger, "get_smart_algo_param", api,
                        args=(params["algoList"],),
                        parameters={"algo_list": params["algoList"]},
                        correlation=correlation)
    if not isinstance(result, dict):
        raise OrderError(500, "INVALID_QMT_RESULT", "get_smart_algo_param must return a dict")
    return qmt_json_value(result)
