import datetime as dt
from .common import OrderError
from .qmt import MAX_HISTORY_DATE_DAYS

ACCOUNT_TYPE = 'STOCK'
ACCOUNT_TYPES = frozenset((
    "STOCK", "CREDIT", "FUTURE", "HUGANGTONG", "SHENGANGTONG", "STOCK_OPTION",
))
METHODS = frozenset(("account", "positions", "account_evidence_v1", "positions_evidence_v1", "get_smart_algo_param",
                     "get_history_trade_detail_data", "history_capabilities"))

def _reject_unknown(params, allowed, method):
    if not isinstance(params, dict):
        raise OrderError(400, "INVALID_PARAMS", "params must be an object")
    unknown = set(params) - allowed
    if unknown:
        raise OrderError(
            400, "INVALID_PARAMS",
            "unsupported {0} params: {1}".format(method, ",".join(sorted(unknown))),
        )


def _normalize_account_params(params, method, account_id):
    _reject_unknown(params, {"accountId", "accountType"}, method)
    account_id = params.get("accountId", account_id)
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


def _normalize_history_params(params, account_id):
    _reject_unknown(params, {"accountId", "accountType", "dataType", "startDate", "endDate"},
                    "get_history_trade_detail_data")
    scope = _normalize_account_params(
        {k: v for k, v in params.items() if k in ("accountId", "accountType")}, "history", account_id)
    kind = params.get("dataType", "DEAL")
    if not isinstance(kind, str) or kind.upper() not in ("POSITION", "ORDER", "DEAL"):
        raise OrderError(400, "INVALID_PARAMS", "dataType must be POSITION, ORDER or DEAL")
    scope["dataType"] = kind.upper()
    dates = []
    for key in ("startDate", "endDate"):
        value = params.get(key)
        if not isinstance(value, str) or len(value) != 8 or any(c not in "0123456789" for c in value):
            raise OrderError(400, "INVALID_PARAMS", key + " must be YYYYMMDD")
        try:
            dates.append(dt.datetime.strptime(value, "%Y%m%d").date())
        except ValueError:
            raise OrderError(400, "INVALID_PARAMS", key + " must be a valid date")
        scope[key] = value
    if dates[1] < dates[0] or (dates[1] - dates[0]).days + 1 > MAX_HISTORY_DATE_DAYS:
        raise OrderError(400, "INVALID_PARAMS", "history range must be ordered and at most 31 calendar days")
    return scope


def normalize_request(method, params, allow_algo_string=True, account_id=None):
    if method not in METHODS:
        raise OrderError(404, "METHOD_NOT_FOUND", "unsupported method")
    if method in ("account", "positions", "account_evidence_v1", "positions_evidence_v1"):
        params = _normalize_account_params(params, method, account_id)
    elif method == "get_history_trade_detail_data":
        params = _normalize_history_params(params, account_id)
    elif method == "history_capabilities":
        _reject_unknown(params, set(), method)
    else:
        params = _normalize_algo_params(params, allow_algo_string)
    return {"method": method, "params": params}
