# -*- coding: utf-8 -*-
"""QMT 函数适配层。调用者必须处于策略调度回调线程。"""
import datetime as dt
import math
import re
from decimal import Decimal, InvalidOperation

from .common import OrderError, copy_json, parse_timestamp


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
        result = self._resolve_base(request, remark)
        if request["execution"]["type"] == "SMART":
            smart = request["execution"]
            data = self.snapshot(self._require("get_smart_algo_param")([smart["algorithm"]]))
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
            data = self.snapshot(self._require("get_smart_algo_param")([smart["algorithm"]]))
            self._resolve_smart(result, smart, order.get("remark"), data)
            return {"stage": "BASKET_GET" if order.get("order_type") == "BASKET" else None,
                    "updates": {"resolved_request": result}}
        if stage not in ("BASKET_GET", "BASKET_SET", "BASKET_VERIFY"):
            _qmt_error("INVALID_PREPARE_STAGE", "unknown preparation stage", 400)
        expected = self._expected_basket(order)
        if stage == "BASKET_SET":
            self._require("set_basket")(copy_json(expected))
            return {"stage": "BASKET_VERIFY", "updates": {}}
        actual = self.snapshot(self._require("get_basket")(expected["name"]))
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
        get_basket = self._require("get_basket")
        set_basket = self._require("set_basket")
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
