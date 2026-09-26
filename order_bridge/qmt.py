# -*- coding: utf-8 -*-
"""QMT 函数适配层。调用者必须处于策略调度回调线程。Last modified: 2026-09-26。"""
import datetime as dt
import math
import re
from decimal import Decimal, InvalidOperation

from .common import OrderError, copy_json, parse_timestamp, qmt_invoke


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
            try:
                rows = self._call("get_history_trade_detail_data",
                                  (self.account_id, self.account_type, kind.upper(), start_date, end_date),
                                  {"accountID": self.account_id, "accountType": self.account_type,
                                   "dataType": kind.upper(), "startDate": start_date, "endDate": end_date},
                                  {"query_kind": kind})
            except Exception as exc:
                _qmt_exception_context(exc, "native_query")
                raise
            if not isinstance(rows, (list, tuple)):
                _qmt_error("INVALID_QMT_RESULT", "history query returned invalid collection", 502)
            result = []
            for row_index, row in enumerate(rows):
                if not isinstance(row, (list, tuple)) or len(row) < 2:
                    _qmt_error("INVALID_QMT_RESULT", "history query returned invalid tuple", 502)
                for raw_item in row[1:]:
                    group = raw_item if isinstance(raw_item, (list, tuple)) else [raw_item]
                    for member in group:
                        try:
                            item = self.snapshot(member)
                        except Exception as exc:
                            _qmt_exception_context(exc, "snapshot", value=member)
                            _qmt_query_result_context(exc, rows, row_index)
                            raise
                        if not isinstance(item, dict):
                            _qmt_error("INVALID_QMT_RESULT", "history entry has no QMT fields", 502)
                        if not item.get("m_strTradingDay"):
                            trading_day = str(row[0])
                            if not _qmt_history_date.match(trading_day):
                                _qmt_error("INVALID_QMT_RESULT", "history entry has no valid trading day", 502)
                            item["m_strTradingDay"] = trading_day
                        result.append(item)
            return result
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
