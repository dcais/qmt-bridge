# -*- coding: utf-8 -*-
# Last modified (Asia/Shanghai): 2026-09-25 23:21:38

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
from .common import OrderError
from .runtime import OrderRuntime, read_pg_config

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
