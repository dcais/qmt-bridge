# -*- coding: utf-8 -*-
"""持久命令与 QMT 调度器；Last modified: 2026-09-26。"""
import datetime as dt
import os
import queue
import socket
import sys
import threading
import time
import uuid

from .common import OrderError, copy_json, fingerprint, iso_datetime, parse_timestamp, public_order, qmt_exception_details, utc_now
from .contracts import normalize_order, normalize_cancel, capabilities
from .qmt import QmtAdapter
from .background import OrderBackground
from .repository import PostgresRepository
from .state import pending_cancellations, recompute_order, mark_reconciled, observation_identifiers


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
