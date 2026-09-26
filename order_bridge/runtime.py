# -*- coding: utf-8 -*-
"""持久命令与 QMT 调度器；HTTP 路径不会调用 QMT。"""
import datetime as dt
import os
import queue
import socket
import sys
import threading
import time
import uuid

from .common import OrderError, copy_json, fingerprint, iso_datetime, parse_timestamp, public_order, utc_now
from .contracts import normalize_order, normalize_cancel, capabilities
from .qmt import QmtAdapter
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
