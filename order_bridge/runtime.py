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
from decimal import Decimal, ROUND_HALF_UP

from .common import OrderError, copy_json, fingerprint, iso_datetime, parse_timestamp, public_order, qmt_exception_details, utc_now
from .contracts import normalize_order, normalize_cancel, capabilities
from .qmt import QmtAdapter, qmt_history_capabilities
from .background import OrderBackground
from .compact_events import COMPACT_VOID, compact_source_ref, project_event
from .evidence import EvidenceCursors, evidence_capabilities, evidence_fee
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
        self.evidence_cursors = EvidenceCursors()
        self.evidence_samples = {}
        self.evidence_sample_lock = threading.Lock()
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
        reads = ("order", "orders", "order_events", "capabilities", "health",
                 "order_events_evidence_v1", "order_events_compact_v1", "order_event_source_v1",
                 "recovery_evidence_v1", "order_fee_evidence_v1")
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
            result['evidence_v1'] = evidence_capabilities()
            result['journal_storage_v1'] = dict(read_schema_versions=[5, 6], writer_default='FULL',
                                                optional_format='BASE_DELTA_V1', baseline_interval=16)
            result['order_events_compact_v1'] = dict(
                mode='READ_ONLY', default_wire_mode='full', supported_wire_modes=['full', 'shadow', 'compact'],
                compact_primary_opt_in=True, grants_trading_authority=False, wire_format='ORDER_EVENT_DELTA_V1',
                source_route='order_event_source_v1', paired_cut='COMPACT_START_CURSOR_V1')
            result['cancel_before_submit_v1'] = dict(supported=True, receipt_type='SUBMISSION_BAN', schema_version=1)
            result["qmt_functions"] = {name: self.adapter.available(name) for name in
                ("passorder", "algo_passorder", "smart_algo_passorder", "cancel", "cancel_task", "set_basket", "get_basket", "get_history_trade_detail_data")}
            result["history_query"] = qmt_history_capabilities(self.adapter.apis)
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
        if method == 'order_event_source_v1':
            is_void = params.get('kind') == COMPACT_VOID
            keys = ('stream_id', 'account_id', 'account_type', 'event_id', 'sha256')
            keys += ('kind',) if is_void else ('order_id', 'order_version')
            self._runtime_keys(params, keys)
            if set(params) != set(keys):
                raise OrderError(400, 'INVALID_PARAMS', 'complete immutable source reference required')
            ref = dict(params)
            for key in (('event_id',) if is_void else ('event_id', 'order_version')):
                ref[key] = self._runtime_integer(ref[key], key, 1, 9223372036854775807)
            if ref['account_id'] != self.account_id or ref['account_type'] != self.repo.account_type:
                raise OrderError(409, 'EVIDENCE_STREAM_CHANGED', 'journal source account differs')
            row = (self.repo.journal_source(ref['stream_id'], ref['event_id'], include_void_receipts=True)
                   if is_void else self.repo.journal_source(ref['stream_id'], ref['event_id']))
            if row is None:
                raise OrderError(409, 'EVIDENCE_SOURCE_MISSING', 'referenced journal row is unavailable')
            if compact_source_ref(ref, row) != ref:
                raise OrderError(409, 'EVIDENCE_SOURCE_CHANGED', 'historical source differs from reference')
            return 200, dict(source_ref=ref, event=row)
        if method == 'order_fee_evidence_v1':
            self._runtime_keys(params, ('client_order_id',))
            value = params.get('client_order_id')
            if not isinstance(value, str) or not value:
                raise OrderError(400, 'INVALID_PARAMS', 'client_order_id is required')
            result = evidence_fee(self.repo.get_order(value))
            result.update(evidence_version=1, account_id=self.account_id, account_type='STOCK')
            return 200, result
        if method in ('order_events_evidence_v1', 'order_events_compact_v1', 'recovery_evidence_v1'):
            keys = ('after', 'limit', 'cursor', 'include_void_receipts')
            if method == 'order_events_evidence_v1' and 'cursor' not in params:
                keys += ('pair_compact',)
            self._runtime_keys(params, keys)
            pair_compact = params.get('pair_compact', False)
            if type(pair_compact) is not bool and not (type(pair_compact) is str and pair_compact in ('true', 'false')):
                raise OrderError(400, 'INVALID_PARAMS', 'pair_compact must be true or false')
            if 'cursor' in params and 'after' in params:
                raise OrderError(400, 'INVALID_PARAMS', 'cursor and after are mutually exclusive')
            checkpoint = self.evidence_cursors.decode(params['cursor']) if 'cursor' in params else None
            if checkpoint and checkpoint['route'] != method:
                raise OrderError(409, 'EVIDENCE_CURSOR_INVALID', 'cursor belongs to another route')
            include_value = params.get('include_void_receipts', checkpoint.get('include_void_receipts', False) if checkpoint else False)
            if not (type(include_value) is bool or type(include_value) is str and include_value in ('true', 'false')):
                raise OrderError(400, 'INVALID_PARAMS', 'include_void_receipts must be true or false')
            include_void_receipts = include_value is True or include_value == 'true'
            if checkpoint and include_void_receipts != checkpoint.get('include_void_receipts', False):
                raise OrderError(409, 'EVIDENCE_CURSOR_INVALID', 'cursor sequence mode differs')
            after = checkpoint['after'] if checkpoint else self._runtime_integer(params.get('after', 0), 'after', 0, 9223372036854775807)
            result = self.repo.evidence_events(after, self._runtime_limit(params.get('limit', 100)), checkpoint,
                                               recovery=method == 'recovery_evidence_v1', include_void_receipts=include_void_receipts)
            anchor = result.pop('anchor')
            for event in result['events']:
                if event['type'] != 'NON_FINANCIAL_SEQUENCE_VOID':
                    event['data'] = public_order(event['data'])
            if method == 'order_events_compact_v1':
                previous = {}
                if after and result['events'] and after >= result['retained_from']:
                    if result['coverage'].get('sequence_kinds') == 'ORIGINAL_OR_ATTESTED_VOID':
                        boundary = self.repo.journal_source(result['stream_id'], after, include_void_receipts=True)
                    else:
                        boundary = self.repo.journal_source(result['stream_id'], after)
                    if boundary is None:
                        raise OrderError(409, 'EVIDENCE_SOURCE_MISSING', 'page boundary row disappeared')
                    if boundary['type'] != COMPACT_VOID:
                        previous[boundary['data']['order_id']] = boundary
                projected = []
                for row in result['events']:
                    if row['type'] == COMPACT_VOID:
                        projected.append(project_event(result, row))
                        continue
                    order_id = row['data']['order_id']
                    projected.append(project_event(result, row, previous.get(order_id)))
                    previous[order_id] = row
                result.update(wire_format='ORDER_EVENT_DELTA_V1', events=projected)
            token = {key: result[key] for key in ('stream_id', 'account_id', 'account_type', 'executor_epoch',
                                                  'executor_instance', 'high_watermark')}
            token.update(anchor=anchor, after=result['next_after'], route=method, include_void_receipts=include_void_receipts,
                         expires_at=checkpoint['expires_at'] if checkpoint else time.time() + 900)
            result['next_cursor'] = self.evidence_cursors.encode(token) if result['has_more'] else None
            if pair_compact is True or pair_compact == 'true':
                paired = dict(token, route='order_events_compact_v1', after=after)
                result['compact_start_cursor'] = self.evidence_cursors.encode(paired)
            result['observed_at'] = iso_datetime()
            result['runtime_health'] = self.health()
            result['runtime_health_consistency'] = 'non_atomic'
            if method == 'recovery_evidence_v1':
                with self.evidence_sample_lock:
                    samples = copy_json(self.evidence_samples)
                matching = {}
                for kind, sample in samples.items():
                    markers = sample.get('event_watermark') or {}
                    before, marker = markers.get('before'), markers.get('after')
                    if (sample.get('event_relation') == 'non_atomic_bracket' and before and marker
                            and before['high_watermark'] <= marker['high_watermark']
                            and before['high_watermark'] >= result['coverage']['after_exclusive']
                            and marker['high_watermark'] <= result['coverage']['through_inclusive']
                            and all(before[key] == marker[key] == result[key] for key in
                                    ('stream_id', 'account_id', 'account_type', 'executor_epoch', 'executor_instance'))):
                        matching[kind] = sample
                result.update(recovery_status='incomplete', sample_evidence=matching or None,
                              sample_evidence_consistency='non_atomic', provable_broker_dates=None,
                              instruction='QUERY_ORIGINAL_ORDER_ID_DO_NOT_RESUBMIT',
                              blockers=['BROKER_COMPLETENESS_UNKNOWN', 'ATOMIC_SAMPLE_UNSUPPORTED',
                                        'FINAL_FEES_UNKNOWN', 'RESTORE_LINEAGE_UNVERIFIED'])
                if result['pending_truncated']:
                    result['blockers'].append('PENDING_REQUEST_LIST_TRUNCATED')
            return 200, result
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
                if existing.get("receipt_type") == "SUBMISSION_BAN":
                    raise OrderError(409, "SUBMISSION_BANNED", "client_order_id is durably cancelled before submission")
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
            self._runtime_keys(params, ("client_order_id", "amount_view"))
            cent_view = self._runtime_cent_view(params)
            value = params.get("client_order_id")
            if not isinstance(value, str) or not value:
                raise OrderError(400, "INVALID_PARAMS", "client_order_id is required")
            document = self.repo.get_order(value)
            return 200, (document if document.get("receipt_type") == "SUBMISSION_BAN"
                         else self._query_order(document, cent_view))
        if method == "orders":
            self._runtime_keys(params, ("active", "limit", "cursor", "amount_view"))
            cent_view = self._runtime_cent_view(params)
            active = params.get("active", "true")
            if active not in ("true", "false", True, False):
                raise OrderError(400, "INVALID_PARAMS", "active must be true or false")
            result = self.repo.list_orders(active=active in ("true", True),
                                          limit=self._runtime_limit(params.get("limit", 100)), cursor=params.get("cursor"))
            result["orders"] = [self._query_order(row, cent_view) for row in result["orders"]]
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
    def _runtime_cent_view(params):
        if "amount_view" not in params:
            return False
        if params["amount_view"] != "cent":
            raise OrderError(400, "INVALID_PARAMS", "amount_view must be cent")
        return True

    @staticmethod
    def _query_order(document, cent_view):
        result = public_order(document)
        if not cent_view or result.get("account_type") != "STOCK":
            return result
        items = result["items"]
        if not items or not all(item["symbol"].endswith((".SH", ".SZ")) for item in items):
            return result
        cent = Decimal("0.01")
        totals = {item["item_id"]: Decimal("0.00") for item in items}
        for fill in result["fills"]:
            amount = Decimal(str(fill["amount"])).quantize(cent, rounding=ROUND_HALF_UP)
            fill["amount"] = str(amount)
            totals[fill["item_id"]] += amount
        for item in items:
            item["filled_amount"] = str(totals[item["item_id"]])
        result["filled_amount"] = str(sum(totals.values()))
        return result

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

    def record_evidence_sample(self, sample):
        """Keep only two latest native samples for this executor account, in memory."""
        if sample['account_id'] != self.account_id or sample['account_type'] != 'STOCK':
            return
        with self.evidence_sample_lock:
            self.evidence_samples[sample['kind']] = copy_json(sample)

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
