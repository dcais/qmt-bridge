"""External pipe owner. HTTP/DB callers never touch pipe handles directly."""
import queue
import threading
import time
import uuid

from .winpipe import PipeEndpoint


class RemoteError(Exception):
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


class BridgeLink:
    def __init__(self, name, role, bridge_id, account_id, *, endpoint_factory=PipeEndpoint,
                 logger=None, heartbeat_timeout=2.0):
        self.name, self.role = name, role
        self.bridge_id, self.account_id = bridge_id, account_id
        self.endpoint_factory = endpoint_factory
        self.logger = logger or (lambda *args, **kwargs: None)
        self.heartbeat_timeout = heartbeat_timeout
        self.instance_id = uuid.uuid4().hex
        self.session_id = self.boot_id = None
        self.capabilities = {}
        self.last_heartbeat = 0
        self.peer_ready = False
        self.last_error = None
        self.gap = False
        self.peer_gap = False
        self._gap_pending = None
        self._next_gap_retry = 0
        self.authorize = lambda: None
        self.on_event = None
        self.on_gap = None
        self.on_late_result = None
        self._lock = threading.RLock()
        self._pending = {}
        self._outgoing = queue.Queue(256)
        self._events = queue.Queue(4096)
        self._event_seen = set()
        self._acked = {}
        self._stop = threading.Event()
        self._thread = self._event_thread = None
        self._endpoint = None
        self._ever_connected = False

    @property
    def ready(self):
        with self._lock:
            return bool(self.session_id and self.peer_ready and not self._stop.is_set()
                        and time.monotonic() - self.last_heartbeat < self.heartbeat_timeout)

    def health(self):
        with self._lock:
            return dict(connected=self.session_id is not None, ready=self.ready,
                        execution_ready=self.execution_ready,
                        role=self.role, boot_id=self.boot_id, session_id=self.session_id,
                        heartbeat_age_seconds=(round(time.monotonic() - self.last_heartbeat, 3)
                                               if self.last_heartbeat else None),
                        pending_requests=len(self._pending), pending_events=self._events.qsize(),
                        observation_gap=self.gap, error_code=self.last_error,
                        logging={"sink": "console", "file_logging": False})

    @property
    def execution_ready(self):
        return self.ready and not self.peer_gap and self._gap_pending is None

    def start(self):
        if self._thread is not None:
            return
        # Fail startup synchronously on duplicate owner or ACL failure.
        self._endpoint = self.endpoint_factory(self.name, server=True)
        self._thread = threading.Thread(target=self._run, name="ipc-" + self.role, daemon=True)
        self._event_thread = threading.Thread(target=self._event_loop,
                                             name="ipc-events-" + self.role, daemon=True)
        self._thread.start()
        self._event_thread.start()

    def stop(self):
        self._stop.set()
        self._fail_pending("IPC_STOPPING", "IPC bridge is stopping")
        if self._thread is not None:
            self._thread.join(3)
        if self._event_thread is not None:
            self._event_thread.join(3)

    def call(self, method, params, timeout=10, metadata=None):
        if not self.ready:
            raise RemoteError(503, "EXECUTOR_NOT_READY", self.role + " IPC is not ready")
        if self._gap_pending and method in ("order.submit", "order.cancel_action", "order.prepare_step"):
            raise RemoteError(503, "EXECUTOR_NOT_READY", "IPC gap has not been recorded")
        timeout = float(timeout)
        if timeout <= 0:
            raise RemoteError(504, "QMT_TIMEOUT", "QMT request deadline expired")
        metadata = dict(metadata or {})
        request_id = metadata.pop("request_id", None) or uuid.uuid4().hex
        pending = {"done": threading.Event(), "deadline": time.monotonic() + timeout,
                   "result": None, "error": None}
        with self._lock:
            if len(self._pending) >= 128 or request_id in self._pending:
                raise RemoteError(429, "QUEUE_FULL", "IPC request capacity reached")
            session = self.session_id
            frame = dict(metadata, type="request", session_id=session, request_id=request_id,
                         method=method, params=params, expires_at=time.time() + timeout)
            self._pending[request_id] = pending
            try:
                self._outgoing.put_nowait((session, frame))
            except queue.Full:
                self._pending.pop(request_id, None)
                raise RemoteError(429, "QUEUE_FULL", "IPC outgoing queue is full") from None
        try:
            if not pending["done"].wait(timeout):
                raise RemoteError(504, "QMT_TIMEOUT", "QMT did not finish in time; execution may have occurred")
            if pending["error"] is not None:
                raise pending["error"]
            return pending["result"]
        finally:
            with self._lock:
                self._pending.pop(request_id, None)

    def _fail_pending(self, code, message):
        with self._lock:
            for pending in self._pending.values():
                if not pending["done"].is_set():
                    pending["error"] = RemoteError(503, code, message)
                    pending["done"].set()

    def _mark_gap(self, reason):
        self.gap = True
        if self.on_gap is not None:
            self._gap_pending = reason
            try:
                self.on_gap(reason)
            except Exception:
                self.last_error = "GAP_PERSISTENCE_UNAVAILABLE"
                self._next_gap_retry = time.monotonic() + .1
            else:
                self._gap_pending = None

    def _disconnect(self, exc):
        was_connected = self.session_id is not None
        with self._lock:
            self.session_id = None
            self.peer_ready = False
            self.last_heartbeat = 0
            self.last_error = getattr(exc, "code", type(exc).__name__)
        self._fail_pending("IPC_DISCONNECTED", "IPC disconnected; execution may have occurred")
        if self._endpoint is not None:
            self._endpoint.close()
        self._endpoint = None
        if was_connected:
            self._mark_gap("IPC_DISCONNECTED")
        self.logger("WARNING", "IPC disconnected", role=self.role, error_code=self.last_error)

    def _frame(self, frame):
        kind = frame.get("type")
        if kind == "hello":
            if (self.session_id is not None or frame.get("protocol") != 1
                    or frame.get("role") != self.role or frame.get("bridge_id") != self.bridge_id
                    or frame.get("account_id") != self.account_id
                    or not isinstance(frame.get("boot_id"), str) or not frame["boot_id"]):
                raise RemoteError(503, "IPC_IDENTITY_MISMATCH", "IPC peer identity mismatch")
            if self._ever_connected:
                self._mark_gap("IPC_SESSION_CHANGED")
            self._ever_connected = True
            self.boot_id = frame["boot_id"]
            self.peer_gap = False
            self.session_id = uuid.uuid4().hex
            self.capabilities = frame.get("capabilities", {})
            self._endpoint.send(dict(type="welcome", protocol=1, role=self.role,
                bridge_id=self.bridge_id, account_id=self.account_id,
                bridge_instance_id=self.instance_id, session_id=self.session_id))
            return
        if (not self.session_id or frame.get("session_id") != self.session_id
                or frame.get("boot_id") != self.boot_id):
            return
        if kind == "heartbeat":
            self.last_heartbeat = time.monotonic()
            self.peer_ready = frame.get("ready") is True
            self.peer_gap = frame.get("gap") is True
            self.capabilities = frame.get("capabilities", self.capabilities)
            self.last_error = None
            if frame.get("gap") and not self.gap:
                self._mark_gap("QMT_EVENT_GAP")
        elif kind == "authority_challenge":
            grant = self.authorize() if not self._gap_pending else None
            if grant is not None:
                authority = grant.get("authority") if isinstance(grant, dict) else grant
                remaining = grant.get("remaining_seconds", 0) if isinstance(grant, dict) else .5
                self._endpoint.send(dict(type="authority_grant", session_id=self.session_id,
                    challenge_id=frame.get("challenge_id"), authority=list(authority),
                    remaining_seconds=remaining))
        elif kind == "result":
            with self._lock:
                pending = self._pending.get(frame.get("request_id"))
                if pending is not None and not pending["done"].is_set():
                    status = frame.get("status", 500)
                    if status >= 400:
                        error = frame.get("error") or frame
                        pending["error"] = RemoteError(status, error.get("code", "QMT_ERROR"),
                                                       error.get("message", "QMT request failed"))
                    else:
                        pending["result"] = frame.get("payload")
                    pending["done"].set()
                elif self.on_late_result is not None:
                    self.on_late_result(frame)
        elif kind == "event":
            seq = frame.get("seq")
            if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
                raise RemoteError(503, "IPC_INVALID_EVENT", "invalid callback sequence")
            key = (self.boot_id, seq)
            acked = self._acked.get(self.boot_id, 0)
            if seq <= acked:
                self._queue_ack(self.boot_id, acked)
            elif key not in self._event_seen:
                self._event_seen.add(key)
                try:
                    self._events.put_nowait(frame)
                except queue.Full:
                    self._event_seen.discard(key)
                    self._mark_gap("IPC_EVENT_QUEUE_FULL")
                    raise RemoteError(503, "IPC_EVENT_QUEUE_FULL", "callback queue full")
        elif kind == "event_gap":
            if not self.gap:
                self._mark_gap("QMT_EVENT_GAP")

    def _queue_ack(self, boot_id, seq):
        session = self.session_id
        if session and boot_id == self.boot_id:
            try:
                self._outgoing.put_nowait((session, dict(type="ack", session_id=session,
                                                       boot_id=boot_id, seq=seq)))
            except queue.Full:
                # The peer retains the event. Retry its ACK on the next pump.
                pass

    def _event_loop(self):
        while not self._stop.is_set():
            try:
                frame = self._events.get(timeout=.1)
            except queue.Empty:
                continue
            while not self._stop.is_set():
                try:
                    if self.on_event is None:
                        raise RuntimeError("event persistence is not configured")
                    self.on_event(frame["event"], frame["payload"], frame["boot_id"], frame["seq"])
                    boot, seq = frame["boot_id"], frame["seq"]
                    previous = self._acked.get(boot)
                    if previous is not None and seq != previous + 1:
                        self._mark_gap("IPC_EVENT_SEQUENCE_GAP")
                    self._acked[boot] = seq
                    self._event_seen.discard((boot, seq))
                    self._queue_ack(boot, seq)
                    break
                except Exception as exc:
                    self.last_error = getattr(exc, "code", "EVENT_PERSISTENCE_UNAVAILABLE")
                    self._stop.wait(.1)

    def _run(self):
        next_open = 0
        try:
            while not self._stop.is_set():
                try:
                    if self._gap_pending and time.monotonic() >= self._next_gap_retry:
                        self._mark_gap(self._gap_pending)
                    if self._endpoint is None:
                        if time.monotonic() < next_open:
                            self._stop.wait(.02)
                            continue
                        self._endpoint = self.endpoint_factory(self.name, server=True)
                    for frame in self._endpoint.poll():
                        self._frame(frame)
                    for _ in range(32):
                        try:
                            session, frame = self._outgoing.get_nowait()
                        except queue.Empty:
                            break
                        if not session or session != self.session_id:
                            continue
                        if frame.get("type") == "request":
                            with self._lock:
                                p = self._pending.get(frame["request_id"])
                                if p is None or p["done"].is_set() or time.monotonic() >= p["deadline"]:
                                    continue
                        self._endpoint.send(frame)
                    # ACKs are cumulative and cheap; retry if a prior enqueue was full.
                    if self.boot_id in self._acked and self.session_id:
                        self._queue_ack(self.boot_id, self._acked[self.boot_id])
                except Exception as exc:
                    self._disconnect(exc)
                    next_open = time.monotonic() + .1
                self._stop.wait(.005)
        finally:
            if self._endpoint is not None:
                self._endpoint.close()
            self.session_id = None
            self.peer_ready = False
