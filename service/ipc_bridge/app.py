"""Single HTTP surface preserving the Feed and Order wire contracts."""
from contextlib import asynccontextmanager
import json
import threading
import time
import uuid
from urllib.parse import parse_qs, unquote

from fastapi import FastAPI, Request
from fastapi.responses import Response
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException

from .config import Config
from .feed.requests import FEED_METHODS
from .feed.service import FeedService
from .transport.pipe.client import BridgeLink, RemoteError
from service.ipc_bridge.ipc_protocol.order_queries import METHODS as ORDER_QUERIES
from .order.async_log import AsyncOrderLogger
from .order.service import OrderService


ORDER_METHODS = set(ORDER_QUERIES) | {
    "submit_order", "cancel_order", "order", "orders", "order_events", "capabilities",
    "order_events_evidence_v1", "recovery_evidence_v1", "order_fee_evidence_v1", "health"}
MAX_BODY_BYTES = 1024 * 1024


class BridgeHost:
    def __init__(self, config):
        self.config = config
        self.instance_id = uuid.uuid4().hex
        self.lifecycle = "STARTING"
        self.logger = AsyncOrderLogger(config.log_directory)
        self.feed_link = BridgeLink(config.pipe_name("feed"), "feed", config.bridge_id,
                                    config.account_id, logger=self.log)
        self.order_link = BridgeLink(config.pipe_name("order"), "order", config.bridge_id,
                                     config.account_id, logger=self.log)
        settings = dict(config.settings, request_timeout_seconds=config.request_timeout)
        self.order = OrderService(config.account_id, config.pg_config, settings,
                                  self.order_link, logger=self.log)
        self.order_link.authorize = self.order.authorize_lease
        self.order_link.on_event = self.order.observe_remote
        self.order_link.on_gap = self.order.report_gap
        self.feed = None
        self.command_slots = threading.BoundedSemaphore(128)

    def log(self, level, message, **fields):
        levels = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
        if levels.get(level, 20) >= levels[self.config.log_level]:
            self.logger(level, message, **fields)

    def start(self):
        self.logger.start()
        try:
            self.feed_link.start()
            self.order_link.start()
            self.feed = FeedService(self.feed_link.call, self.config.request_timeout,
                                    self.config.download_timeout)
            self.order.start()
            self.lifecycle = "RUNNING"
            self.log("INFO", "IPC bridge started", bridge_id=self.config.bridge_id,
                     account_id=self.config.account_id, http_port=self.config.port)
        except Exception:
            self.stop()
            raise

    def stop(self):
        self.lifecycle = "STOPPING"
        if self.feed is not None:
            self.feed.stop()
        self.order.stop()
        # Interrupt remote waiters so the durable worker can record UNKNOWN and drain.
        self.feed_link.stop()
        self.order_link.stop()
        self.order.runtime.stopped_event.wait(self.config.request_timeout + 2)
        self.lifecycle = "STOPPED"
        self.logger.request_stop()
        self.logger.join(2)

    def health(self):
        result = self.order.health()
        result.update(http_running=self.lifecycle == "RUNNING",
                      http_lifecycle=self.lifecycle, instance_id=self.instance_id,
                      account_id=self.config.account_id, schedule_settings=self.config.settings,
                      logging=self.logger.health(), feed=self.feed_link.health(),
                      order=self.order_link.health(), bridge={"id": self.config.bridge_id,
                      "instance_id": self.instance_id, "lifecycle": self.lifecycle})
        return result

    def handle(self, method, params, verb):
        if method == "health":
            if verb != "GET":
                raise RemoteError(405, "METHOD_NOT_ALLOWED", "health only accepts GET")
            if params:
                raise RemoteError(400, "INVALID_PARAMS", "health accepts no query parameters")
            return 200, self.health()
        if self.lifecycle != "RUNNING":
            raise RemoteError(503, "ORDER_STOPPING", "IPC bridge is stopping")
        if method in ORDER_METHODS:
            command = method in ("submit_order", "cancel_order") and verb == "POST"
            if command and not self.command_slots.acquire(False):
                raise RemoteError(429, "COMMAND_CAPACITY_EXCEEDED", "command capacity reached; retry with the same id")
            try:
                return self.order.handle(method, params, verb)
            finally:
                if command:
                    self.command_slots.release()
        if method in FEED_METHODS:
            return self.feed.handle(method, params, verb)
        raise RemoteError(404, "METHOD_NOT_FOUND", "unknown method")


def json_response(status, payload, feed=False):
    body = json.dumps(payload, ensure_ascii=False, default=str if feed else None,
                      allow_nan=feed, separators=(",", ":")).encode("utf-8")
    return Response(body, status_code=status,
                    headers={"Content-Type": "application/json; charset=utf-8"})


def error_response(status, code, message):
    return json_response(status, {"error": {"code": code, "message": message}})


def create_app(config=None, host=None):
    config = config or Config.from_env()
    host = host or BridgeHost(config)

    @asynccontextmanager
    async def lifespan(app):
        await run_in_threadpool(host.start)
        try:
            yield
        finally:
            await run_in_threadpool(host.stop)

    app = FastAPI(title="QMT IPC Bridge", lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None,
                  redirect_slashes=False)
    app.state.bridge = host

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        return error_response(exc.status_code,
                              "METHOD_NOT_ALLOWED" if exc.status_code == 405 else "METHOD_NOT_FOUND",
                              "only GET and POST are supported" if exc.status_code == 405 else "unknown method")

    async def dispatch(request: Request):
        began, request_id = time.monotonic(), uuid.uuid4().hex
        parts = [unquote(part) for part in request.url.path.split("/") if part]
        method = parts[0] if len(parts) == 1 else None
        feed = method in FEED_METHODS and method not in ORDER_METHODS
        status = 500
        try:
            if request.method not in ("GET", "POST"):
                raise RemoteError(405, "METHOD_NOT_ALLOWED", "only GET and POST are supported")
            if method is None:
                raise RemoteError(404, "METHOD_NOT_FOUND", "expected /{method}")
            query = request.url.query
            if request.method == "GET":
                params = {key: values[0] if len(values) == 1 else values
                          for key, values in parse_qs(query, keep_blank_values=True).items()}
            else:
                if method in ("health", "get_history_trade_detail_data", "history_capabilities"):
                    raise RemoteError(405, "METHOD_NOT_ALLOWED", "this query only accepts GET")
                if not feed and query:
                    raise RemoteError(400, "INVALID_PARAMS", "POST query params are unsupported")
                try:
                    length = int(request.headers.get("content-length", "0"))
                except ValueError:
                    raise RemoteError(400, "INVALID_CONTENT_LENGTH", "invalid Content-Length") from None
                if length < 0:
                    raise RemoteError(413 if feed else 400,
                                      "BODY_TOO_LARGE" if feed else "INVALID_CONTENT_LENGTH",
                                      "invalid Content-Length")
                if length > MAX_BODY_BYTES:
                    raise RemoteError(413, "BODY_TOO_LARGE", "JSON body is too large")
                raw = bytearray()
                async for chunk in request.stream():
                    if len(raw) + len(chunk) > MAX_BODY_BYTES:
                        raise RemoteError(413, "BODY_TOO_LARGE", "JSON body is too large")
                    raw.extend(chunk)
                try:
                    def invalid_constant(value):
                        raise ValueError("non-finite JSON value")
                    kwargs = {} if feed else {"parse_constant": invalid_constant}
                    params = json.loads(raw.decode("utf-8"), **kwargs) if raw else {}
                except (ValueError, UnicodeDecodeError):
                    raise RemoteError(400, "INVALID_JSON", "request body must be UTF-8 JSON") from None
                if not isinstance(params, dict):
                    raise RemoteError(400, "INVALID_PARAMS", "JSON body must be an object")
            status, payload = await run_in_threadpool(host.handle, method, params, request.method)
            return json_response(status, payload, feed=feed)
        except Exception as exc:
            if all(hasattr(exc, key) for key in ("status", "code", "message")):
                status = exc.status
                return error_response(status, exc.code, exc.message)
            status = 503
            # Never expose database driver text, configuration or connection credentials.
            host.log("ERROR", "Request failed", request_id=request_id, method=method,
                     error_type=type(exc).__name__)
            return error_response(503, "PERSISTENCE_UNAVAILABLE" if method in ORDER_METHODS else "QMT_ERROR",
                                  "bridge could not complete the request")
        finally:
            host.log("INFO", "Request finished", request_id=request_id, method=method,
                     status=status, elapsed_ms=round((time.monotonic() - began) * 1000, 2))

    # Preserve repeated/trailing slash normalization, without FastAPI redirects.
    verbs = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE", "CONNECT"]
    app.add_api_route("/", dispatch, methods=verbs)
    app.add_api_route("/{path:path}", dispatch, methods=verbs)
    return app
