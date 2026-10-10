"""Feed HTTP business API backed by an injected QMT IPC call function."""

import queue
import threading
from collections import OrderedDict

from .qmt_api import FeedError, normalize_download_history_params, normalize_download_task_id
from .requests import FEED_METHODS, normalize_request
from .tasks import (
    create_download_task, get_download_task_snapshot, mark_download_task_failed,
    remove_download_task, update_download_task,
)


class FeedService:
    def __init__(self, call, request_timeout=10, download_timeout=120, queue_size=64):
        if request_timeout <= 0 or download_timeout <= 0:
            raise ValueError("Feed timeouts must be positive")
        if queue_size <= 0:
            raise ValueError("Feed queue_size must be positive")
        self.call = call
        self.request_timeout = request_timeout
        self.download_timeout = download_timeout
        self.download_tasks = OrderedDict()
        self.download_tasks_lock = threading.Lock()
        self._admission_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._downloads = queue.Queue(maxsize=queue_size)
        self._worker = threading.Thread(
            target=self._run_downloads, name="ipc-feed-downloads", daemon=True
        )
        self._worker.start()

    @staticmethod
    def _error(exc):
        status = getattr(exc, "status", 500)
        code = getattr(exc, "code", "QMT_TIMEOUT" if isinstance(exc, TimeoutError) else "QMT_ERROR")
        message = getattr(exc, "message", str(exc))
        if isinstance(exc, TimeoutError) and status == 500:
            status = 504
        return status, {"error": {"code": code, "message": message}}

    def handle(self, method, params, verb="GET"):
        try:
            params = normalize_request(method, params, verb)
            # Status queries remain available while QMT is busy or Feed is stopping.
            if method == "get_download_status":
                task_id = normalize_download_task_id(params)
                return 200, get_download_task_snapshot(self, task_id)
            if method == "download_history_data":
                return self._submit_download(params)
            with self._admission_lock:
                self._ensure_running()
            return 200, self.call(method, params, timeout=self.request_timeout)
        except Exception as exc:
            return self._error(exc)

    def _ensure_running(self):
        if self._stop_event.is_set():
            raise FeedError(503, "FEED_STOPPING", "HTTP feed is stopping")

    def _submit_download(self, params):
        with self._admission_lock:
            self._ensure_running()
            normalized = normalize_download_history_params(params)
            task = create_download_task(self, normalized)
            try:
                self._downloads.put_nowait((task["task_id"], normalized))
            except queue.Full:
                remove_download_task(self, task["task_id"])
                raise FeedError(429, "QUEUE_FULL", "request queue is full")
            return 202, task

    def _run_downloads(self):
        while not self._stop_event.is_set():
            try:
                task_id, params = self._downloads.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                with self._admission_lock:
                    if self._stop_event.is_set():
                        continue
                    update_download_task(self, task_id, "running", "download_history_data is running")
                try:
                    self.call("download_history_data", params, timeout=self.download_timeout)
                except Exception as exc:
                    _, payload = self._error(exc)
                    error = payload["error"]
                    with self._admission_lock:
                        if not self._stop_event.is_set():
                            mark_download_task_failed(self, task_id, error["code"], error["message"])
                else:
                    with self._admission_lock:
                        if not self._stop_event.is_set():
                            update_download_task(
                                self, task_id, "completed",
                                "download_history_data returned; data coverage was not verified",
                            )
            finally:
                self._downloads.task_done()

    def close(self):
        """Stop admission without waiting for an in-flight native QMT call."""
        with self._admission_lock:
            self._stop_event.set()
            with self.download_tasks_lock:
                active = [
                    task_id for task_id, task in self.download_tasks.items()
                    if task["status"] in ("queued", "running")
                ]
            for task_id in active:
                mark_download_task_failed(self, task_id, "FEED_STOPPING", "HTTP feed is stopping")
            while True:
                try:
                    self._downloads.get_nowait()
                except queue.Empty:
                    break
                self._downloads.task_done()

    stop = close
