# -*- coding: utf-8 -*-
"""ORDER 有界异步日志；调用线程只提交普通数据快照。"""
import datetime as order_log_datetime
import json as order_log_json
import os as order_log_os
import queue as order_log_queue
import threading as order_log_threading
import time as order_log_time


class AsyncOrderLogger(object):
    """后台写日志。sink(record) 仅在工作线程调用，并替代默认双写。"""

    _ZONE = order_log_datetime.timezone(order_log_datetime.timedelta(hours=8))
    _MAX_FIELDS = 32
    _MAX_TEXT = 1024

    def __init__(self, log_directory, capacity=2048, sink=None):
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError("log capacity must be a positive integer")
        self.log_directory = log_directory
        self._queue = order_log_queue.Queue(maxsize=capacity)
        self._sink = sink
        self._state_lock = order_log_threading.Lock()
        self._stop_event = order_log_threading.Event()
        self._thread = None
        self._running = False
        self._stopped = False
        self._dropped = 0
        self._write_errors = 0
        self._sampled_at = None

    @classmethod
    def _plain(cls, value):
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, str):
            return value[:cls._MAX_TEXT]
        return "<unsupported>"

    def __call__(self, level, message, **fields):
        # 不在 QMT 调用线程格式化、打印、写文件或遍历原生对象。
        snapshot = {}
        for index, (key, value) in enumerate(fields.items()):
            if index >= self._MAX_FIELDS:
                break
            snapshot[key[:64]] = self._plain(value)
        record = (order_log_time.time(), self._plain(level), self._plain(message), snapshot)
        with self._state_lock:
            if self._stop_event.is_set():
                self._dropped += 1
                return False
            try:
                self._queue.put_nowait(record)
            except order_log_queue.Full:
                self._dropped += 1
                return False
        return True

    def start(self):
        with self._state_lock:
            if self._stop_event.is_set():
                return False
            if self._thread is not None:
                return True
            thread = order_log_threading.Thread(target=self._run, name="order-log-writer")
            thread.daemon = True
            self._thread = thread
            self._running = True
            try:
                thread.start()
            except Exception:
                self._thread = None
                self._running = False
                raise
        return True

    def request_stop(self):
        # stop 回调不等待写入；队列在后台自然排空。
        with self._state_lock:
            self._stop_event.set()
            if self._thread is None:
                self._stopped = True

    def join(self, timeout=None):
        with self._state_lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout)
        with self._state_lock:
            return self._stopped

    def health(self):
        with self._state_lock:
            return {"running": self._running, "queue_size": self._queue.qsize(),
                    "dropped": self._dropped, "write_errors": self._write_errors,
                    "stopping": self._stop_event.is_set(), "stopped": self._stopped,
                    "sampled_at": self._sampled_at}

    def _sample(self):
        now = order_log_datetime.datetime.now(self._ZONE).isoformat()
        with self._state_lock:
            self._sampled_at = now

    def _write_default(self, record):
        line = "{0} [{1}] {2}".format(record["timestamp"], record["level"], record["message"])
        if record["fields"]:
            line += " " + order_log_json.dumps(record["fields"], ensure_ascii=False, default=str)
        line = line.replace("\r", "\\r").replace("\n", "\\n")
        try:
            print(line, flush=True)
        except Exception:
            self._write_failed()
        try:
            order_log_os.makedirs(self.log_directory, exist_ok=True)
            path = order_log_os.path.join(self.log_directory, "order-" + record["day"] + ".log")
            with open(path, "a", encoding="utf-8") as output:
                output.write(line + "\n")
        except Exception:
            self._write_failed()

    def _write_failed(self):
        with self._state_lock:
            self._write_errors += 1

    def _run(self):
        try:
            while True:
                try:
                    item = self._queue.get(timeout=0.1)
                except order_log_queue.Empty:
                    self._sample()
                    if self._stop_event.is_set():
                        break
                    continue
                try:
                    when = order_log_datetime.datetime.fromtimestamp(item[0], self._ZONE)
                    record = {"timestamp": when.strftime("%Y-%m-%d %H:%M:%S"),
                              "day": when.strftime("%Y-%m-%d"), "level": item[1],
                              "message": item[2], "fields": item[3]}
                    if self._sink is None:
                        self._write_default(record)
                    else:
                        try:
                            self._sink(record)
                        except Exception:
                            self._write_failed()
                except Exception:
                    self._write_failed()
                finally:
                    self._queue.task_done()
                    self._sample()
        finally:
            with self._state_lock:
                self._running = False
                self._stopped = True
                self._sampled_at = order_log_datetime.datetime.now(self._ZONE).isoformat()
