# -*- coding: utf-8 -*-
"""ORDER 有界异步日志；调用线程只提交普通数据快照。Last modified: 2026-09-26。"""
import datetime as order_log_datetime
import json as order_log_json
import math as order_log_math
import os as order_log_os
import queue as order_log_queue
import threading as order_log_threading
import time as order_log_time


class AsyncOrderLogger(object):
    """后台写日志。sink(record) 仅在工作线程调用，并替代默认双写。"""

    _ZONE = order_log_datetime.timezone(order_log_datetime.timedelta(hours=8))
    _MAX_FIELDS = 32
    _MAX_TEXT = 1024
    _DIAGNOSTIC_LIMITS = {"traceback": 16384, "error_message": 4096}

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
    def _plain(cls, value, limit=None):
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, str):
            return value[:cls._MAX_TEXT if limit is None else limit]
        return "<unsupported>"

    @classmethod
    def _return_snapshot(cls, value):
        """只复制有界普通诊断数据；不读取原生属性或调用 repr。"""
        budget = {"nodes": 2048, "chars": 65536, "truncated": False}

        def plain(item, depth=0):
            budget["nodes"] -= 1
            if budget["nodes"] < 0 or depth > 8:
                budget["truncated"] = True
                return "<truncated>"
            if item is None or isinstance(item, (bool, int)):
                return item
            if isinstance(item, float):
                return item if order_log_math.isfinite(item) else None
            if isinstance(item, str):
                limit = min(4096, max(0, budget["chars"]))
                budget["truncated"] |= len(item) > limit
                budget["chars"] -= min(len(item), limit)
                return item[:limit]
            if type(item) is dict:
                result = {}
                for index, (key, member) in enumerate(item.items()):
                    if index >= 128 or budget["nodes"] <= 0 or budget["chars"] <= 0:
                        budget["truncated"] = True
                        break
                    if not isinstance(key, str):
                        budget["truncated"] = True
                        continue
                    bounded_key = key[:256]
                    budget["truncated"] |= len(key) > 256
                    budget["chars"] -= len(bounded_key)
                    result[bounded_key] = plain(member, depth + 1)
                return result
            if type(item) in (list, tuple):
                result = []
                for index, member in enumerate(item):
                    if index >= 128 or budget["nodes"] <= 0 or budget["chars"] <= 0:
                        budget["truncated"] = True
                        break
                    result.append(plain(member, depth + 1))
                return result
            budget["truncated"] = True
            return "<unsupported>"

        result = plain(value)
        if type(result) is dict and budget["truncated"]:
            result["truncated"] = True
        return result

    def __call__(self, level, message, **fields):
        # 不在 QMT 调用线程格式化、打印、写文件或遍历原生对象。
        snapshot = {}
        for index, (key, value) in enumerate(fields.items()):
            if index >= self._MAX_FIELDS:
                break
            # 异常堆栈单独限长，避免普通字段的 1 KiB 截断掉底部根因。
            snapshot[key[:64]] = (self._return_snapshot(value) if key in ("return_snapshot", "qmt_parameters") else
                                  self._plain(value, self._DIAGNOSTIC_LIMITS.get(key)))
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
