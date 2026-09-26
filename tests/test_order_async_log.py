# -*- coding: utf-8 -*-
"""异步 ORDER 日志的队列、停机及双写边界。"""
import datetime as dt
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from order_bridge.async_log import AsyncOrderLogger


class AsyncOrderLoggerTests(unittest.TestCase):
    def test_full_queue_and_stop_drop_without_waiting_for_blocked_sink(self):
        entered, release = threading.Event(), threading.Event()
        written = []

        def sink(record):
            entered.set()
            release.wait(2)
            written.append(record)

        with tempfile.TemporaryDirectory() as directory:
            logger = AsyncOrderLogger(directory, capacity=1, sink=sink)
            logger.start()
            try:
                self.assertTrue(logger("INFO", "first"))
                self.assertTrue(entered.wait(1))
                self.assertTrue(logger("INFO", "second"))
                start = time.monotonic()
                self.assertFalse(logger("INFO", "full"))
                logger.request_stop()
                self.assertFalse(logger("INFO", "stopped"))
                self.assertLess(time.monotonic() - start, 0.2)
                status = logger.health()
                self.assertEqual(status["queue_size"], 1)
                self.assertEqual(status["dropped"], 2)
                self.assertTrue(status["stopping"])
                self.assertTrue(status["running"])
            finally:
                release.set()
                self.assertTrue(logger.join(2))
            self.assertEqual([row["message"] for row in written], ["first", "second"])
            self.assertTrue(logger.health()["stopped"])

    def test_caller_keeps_only_bounded_plain_snapshot_and_does_no_output(self):
        with tempfile.TemporaryDirectory() as directory:
            received = []
            logger = AsyncOrderLogger(directory, sink=received.append)
            native = object()
            fields = {"native": native, "mutable": [1], "text": "x" * 2000}
            fields.update(("extra_{0}".format(index), index) for index in range(50))
            with patch("builtins.print", side_effect=AssertionError("caller printed")):
                self.assertTrue(logger("INFO", "safe", **fields))
            self.assertFalse(os.path.exists(os.path.join(directory, "order-2026-09-26.log")))
            logger.start()
            logger.request_stop()
            self.assertTrue(logger.join(2))
            row = received[0]
            self.assertEqual(row["fields"]["native"], "<unsupported>")
            self.assertEqual(row["fields"]["mutable"], "<unsupported>")
            self.assertEqual(len(row["fields"]["text"]), 1024)
            self.assertEqual(len(row["fields"]), 32)
            self.assertFalse(any(value is native for value in row["fields"].values()))

    def test_console_failure_does_not_prevent_daily_file(self):
        with tempfile.TemporaryDirectory() as directory:
            logger = AsyncOrderLogger(directory)
            with patch("builtins.print", side_effect=OSError("console failed")):
                logger.start()
                logger("WARNING", "file survives", request_id="r1")
                logger.request_stop()
                self.assertTrue(logger.join(2))
            paths = [name for name in os.listdir(directory) if name.startswith("order-")]
            self.assertEqual(len(paths), 1)
            with open(os.path.join(directory, paths[0]), encoding="utf-8") as stream:
                line = stream.read()
            self.assertIn("[WARNING] file survives", line)
            self.assertIn('"request_id": "r1"', line)
            self.assertEqual(logger.health()["write_errors"], 1)

    def test_file_failure_does_not_prevent_console(self):
        with tempfile.TemporaryDirectory() as directory:
            console = []
            logger = AsyncOrderLogger(directory)
            with patch("builtins.print", side_effect=lambda *args, **kwargs: console.append(args[0])):
                with patch("order_bridge.async_log.order_log_os.makedirs", side_effect=OSError("disk failed")):
                    logger.start()
                    logger("ERROR", "console survives")
                    logger.request_stop()
                    self.assertTrue(logger.join(2))
            self.assertEqual(len(console), 1)
            self.assertIn("[ERROR] console survives", console[0])
            self.assertEqual(logger.health()["write_errors"], 1)

    def test_sink_receives_shanghai_timestamp_and_errors_are_counted(self):
        fixed = dt.datetime(2026, 9, 26, 0, 0, 1, tzinfo=dt.timezone.utc).timestamp()
        received = []

        def sink(record):
            received.append(record)
            raise OSError("sink failed")

        with tempfile.TemporaryDirectory() as directory:
            logger = AsyncOrderLogger(directory, sink=sink)
            with patch("order_bridge.async_log.order_log_time.time", return_value=fixed):
                logger("INFO", "clock")
            logger.start()
            logger.request_stop()
            self.assertTrue(logger.join(2))
            self.assertEqual(received[0]["timestamp"], "2026-09-26 08:00:01")
            self.assertEqual(received[0]["day"], "2026-09-26")
            self.assertEqual(logger.health()["write_errors"], 1)
            self.assertIsNotNone(logger.health()["sampled_at"])


if __name__ == "__main__":
    unittest.main()
