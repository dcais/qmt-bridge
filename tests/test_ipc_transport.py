import ast
import multiprocessing
import os
from pathlib import Path
import struct
import time
import unittest
import uuid

from service.ipc_bridge.ipc_protocol.wire import Framer, ProtocolError, encode_frame
from service.ipc_bridge.ipc_protocol.winpipe import PipeEndpoint, PipeDisconnected, QueueFull


def _echo_child(name):
    endpoint = PipeEndpoint(name)
    deadline = time.monotonic() + 20
    try:
        received = False
        while time.monotonic() < deadline:
            for message in endpoint.poll():
                endpoint.send(message)
                received = True
            if received and not endpoint.queued_bytes:
                return
            time.sleep(0.001)
        raise RuntimeError("child timed out")
    finally:
        endpoint.close()


class WireTests(unittest.TestCase):
    def test_partial_concatenated_unicode(self):
        values = [{"text": "中文"}, {"empty": []}]
        data = b"".join(encode_frame(x) for x in values)
        framer = Framer()
        output = []
        for byte in data:
            output.extend(framer.feed(bytes([byte])))
        self.assertEqual(output, values)
        self.assertFalse(framer.buffer)

    def test_reject_invalid(self):
        for data in [struct.pack("!I", 0), struct.pack("!I", 65 * 1024 * 1024),
                     struct.pack("!I", 2) + b"[]",
                     struct.pack("!I", 1) + b"\xff",
                     struct.pack("!I", 9) + b'{"x":NaN}']:
            with self.assertRaises(ProtocolError):
                Framer().feed(data)
        with self.assertRaises(ProtocolError):
            encode_frame({"x": "long"}, 2)

    def test_python36_inline_source(self):
        root = Path(__file__).resolve().parents[1] / "service/ipc_bridge/ipc_protocol"
        wire = (root / "wire.py").read_text(encoding="utf-8")
        pipe = (root / "winpipe.py").read_text(encoding="utf-8")
        pipe = "\n".join(line for line in pipe.splitlines() if not line.startswith("from .wire import "))
        source = wire + "\n" + pipe
        ast.parse(source, feature_version=(3, 6))
        namespace = {"__name__": "inline_transport"}
        exec(compile(source, "<inline_transport>", "exec"), namespace)
        self.assertEqual(namespace["Framer"]().feed(namespace["encode_frame"]({"x": 1})), [{"x": 1}])


@unittest.skipUnless(os.name == "nt", "Windows named pipes")
class PipeTests(unittest.TestCase):
    def test_duplex_and_cancel_pending_read_write(self):
        name = "qmt-test-" + uuid.uuid4().hex
        server = PipeEndpoint(name, server=True)
        client = PipeEndpoint(name)
        left, right = {"left": "a" * 300000}, {"right": "b" * 300000}
        server.send(left)
        client.send(right)
        to_server, to_client = [], []
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                to_server.extend(server.poll())
                to_client.extend(client.poll())
                if to_server and to_client:
                    break
            self.assertEqual(to_server, [right])
            self.assertEqual(to_client, [left])
            # Leave the peer unpumped so output exceeds the kernel pipe buffer.
            server.send({"blocked": "x" * 2000000})
            server.poll()
            self.assertTrue(server.write_op.pending)
            self.assertTrue(server.read_op.pending)
            started = time.monotonic()
            server.close()
            self.assertLess(time.monotonic() - started, 1.0)
            self.assertFalse(server.ops)
        finally:
            server.close()
            client.close()

    def test_two_process_large_frame_and_disconnect(self):
        name = "qmt-test-" + uuid.uuid4().hex
        endpoint = PipeEndpoint(name, server=True)
        child = multiprocessing.get_context("spawn").Process(target=_echo_child, args=(name,))
        payload = {"text": "中文abcd" * 200000}
        endpoint.send(payload)
        child.start()
        try:
            received = []
            disconnected = False
            deadline = time.monotonic() + 25
            while time.monotonic() < deadline:
                try:
                    started = time.monotonic()
                    received.extend(endpoint.poll())
                    self.assertLess(time.monotonic() - started, 1.0)
                except PipeDisconnected:
                    disconnected = True
                    break
                time.sleep(0.001)
            child.join(3)
            self.assertEqual(child.exitcode, 0)
            self.assertEqual(received, [payload])
            self.assertTrue(disconnected)
            self.assertFalse(endpoint.connected)
        finally:
            endpoint.close()
            if child.is_alive():
                child.terminate()
                child.join()

    def test_first_instance_lazy_connect_queue_and_cancel(self):
        name = "qmt-test-" + uuid.uuid4().hex
        client = PipeEndpoint(name, max_queue_bytes=20)
        self.assertEqual(client.poll(), [])
        self.assertFalse(client.connected)
        with self.assertRaises(QueueFull):
            client.send({"text": "a" * 20})
        server = PipeEndpoint(name, server=True)
        try:
            with self.assertRaises(OSError):
                PipeEndpoint(name, server=True)
            server.poll()  # pending ConnectNamedPipe, cancellation must drain safely
        finally:
            server.close()
            client.close()
        replacement = PipeEndpoint(name, server=True)
        replacement.close()


if __name__ == "__main__":
    unittest.main()
