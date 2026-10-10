"""Protocol integration over real Windows pipes; no QMT, database or trading."""
import os
import queue
import threading
import time
import unittest
import uuid

from service.ipc_bridge.transport.pipe.client import BridgeLink, RemoteError
from service.ipc_bridge.qmt_runtime.executor import QmtRuntime


def wait_for(predicate, timeout=4):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.005)
    raise AssertionError("condition did not become true")


class RuntimeThread:
    def __init__(self, name, role="feed", bridge_id="bridge"):
        self.calls = []
        self.runtime = QmtRuntime(role, bridge_id, "account", name, self.execute)
        self.stopped = threading.Event()
        self.paused = threading.Event()
        self.paused_ack = threading.Event()
        self.commands = queue.Queue()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def execute(self, method, params):
        self.calls.append((method, params))
        return {"method": method, "params": params}

    def run(self):
        try:
            while not self.stopped.is_set():
                try:
                    command = self.commands.get_nowait()
                except queue.Empty:
                    command = None
                if command is not None:
                    command()
                if self.paused.is_set():
                    self.paused_ack.set()
                else:
                    self.paused_ack.clear()
                    self.runtime.tick()
                self.stopped.wait(.002)
        finally:
            self.runtime.close()

    def pause(self):
        self.paused.set()
        if not self.paused_ack.wait(2):
            raise AssertionError("runtime did not pause")

    def resume(self):
        self.paused.clear()

    def close(self):
        self.stopped.set()
        self.thread.join(3)
        if self.thread.is_alive():
            raise AssertionError("runtime thread did not stop")


@unittest.skipUnless(os.name == "nt", "Windows named pipes")
class LinkIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.name = "qmt-link-test-" + uuid.uuid4().hex
        self.link = BridgeLink(self.name, "feed", "bridge", "account")
        self.runtime = None
        self.link.start()

    def tearDown(self):
        if self.runtime is not None:
            self.runtime.close()
        self.link.stop()

    def connect(self, **kwargs):
        self.runtime = RuntimeThread(self.name, **kwargs)
        wait_for(lambda: self.link.ready and self.link.boot_id == self.runtime.runtime.boot_id)
        return self.runtime

    def test_hello_heartbeat_request_and_restart(self):
        peer = self.connect()
        result = self.link.call("feed.echo", {"text": "中文"})
        self.assertEqual(result, {"method": "feed.echo", "params": {"text": "中文"}})
        first_session = self.link.session_id
        boot = peer.runtime.boot_id
        peer.commands.put(peer.runtime.disconnect)
        wait_for(lambda: self.link.ready and self.link.session_id != first_session)
        self.assertEqual(self.link.boot_id, boot)
        self.assertTrue(self.link.health()["observation_gap"])
        self.assertEqual(self.link.call("feed.after_reconnect", {})["method"], "feed.after_reconnect")
        peer.close()
        self.runtime = None
        peer = self.connect()
        self.assertNotEqual(peer.runtime.boot_id, boot)
        self.assertEqual(self.link.call("feed.after_restart", {})["method"], "feed.after_restart")

    def test_identity_mismatch_never_ready(self):
        self.runtime = RuntimeThread(self.name, bridge_id="wrong-bridge")
        wait_for(lambda: self.link.last_error == "IPC_IDENTITY_MISMATCH")
        self.assertFalse(self.link.ready)
        self.assertFalse(self.runtime.calls)
        with self.assertRaises(RemoteError) as raised:
            self.link.call("feed.echo", {})
        self.assertEqual(raised.exception.code, "EXECUTOR_NOT_READY")

    def test_gap_persistence_failure_does_not_kill_connection_pump(self):
        peer = self.connect()
        failures = []

        def gap(reason):
            failures.append(reason)
            raise RuntimeError("gap persistence temporarily unavailable")

        self.link.on_gap = gap
        previous = self.link.session_id
        peer.commands.put(peer.runtime.disconnect)
        wait_for(lambda: bool(failures))
        # Restore persistence; the owning pump must survive and reconnect.
        self.link.on_gap = lambda reason: None
        wait_for(lambda: self.link.ready and self.link.session_id != previous)
        self.assertTrue(self.link._thread.is_alive())
        self.assertTrue(self.link.gap)

    def test_timeout_rejects_request_already_in_pipe(self):
        peer = self.connect()
        late = []
        self.link.on_late_result = late.append
        peer.pause()
        with self.assertRaises(RemoteError) as raised:
            self.link.call("feed.expired", {}, timeout=.06)
        self.assertEqual(raised.exception.code, "QMT_TIMEOUT")
        peer.resume()
        wait_for(lambda: bool(late))
        self.assertEqual(late[0]["code"], "REQUEST_EXPIRED")
        self.assertFalse(peer.calls)
        self.assertEqual(self.link.call("feed.valid", {})["method"], "feed.valid")

    def test_heartbeat_expiry_blocks_new_requests(self):
        peer = self.connect()
        self.link.heartbeat_timeout = .1
        peer.pause()
        wait_for(lambda: not self.link.ready)
        with self.assertRaises(RemoteError) as raised:
            self.link.call("feed.echo", {})
        self.assertEqual(raised.exception.code, "EXECUTOR_NOT_READY")
        peer.resume()
        wait_for(lambda: self.link.ready)

    def test_timeout_discards_request_still_in_outgoing_queue(self):
        peer = self.connect()
        # Hold the pending-state lock so the pipe pump cannot dispatch this call.
        with self.link._lock:
            with self.assertRaises(RemoteError) as raised:
                self.link.call("feed.never_sent", {}, timeout=.04)
        self.assertEqual(raised.exception.code, "QMT_TIMEOUT")
        self.assertEqual(self.link.call("feed.after_timeout", {})["method"], "feed.after_timeout")
        self.assertEqual(peer.calls, [("feed.after_timeout", {})])

    def test_disconnect_fails_pending_request_without_resending(self):
        peer = self.connect()
        peer.pause()
        errors = []

        def request():
            try:
                self.link.call("feed.disconnect", {}, timeout=2)
            except RemoteError as exc:
                errors.append(exc)

        caller = threading.Thread(target=request, daemon=True)
        caller.start()
        wait_for(lambda: self.link.health()["pending_requests"] == 1)
        previous = self.link.session_id
        peer.commands.put(peer.runtime.disconnect)
        caller.join(3)
        self.assertFalse(caller.is_alive())
        self.assertEqual([error.code for error in errors], ["IPC_DISCONNECTED"])
        peer.resume()
        wait_for(lambda: self.link.ready and self.link.session_id != previous)
        self.assertEqual(self.link.call("feed.new_session", {})["method"], "feed.new_session")
        self.assertEqual(peer.calls, [("feed.new_session", {})])

    def test_order_authority_challenge_and_revocation(self):
        self.link.stop()
        self.link = BridgeLink(self.name, "order", "bridge", "account")
        self.link.start()
        peer = self.connect(role="order")
        with self.assertRaises(RemoteError) as raised:
            self.link.call("order.submit", {}, metadata={"authority": ["owner", 1]})
        self.assertEqual(raised.exception.code, "AUTHORITY_UNAVAILABLE")
        self.assertFalse(peer.calls)
        self.link.authorize = lambda: ("owner", 1)
        wait_for(lambda: peer.runtime.authority == ["owner", 1])
        self.assertEqual(self.link.call("order.submit", {"fake": True},
                                       metadata={"authority": ["owner", 1]})["method"], "order.submit")
        self.link.authorize = lambda: None
        wait_for(lambda: time.monotonic() >= peer.runtime.authority_until)
        with self.assertRaises(RemoteError) as raised:
            self.link.call("order.submit", {}, metadata={"authority": ["owner", 1]})
        self.assertEqual(raised.exception.code, "AUTHORITY_UNAVAILABLE")
        self.assertEqual(peer.calls, [("order.submit", {"fake": True})])

    def test_callback_ack_only_after_persistence_success(self):
        peer = self.connect()
        attempted = threading.Event()
        committed = threading.Event()
        persisted = []

        def persist(event, payload, boot, seq):
            attempted.set()
            if not committed.is_set():
                raise RuntimeError("database unavailable (simulated)")
            persisted.append((event, payload, boot, seq))

        self.link.on_event = persist
        peer.runtime.observe("order_callback", {"id": "fake"})
        self.assertTrue(attempted.wait(2))
        self.assertEqual(len(peer.runtime.events), 1)
        self.assertNotIn(peer.runtime.boot_id, self.link._acked)
        previous_session = self.link.session_id
        peer.commands.put(peer.runtime.disconnect)
        wait_for(lambda: self.link.ready and self.link.session_id != previous_session)
        self.assertEqual(len(peer.runtime.events), 1)
        self.assertNotIn(peer.runtime.boot_id, self.link._acked)
        committed.set()
        wait_for(lambda: len(peer.runtime.events) == 0)
        self.assertEqual(persisted, [("order_callback", {"id": "fake"}, peer.runtime.boot_id, 1)])


if __name__ == "__main__":
    unittest.main()
