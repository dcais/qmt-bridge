"""Real Windows pipes and disposable PostgreSQL schemas, with fake native QMT."""
import datetime as dt
import os
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest
import uuid
from unittest.mock import patch

from service.ipc_bridge.transport.pipe.client import BridgeLink
from service.ipc_bridge.qmt_runtime.order.adapter import QmtAdapter
from service.ipc_bridge.order.repository import PostgresRepository
from service.ipc_bridge.order.service import OrderService
from service.ipc_bridge.qmt_runtime.order.entrypoint import _submit_document
from tests.test_ipc_link import RuntimeThread, wait_for
from tests.test_order_repository import repo_test_config
from tests.test_order_runtime import _FakeQmt, _LocalLock
from tools.order_schema import initialize_schema


class _OrderPeer(RuntimeThread):
    def __init__(self, name):
        self.qmt = _FakeQmt()
        self.adapter = QmtAdapter(self.qmt.apis(), object(), account_id="account")
        self.disconnect_after_submit = False
        super().__init__(name, role="order")

    def execute(self, method, params):
        self.calls.append((method, params))
        if method == "order.submit":
            result = _submit_document(self.adapter, params["document"])
            if self.disconnect_after_submit:
                self.disconnect_after_submit = False
                # Execute the native call, then lose its result before sending a reply.
                self.runtime.disconnect()
            return result
        if method == "order.prepare_step":
            return self.adapter.prepare_step(params["document"], params["stage"], params.get("payload"))
        if method == "order.cancel_action":
            return self.adapter.cancel_action(params["action"])
        if method == "order.query":
            return self.adapter.query(params["kind"])
        if method == "order.capabilities":
            return {name: callable(api) for name, api in self.qmt.apis().items()}
        raise AssertionError("unexpected QMT method: " + method)


@unittest.skipUnless(os.name == "nt" and os.environ.get("ORDER_TEST_PGHOST"),
                     "Windows pipes and ORDER_TEST_PGHOST required")
class IpcDatabaseIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.config = repo_test_config()
        self.repo = PostgresRepository(self.config, "account")
        self.addCleanup(self.drop_schema)
        initialize_schema(self.repo)
        script = (Path(__file__).resolve().parents[1] / "sql" / "order_evidence_v1.sql").read_text()
        self.repo.repo_run(lambda cur: cur.execute(script.replace("qmt_order", self.repo.repo_s)))
        self.name = "qmt-ipc-db-test-" + uuid.uuid4().hex
        self.link = BridgeLink(self.name, "order", "bridge", "account")
        self.addCleanup(self.link.stop)
        self.service = OrderService("account", self.config, {"request_timeout_seconds": 2,
                                                           "reconcile_interval_seconds": 1}, self.link,
                                    repository=self.repo, local_lock=_LocalLock())
        self.link.authorize = self.service.authorize_lease
        self.link.on_event = self.service.observe_remote
        self.link.on_gap = self.service.report_gap
        self.link.start()
        self.service.start()
        self.addCleanup(self.stop_service)
        self.peer = _OrderPeer(self.name)
        self.addCleanup(self.peer.close)
        wait_for(lambda: self.service.health()["accepting_orders"] and self.peer.runtime.authority, timeout=10)

    def stop_service(self):
        self.service.stop()
        self.assertTrue(self.service.runtime.stopped_event.wait(10))
        self.service.worker.join(3)
        self.assertFalse(self.service.worker.is_alive())

    def drop_schema(self):
        self.repo.close()
        conn = self.repo.repo_connect()
        try:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS " + self.repo.repo_s + " CASCADE")
            conn.commit()
        finally:
            conn.close()

    def request(self, client="ipc-one"):
        return dict(client_order_id=client, account_id="account", order_type="SINGLE",
                    symbol="600000.SH", side="BUY", sizing_type="QUANTITY", quantity=100,
                    price_type="LIMIT", limit_price="10.50", execution={"type": "DIRECT"})

    def wait_order(self, client, predicate):
        wait_for(lambda: predicate(self.repo.get_order(client)), timeout=10)
        return self.repo.get_order(client)

    def test_submit_replay_callback_commit_ack_evidence_and_cancel(self):
        request = self.request()
        status, accepted = self.service.handle("submit_order", request, "POST")
        self.assertEqual(status, 202)
        status, replay = self.service.handle("submit_order", request, "POST")
        self.assertEqual(status, 200)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["order_id"], accepted["order_id"])
        doc = self.wait_order("ipc-one", lambda row: bool(row["attempts"]) and row["attempts"][-1]["status"] == "RETURNED")
        self.assertEqual(len(self.peer.qmt.side_effects("passorder")), 1)
        native = SimpleNamespace(m_strAccountID="account", m_strRemark=doc["remark"],
                                 m_strOrderSysID="ipc-qmt-one", m_nOrderStatus=50,
                                 m_nVolumeTotalOriginal=100, m_nVolumeTraded=0,
                                 m_strInstrumentID="600000", m_strExchangeID="SH", m_nOffsetFlag=48,
                                 m_strInsertDate=dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y%m%d"))
        entered, release = threading.Event(), threading.Event()
        original_ingest = self.repo.ingest_observation

        def blocked_ingest(kind, payload, *args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise AssertionError("test did not release callback persistence")
            return original_ingest(kind, payload, *args, **kwargs)

        def callback():
            self.peer.runtime.observe("order", self.peer.adapter.snapshot(native))
            native.m_nVolumeTotalOriginal = 999

        with patch.object(self.repo, "ingest_observation", side_effect=blocked_ingest):
            self.peer.commands.put(callback)
            try:
                self.assertTrue(entered.wait(5))
                self.assertEqual(len(self.peer.runtime.events), 1)
                self.assertEqual(self.link._acked.get(self.peer.runtime.boot_id, 0), 0)
                self.assertEqual(self.repo.get_order("ipc-one")["qmt_orders"], [])
            finally:
                release.set()
            wait_for(lambda: not self.peer.runtime.events, timeout=10)
        committed = self.repo.get_order("ipc-one")
        self.assertEqual(committed["qmt_orders"][0]["qmt_order_id"], "ipc-qmt-one")
        self.assertEqual(committed["qmt_orders"][0]["quantity"], 100)
        self.assertEqual(self.link._acked[self.peer.runtime.boot_id], 1)
        status, evidence = self.service.handle("order_events_evidence_v1", {"after": 0, "limit": 100}, "GET")
        self.assertEqual(status, 200)
        self.assertTrue(any(event["type"] == "QMT_OBSERVATION" and
                            event["data"]["order_id"] == accepted["order_id"] for event in evidence["events"]))
        status, unused = self.service.handle("cancel_order", dict(cancel_request_id="ipc-cancel",
                                                                  account_id="account", client_order_id="ipc-one"), "POST")
        self.assertEqual(status, 202)
        self.wait_order("ipc-one", lambda row: row["cancel_status"] == "PENDING")
        self.assertEqual(len(self.peer.qmt.side_effects("cancel")), 1)
        self.assertEqual(len(self.peer.qmt.side_effects("passorder")), 1)

    def test_disconnect_after_native_submit_stays_unknown_after_recovery(self):
        self.peer.disconnect_after_submit = True
        first_session = self.link.session_id
        request = self.request("ipc-unknown")
        self.assertEqual(self.service.handle("submit_order", request, "POST")[0], 202)
        frozen = self.wait_order("ipc-unknown", lambda row: row["submission_status"] == "UNKNOWN")
        self.assertEqual(frozen["attempts"][-1]["status"], "UNKNOWN")
        self.assertEqual(len(self.peer.qmt.side_effects("passorder")), 1)
        try:
            wait_for(lambda: self.link.session_id != first_session and self.service.health()["accepting_orders"]
                     and self.peer.runtime.authority, timeout=10)
        except AssertionError:
            self.fail(str(dict(service=self.service.health(), link=self.link.health(),
                               peer_session=self.peer.runtime.session_id, peer_authority=self.peer.runtime.authority)))
        status, replay = self.service.handle("submit_order", request, "POST")
        self.assertEqual(status, 200)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["submission_status"], "UNKNOWN")
        self.assertEqual(self.service.handle("submit_order", self.request("ipc-after-recovery"), "POST")[0], 202)
        self.wait_order("ipc-after-recovery", lambda row: bool(row["attempts"]) and row["attempts"][-1]["status"] == "RETURNED")
        self.assertEqual(len(self.peer.qmt.side_effects("passorder")), 2)
        current = self.repo.get_order("ipc-unknown")
        self.assertEqual(current["submission_status"], "UNKNOWN")
        self.assertEqual(len(current["attempts"]), 1)


if __name__ == "__main__":
    unittest.main()
