# -*- coding: utf-8 -*-
"""真实 PostgreSQL 加假 QMT 验证交易调度；不会连接交易终端。"""
import os
import json
import importlib.util
import threading
import time
import unittest
import urllib.error
import urllib.request
import uuid
import datetime as dt
from pathlib import Path
from unittest.mock import patch

from order_bridge.common import OrderError
from order_bridge.contracts import capabilities
from order_bridge.repository import PostgresRepository
from tools.order_schema import initialize_schema
from order_bridge.runtime import OrderRuntime


class _LocalLock(object):
    def __init__(self):
        self.held = False

    def acquire(self):
        self.held = True

    def release(self):
        self.held = False


class _FakeQmt(object):
    def __init__(self):
        self.calls = []
        self.rows = {"task": [], "order": [], "deal": []}
        self.baskets = {}
        self.submit_exception = None
        self.history_exception = None
        self.metadata = {"VWAP": [
            {"key": "m_dLimitOverRate", "dataType": "浮点", "unit": "%",
             "valueRange": "0-100", "defaultValue": "10"},
            {"key": "m_strCmdRemark", "dataType": "字符串", "defaultValue": ""},
        ]}

    def apis(self):
        return {
            "passorder": self.passorder,
            "algo_passorder": self.algo_passorder,
            "smart_algo_passorder": self.smart_algo_passorder,
            "get_smart_algo_param": self.get_smart_algo_param,
            "set_basket": self.set_basket,
            "get_basket": self.get_basket,
            "cancel": self.cancel,
            "cancel_task": self.cancel_task,
            "get_trade_detail_data": self.get_trade_detail_data,
            "get_history_trade_detail_data": self.get_history_trade_detail_data,
        }

    def _submit(self, name, args):
        self.calls.append((name, args))
        if self.submit_exception is not None:
            raise self.submit_exception

    def passorder(self, *args):
        self._submit("passorder", args)

    def algo_passorder(self, *args):
        self._submit("algo_passorder", args)

    def smart_algo_passorder(self, *args):
        self._submit("smart_algo_passorder", args)

    def get_smart_algo_param(self, algorithms):
        return {name: self.metadata[name] for name in algorithms if name in self.metadata}

    def set_basket(self, definition):
        self.calls.append(("set_basket", definition))
        self.baskets[definition["name"]] = definition

    def get_basket(self, name):
        self.calls.append(("get_basket", name))
        return self.baskets.get(name)

    def cancel(self, *args):
        self.calls.append(("cancel", args))
        return True

    def cancel_task(self, *args):
        self.calls.append(("cancel_task", args))
        return True

    def get_trade_detail_data(self, account, account_type, kind):
        self.calls.append(("query_" + kind.lower(), account))
        return self.rows[kind.lower()]

    def get_history_trade_detail_data(self, account, account_type, kind, start, end):
        self.calls.append(("query_history_" + kind.lower(), (start, end)))
        if self.history_exception is not None:
            raise self.history_exception
        return []

    def side_effects(self, name):
        return [row for row in self.calls if row[0] == name]


@unittest.skipUnless(os.environ.get("ORDER_TEST_PGHOST"), "ORDER_TEST_PGHOST unset")
class OrderRuntimeIntegrationTests(unittest.TestCase):
    account_id = "test-order-runtime-account"

    def setUp(self):
        self.config = {
            "pg_host": os.environ.get("ORDER_TEST_PGHOST", "127.0.0.1"),
            "pg_port": int(os.environ.get("ORDER_TEST_PGPORT", "15439")),
            "pg_database": os.environ.get("ORDER_TEST_PGDATABASE", "qmt_order_test"),
            "pg_user": os.environ.get("ORDER_TEST_PGUSER", "postgres"),
            "pg_password": os.environ.get("ORDER_TEST_PGPASSWORD", "qmt_local_test_only"),
            "pg_schema": "qmt_runtime_test_" + uuid.uuid4().hex,
        }
        self.repositories = []
        self.runtimes = []
        first = self._new_runtime(_FakeQmt())
        initialize_schema(first.repo)
        def registered_accounts(cur):
            cur.execute("SELECT count(*) FROM " + first.repo.repo_s + ".account_runtime")
            return cur.fetchone()[0]
        self.assertEqual(first.repo.repo_run(registered_accounts), 0)
        first.initialize()
        self._until(lambda: first.recovery_complete, first)
        self.runtime = first

    def tearDown(self):
        for runtime in reversed(self.runtimes):
            runtime.stop()
            self._until_stopped(runtime)
        conn = self.repositories[0].repo_connect()
        try:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA " + self.repositories[0].repo_s + " CASCADE")
            conn.commit()
        finally:
            conn.close()

    def _new_runtime(self, qmt, account_id=None):
        account_id = self.account_id if account_id is None else account_id
        repo = PostgresRepository(self.config, account_id)
        self.repositories.append(repo)
        runtime = OrderRuntime(qmt.apis(), object(), account_id,
                               pg_config=self.config, repository=repo, local_lock=_LocalLock())
        self.runtimes.append(runtime)
        runtime.fake_qmt = qmt
        return runtime

    def single(self, client="single-one", quantity=1000):
        return {"client_order_id": client, "account_id": self.account_id,
                "order_type": "SINGLE", "symbol": "600000.SH", "side": "BUY",
                "sizing_type": "QUANTITY", "quantity": quantity,
                "price_type": "LIMIT", "limit_price": "10.50",
                "execution": {"type": "DIRECT"}}

    def basket(self, client, execution):
        return {"client_order_id": client, "account_id": self.account_id,
                "order_type": "BASKET", "sizing_type": "QUANTITY", "items": [
                    {"item_id": "one", "symbol": "510300.SH", "side": "BUY", "quantity": 100},
                    {"item_id": "two", "symbol": "600000.SH", "side": "SELL", "quantity": 500}],
                "price_type": "MARKET" if execution["type"] == "SMART" else "QUOTE",
                **({} if execution["type"] == "SMART" else {"quote_type": "OPPONENT_BEST"}),
                "execution": execution}

    def cancel(self, client="single-one", cancel_id="cancel-one"):
        return {"cancel_request_id": cancel_id, "account_id": self.account_id,
                "client_order_id": client}

    def sliced_params(self):
        fields = capabilities()["sliced"]["required_params"]
        params = {name: 0 for name in fields}
        params.update(MaxOrderCount=20, PlaceOrderInterval=3, SingleNumMax=500,
                      SingleNumMin=100, VolumeRate=0.2, ValidTimeElapse=600)
        return params

    def _tick(self, runtime=None, count=1):
        runtime = runtime or self.runtime
        for unused in range(count):
            runtime.tick()

    def _until(self, predicate, runtime=None, timeout=10):
        """Pump QMT while background DB work converges; never assume one tick commits."""
        runtime = runtime or self.runtime
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            runtime.tick()
            if predicate():
                return
            time.sleep(0.005)
        self.fail("runtime did not converge: error={!r}, health={!r}".format(
            runtime.last_error, runtime.health()))

    def _until_order(self, client, predicate, runtime=None):
        runtime = runtime or self.runtime
        try:
            self._until(lambda: predicate(runtime.repo.get_order(client)), runtime)
        except AssertionError:
            self.fail("order {!r} did not converge: {!r}".format(
                client, runtime.repo.get_order(client)))

    def _until_call(self, qmt, name, count=1, runtime=None):
        self._until(lambda: len(qmt.side_effects(name)) >= count, runtime)

    def _until_stopped(self, runtime, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if runtime.stopped_event.is_set():
                return
            time.sleep(0.005)
        self.fail("background did not stop: {!r}".format(runtime.health()))

    def _reconcile_now(self, client):
        """Advance the persisted due time after a prior round has finished."""
        self._until(lambda: self.runtime.background.round is None)
        doc = self.runtime.repo.get_order(client)
        self.runtime.repo.update_order(doc["order_id"], "TEST_RECONCILE_DUE",
                                       lambda row: row.update(
                                           reconcile_due_at=dt.datetime.now(dt.timezone.utc).isoformat()))
        self.runtime.background.next_round = 0

    def _age_order_column(self, order_id, created_at):
        """Keep the indexed SQL timestamp aligned with the synthetic JSON age."""
        def age(cur):
            cur.execute("UPDATE " + self.runtime.repo.repo_s +
                        ".orders SET created_at=%s WHERE account_type=%s AND account_id=%s AND order_id=%s",
                        (created_at,) + self.runtime.repo.repo_scope + (order_id,))
        self.runtime.repo.repo_run(age, mutation=True)

    def test_startup_uses_configured_account_and_restart_preserves_event_cursor(self):
        account_id = "0012345678"
        runtime = self._new_runtime(_FakeQmt(), account_id=account_id)
        runtime.initialize()
        self._until(lambda: runtime.recovery_complete, runtime)
        request = self.single(client="startup-order")
        request["account_id"] = account_id
        runtime.handle("submit_order", request, "POST")
        runtime.handle("cancel_order", {"account_id": account_id, "client_order_id": "startup-order",
                                       "cancel_request_id": "startup-cancel"}, "POST")
        def account_row(cur):
            cur.execute("SELECT account_id,event_seq,executor_host,executor_instance,executor_epoch FROM " +
                        runtime.repo.repo_s + ".account_runtime WHERE account_type=%s AND account_id=%s",
                        runtime.repo.repo_scope)
            return tuple(cur.fetchone())
        before = runtime.repo.repo_run(account_row)
        previous_events = runtime.repo.events()["events"]
        self.assertEqual(before[0], account_id)
        self.assertGreater(before[1], 0)
        runtime.stop()
        self._until_stopped(runtime)
        restarted = self._new_runtime(_FakeQmt(), account_id=account_id)
        restarted.initialize()
        self._until(lambda: restarted.recovery_complete, restarted)
        after = restarted.repo.repo_run(account_row)
        self.assertEqual(after[0], before[0])
        self.assertGreaterEqual(after[1], before[1])  # 重启对账可追加事件，但不能重置游标。
        self.assertEqual(after[2], before[2])
        self.assertEqual(after[3], restarted.instance_id)
        self.assertEqual(after[4], before[4] + 1)
        events = restarted.repo.events()["events"]
        self.assertEqual(events[:len(previous_events)], previous_events)
        self.assertEqual([event["event_id"] for event in events], list(range(1, after[1] + 1)))
        replay = restarted.handle("submit_order", request, "POST")[1]
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["submission_status"], "CANCELLED_LOCAL")

    def test_submit_replay_conflict_and_single_dispatch_once(self):
        qmt = self.runtime.fake_qmt
        code, accepted = self.runtime.handle("submit_order", self.single(), "POST")
        self.assertEqual(code, 202)
        self.assertEqual(accepted["submission_status"], "QUEUED")
        code, replayed = self.runtime.handle("submit_order", self.single(), "POST")
        self.assertEqual(code, 200)
        self.assertEqual(accepted["order_id"], replayed["order_id"])
        self.assertTrue(replayed["replayed"])
        with self.assertRaises(OrderError) as error:
            self.runtime.handle("submit_order", self.single(quantity=200), "POST")
        self.assertEqual(error.exception.status, 409)
        self._until_call(qmt, "passorder")
        self._until_order("single-one", lambda row: row["submission_status"] == "SUBMITTING")
        self.assertEqual(len(qmt.side_effects("passorder")), 1)
        self.assertEqual(self.runtime.repo.get_order("single-one")["submission_status"], "SUBMITTING")
        events = self.runtime.handle("order_events", {"after": 0}, "GET")[1]["events"]
        self.assertEqual([e["event_id"] for e in events], list(range(1, len(events) + 1)))
        self.assertEqual(self.runtime.handle("order", {"client_order_id": "single-one"}, "GET")[1]["order_id"], accepted["order_id"])

    def test_blocked_submit_result_commit_does_not_dispatch_twice(self):
        qmt, repo = self.runtime.fake_qmt, self.runtime.repo
        entered, release = threading.Event(), threading.Event()
        original_update = repo.update_order

        def block_result(order_id, event_type, mutator):
            if event_type == "SUBMIT_CALL_RETURNED":
                entered.set()
                release.wait(3)
            return original_update(order_id, event_type, mutator)

        with patch.object(repo, "update_order", side_effect=block_result):
            self.runtime.handle("submit_order", self.single(), "POST")
            try:
                self._until(lambda: entered.is_set())
                for unused in range(20):
                    self.runtime.tick()
                self.assertEqual(len(qmt.side_effects("passorder")), 1)
                self.assertEqual(repo.get_order("single-one")["attempts"][-1]["status"], "CALLING")
            finally:
                release.set()
            self._until_order("single-one", lambda row: row["attempts"][-1]["status"] == "RETURNED")
        self.assertEqual(len(qmt.side_effects("passorder")), 1)

    def test_queued_cancel_prevents_qmt_submit(self):
        qmt = self.runtime.fake_qmt
        self.runtime.handle("submit_order", self.single(), "POST")
        code, response = self.runtime.handle("cancel_order", self.cancel(), "POST")
        self.assertEqual(code, 200)
        self.assertEqual(response["cancel_status"], "CONFIRMED")
        self._tick(count=2)
        self.assertEqual(qmt.side_effects("passorder"), [])
        self.assertEqual(self.runtime.repo.get_order("single-one")["submission_status"], "CANCELLED_LOCAL")
        code, replayed = self.runtime.handle("cancel_order", self.cancel(), "POST")
        self.assertEqual(code, 200)
        self.assertTrue(replayed["replayed"])

    def test_submitting_cancel_waits_for_qmt_id_then_cancels_remainder(self):
        qmt = self.runtime.fake_qmt
        self.runtime.handle("submit_order", self.single(), "POST")
        self._until_call(qmt, "passorder")
        self._until_order("single-one", lambda row: row["submission_status"] == "SUBMITTING")
        code, waiting = self.runtime.handle("cancel_order", self.cancel(), "POST")
        self.assertEqual(code, 202)
        self.assertEqual(waiting["cancel_status"], "WAITING_QMT_ID")
        self._tick()
        self.assertEqual(qmt.side_effects("cancel"), [])
        doc = self.runtime.repo.get_order("single-one")
        self.runtime.observe("order", {"remark": doc["remark"], "qmt_order_id": "qmt-1",
                                       "trading_day": "20260926", "market": "SH", "symbol": "600000.SH",
                                       "side": "BUY", "quantity": 1000, "filled_quantity": 400, "status": 52})
        self.runtime.observe("deal", {"qmt_order_id": "qmt-1", "trade_id": "fill-1",
                                      "trading_day": "20260926", "market": "SH", "symbol": "600000.SH",
                                      "side": "BUY", "quantity": 400, "amount": "4200"})
        self.runtime.observe("deal", {"qmt_order_id": "qmt-1", "trade_id": "fill-1",
                                      "trading_day": "20260926", "market": "SH", "symbol": "600000.SH",
                                      "side": "BUY", "quantity": 400, "amount": "4200"})
        self._until_call(qmt, "cancel")
        self._until_order("single-one", lambda row: row["filled_quantity"] == 400 and row["cancel_status"] == "PENDING")
        self.assertEqual(len(qmt.side_effects("cancel")), 1)
        current = self.runtime.repo.get_order("single-one")
        self.assertEqual(current["filled_quantity"], 400)
        self.assertEqual(current["cancel_status"], "PENDING")
        self.runtime.observe("order", {"remark": doc["remark"], "qmt_order_id": "qmt-1",
                                       "trading_day": "20260926", "market": "SH", "symbol": "600000.SH",
                                       "side": "BUY", "quantity": 1000, "filled_quantity": 500, "status": 53})
        self.runtime.observe("deal", {"qmt_order_id": "qmt-1", "trade_id": "fill-2",
                                      "trading_day": "20260926", "market": "SH", "symbol": "600000.SH",
                                      "side": "BUY", "quantity": 100, "amount": "1050"})
        self._until_order("single-one", lambda row: row["filled_quantity"] == 500 and
                          row["cancelled_quantity"] == 500)
        self._reconcile_now("single-one")
        self._until_order("single-one", lambda row: row["execution_status"] == "PARTIALLY_CANCELLED")
        current = self.runtime.repo.get_order("single-one")
        self.assertEqual(current["filled_quantity"], 500)
        self.assertEqual(current["cancelled_quantity"], 500)
        self.assertEqual(current["execution_status"], "PARTIALLY_CANCELLED")
        self.assertEqual(current["cancel_status"], "CONFIRMED")
        self.assertEqual(len(qmt.side_effects("cancel")), 1)

    def test_restart_keeps_unknown_frozen_and_new_order_proceeds(self):
        original = self.runtime
        qmt = original.fake_qmt
        qmt.submit_exception = RuntimeError("call outcome hidden")
        original.handle("submit_order", self.single(), "POST")
        self._until_order("single-one", lambda row: row["submission_status"] == "UNKNOWN")
        self.assertEqual(original.repo.get_order("single-one")["submission_status"], "UNKNOWN")
        original.stop()
        self._until_stopped(original)
        next_qmt = _FakeQmt()
        restarted = self._new_runtime(next_qmt)
        restarted.initialize()
        self._until(lambda: restarted.recovery_complete, restarted)
        self.assertEqual(restarted.repo.get_order("single-one")["submission_status"], "UNKNOWN")
        restarted.handle("submit_order", self.single(client="second"), "POST")
        self._until_call(next_qmt, "passorder", runtime=restarted)
        self._until_order("second", lambda row: row["submission_status"] == "SUBMITTING", restarted)
        self.assertEqual(len(next_qmt.side_effects("passorder")), 1)
        self.assertEqual(restarted.repo.get_order("second")["submission_status"], "SUBMITTING")

    def test_frozen_smart_request_expired_on_restart_is_not_dispatched(self):
        original = self.runtime
        today = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date().isoformat()
        request = self.single()
        request["execution"] = {"type": "SMART", "algorithm": "VWAP",
                                "start_at": today + "T00:00:00+08:00",
                                "end_at": today + "T23:59:59+08:00",
                                "params": {"m_dLimitOverRate": 0.1}}
        original.handle("submit_order", request, "POST")
        doc = original.repo.get_order("single-one")
        frozen = original.adapter.resolve(doc["request"], doc["remark"])
        original.repo.update_order(doc["order_id"], "PARAMETERS_RESOLVED",
                                   lambda row: row.update(resolved_request=frozen))
        original.stop()
        self._until_stopped(original)
        qmt = _FakeQmt()
        restarted = self._new_runtime(qmt)
        expired_clock = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
        from order_bridge import runtime as runtime_module
        with patch.object(runtime_module, "utc_now", return_value=expired_clock):
            restarted.initialize()
            self._until_order("single-one", lambda row: row["submission_status"] == "EXPIRED", restarted)
        current = restarted.repo.get_order("single-one")
        self.assertEqual(current["submission_status"], "EXPIRED")
        self.assertEqual(current["attempts"], [])
        self.assertEqual(qmt.side_effects("smart_algo_passorder"), [])

    def test_history_gap_is_local_and_does_not_block_new_orders(self):
        qmt = self.runtime.fake_qmt
        old = self.runtime.handle("submit_order", self.single(client="old"), "POST")[1]
        yesterday = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)).isoformat()
        self.runtime.repo.update_order(old["order_id"], "TEST_OLD_UNCERTAIN",
                                       lambda row: row.update(created_at=yesterday,
                                                              submission_status="UNKNOWN",
                                                              execution_status="UNKNOWN"))
        self._age_order_column(old["order_id"], yesterday)
        qmt.history_exception = OrderError(502, "HISTORY_UNAVAILABLE", "synthetic coverage gap")
        self.runtime.handle("submit_order", self.single(client="new"), "POST")
        self._reconcile_now("old")
        self._until_call(qmt, "query_history_order")
        self._until_call(qmt, "query_history_deal")
        self._until(lambda: self.runtime.background.round is None)
        self._until_order("old", lambda row: row["sync_status"] == "INCOMPLETE")
        self._until_call(qmt, "passorder")
        self._until_order("new", lambda row: row["submission_status"] == "SUBMITTING")
        self.assertFalse(self.runtime.history_coverage_complete)
        self.assertTrue(self.runtime.recovery_complete)
        self.assertEqual(self.runtime.repo.get_order("old")["sync_status"], "INCOMPLETE")
        self.assertEqual(self.runtime.repo.get_order("new")["submission_status"], "SUBMITTING")
        self.assertEqual(len(qmt.side_effects("passorder")), 1)

    def test_terminal_order_still_gets_historical_reconciliation(self):
        qmt = self.runtime.fake_qmt
        self.runtime.handle("submit_order", self.single(), "POST")
        self._until_call(qmt, "passorder")
        self._until_order("single-one", lambda row: row["submission_status"] == "SUBMITTING")
        doc = self.runtime.repo.get_order("single-one")
        yesterday = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)).isoformat()
        self.runtime.repo.update_order(doc["order_id"], "TEST_HISTORICAL_TERMINAL",
                                       lambda row: row.update(created_at=yesterday))
        self._age_order_column(doc["order_id"], yesterday)
        trading_day = (dt.datetime.now(dt.timezone(dt.timedelta(hours=8))) -
                       dt.timedelta(days=2)).strftime("%Y%m%d")
        self.runtime.observe("order", {"remark": doc["remark"], "qmt_order_id": "filled-historical",
                                       "trading_day": trading_day, "market": "SH", "symbol": "600000.SH",
                                       "side": "BUY", "quantity": 1000, "filled_quantity": 1000, "status": 56})
        self.runtime.observe("deal", {"qmt_order_id": "filled-historical", "trade_id": "hist-fill",
                                      "trading_day": trading_day, "market": "SH", "symbol": "600000.SH",
                                      "side": "BUY", "quantity": 1000, "amount": "10500"})
        self._until_order("single-one", lambda row: row["filled_quantity"] == 1000)
        self._reconcile_now("single-one")
        self._until_order("single-one", lambda row: row["execution_status"] == "FILLED")
        self.assertEqual(self.runtime.repo.get_order("single-one")["execution_status"], "FILLED")
        self._until_call(qmt, "query_history_order")
        self._until_call(qmt, "query_history_deal")
        self.assertGreaterEqual(len(qmt.side_effects("query_history_order")), 1)
        self.assertGreaterEqual(len(qmt.side_effects("query_history_deal")), 1)

    def test_native_basket_direct_sliced_smart_each_uses_own_qmt_path(self):
        qmt = self.runtime.fake_qmt
        today = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date().isoformat()
        executions = [
            ("direct", {"type": "DIRECT"}, "passorder"),
            ("sliced", {"type": "SLICED", "mode": "ALGO", "params": self.sliced_params()}, "algo_passorder"),
            ("smart", {"type": "SMART", "algorithm": "VWAP",
                       "start_at": today + "T00:00:00+08:00", "end_at": today + "T23:59:00+08:00",
                       "params": {"m_dLimitOverRate": 0.1}}, "smart_algo_passorder"),
        ]
        for client, execution, qmt_name in executions:
            self._until(lambda: self.runtime.health()["accepting_orders"])
            self.runtime.handle("submit_order", self.basket(client, execution), "POST")
            self._until_call(qmt, qmt_name)
            self._until_order(client, lambda row: row["submission_status"] == "SUBMITTING" and
                              row["basket_state"] == "VERIFIED")
            doc = self.runtime.repo.get_order(client)
            self.assertEqual(doc["submission_status"], "SUBMITTING", client)
            self.assertEqual(doc["basket_state"], "VERIFIED", client)
            self.assertIn(doc["basket_name"], qmt.baskets)
            self.assertEqual(len(qmt.side_effects(qmt_name)), 1)
            self.assertEqual(qmt.baskets[doc["basket_name"]]["stocks"][1]["optType"], 24)
        self.assertEqual(len(qmt.baskets), 3)

    def test_restart_verifies_existing_basket_after_unpersisted_set_result(self):
        original = self.runtime
        request = self.basket("restart-basket", {"type": "DIRECT"})
        original.handle("submit_order", request, "POST")
        doc = original.repo.get_order("restart-basket")
        resolved = original.adapter.resolve(doc["request"], doc["remark"])
        original.repo.update_order(doc["order_id"], "TEST_SET_RESULT_NOT_PERSISTED",
                                   lambda row: row.update(resolved_request=resolved,
                                                          preparation_stage="BASKET_SET",
                                                          preparation_complete=False))
        prepared = original.repo.get_order("restart-basket")
        expected = original.adapter._expected_basket(prepared)
        self.assertEqual(prepared["submission_status"], "QUEUED")
        self.assertEqual(prepared["attempts"], [])
        original.stop()
        self._until_stopped(original)

        qmt = _FakeQmt()
        qmt.baskets[expected["name"]] = expected  # QMT accepted set_basket before the prior result was saved.
        restarted = self._new_runtime(qmt)
        restarted.initialize()
        self._until(lambda: restarted.recovery_complete, restarted)
        self._until_call(qmt, "passorder", runtime=restarted)
        self._until_order("restart-basket", lambda row: row["basket_state"] == "VERIFIED" and
                          row["submission_status"] == "SUBMITTING", restarted)
        current = restarted.repo.get_order("restart-basket")
        self.assertEqual(current["resolved_request"], resolved)
        self.assertGreaterEqual(len(qmt.side_effects("get_basket")), 1)
        self.assertEqual(qmt.side_effects("set_basket"), [])
        self.assertEqual(len(qmt.side_effects("passorder")), 1)
        self.assertEqual(len(current["attempts"]), 1)

    def test_algorithm_cancel_stops_task_then_catches_late_child_order(self):
        qmt = self.runtime.fake_qmt
        request = self.single()
        request["execution"] = {"type": "SLICED", "mode": "ALGO", "params": self.sliced_params()}
        self.runtime.handle("submit_order", request, "POST")
        self._until_call(qmt, "algo_passorder")
        self._until_order("single-one", lambda row: row["submission_status"] == "SUBMITTING")
        doc = self.runtime.repo.get_order("single-one")
        task = {"remark": doc["remark"], "qmt_task_id": "task-1", "trading_day": "20260926",
                "market": "SH", "symbol": "600000.SH", "side": "BUY", "status": 3}
        self.runtime.observe("task", task)
        self._until_order("single-one", lambda row: any(
            item.get("qmt_task_id") == "task-1" for item in row.get("qmt_tasks", [])))
        self.runtime.handle("cancel_order", self.cancel(), "POST")
        self._until_call(qmt, "cancel_task")
        self.assertEqual(len(qmt.side_effects("cancel_task")), 1)
        self.assertEqual(qmt.side_effects("cancel"), [])
        self.runtime.observe("task", dict(task, status=8))
        self.runtime.observe("order", {"remark": doc["remark"], "qmt_task_id": "task-1",
                                       "qmt_order_id": "late-child", "trading_day": "20260926",
                                       "market": "SH", "symbol": "600000.SH", "side": "BUY",
                                       "quantity": 500, "filled_quantity": 0, "status": 50})
        self._until_call(qmt, "cancel")
        self.assertEqual(len(qmt.side_effects("cancel")), 1)
        self.assertEqual(qmt.side_effects("cancel")[0][1][0], "late-child")

    def test_stop_does_not_cancel_live_qmt_order(self):
        qmt = self.runtime.fake_qmt
        self.runtime.handle("submit_order", self.single(), "POST")
        self._until_call(qmt, "passorder")
        self.runtime.stop()
        self.assertEqual(qmt.side_effects("cancel"), [])
        self.assertEqual(qmt.side_effects("cancel_task"), [])

    def test_real_http_routes_use_durable_runtime(self):
        # 使用实际部署的 GBK 单文件，发现打包后导入别名、顶层名称冲突。
        strategy = Path(__file__).resolve().parents[1] / "strategies" / "http_order.py"
        spec = importlib.util.spec_from_file_location("qmt_order_generated_runtime_test", strategy)
        http_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(http_module)

        self.runtime.stop()
        self._until_stopped(self.runtime)
        repo = http_module.PostgresRepository(self.config, self.account_id)
        self.repositories.append(repo)
        generated_runtime = http_module.OrderRuntime(self.runtime.fake_qmt.apis(), object(),
                                                     self.account_id, pg_config=self.config,
                                                     repository=repo, local_lock=_LocalLock())
        self.runtimes.append(generated_runtime)
        generated_runtime.initialize()
        self._until(lambda: generated_runtime.recovery_complete, generated_runtime)
        log_patch = patch.object(http_module, "log_message")
        log_patch.start()
        state = http_module.OrderState()
        state.runtime = generated_runtime
        server = http_module.ThreadingHTTPServer(("127.0.0.1", 0), http_module.OrderRequestHandler)
        server.timeout = 0.1
        server.order_state = state
        state.server = server
        thread = threading.Thread(target=http_module.serve_http, args=(state,))
        thread.start()
        address = "http://127.0.0.1:" + str(server.server_address[1])

        def request(method, path, body=None):
            data = json.dumps(body).encode("utf-8") if body is not None else None
            req = urllib.request.Request(address + path, data=data, method=method)
            if data is not None:
                req.add_header("Content-Type", "application/json")
            try:
                with urllib.request.urlopen(req, timeout=3) as response:
                    return response.status, json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as error:
                try:
                    return error.code, json.loads(error.read().decode("utf-8"))
                finally:
                    error.close()

        try:
            status, accepted = request("POST", "/submit_order", self.single())
            self.assertEqual(status, 202)
            status, replayed = request("POST", "/submit_order", self.single())
            self.assertEqual(status, 200)
            self.assertEqual(accepted["order_id"], replayed["order_id"])
            self.assertTrue(replayed["replayed"])
            status, fetched = request("GET", "/order?client_order_id=single-one")
            self.assertEqual(status, 200)
            self.assertEqual(fetched["client_order_id"], "single-one")
            self.assertEqual(request("GET", "/submit_order")[0], 405)
            self.assertEqual(request("GET", "/orders")[0], 200)
            self.assertEqual(request("GET", "/order_events?after=0")[0], 200)
            self.assertEqual(request("GET", "/capabilities")[0], 200)
            self.assertEqual(request("GET", "/health")[0], 200)
            self.assertEqual(request("POST", "/cancel_order", self.cancel())[0], 200)
        finally:
            state.stop_event.set()
            state.http_closed.set()
            thread.join(timeout=3)
            server.server_close()
            log_patch.stop()
        self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
