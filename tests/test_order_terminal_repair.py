# -*- coding: utf-8 -*-
# Last modified (Asia/Shanghai): 2026-09-28
"""终态修复工具：纯本地事务替身，不连接 PostgreSQL/QMT。"""
import copy
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from service.ipc_bridge.ipc_protocol.order_common import OrderError, new_order_document
from service.ipc_bridge.order.state import recompute_order, state_reconciliation_facts


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("order_terminal_repair", str(ROOT / "tools" / "order_terminal_repair.py"))
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)


def snapshots(status="REJECTED", cancel_attempt=None, internal_checkpoint=False,
              cancel_kind="CANCEL_ORDER"):
    request = {"client_order_id": "client-1", "account_id": "account-1", "order_type": "SINGLE",
               "sizing_type": "QUANTITY", "symbol": "511880.SH", "side": "BUY", "quantity": 100,
               "execution": {"type": "DIRECT"}, "price_type": "LIMIT", "limit_price": "10"}
    terminal = new_order_document(request)
    terminal.update(order_id="order-1", remark="qbtest", submission_status="CONFIRMED",
                    reconciliation_complete=True, sync_status="COMPLETE", version=10, fact_version=4,
                    last_reconciled_at="2026-09-26T14:54:00+00:00")
    terminal["attempts"] = [{"attempt_id": "submit-1", "kind": "SUBMIT", "status": "RETURNED"}]
    terminal["qmt_tasks"] = [{"qmt_task_id": "2", "status": "STOPPED", "terminal": True}]
    quantity = {"FILLED": 100, "PARTIALLY_CANCELLED": 40}.get(status, 0)
    terminal["qmt_orders"] = [{"qmt_order_id": "xt1090519043", "item_id": "single",
                               "status": status, "terminal": True, "trading_day": "20260926",
                               "market": "SH", "quantity": 100, "filled_quantity": quantity}]
    if quantity:
        terminal["fills"] = [{"trade_id": "trade-1", "qmt_order_id": "xt1090519043",
                              "fill_key": ["20260926", "SH", "trade-1"],
                              "item_id": "single", "trading_day": "20260926", "market": "SH",
                              "quantity": quantity, "amount": str(quantity * 10)}]
    if cancel_attempt:
        terminal.update(cancel_requested=True, active_cancel_request_id="cancel-1")
        terminal["cancel_requests"] = [{"cancel_request_id": "cancel-1",
                                       "canonical_cancel_request_id": "cancel-1"}]
        target = "2" if cancel_kind == "CANCEL_TASK" else "xt1090519043"
        terminal["attempts"].append({"attempt_id": "cancel-attempt-1", "kind": cancel_kind,
                                     "status": cancel_attempt, "cancel_request_id": "cancel-1",
                                     "target_id": target})
    recompute_order(terminal)
    if internal_checkpoint:
        terminal["terminal_reconciliation_checkpoint"] = {
            "facts_fingerprint": state_reconciliation_facts(terminal),
            "reconciled_at": terminal["last_reconciled_at"]}
    gap = copy.deepcopy(terminal)
    gap.update(version=11, fact_version=5, reconcile_requested=True, reconcile_pending=True,
               last_reconcile_gap_since="2026-09-28T00:00:00+00:00")
    current = copy.deepcopy(gap)
    current.update(version=12, reconciliation_complete=False, sync_status="INCOMPLETE",
                   execution_status="INCOMPLETE")
    current["items"][0]["execution_status"] = "INCOMPLETE"
    if cancel_attempt:
        current["cancel_status"] = "PENDING"
        current["cancel_requests"][0]["status"] = "PENDING"
    events = [(5, "RECONCILE_STATE_CHANGED", "2026-09-26T14:54:00+00:00", terminal),
              (6, "RECONCILE_GAP", "2026-09-28T00:00:00+00:00", gap),
              (7, "RECONCILE_STATE_CHANGED", "2026-09-28T00:00:01+00:00", current)]
    return copy.deepcopy(current), events


class Cursor:
    def __init__(self, repo):
        self.repo = repo
        self.rows = []
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if ".order_events WHERE" in sql:
            self.rows = [row for row in self.repo.events if row[0] >= params[-1]]
        elif ".qmt_observations WHERE" in sql:
            self.rows = list(self.repo.observations)
        elif sql != "SET TRANSACTION READ ONLY":
            raise AssertionError("unexpected SQL: " + sql)

    def fetchall(self):
        return self.rows


class Repo:
    account_type = "STOCK"
    account_id = "account-1"
    repo_scope = ("STOCK", "account-1")
    repo_s = '"qmt_order"'

    def __init__(self, current, events):
        self.current = current
        self.events = events
        self.observations = []
        self.saved = []
        self.mutations = []
        self.locks = []
        self.cursor = None

    def repo_run(self, callback, mutation=False):
        self.cursor = Cursor(self)
        self.mutations.append(mutation)
        return callback(self.cursor)

    def repo_load(self, cursor, order_id, lock=False):
        self.locks.append(lock)
        return copy.deepcopy(self.current) if order_id == self.current["order_id"] else None

    def repo_require(self, doc):
        if doc is None:
            raise OrderError(404, "ORDER_NOT_FOUND", "missing")
        return doc

    def repo_save(self, cursor, doc, event_type, child_fields=None):
        doc["version"] += 1
        doc["reconcile_pending"] = False
        self.saved.append((copy.deepcopy(doc), event_type, child_fields))
        self.current = copy.deepcopy(doc)


class TerminalRepairTests(unittest.TestCase):
    def setUp(self):
        current, events = snapshots()
        self.repo = Repo(current, events)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "receipt.json"
        self.args = SimpleNamespace(order_id="order-1", expected_version=12,
                                    checkpoint_event_seq=5, reason="gap reopened a terminal rejection",
                                    evidence_out=self.path, apply=False)

    def test_dry_run_is_read_only_and_proves_checkpoint(self):
        result = tool.repair(self.repo, self.args)
        self.assertEqual(result["proof"]["intervening_event_seqs"], [6, 7])
        self.assertEqual(self.repo.mutations, [False])
        self.assertEqual(self.repo.locks, [False])
        self.assertIn("SET TRANSACTION READ ONLY", self.repo.cursor.statements)
        self.assertFalse(self.path.exists())
        self.assertEqual(self.repo.saved, [])

    def test_apply_writes_receipt_then_audit_and_rejects_repeat(self):
        self.args.apply = True
        result = tool.repair(self.repo, self.args)
        self.assertEqual(result["execution_status"], "REJECTED")
        self.assertTrue(self.path.exists())
        self.assertEqual(self.repo.mutations, [True])
        self.assertEqual(self.repo.locks, [True])
        self.assertEqual(self.repo.saved[0][1:], ("MANUAL_TERMINAL_REPAIR", ("items",)))
        after = self.repo.saved[0][0]
        self.assertEqual(after["last_reconciled_at"], self.repo.events[0][3]["last_reconciled_at"])
        self.assertEqual(after["fact_version"], 6)
        self.assertFalse(after["reconcile_pending"])
        self.assertEqual(after["terminal_repairs"][0]["evidence_sha256"], result["evidence_sha256"])
        self.assertEqual(after["terminal_repairs"][0]["before"]["execution_status"], "INCOMPLETE")
        self.assertEqual(after["terminal_repairs"][0]["after"]["execution_status"], "REJECTED")
        with self.assertRaisesRegex(OrderError, "version changed"):
            tool.repair(self.repo, self.args)
        self.assertEqual(len(self.repo.saved), 1)

    def test_all_four_terminal_states_restore_original_closure_and_facts(self):
        for status in ("FILLED", "CANCELLED", "PARTIALLY_CANCELLED", "REJECTED"):
            with self.subTest(status=status):
                self.repo = Repo(*snapshots(status))
                self.args.apply = True
                self.args.evidence_out = Path(self.temp.name) / (status + ".json")
                before_facts = tool.business_facts(self.repo.current)
                result = tool.repair(self.repo, self.args)
                self.assertEqual(result["execution_status"], status)
                after = self.repo.current
                self.assertEqual(tool.business_facts(after), before_facts | {
                    "terminal_repairs": after["terminal_repairs"]})
                self.assertEqual(after["last_reconciled_at"], self.repo.events[0][3]["last_reconciled_at"])
                self.assertFalse(after["reconcile_pending"])
                self.assertEqual(after["terminal_reconciliation_checkpoint"], {
                    "facts_fingerprint": state_reconciliation_facts(after),
                    "reconciled_at": self.repo.events[0][3]["last_reconciled_at"]})

    def test_previous_closed_cancel_returned_or_unknown_is_proved_by_terminal_target(self):
        for status in ("FILLED", "CANCELLED", "PARTIALLY_CANCELLED", "REJECTED"):
            for call_status in ("RETURNED", "UNKNOWN"):
                with self.subTest(status=status, call_status=call_status):
                    self.repo = Repo(*snapshots(status, cancel_attempt=call_status))
                    tool.repair(self.repo, self.args)
                    self.assertEqual(self.repo.saved, [])
                    self.args.apply = True
                    self.args.evidence_out = Path(self.temp.name) / (status + call_status + ".json")
                    result = tool.repair(self.repo, self.args)
                    self.assertEqual(result["execution_status"], status)
                    self.assertEqual(self.repo.current["attempts"][-1]["status"], call_status)
                    self.assertEqual(self.repo.current["cancel_status"],
                                     "NOT_NEEDED" if status == "FILLED" else "CONFIRMED")
                    self.args.apply = False

    def test_unresolved_cancel_and_invalid_target_checkpoint_refused(self):
        for bad in ("CALLING", "target", "request", "live", "pending"):
            with self.subTest(bad=bad):
                self.repo = Repo(*snapshots("CANCELLED", cancel_attempt="UNKNOWN"))
                checkpoint = self.repo.events[0][3]
                if bad == "CALLING":
                    checkpoint["attempts"][-1]["status"] = "CALLING"
                elif bad == "target":
                    checkpoint["attempts"][-1]["target_id"] = "missing"
                elif bad == "request":
                    checkpoint["attempts"][-1]["cancel_request_id"] = "missing"
                elif bad == "live":
                    checkpoint["qmt_orders"][0]["terminal"] = False
                else:
                    checkpoint["cancel_requests"][0]["status"] = "PENDING"
                with self.assertRaisesRegex(OrderError, "checkpoint is not a complete"):
                    tool.repair(self.repo, self.args)

    def test_cancel_task_requires_closed_task_and_direct_order_needs_no_task(self):
        self.repo = Repo(*snapshots("PARTIALLY_CANCELLED", cancel_attempt="UNKNOWN",
                                   cancel_kind="CANCEL_TASK"))
        tool.repair(self.repo, self.args)
        self.repo.events[0][3]["qmt_tasks"][0]["terminal"] = False
        with self.assertRaisesRegex(OrderError, "checkpoint is not a complete"):
            tool.repair(self.repo, self.args)
        self.repo = Repo(*snapshots("FILLED"))
        for doc in [self.repo.current] + [event[3] for event in self.repo.events]:
            doc["qmt_tasks"] = []
        tool.repair(self.repo, self.args)

    def test_existing_receipt_is_never_overwritten_or_saved(self):
        self.path.write_bytes(b"original evidence")
        self.args.apply = True
        with self.assertRaises(FileExistsError):
            tool.repair(self.repo, self.args)
        self.assertEqual(self.path.read_bytes(), b"original evidence")
        self.assertEqual(self.repo.saved, [])

    def test_new_cancel_unknown_or_changed_call_outcome_cannot_use_old_closure(self):
        self.repo = Repo(*snapshots("CANCELLED", cancel_attempt="RETURNED"))
        self.repo.current["attempts"][-1]["status"] = "UNKNOWN"
        with self.assertRaisesRegex(OrderError, "business facts differ"):
            tool.repair(self.repo, self.args)
        self.repo = Repo(*snapshots("CANCELLED"))
        self.repo.current["attempts"].append({"kind": "CANCEL_ORDER", "status": "UNKNOWN",
                                              "target_id": "xt1090519043"})
        with self.assertRaisesRegex(OrderError, "business facts differ"):
            tool.repair(self.repo, self.args)

    def test_never_complete_or_missing_fill_evidence_cannot_be_repaired(self):
        for bad in ("incomplete", "fill_gap", "inconsistent_status", "unassociated"):
            with self.subTest(bad=bad):
                self.repo = Repo(*snapshots("FILLED"))
                checkpoint = self.repo.events[0][3]
                if bad == "incomplete":
                    checkpoint["reconciliation_complete"] = False
                elif bad == "fill_gap":
                    checkpoint["fills"] = []
                elif bad == "inconsistent_status":
                    checkpoint["execution_status"] = "CANCELLED"
                else:
                    checkpoint["unassociated_evidence"] = [{"reason": "conflict"}]
                with self.assertRaisesRegex(OrderError, "checkpoint is not a complete"):
                    tool.repair(self.repo, self.args)

    def test_internal_checkpoint_checked_and_legacy_event_remains_supported(self):
        self.repo = Repo(*snapshots("FILLED", internal_checkpoint=True))
        tool.repair(self.repo, self.args)
        for key in ("facts_fingerprint", "reconciled_at"):
            with self.subTest(key=key):
                self.repo = Repo(*snapshots("FILLED", internal_checkpoint=True))
                self.repo.current["terminal_reconciliation_checkpoint"][key] = "injected"
                with self.assertRaisesRegex(OrderError, "internal terminal checkpoint"):
                    tool.repair(self.repo, self.args)
        self.repo = Repo(*snapshots("FILLED", internal_checkpoint=True))
        self.repo.events[0][3]["reconciliation_complete"] = False
        with self.assertRaisesRegex(OrderError, "checkpoint is not a complete"):
            tool.repair(self.repo, self.args)

    def test_late_fill_and_ambiguous_unassociated_observation_refused(self):
        for kind, raw in (("deal", {"account_id": "account-1", "remark": "qbtest",
                                    "symbol": "511880.SH", "side": "BUY", "trade_id": "late-fill",
                                    "qmt_order_id": "xt1090519043", "trading_day": "20260926",
                                    "market": "SH", "quantity": 10, "amount": "100"}),
                          ("order", {"account_id": "account-1", "symbol": "511880.SH",
                                     "side": "BUY", "qmt_order_id": "foreign-order"})):
            with self.subTest(kind=kind):
                self.repo = Repo(*snapshots("PARTIALLY_CANCELLED"))
                self.repo.observations = [(99, kind, raw, "callback", "2026-09-28T00:00:02+00:00",
                                           False, None)]
                with self.assertRaises((OrderError, ValueError)):
                    tool.repair(self.repo, self.args)

    def test_missing_or_unsuccessful_checkpoint_refused(self):
        self.repo.events = self.repo.events[1:]
        with self.assertRaisesRegex(OrderError, "checkpoint event is missing"):
            tool.repair(self.repo, self.args)
        self.repo.events = snapshots()[1]
        self.repo.events[0] = (5, "QMT_OBSERVATION", self.repo.events[0][2], self.repo.events[0][3])
        with self.assertRaisesRegex(OrderError, "checkpoint is not"):
            tool.repair(self.repo, self.args)

    def test_new_business_fact_and_unresolved_observation_refused(self):
        self.repo.current["qmt_orders"][0]["quantity"] = 200
        with self.assertRaisesRegex(OrderError, "business facts differ"):
            tool.repair(self.repo, self.args)
        self.repo.current = copy.deepcopy(self.repo.events[2][3])
        raw = {"account_id": "account-1", "remark": "qbtest", "symbol": "511880.SH",
               "side": "BUY", "qmt_order_id": "new-order", "trading_day": "20260928",
               "market": "SH", "status": "WORKING", "quantity": 100}
        self.repo.observations = [(99, "order", raw, "query", "2026-09-28T00:00:02+00:00", True, "order-1")]
        with self.assertRaisesRegex(OrderError, "newer observation 99"):
            tool.repair(self.repo, self.args)

    def test_version_race_and_unexpected_intervening_event_refused(self):
        self.repo.current["version"] = 13
        with self.assertRaisesRegex(OrderError, "version changed"):
            tool.repair(self.repo, self.args)
        self.repo.current["version"] = 12
        self.repo.events[2] = (7, "QMT_OBSERVATION", self.repo.events[2][2], self.repo.events[2][3])
        with self.assertRaisesRegex(OrderError, "unexpected intervening event"):
            tool.repair(self.repo, self.args)


if __name__ == "__main__":
    unittest.main()
