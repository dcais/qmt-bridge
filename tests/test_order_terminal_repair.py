# -*- coding: utf-8 -*-
# Last modified (Asia/Shanghai): 2026-10-14
"""终态修复工具：纯本地事务替身，不连接 PostgreSQL/QMT。"""
import copy
import importlib.util
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from service.ipc_bridge.ipc_protocol.order_common import OrderError, new_order_document
from service.ipc_bridge.order.state import (recompute_order, state_reconciliation_facts,
    mark_reconciled, is_order_active, apply_observation)


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
        elif ".orders WHERE" in sql:
            self.rows = ([(doc["order_id"], doc) for doc in self.repo.account_orders]
                         if sql.startswith("SELECT order_id,document") else
                         [(doc,) for doc in self.repo.account_orders])
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
        self.account_orders = [current]
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
        self.account_orders = [copy.deepcopy(doc) if old["order_id"] == doc["order_id"] else old
                               for old in self.account_orders]


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


def equivalent_snapshots():
    _, old_events = snapshots("FILLED")
    checkpoint = old_events[0][3]
    checkpoint.update(unassociated_evidence=[], resolved_evidence=[])
    raw = {"account_id": "account-1", "remark": "qbtest", "qmt_order_id": "xt1090519043",
           "qmt_task_id": "2", "trading_day": "20260926", "market": "SH",
           "symbol": "511880.SH", "side": "BUY", "quantity": 100, "filled_quantity": 100,
           "status": "FILLED", "m_dTradedPrice": 10, "m_dTradeAmount": 1000,
           "m_dPrice": 10, "m_dCommission": 0.21, "m_dComssion": 0.21,
           "m_nRef": 1234, "m_strOrderRef": "native-1234"}
    row = checkpoint["qmt_orders"][0]
    row.update({key: value for key, value in tool.observation_identifiers("order", raw).items()})
    row.update(raw=copy.deepcopy(raw), source="callback", native_ref="1234",
               native_order_ref="native-1234", observed_at="2026-09-26T14:53:00+00:00")
    fill = checkpoint["fills"][0]
    fill.update(account_id="account-1", remark="qbtest", symbol="511880.SH", side="BUY",
                qmt_task_id="2", raw={**raw, "trade_id": "trade-1", "amount": "1000"})
    checkpoint["fills"][0]["amount"] = "1000"
    mark_reconciled(checkpoint, now=checkpoint["last_reconciled_at"])
    events = [(5, "RECONCILE_STATE_CHANGED", "2026-09-26T14:54:00+00:00", checkpoint)]
    observations = []
    previous = checkpoint
    for seq, kind, reason, query in (
        (6, "deal", "CONFLICTING_FILL_IDENTITY", {**fill["raw"], "amount": "999.999"}),
        (7, "order", "CONFLICTING_ORDER_REFERENCE", {**raw, "m_nRef": 0,
                                                   "m_strOrderRef": "SH_xt1090519043"})):
        stamp = "2026-09-28T00:00:0{}+00:00".format(seq)
        evidence = {"kind": kind, "raw": query, "source": "query", "reason": reason,
                    "observed_at": stamp,
                    "evidence_id": tool.fingerprint({"kind": kind, "raw": query, "reason": reason})}
        doc = copy.deepcopy(previous)
        doc["unassociated_evidence"].append(evidence)
        doc.update(version=previous["version"] + 1, reconciliation_complete=False,
                   reconcile_requested=True, reconcile_priority=True)
        recompute_order(doc)
        events.append((seq, "QMT_OBSERVATION", stamp, doc))
        observations.append((seq + 100, kind, query, "query", stamp, True, "order-1"))
        previous = doc
    gap = copy.deepcopy(previous)
    gap.update(version=previous["version"] + 1, fact_version=previous["fact_version"] + 1)
    events.append((8, "RECONCILE_GAP", "2026-09-28T00:00:08+00:00", gap))
    current = copy.deepcopy(gap)
    current.update(version=gap["version"] + 2, last_reconciled_at="2026-09-28T23:59:00+00:00")
    return current, events, observations


class EquivalentObservationRepairTests(unittest.TestCase):
    def setUp(self):
        current, events, observations = equivalent_snapshots()
        self.repo = Repo(current, events)
        self.repo.observations = observations
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "receipt.json"
        self.args = SimpleNamespace(order_id="order-1", expected_version=current["version"],
            checkpoint_event_seq=5, reason="audited equivalent query conflicts", evidence_out=self.path,
            apply=False, resolve_equivalent_observations=True)

    def test_explicit_mode_dry_run_proves_each_event_observation_and_candidate(self):
        before = copy.deepcopy(self.repo.current)
        result = tool.repair(self.repo, self.args)
        self.assertEqual(result["proof"]["checked_observation_ids"], [106, 107])
        self.assertEqual(len(result["proof"]["evidence_proofs"]), 2)
        self.assertEqual(result["expected_state"]["execution_status"], "FILLED")
        self.assertFalse(result["expected_active"])
        self.assertFalse(result["expected_state"]["reconcile_pending"])
        self.assertEqual(result["expected_state"]["last_reconciled_at"],
                         self.repo.events[0][3]["last_reconciled_at"])
        self.assertEqual(self.repo.current, before)
        self.assertEqual(self.repo.saved, [])
        self.assertFalse(self.path.exists())
        self.assertIn("SET TRANSACTION READ ONLY", self.repo.cursor.statements)

    def test_apply_preserves_native_facts_and_receipt_and_restart_stays_closed(self):
        before = copy.deepcopy(self.repo.current)
        self.args.apply = True
        result = tool.repair(self.repo, self.args)
        after = self.repo.current
        self.assertEqual(after["qmt_orders"], before["qmt_orders"])
        self.assertEqual(after["fills"], before["fills"])
        self.assertEqual(after["attempts"], before["attempts"])
        self.assertEqual(after["fact_version"], before["fact_version"] + 1)
        self.assertEqual(after["unassociated_evidence"], [])
        for original, resolved in zip(before["unassociated_evidence"], after["resolved_evidence"]):
            self.assertEqual({key: resolved[key] for key in original}, original)
            self.assertEqual(resolved["resolution"]["evidence_sha256"], result["evidence_sha256"])
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), result["evidence_sha256"])
        receipt = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(receipt["before"], before)
        self.assertEqual(self.repo.saved[0][1:], ("MANUAL_TERMINAL_REPAIR", ("items", "cancel_requests")))
        self.assertTrue(any(sql.endswith("FOR UPDATE") and ".orders WHERE" in sql
                            for sql in self.repo.cursor.statements))
        self.assertFalse(is_order_active(after))
        startup = tool.PostgresRepository.mark_reconcile_gap(self.repo, "2026-09-30T00:00:00+00:00", startup=True)
        self.assertEqual(startup["marked"], 0)
        self.assertEqual(len(self.repo.saved), 1)
        self.assertEqual(after["terminal_reconciliation_checkpoint"]["facts_fingerprint"],
                         state_reconciliation_facts(after))
        for _, kind, raw, source, stamp, _, _ in self.repo.observations:
            self.assertFalse(apply_observation(after, kind, raw, source, observed_at=stamp))
        self.assertFalse(is_order_active(after))
        with self.assertRaisesRegex(OrderError, "version changed"):
            tool.repair(self.repo, self.args)

    def test_default_strict_mode_still_refuses_equivalent_conflicts(self):
        self.args.resolve_equivalent_observations = False
        with self.assertRaises(OrderError):
            tool.repair(self.repo, self.args)

    def test_new_fact_error_submission_and_cancel_changes_refused(self):
        for bad in ("fills", "orders", "quantity", "error", "attempt", "cancel"):
            with self.subTest(bad=bad):
                self.setUp()
                if bad == "fills":
                    self.repo.current["fills"].append({**self.repo.current["fills"][0], "trade_id": "late"})
                elif bad == "orders":
                    self.repo.current["qmt_orders"][0]["status"] = "CANCELLED"
                elif bad == "quantity":
                    self.repo.current["items"][0]["requested_quantity"] = 101
                elif bad == "error":
                    self.repo.current["error"] = {"raw": {"error": "unknown"}}
                elif bad == "attempt":
                    self.repo.current["attempts"][0]["status"] = "UNKNOWN"
                else:
                    self.repo.current["cancel_requested"] = True
                with self.assertRaisesRegex(OrderError, "current differs"):
                    tool.repair(self.repo, self.args)

    def test_event_mutation_or_unexpected_event_or_fact_generation_refused(self):
        for bad in ("fact", "type", "fill", "evidence", "checkpoint"):
            with self.subTest(bad=bad):
                self.setUp()
                seq, kind, stamp, doc = self.repo.events[2]
                if bad == "fact":
                    doc["fact_version"] += 1
                elif bad == "type":
                    self.repo.events[2] = (seq, "SUBMIT_RETURNED", stamp, doc)
                elif bad == "fill":
                    doc["fills"][0]["amount"] = "999"
                elif bad == "evidence":
                    doc["unassociated_evidence"][0]["observed_at"] = "tampered"
                else:
                    doc["terminal_reconciliation_checkpoint"]["reconciled_at"] = "tampered"
                with self.assertRaises(OrderError):
                    tool.repair(self.repo, self.args)

    def test_missing_duplicate_or_changed_raw_observation_refused(self):
        for bad in ("missing", "duplicate", "unapplied", "raw", "owner", "source"):
            with self.subTest(bad=bad):
                self.setUp()
                row = list(self.repo.observations[0])
                if bad == "missing":
                    self.repo.observations.pop(0)
                elif bad == "duplicate":
                    self.repo.observations.append(tuple(row))
                else:
                    if bad == "unapplied":
                        row[5] = False
                    elif bad == "raw":
                        row[2] = {**row[2], "amount": "998"}
                    elif bad == "owner":
                        row[6] = "foreign"
                    else:
                        row[3] = "callback"
                    self.repo.observations[0] = tuple(row)
                with self.assertRaisesRegex(OrderError, "unique applied raw observation"):
                    tool.repair(self.repo, self.args)

    def test_cross_order_identity_collision_and_missing_internal_fields_refused(self):
        other = copy.deepcopy(self.repo.current)
        other["order_id"] = "foreign"
        self.repo.account_orders.append(other)
        with self.assertRaisesRegex(OrderError, "identity collision"):
            tool.repair(self.repo, self.args)
        self.repo.account_orders.pop()
        del self.repo.events[0][3]["attempts"]
        with self.assertRaisesRegex(OrderError, "complete internal"):
            tool.repair(self.repo, self.args)

    def test_pending_late_unknown_callback_or_ambiguous_observation_refused(self):
        for kind, raw, source, owner in (
            ("deal", {"trade_id": "late", "amount": "10", "quantity": 1}, "query", "order-1"),
            ("error", {"error": "unknown"}, "query", "order-1"),
            ("order", {}, "callback", "order-1"),
            ("order", {"qmt_order_id": "other", "remark": None}, "query", None)):
            with self.subTest(kind=kind, source=source):
                self.setUp()
                query = {**self.repo.observations[1][2], **raw}
                self.repo.observations.append((999, kind, query, source,
                    "2026-09-29T00:00:00+00:00", False, owner))
                with self.assertRaises((OrderError, ValueError)):
                    tool.repair(self.repo, self.args)

    def test_every_query_raw_economics_checked_even_when_apply_is_noop(self):
        for kind, changes in (
            ("order", {"m_dTradeAmount": 9999}),
            ("order", {"m_dTradedPrice": 99.99}),
            ("order", {"quantity": 101}),
            ("deal", {"m_dPrice": 99.99}),
            ("deal", {"m_dCommission": 99.99}),
            ("deal", {"m_strOrderRef": "OTHER_NATIVE_REF"}),
            ("deal", {"amount": "999.9989"}),
            ("deal", {"unknown_fact": "new fact"})):
            with self.subTest(kind=kind, changes=changes):
                self.setUp()
                checkpoint = self.repo.events[0][3]
                baseline = checkpoint["qmt_orders" if kind == "order" else "fills"][0]["raw"]
                raw = {**baseline, **changes}
                self.repo.observations.append((999, kind, raw, "query",
                    "2026-09-29T00:00:00+00:00", True, "order-1"))
                with self.assertRaises(OrderError):
                    tool.repair(self.repo, self.args)

    def test_missing_query_ref_with_same_native_order_ref_and_amount_boundary_accepted(self):
        raw = copy.deepcopy(self.repo.events[0][3]["qmt_orders"][0]["raw"])
        raw.update(m_nRef=0, m_dTradeAmount="1000.001", m_nOrderStrategyType=777,
                   m_strInsertTime="145959")
        # This query has no alias conflict, but still needs the same full proof.
        self.repo.observations.append((999, "order", raw, "query",
            "2026-09-29T00:00:00+00:00", True, "order-1"))
        self.assertIn(999, tool.repair(self.repo, self.args)["proof"]["checked_observation_ids"])

    def test_existing_receipt_and_version_race_refuse_equivalent_apply(self):
        self.args.apply = True
        self.path.write_bytes(b"existing")
        with self.assertRaises(FileExistsError):
            tool.repair(self.repo, self.args)
        self.assertEqual(self.repo.saved, [])
        self.assertEqual(self.path.read_bytes(), b"existing")
        self.repo.current["version"] += 1
        with self.assertRaisesRegex(OrderError, "version changed"):
            tool.repair(self.repo, self.args)

    def test_raw_observation_owned_by_another_order_or_incomplete_owner_scope_refused(self):
        raw = copy.deepcopy(self.repo.observations[1][2])
        self.repo.observations.append((999, "order", raw, "query",
            "2026-09-29T00:00:00+00:00", True, "foreign"))
        with self.assertRaisesRegex(OrderError, "colliding order"):
            tool.repair(self.repo, self.args)
        self.repo.observations.pop()
        other = copy.deepcopy(self.repo.current)
        other["order_id"] = "foreign"
        other["qmt_orders"][0]["trading_day"] = None
        self.repo.account_orders.append(other)
        with self.assertRaisesRegex(OrderError, "incomplete scope"):
            tool.repair(self.repo, self.args)


if __name__ == "__main__":
    unittest.main()
