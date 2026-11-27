# -*- coding: utf-8 -*-
# Last modified (Asia/Shanghai): 2026-10-14
"""终态修复工具：纯本地事务替身，不连接 PostgreSQL/QMT。"""
import copy
import contextlib
import io
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
        if '.account_runtime r WHERE' in sql:
            self.rows = [(self.repo.stream_id, max((row[0] for row in self.repo.events), default=0))]
        elif ".order_events WHERE" in sql:
            self.rows = [row for row in self.repo.events if row[0] >= params[-1]]
        elif ".qmt_observations WHERE" in sql:
            self.rows = list(self.repo.observations)
        elif ".orders WHERE" in sql:
            self.rows = ([(doc["order_id"], doc) for doc in self.repo.account_orders]
                         if sql.startswith("SELECT order_id,document") else
                         [(doc,) for doc in self.repo.account_orders])
        elif sql != "SET TRANSACTION READ ONLY":
            raise AssertionError("unexpected SQL: " + sql)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class Repo:
    account_type = "STOCK"
    account_id = "account-1"
    repo_scope = ("STOCK", "account-1")
    repo_s = '"qmt_order"'
    stream_id = 'de7932f5-481f-4d64-8f88-008f846b239d'

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

    def test_append_only_repair_persists_exact_original_stream_sequence_and_public_digest(self):
        from service.ipc_bridge.ipc_protocol.order_common import fingerprint, public_order
        original = copy.deepcopy(self.repo.events)
        self.args.apply = True
        result = tool.repair(self.repo, self.args)
        refs = result['source_event_refs']
        self.assertEqual([ref['event_id'] for ref in refs], [5, 6, 7])
        for ref, (seq, kind, stamp, document) in zip(refs, original):
            self.assertEqual(ref['stream_id'], self.repo.stream_id)
            self.assertEqual(ref['order_id'], document['order_id'])
            self.assertEqual(ref['order_version'], document['version'])
            self.assertEqual(ref['sha256'], fingerprint(dict(event_id=seq, type=kind, recorded_at=stamp, data=public_order(document))))
        saved = self.repo.saved[0][0]
        self.assertEqual(saved['terminal_repairs'][-1]['source_event_refs'], refs)
        self.assertEqual(saved['event_corrections_v1'][-1]['source_event_refs'], refs)
        self.assertEqual(saved['event_corrections_v1'][-1]['financial_revision'], 'NOT_AUTHORIZED')
        receipt = json.loads(self.path.read_text(encoding='utf-8'))
        self.assertEqual(receipt['source_event_refs'], refs)
        self.assertEqual(self.repo.events, original)
        self.assertEqual(saved['fills'], original[-1][3]['fills'])

    def test_missing_durable_stream_cannot_create_a_new_unanchored_repair(self):
        self.repo.stream_id = None
        self.args.apply = True
        with self.assertRaises(OrderError) as caught:
            tool.repair(self.repo, self.args)
        self.assertEqual(caught.exception.code, 'EVIDENCE_SCHEMA_REQUIRED')
        self.assertFalse(self.path.exists())
        self.assertEqual(self.repo.saved, [])

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
                    "terminal_repairs": after["terminal_repairs"], "event_corrections_v1": after["event_corrections_v1"]})
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
            self.assertNotIn("raw", resolved)
            for key in ("evidence_id", "kind", "source", "reason", "observed_at"):
                self.assertEqual(resolved[key], original[key])
            self.assertEqual(resolved["raw_sha256"], tool.fingerprint(original["raw"]))
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
        self.setUp()
        bad_callback = {**self.repo.observations[1][2], "m_strOrderRef": "OTHER_xt1090519043"}
        self.repo.observations.append((999, "order", bad_callback, "callback",
            "2026-09-29T00:00:00+00:00", True, "order-1"))
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


def precision_snapshots(query_changes=None, count=2):
    """Legacy history with the two production quantity/amount precision pairs."""
    _, events, _ = equivalent_snapshots()
    checkpoint = events[0][3]
    checkpoint["account_type"] = "STOCK"
    checkpoint["items"][0]["requested_quantity"] = 217600
    checkpoint["request"]["quantity"] = 217600
    order = checkpoint["qmt_orders"][0]
    order.update(quantity=217600, filled_quantity=217600)
    order["raw"].update(quantity=217600, filled_quantity=217600,
                        m_dTradeAmount=405388.8, m_dPrice=1.863, m_dTradedPrice=1.863)
    prototype = copy.deepcopy(checkpoint["fills"][0])
    checkpoint["fills"] = []
    for index, quantity, amount in ((1, 164548, "306552.924"), (2, 53052, "98835.876")):
        fill = copy.deepcopy(prototype)
        trade_id = "trade-{}".format(index)
        fill.update(trade_id=trade_id, fill_key=["20260926", "SH", trade_id],
                    quantity=quantity, amount=amount, source="callback")
        fill["raw"].update(trade_id=trade_id, quantity=quantity, filled_quantity=quantity,
                           amount=amount, m_dTradeAmount=amount, m_dPrice=1.863,
                           m_dTradedPrice=1.863)
        checkpoint["fills"].append(fill)
    mark_reconciled(checkpoint, now=checkpoint["last_reconciled_at"])
    events = [(5, "RECONCILE_STATE_CHANGED", "2026-09-26T14:54:00+00:00", checkpoint)]
    observations = []
    previous = checkpoint
    for index in range(count):
        fill = checkpoint["fills"][index % 2]
        query = copy.deepcopy(fill["raw"])
        amount = "306552.92" if index % 2 == 0 else "98835.88"
        query.update(amount=amount, m_dTradeAmount=amount, m_nOrderStrategyType=index,
                     m_dCommission=0, m_dComssion=123.456, m_dComission="unavailable")
        if index == 0:
            query.update(query_changes or {})
        seq = index + 6
        stamp = "2026-09-28T00:00:00+00:00"
        reason = "CONFLICTING_FILL_IDENTITY"
        evidence = {"kind": "deal", "raw": query, "source": "query", "reason": reason,
                    "observed_at": stamp,
                    "evidence_id": tool.fingerprint({"kind": "deal", "raw": query, "reason": reason})}
        doc = copy.deepcopy(previous)
        doc["unassociated_evidence"].append(evidence)
        doc.update(version=previous["version"] + 1, reconciliation_complete=False,
                   reconcile_requested=True, reconcile_priority=True)
        recompute_order(doc)
        events.append((seq, "QMT_OBSERVATION", stamp, doc))
        observations.append((seq + 100, "deal", query, "query", stamp, True, "order-1"))
        previous = doc
    gap = copy.deepcopy(previous)
    gap.update(version=previous["version"] + 1, fact_version=previous["fact_version"] + 1)
    events.append((count + 6, "RECONCILE_GAP", "2026-09-28T00:00:08+00:00", gap))
    return copy.deepcopy(gap), events, observations


class FillPrecisionObservationRepairTests(unittest.TestCase):
    def setUp(self):
        self.load_fixture()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "receipt.json"
        self.args = SimpleNamespace(order_id="order-1", expected_version=self.repo.current["version"],
            checkpoint_event_seq=5, reason="audited callback/query cent precision", evidence_out=self.path,
            apply=False, resolve_equivalent_observations=False, resolve_fill_precision_observations=True)

    def load_fixture(self, **kwargs):
        current, events, observations = precision_snapshots(**kwargs)
        self.repo = Repo(current, events)
        self.repo.observations = observations

    def test_precision_dry_run_proves_both_rounding_directions_without_writes(self):
        before = copy.deepcopy((self.repo.current, self.repo.events, self.repo.observations))
        result = tool.repair(self.repo, self.args)
        self.assertEqual(result["repair_kind"], "fill-precision-query-observations")
        self.assertEqual(result["proof"]["checked_observation_ids"], [106, 107])
        for proof in result["proof"]["evidence_proofs"]:
            self.assertEqual(proof["rule"], "QUERY_FILL_CALLBACK_QUERY_CENT_PRECISION")
        for proof in result["proof"]["observation_proofs"]:
            self.assertEqual(proof["ignored_raw_fields"], ["m_dCommission", "m_dComssion", "m_dComission"])
        self.assertEqual((self.repo.current, self.repo.events, self.repo.observations), before)
        self.assertEqual(self.repo.saved, [])
        self.assertFalse(self.path.exists())
        self.assertFalse(result["expected_active"])

    def test_apply_preserves_native_facts_and_receipt_and_restart_stays_closed(self):
        # The existing invariant test also replays every query after startup recovery.
        EquivalentObservationRepairTests.test_apply_preserves_native_facts_and_receipt_and_restart_stays_closed(self)
        receipt = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(receipt["repair_kind"], "fill-precision-query-observations")
        self.assertEqual(receipt["events"], [
            {"seq": row[0], "type": row[1], "at": row[2], "document": row[3]} for row in self.repo.events])
        self.assertEqual([row["raw"] for row in receipt["observations"]],
                         [row[2] for row in self.repo.observations])
        self.assertTrue(all(row["resolution"]["kind"] == "MANUAL_FILL_PRECISION_QUERY_OBSERVATION"
                            for row in self.repo.current["resolved_evidence"]))

    def test_default_and_old_explicit_mode_still_refuse_precision_pairs(self):
        self.args.resolve_fill_precision_observations = False
        for old_mode in (False, True):
            with self.subTest(old_mode=old_mode):
                self.args.resolve_equivalent_observations = old_mode
                with self.assertRaises(OrderError):
                    tool.repair(self.repo, self.args)

    def test_true_economics_identity_unknown_fields_and_noncent_amounts_refused(self):
        for changes in ({"quantity": 164549}, {"m_dPrice": 1.864},
                        {"amount": "306552.93", "m_dTradeAmount": "306552.93"},
                        {"amount": "306552.9251", "m_dTradeAmount": "306552.9251"},
                        {"m_dTradeAmount": "306552.93"}, {"trade_id": "other"},
                        {"qmt_order_id": "other"}, {"account_id": "other"},
                        {"symbol": "other"}, {"side": "SELL"}, {"remark": "other"},
                        {"m_strOrderRef": "other"}, {"unknown_fact": 1}):
            with self.subTest(changes=changes):
                self.load_fixture(query_changes=changes)
                with self.assertRaises(OrderError):
                    tool.repair(self.repo, self.args)

    def test_unaudited_noop_query_and_order_branch_stay_strict(self):
        for kind, changes in (("deal", {"m_dPrice": 1.864}),
                              ("deal", {"amount": "306552.93", "m_dTradeAmount": "306552.93"}),
                              ("order", {"m_dTradeAmount": "405388.804"}),
                              ("order", {"m_dCommission": 0}),
                              ("order", {"m_dComssion": 0}),
                              ("order", {"m_dComission": 0})):
            with self.subTest(kind=kind, changes=changes):
                self.load_fixture()
                checkpoint = self.repo.events[0][3]
                raw = copy.deepcopy(checkpoint["fills" if kind == "deal" else "qmt_orders"][0]["raw"])
                raw.update(changes)
                self.repo.observations.append((999, kind, raw, "query",
                    "2026-09-29T00:00:00+00:00", True, "order-1"))
                with self.assertRaises(OrderError):
                    tool.repair(self.repo, self.args)

    def test_noncallback_origin_and_nonstock_account_cannot_use_cent_rule(self):
        for bad in ("source", "account_type", "callback_price"):
            with self.subTest(bad=bad):
                self.load_fixture()
                for doc in [self.repo.current] + [row[3] for row in self.repo.events]:
                    if bad == "source":
                        doc["fills"][0]["source"] = "query"
                    elif bad == "account_type":
                        doc["account_type"] = "FUTURE"
                    else:
                        doc["fills"][0]["raw"]["m_dPrice"] = 1.864
                checkpoint = self.repo.events[0][3]
                checkpoint["terminal_reconciliation_checkpoint"]["facts_fingerprint"] = state_reconciliation_facts(checkpoint)
                for doc in [self.repo.current] + [row[3] for row in self.repo.events[1:]]:
                    doc["terminal_reconciliation_checkpoint"] = copy.deepcopy(checkpoint["terminal_reconciliation_checkpoint"])
                with self.assertRaises(OrderError):
                    tool.repair(self.repo, self.args)


    def test_history_and_observation_proofs_cannot_be_bypassed_in_precision_mode(self):
        EquivalentObservationRepairTests.test_missing_duplicate_or_changed_raw_observation_refused(self)
        EquivalentObservationRepairTests.test_event_mutation_or_unexpected_event_or_fact_generation_refused(self)
        EquivalentObservationRepairTests.test_new_fact_error_submission_and_cancel_changes_refused(self)
        EquivalentObservationRepairTests.test_cross_order_identity_collision_and_missing_internal_fields_refused(self)

    def test_commission_only_difference_requires_new_mode(self):
        current, events, observations = equivalent_snapshots()
        self.repo = Repo(current, events)
        self.repo.observations = observations
        self.args.expected_version = current["version"]
        raw = copy.deepcopy(events[0][3]["fills"][0]["raw"])
        raw.update(m_dCommission=999, m_dComssion=0, m_dComission=None)
        self.repo.observations.append((999, "deal", raw, "query",
            "2026-09-29T00:00:00+00:00", True, "order-1"))
        self.assertIn(999, tool.repair(self.repo, self.args)["proof"]["checked_observation_ids"])
        self.args.resolve_fill_precision_observations = False
        self.args.resolve_equivalent_observations = True
        with self.assertRaisesRegex(OrderError, "query raw field"):
            tool.repair(self.repo, self.args)

    def test_receipt_collision_version_race_and_cli_modes(self):
        EquivalentObservationRepairTests.test_existing_receipt_and_version_race_refuse_equivalent_apply(self)
        argv = ["--order-id", "order-1", "--expected-version", "14", "--checkpoint-event-seq", "5",
                "--reason", "audit", "--resolve-fill-precision-observations"]
        args = tool.parser().parse_args(argv)
        self.assertTrue(args.resolve_fill_precision_observations)
        self.assertFalse(args.resolve_equivalent_observations)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            tool.parser().parse_args(argv + ["--resolve-equivalent-observations"])

    def test_callback_alias_stripped_deal_float_noise_and_reconciled_incomplete_repair(self):
        _, old_events = snapshots("FILLED")
        checkpoint = old_events[0][3]
        checkpoint.update(account_type="STOCK", unassociated_evidence=[], resolved_evidence=[])
        raw = {"account_id": "account-1", "remark": "qbtest", "qmt_order_id": "xt1090519043",
               "qmt_task_id": "17", "trading_day": "20260926", "market": "SH",
               "symbol": "511880.SH", "side": "BUY", "quantity": 100, "filled_quantity": 100,
               "status": "FILLED", "m_dTradedPrice": 101.00399999999999, "m_dTradeAmount": 10100.4,
               "m_dPrice": 101.004, "m_dCommission": 0.21, "m_dComssion": 0.21,
               "m_nRef": 1090551164, "m_strOrderRef": "native-1090", "m_strInsertTime": "131052",
               "m_nTaskId": 17, "m_strRemark": "qbtest", "m_strSource": "IPC_ORDER",
               "m_strOrderStrategyType": "函数下单", "m_strXTTrade": "本终端",
               "m_nOrderStrategyType": 555157360, "m_nOpType": 24}
        row = checkpoint["qmt_orders"][0]
        row.update({key: value for key, value in tool.observation_identifiers("order", raw).items()})
        row.update(raw=copy.deepcopy(raw), source="callback", native_ref="1090551164",
                   native_order_ref="native-1090", observed_at="2026-09-26T14:53:00+00:00")
        fill = checkpoint["fills"][0]
        fill.update(account_id="account-1", remark="qbtest", symbol="511880.SH", side="BUY",
                    qmt_task_id="17", raw={**raw, "trade_id": "trade-1", "amount": "10100.4"})
        checkpoint["fills"][0]["amount"] = "10100.4"
        mark_reconciled(checkpoint, now=checkpoint["last_reconciled_at"])
        events = [(5, "RECONCILE_STATE_CHANGED", "2026-09-26T14:54:00+00:00", checkpoint)]
        observations = []
        previous = checkpoint
        ue_specs = (
            (6, "order", "callback", "CONFLICTING_ORDER_REFERENCE",
             {**raw, "m_nRef": 0, "m_strOrderRef": "SH_xt1090519043",
              "m_strInsertTime": "131118", "m_dTradedPrice": 101.004}, 1),
            (7, "deal", "query", "CONFLICTING_FILL_IDENTITY",
             {**fill["raw"], "m_nRef": 0, "m_strOrderRef": "SH_xt1090519043",
              "m_nOrderStrategyType": 282386400}, 0),
            (8, "deal", "callback", "CONFLICTING_FILL_IDENTITY",
             {**fill["raw"], "m_nTaskId": -1, "m_strRemark": "", "m_strSource": "",
              "m_strOrderStrategyType": "", "m_strXTTrade": "其他终端",
              "m_nOrderStrategyType": 0}, 1),
        )
        for seq, kind, source, reason, obs_raw, fv_delta in ue_specs:
            stamp = "2026-09-28T00:00:0{}+00:00".format(seq)
            evidence = {"kind": kind, "raw": obs_raw, "source": source, "reason": reason,
                        "observed_at": stamp,
                        "evidence_id": tool.fingerprint({"kind": kind, "raw": obs_raw, "reason": reason})}
            doc = copy.deepcopy(previous)
            doc["unassociated_evidence"].append(evidence)
            doc.update(version=previous["version"] + 1,
                       fact_version=previous["fact_version"] + fv_delta,
                       reconciliation_complete=False,
                       reconcile_requested=True, reconcile_priority=True)
            recompute_order(doc)
            events.append((seq, "QMT_OBSERVATION", stamp, doc))
            observations.append((seq + 100, kind, obs_raw, source, stamp, True, "order-1"))
            previous = doc
        stripped_order_query = {**raw, "m_nTaskId": -1, "m_strRemark": "", "m_strSource": "",
                                "m_strOrderStrategyType": "", "m_strXTTrade": "其他终端",
                                "m_nOrderStrategyType": 0, "m_nOpType": 2147483647}
        observations.append((199, "order", stripped_order_query, "query",
                             "2026-09-28T00:00:11+00:00", True, "order-1"))
        reconciled = copy.deepcopy(previous)
        reconciled.update(version=previous["version"] + 1, reconciliation_complete=True,
                          last_reconciled_at=checkpoint["last_reconciled_at"])
        events.append((9, "RECONCILE_STATE_CHANGED", "2026-09-28T00:00:09+00:00", reconciled))
        gap = copy.deepcopy(reconciled)
        gap.update(version=reconciled["version"] + 1, fact_version=reconciled["fact_version"] + 1)
        events.append((10, "RECONCILE_GAP", "2026-09-28T00:00:10+00:00", gap))

        self.repo = Repo(copy.deepcopy(gap), events)
        self.repo.observations = observations
        bad_order_query = {**stripped_order_query, "m_nOpType": 23}
        self.repo.observations.append((200, "order", bad_order_query, "query",
                                       "2026-09-28T00:00:12+00:00", True, "order-1"))
        self.args = SimpleNamespace(order_id="order-1", expected_version=gap["version"],
                                    checkpoint_event_seq=5, reason="2026-11-26 alias & stripped deal repair",
                                    evidence_out=self.path, apply=False,
                                    resolve_equivalent_observations=True,
                                    resolve_fill_precision_observations=False)
        with self.assertRaisesRegex(OrderError, "changes audited facts"):
            tool.repair(self.repo, self.args)
        self.repo.observations.pop()
        self.args.apply = True
        result = tool.repair(self.repo, self.args)
        self.assertEqual(result["execution_status"], "FILLED")
        self.assertFalse(result["expected_active"])
        self.assertFalse(self.repo.current["reconcile_pending"])
        self.assertEqual(self.repo.current["sync_status"], "COMPLETE")
        self.assertEqual(self.repo.current["unassociated_evidence"], [])

    def test_noop_reconcile_and_duplicate_gap_do_not_bump_version_or_emit_events(self):
        doc = copy.deepcopy(self.repo.current)
        # Setup minimal FakeRepo around real PostgresRepository methods (noop versionless save).
        saved_calls = []

        class MiniCur:
            def __init__(self, row_doc):
                self.row_doc = row_doc
                self.rows = []

            def execute(self, sql, params=()):
                self.rows = []
                if "SELECT order_id,document,last_reconcile_attempt_at" in sql:
                    self.rows = [(self.row_doc["order_id"], copy.deepcopy(self.row_doc), None, False)]
                elif "SELECT order_id,document" in sql:
                    self.rows = [(self.row_doc["order_id"], copy.deepcopy(self.row_doc))]
                elif "SELECT document FROM" in sql:
                    self.rows = [(copy.deepcopy(self.row_doc),)]

            def fetchall(self):
                return self.rows

            def fetchone(self):
                return self.rows[0] if self.rows else None

        fake_repo = object.__new__(tool.PostgresRepository)
        fake_repo.repo_s = "qmt_order"
        fake_repo.repo_scope = ("STOCK", "account-1")
        fake_repo.account_type = "STOCK"
        fake_repo.account_id = "account-1"
        fake_repo.repo_run = lambda cb, mutation=False: cb(MiniCur(doc))
        fake_repo.repo_load = lambda cur, oid, lock=False: copy.deepcopy(cur.row_doc)
        fake_repo.repo_require = lambda d: d
        fake_repo.repo_save = lambda cur, d, ev, **kw: saved_calls.append((ev, kw.get("bump_version", True), d["version"]))

        start_ver = doc["version"]
        batch = tool.PostgresRepository.begin_reconcile_batch(fake_repo, limit=10, round_id="r-1")
        self.assertEqual(len(batch["orders"]), 1)
        self.assertEqual(saved_calls[-1], (None, False, start_ver))

        ok = tool.PostgresRepository.finish_reconcile(fake_repo, doc["order_id"], doc["fact_version"], False)
        self.assertTrue(ok)
        self.assertEqual(saved_calls[-1], (None, False, start_ver))

        gap = tool.PostgresRepository.mark_reconcile_gap(fake_repo, "2026-11-27T00:00:00+00:00")
        self.assertEqual(gap["marked"], 0)
        self.assertEqual(saved_calls[-1], (None, False, start_ver))


if __name__ == "__main__":
    unittest.main()
