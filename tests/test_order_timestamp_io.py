"""Offline timestamptz boundary regressions for both ORDER repositories."""
import copy
import datetime as dt
import importlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from test_order_evidence import MemoryEvidenceCursor, MemoryEvidenceRepository
from test_order_state import document, order
from test_order_terminal_repair import Repo, equivalent_snapshots, tool


UTC = dt.timezone.utc
SHANGHAI = dt.timezone(dt.timedelta(hours=8))
STAMP = dt.datetime(2026, 10, 15, 1, 30, 12, 123456, tzinfo=UTC)
ISO = "2026-10-15T01:30:12.123456+00:00"


class TimestampRepositoryTests(unittest.TestCase):
    def repositories(self):
        for base in ("order_bridge", "service.ipc_bridge.order"):
            module = importlib.import_module(base + ".repository")
            yield base, module, module.PostgresRepository({}, "acct")

    def bind_cursor(self, repo, cur):
        repo.repo_run = lambda callback, mutation=False: callback(cur)

    def test_event_api_is_json_serializable_with_utc_microseconds(self):
        for base, module, repo in self.repositories():
            for stamp in (STAMP, STAMP.astimezone(SHANGHAI), ISO):
                with self.subTest(base=base, stamp=stamp):
                    doc = document()
                    cur = Mock()
                    cur.fetchall.return_value = [(1, doc["order_id"], "ACCEPTED", stamp, doc)]
                    self.bind_cursor(repo, cur)
                    result = repo.events()
                    event = result["events"][0]
                    self.assertEqual(event["recorded_at"], ISO)
                    self.assertEqual(event["occurred_at"], ISO)
                    module.json_text(result)
                    if base == "order_bridge":
                        runtime_class = importlib.import_module(base + ".runtime").OrderRuntime
                    else:
                        from tests.order_runtime_helpers import OrderRuntime as runtime_class
                    runtime = runtime_class({}, None, "acct", repository=repo)
                    status, response = runtime.handle("order_events", {}, "GET")
                    self.assertEqual(status, 200)
                    self.assertEqual(response["events"][0]["recorded_at"], ISO)
                    json.dumps(response, allow_nan=False)

    def test_evidence_anchor_and_pages_survive_database_session_timezone_changes(self):
        for base, module, repo in self.repositories():
            with self.subTest(base=base):
                store = MemoryEvidenceRepository()
                store.append()
                store.append()
                for row in store.state["events"].values():
                    row["recorded_at"] = STAMP.astimezone(SHANGHAI)
                repo.account_id, repo.repo_scope = store.account_id, store.repo_scope
                repo.repo_run = lambda callback, mutation=False: callback(MemoryEvidenceCursor(store))
                first = repo.evidence_events(limit=1)
                expected = module.fingerprint([store.state["events"][2]["data"], "QMT_OBSERVATION", ISO])
                self.assertEqual(first["anchor"], expected)
                self.assertEqual(first["events"][0]["recorded_at"], ISO)
                checkpoint = {key: first[key] for key in ("stream_id", "executor_epoch", "executor_instance",
                    "account_id", "account_type", "high_watermark", "anchor")}
                for row in store.state["events"].values():
                    row["recorded_at"] = STAMP
                second = repo.evidence_events(after=1, limit=1, checkpoint=checkpoint)
                self.assertEqual(second["anchor"], expected)
                self.assertEqual(second["events"][0]["recorded_at"], ISO)
                module.json_text(second)
                store.state["events"][2]["recorded_at"] += dt.timedelta(microseconds=1)
                with self.assertRaisesRegex(module.OrderError, "anchor changed"):
                    repo.evidence_events(after=1, checkpoint=checkpoint)

    def test_observation_lookup_normalizes_database_datetime(self):
        for base, module, repo in self.repositories():
            for stamp in (STAMP, STAMP.astimezone(SHANGHAI), ISO):
                with self.subTest(base=base, stamp=stamp):
                    doc = document()
                    repo.repo_load = Mock(return_value=doc)
                    cur = Mock()
                    cur.fetchall.return_value = [(7, "order", order(), "query", stamp, True, doc["order_id"])]
                    self.bind_cursor(repo, cur)
                    rows = repo.lookup_observations(doc["order_id"])
                    self.assertEqual(rows[0]["observed_at"], ISO)
                    module.json_text(rows)

    def test_pending_replay_normalizes_before_state_serialization(self):
        for base, module, repo in self.repositories():
            for stamp in (STAMP, STAMP.astimezone(SHANGHAI), ISO):
                for invalid_identity in (False, True):
                    with self.subTest(base=base, stamp=stamp, invalid_identity=invalid_identity):
                        doc = document()
                        raw = order()
                        if invalid_identity:
                            raw["m_strExchangeID"] = ["SH"]
                        cur = Mock()
                        cur.fetchall.return_value = [(7, "order", raw, "query", stamp)]
                        repo.repo_match = Mock(return_value=doc)
                        repo.repo_save = Mock(side_effect=lambda cursor, value, *args, **kwargs: module.json_text(value))
                        self.assertEqual(repo.repo_apply_pending(cur), 1)
                        repo.repo_save.assert_called_once()
                        row = doc["unassociated_evidence" if invalid_identity else "qmt_orders"][0]
                        self.assertEqual(row["observed_at"], ISO)
                        module.fingerprint(doc)

    def test_manual_association_normalizes_before_apply_and_save(self):
        for base, module, repo in self.repositories():
            for stamp in (STAMP, STAMP.astimezone(SHANGHAI), ISO):
                with self.subTest(base=base, stamp=stamp):
                    doc = document()
                    doc["submission_status"] = "UNKNOWN"
                    raw = dict(order(), m_strRemark=doc["remark"], m_strAccountID="acct")
                    cur = Mock()
                    cur.fetchone.side_effect = [("order", raw, "query", stamp, None), None,
                        ('de7932f5-481f-4d64-8f88-008f846b239d', 1), (1,)]
                    repo.repo_journal_row = Mock(return_value=dict(event_id=1, type='SUBMISSION_UNKNOWN',
                        recorded_at=ISO, data=copy.deepcopy(doc)))
                    self.bind_cursor(repo, cur)
                    repo.repo_load = Mock(return_value=doc)
                    repo.repo_apply_pending = Mock()
                    repo.repo_save = Mock(side_effect=lambda cursor, value, *args, **kwargs: module.json_text(value))
                    result = repo.manual_associate(doc["order_id"], [7], doc["version"],
                        {"reason": "audited QMT observation", "operator": "tester", "evidence": {"receipt": "r1"}})
                    self.assertEqual(result["submission_status"], "CONFIRMED")
                    self.assertEqual(result["qmt_orders"][0]["observed_at"], ISO)
                    repo.repo_save.assert_called_once()
                    module.fingerprint(result)

    def test_strings_remain_compatible_and_json_serializer_stays_strict(self):
        for base, module, repo in self.repositories():
            with self.subTest(base=base):
                self.assertEqual(module.repo_SCHEMA_VERSION, 5)
                legacy = "2026-10-15T09:30:12.123456+08:00"
                self.assertEqual(module.repo_timestamp(legacy), legacy)
                with self.assertRaises(TypeError):
                    module.json_text({"unexpected_datetime": STAMP})


class TimestampRepairTests(unittest.TestCase):
    def test_conflict_matching_compares_instants_and_keeps_microsecond_precision(self):
        current, events, observations = equivalent_snapshots()
        checkpoint = events[0][3]
        evidence = copy.deepcopy(current["unassociated_evidence"][0])
        evidence["observed_at"] = ISO.replace("+00:00", "Z")
        row = list(observations[0])
        for stamp in (STAMP, STAMP.astimezone(SHANGHAI), "2026-10-15T09:30:12.123456+08:00"):
            with self.subTest(stamp=stamp):
                row[4] = stamp
                proof = tool.prove_equivalent_evidence(checkpoint, evidence, [tuple(row)])
                self.assertEqual(proof["observation_id"], row[0])
        row[4] = STAMP + dt.timedelta(microseconds=1)
        with self.assertRaisesRegex(tool.OrderError, "unique applied raw observation"):
            tool.prove_equivalent_evidence(checkpoint, evidence, [tuple(row)])

    def test_repair_receipt_normalizes_db_events_and_observations_to_utc_iso(self):
        for zone in (UTC, SHANGHAI):
            with self.subTest(zone=zone), tempfile.TemporaryDirectory() as temp:
                current, events, observations = equivalent_snapshots()
                repo = Repo(current, [(seq, kind,
                    tool.parse_timestamp(stamp).replace(microsecond=123456).astimezone(zone), doc)
                    for seq, kind, stamp, doc in events])
                repo.observations = [(oid, kind, raw, source, tool.parse_timestamp(stamp).astimezone(zone), applied, owner)
                    for oid, kind, raw, source, stamp, applied, owner in observations]
                path = Path(temp) / "receipt.json"
                args = SimpleNamespace(order_id=current["order_id"], expected_version=current["version"],
                    checkpoint_event_seq=5, reason="audited equivalent conflicts", evidence_out=path,
                    apply=True, resolve_equivalent_observations=True)
                result = tool.repair(repo, args)
                receipt = json.loads(path.read_text(encoding="utf-8"))
                self.assertEqual(result["proof"]["checked_observation_ids"], [106, 107])
                self.assertEqual(result["proof"]["checkpoint_at"], "2026-09-26T14:54:00.123456+00:00")
                self.assertEqual(receipt["events"][0]["at"], "2026-09-26T14:54:00.123456+00:00")
                self.assertEqual(receipt["observations"][0]["observed_at"], observations[0][4])
                self.assertTrue(all("T" in row["at"] and row["at"].endswith("+00:00") for row in receipt["events"]))
                self.assertEqual(len(repo.saved), 1)


if __name__ == "__main__":
    unittest.main()
