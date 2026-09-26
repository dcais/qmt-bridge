# -*- coding: utf-8 -*-
"""ORDER 交付工具的无 QMT/PG 验证。"""
import importlib.util
import json
from pathlib import Path
import re
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


def load_tool(name):
    path = ROOT / "tools" / (name + ".py")
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BuildTests(unittest.TestCase):
    def setUp(self):
        self.tool = load_tool("build_order_strategy")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = Path(self.temp.name) / "order_bridge"
        self.source.mkdir()
        self.output = Path(self.temp.name) / "http_order.py"
        for name in self.tool.MODULES:
            (self.source / (name + ".py")).write_text(
                "# -*- coding: utf-8 -*-\nfrom .common import (\n    OrderError,\n    utc_now,\n)\nimport os\n\ndef {0}_function():\n    return os.name\n".format(name), encoding="utf-8")

    def test_build_check_and_timestamp_preservation(self):
        args = ["--source-dir", str(self.source), "--output", str(self.output)]
        self.assertEqual(self.tool.main(args + ["--check"]), 1)
        self.assertFalse(self.output.exists())
        self.assertEqual(self.tool.main(args), 0)
        original = self.output.read_bytes()
        rendered = original.decode("gbk")
        self.assertTrue(rendered.startswith("# -*- coding: gbk -*-\n# Last modified (Asia/Shanghai): "))
        self.assertNotIn("from .common import", rendered)
        self.assertEqual(rendered.count("import os"), len(self.tool.MODULES))
        self.assertEqual(self.tool.main(args + ["--check"]), 0)
        with patch.object(self.tool.dt, "datetime") as mocked:
            mocked.now.return_value.strftime.return_value = "2099-01-01 00:00:00"
            self.assertEqual(self.tool.main(args), 0)
        self.assertEqual(self.output.read_bytes(), original)
        with (self.source / "http.py").open("a", encoding="utf-8") as stream:
            stream.write("\nNEW_VALUE = 1\n")
        self.assertEqual(self.tool.main(args + ["--check"]), 1)

    def test_duplicate_top_level_definition_fails(self):
        with (self.source / "http.py").open("a", encoding="utf-8") as stream:
            stream.write("\ndef common_function():\n    pass\n")
        with self.assertRaisesRegex(ValueError, "common.*http"):
            self.tool.render(self.source, "2026-01-01 00:00:00")


class DependencyInstallerTests(unittest.TestCase):
    def test_dry_run_does_not_install(self):
        tool = load_tool("install_order_dependencies")
        with patch.object(tool.subprocess, "call") as call:
            self.assertEqual(tool.main(["--dry-run", "--python36"]), 0)
            call.assert_not_called()


class SchemaInstallerTests(unittest.TestCase):
    class Cursor:
        def __init__(self, existing_version=None):
            self.existing_version = existing_version
            self.version = existing_version
            self.statements = []
            self.last = ""

        def execute(self, statement, params=None):
            self.statements.append((statement, params))
            self.last = statement
            if statement.startswith("INSERT INTO ") and ".schema_version" in statement:
                if self.version is None:
                    self.version = 1

        def fetchone(self):
            if self.last.startswith("SELECT to_regclass"):
                return ("schema_version" if self.existing_version is not None else None,)
            if ".account_runtime WHERE " in self.last:
                return (1,)
            raise AssertionError("unexpected fetchone: " + self.last)

        def fetchall(self):
            if self.last.startswith("SELECT version FROM "):
                return [] if self.version is None else [(self.version,)]
            raise AssertionError("unexpected fetchall: " + self.last)

    class Repo:
        def __init__(self, cursor, schema='"qmt_order"'):
            self.cursor = cursor
            self.repo_s = schema
            self.repo_scope = ("STOCK", "test-account")

        def repo_run(self, callback, mutation=False):
            assert not mutation
            return callback(self.cursor)

    def test_installer_reads_external_sql_and_replaces_validated_schema(self):
        tool = load_tool("order_schema")
        cursor = self.Cursor()
        repo = self.Repo(cursor, '"test_schema"')
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "schema.sql"
            source.write_text('-- a comment with ; and CREATE TABLE bad;\n'
                              'CREATE SCHEMA IF NOT EXISTS "qmt_order";\n'
                              'CREATE TABLE "qmt_order".probe(value text DEFAULT \'a;b\'); '
                              '-- another ; comment\n', encoding="utf-8")
            self.assertEqual(tool.initialize_schema(repo, source), {"schema_version": 1})
        ddl = [sql for sql, _ in cursor.statements if sql.startswith("CREATE ")]
        self.assertEqual(len(ddl), 2)
        self.assertEqual(ddl[0], 'CREATE SCHEMA IF NOT EXISTS "test_schema"')
        self.assertIn('"test_schema".probe', ddl[1])
        self.assertIn("'a;b'", ddl[1])
        self.assertNotIn("bad", "\n".join(ddl))
        self.assertEqual(cursor.statements[0],
                         ("SELECT to_regclass(%s)", ('"test_schema".schema_version',)))
        self.assertIn(("STOCK", "test-account"), [params for _, params in cursor.statements])

    def test_existing_incompatible_version_blocks_all_ddl(self):
        from order_bridge.common import OrderError
        tool = load_tool("order_schema")
        cursor = self.Cursor(existing_version=2)
        with self.assertRaises(OrderError) as caught:
            tool.initialize_schema(self.Repo(cursor))
        self.assertEqual(caught.exception.code, "SCHEMA_VERSION_MISMATCH")
        self.assertFalse(any(sql.startswith(("CREATE ", "ALTER ", "INSERT "))
                             for sql, _ in cursor.statements))

    def test_default_sql_contains_all_order_tables(self):
        tool = load_tool("order_schema")
        cursor = self.Cursor()
        self.assertEqual(tool.initialize_schema(self.Repo(cursor)), {"schema_version": 1})
        ddl = "\n".join(sql for sql, _ in cursor.statements)
        for table in ("orders", "order_events", "qmt_observations", "order_items", "fills"):
            self.assertIn('"qmt_order".' + table, ddl)


class AdminSafetyTests(unittest.TestCase):
    def setUp(self):
        self.tool = load_tool("order_admin")
        from order_bridge.common import new_order_document
        request = {"client_order_id": "client-1", "account_id": "12345", "order_type": "SINGLE",
                   "sizing_type": "QUANTITY", "symbol": "600000.SH", "side": "BUY", "quantity": 100,
                   "execution": {"type": "DIRECT"}, "price_type": "LIMIT", "limit_price": "10"}
        self.doc = new_order_document(request)
        self.doc.update(order_id="order-1", remark="qb123", submission_status="UNKNOWN", version=7)
        self.doc["qmt_orders"] = [{"qmt_order_id": "abc", "status": "WORKING", "terminal": False,
                                  "item_id": "single", "quantity": 100, "filled_quantity": 0}]

    def test_schema_check_does_not_run_installer(self):
        class Repo:
            def check_schema(self):
                return {"schema_version": 1, "ready": True}

        with patch.object(self.tool, "initialize_schema") as install:
            result = self.tool.execute(Repo(), SimpleNamespace(command="schema", action="check"))
        self.assertEqual(result, {"schema_version": 1, "ready": True})
        install.assert_not_called()

    def test_schema_init_uses_external_installer(self):
        repo = object()
        with patch.object(self.tool, "initialize_schema", return_value={"schema_version": 1}) as install:
            result = self.tool.execute(repo, SimpleNamespace(command="schema", action="init"))
        self.assertEqual(result, {"schema_version": 1})
        install.assert_called_once_with(repo)

    def test_admin_uses_fixed_schema_and_rejects_legacy_setting(self):
        config = {"pg_host": "localhost", "pg_port": 5432, "pg_database": "order_test",
                  "pg_user": "order_user", "pg_password": "secret", "account_id": "test-account"}
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "config.json"
            source.write_text(json.dumps(config), encoding="utf-8")
            args = SimpleNamespace(config=source, account_id=None)
            with patch.dict(self.tool.os.environ, {}, clear=True):
                self.assertEqual(self.tool.config_from_args(args)["pg_schema"], "qmt_order")
                source.write_text(json.dumps(dict(config, pg_schema="legacy")), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "remove pg_schema"):
                    self.tool.config_from_args(args)
                source.write_text(json.dumps(dict(config, PG_SCHEMA="legacy")), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "PG_SCHEMA"):
                    self.tool.config_from_args(args)
                source.write_text(json.dumps(config), encoding="utf-8")
            with patch.dict(self.tool.os.environ, {"ORDER_PG_SCHEMA": "legacy"}, clear=True):
                with self.assertRaisesRegex(ValueError, "ORDER_PG_SCHEMA"):
                    self.tool.config_from_args(args)

    def test_observed_requires_explicit_distinct_identifiers(self):
        args = SimpleNamespace(observation_id=[19], qmt_order_id=["abc"], qmt_task_id=None)
        self.assertEqual(self.tool.observed_arguments(args), ([19], ["abc"], []))
        args.observation_id = [19, 19]
        with self.assertRaisesRegex(ValueError, "distinct"):
            self.tool.observed_arguments(args)
        args.observation_id = [19]
        args.qmt_order_id = []
        with self.assertRaisesRegex(ValueError, "requires QMT"):
            self.tool.observed_arguments(args)

    def test_not_submitted_rejects_absence_only_or_existing_qmt(self):
        self.doc["qmt_orders"] = []
        proof = {"proof_type": "pre_call_boundary", "qmt_call_never_started": True,
                 "basis": "Operator verified the pre-call checkpoint failed before invocation"}
        self.tool.verify_not_submitted(proof, self.doc)
        proof["basis"] = "No records found in all queries"
        with self.assertRaisesRegex(ValueError, "absence"):
            self.tool.verify_not_submitted(proof, self.doc)
        proof["basis"] = "Operator verified the pre-call checkpoint failed before invocation"
        self.doc["qmt_orders"] = [{"qmt_order_id": "abc"}]
        with self.assertRaisesRegex(ValueError, "contradicts"):
            self.tool.verify_not_submitted(proof, self.doc)

    def test_resolve_preserves_evidence_and_checks_version_in_mutation(self):
        class Repo:
            def __init__(self, doc):
                self.doc = doc

            def get_by_id(self, order_id):
                return dict(self.doc)

            def manual_associate(self, order_id, observation_ids, expected_version, audit):
                self.observation_ids = observation_ids
                self.expected_version = expected_version
                self.doc["manual_resolutions"] = [audit]
                self.doc["submission_status"] = "CONFIRMED"
                self.doc["resolution"] = "OBSERVED"
                self.doc["version"] += 1
                return self.doc

        repo = Repo(self.doc)
        with tempfile.TemporaryDirectory() as temp:
            evidence_path = Path(temp) / "evidence.json"
            evidence_path.write_text('{"source":"broker statement"}', encoding="utf-8")
            args = SimpleNamespace(order_id="order-1", expected_version=7, resolution="observed",
                                   reason="Verified order record", evidence_file=evidence_path,
                                   observation_id=[19], qmt_order_id=["abc"], qmt_task_id=None)
            result = self.tool.resolve(repo, args)
        self.assertEqual(repo.observation_ids, [19])
        self.assertEqual(repo.expected_version, 7)
        self.assertEqual(result["submission_status"], "CONFIRMED")
        self.assertEqual(repo.doc["manual_resolutions"][0]["evidence"],
                         {"source": "broker statement"})
        self.assertEqual(repo.doc["manual_resolutions"][0]["observation_ids"], [19])

    def test_not_submitted_resolution_stays_terminal(self):
        self.doc["qmt_orders"] = []
        class Repo:
            def __init__(self, doc):
                self.doc = doc

            def get_by_id(self, order_id):
                return dict(self.doc)

            def update_order(self, order_id, event_type, mutator):
                self.event_type = event_type
                mutator(self.doc)
                self.doc["version"] += 1
                return self.doc

        repo = Repo(self.doc)
        with tempfile.TemporaryDirectory() as temp:
            evidence_path = Path(temp) / "pre-call.json"
            evidence_path.write_text(json.dumps({"proof_type": "pre_call_boundary",
                                                 "qmt_call_never_started": True,
                                                 "basis": "Operator verified the pre-call boundary was never crossed"}),
                                     encoding="utf-8")
            args = SimpleNamespace(order_id="order-1", expected_version=7, resolution="not-submitted",
                                   reason="Confirmed pre-call stop", evidence_file=evidence_path,
                                   observation_id=None, qmt_order_id=None, qmt_task_id=None)
            result = self.tool.resolve(repo, args)
        self.assertEqual(repo.event_type, "MANUAL_UNKNOWN_RESOLUTION")
        self.assertEqual(result["resolution"], "RESOLVED_NOT_SUBMITTED")
        self.assertEqual(self.doc["submission_status"], "REJECTED")
        self.assertNotEqual(self.doc["submission_status"], "QUEUED")


class DocumentedContractTests(unittest.TestCase):
    def test_http_order_examples_match_normalizer(self):
        from order_bridge.contracts import normalize_order, normalize_cancel
        body = (ROOT / "HTTP_ORDER.md").read_text(encoding="utf-8")
        fences = re.findall(r"```(?:json|http)\n(.*?)\n```", body, re.DOTALL)
        examples = []
        for fence in fences:
            fragment = fence[fence.find("{"):]
            if not fragment:
                continue
            try:
                value = json.loads(fragment)
            except ValueError:
                continue
            if isinstance(value, dict) and value.get("client_order_id") and value.get("order_type"):
                examples.append(normalize_order(value, "<account>"))
            if isinstance(value, dict) and value.get("cancel_request_id") and value.get("account_id"):
                self.assertEqual(normalize_cancel(value, "<account>")["cancel_request_id"],
                                 "cancel-20260926-001")
        self.assertEqual({row["order_type"] for row in examples}, {"SINGLE", "BASKET"})
        self.assertEqual({row["execution"]["type"] for row in examples}, {"DIRECT", "SLICED"})


if __name__ == "__main__":
    unittest.main()
