# -*- coding: utf-8 -*-
# Last modified (Asia/Shanghai): 2026-09-26
"""ORDER 交付工具的无 QMT/PG 验证。"""
import importlib.util
import io
import json
from contextlib import redirect_stderr, redirect_stdout
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
        output = io.StringIO()
        with patch.object(tool.subprocess, "call") as call:
            with redirect_stdout(output):
                self.assertEqual(tool.main(["--dry-run", "--python36"]), 0)
            call.assert_not_called()
        command = output.getvalue()
        self.assertIn("--abi cp36m", command)
        self.assertIn("--platform win_amd64", command)
        self.assertIn("-r " + str(ROOT / "requirements-order.txt"), command)
        self.assertEqual((ROOT / "requirements-order.txt").read_text(encoding="utf-8").strip(),
                         "psycopg2-binary==2.9.5")
