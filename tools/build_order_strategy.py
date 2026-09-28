#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 ORDER 源模块合并成可直接导入 QMT 的 GBK 单文件策略。"""
import argparse
import ast
import datetime as dt
import io
import re
import sys
import tokenize
from pathlib import Path


MODULES = ("common", "contracts", "state", "storage_schema", "repository", "qmt", "async_log", "background", "runtime", "http")
ROOT = Path(__file__).resolve().parents[1]
TIMESTAMP = re.compile(r"^# Last modified \(Asia/Shanghai\): .*?$", re.MULTILINE)
ENCODING = re.compile(r"^[ \t]*#.*?coding[:=][ \t]*[-\w.]+.*(?:\n|$)", re.MULTILINE)


def _without_relative_imports(source, name):
    tree = ast.parse(source, filename=name)
    lines = source.splitlines(keepends=True)
    remove = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level:
            end = getattr(node, "end_lineno", None)
            if end is None:  # Python 3.6 AST 尚无 end_lineno。
                end = next(token.end[0] for token in tokenize.generate_tokens(io.StringIO(source).readline)
                           if token.type == tokenize.NEWLINE and token.start[0] >= node.lineno)
            remove.update(range(node.lineno - 1, end))
    return "".join(line for index, line in enumerate(lines) if index not in remove), tree


def render(source_dir, timestamp):
    names = {}
    sections = []
    for module in MODULES:
        path = source_dir / (module + ".py")
        original = path.read_text(encoding="utf-8-sig")
        content, tree = _without_relative_imports(original, str(path))
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                if node.name in names:
                    raise ValueError("top-level {0} is defined in both {1} and {2}".format(
                        node.name, names[node.name], module))
                names[node.name] = module
        content = ENCODING.sub("", content)
        content = TIMESTAMP.sub("", content)
        sections.append("# ---- order_bridge/{0}.py ----\n{1}".format(module, content.strip() + "\n"))
    generated = "# -*- coding: gbk -*-\n# Last modified (Asia/Shanghai): {0}\n\n{1}".format(
        timestamp, "\n".join(sections))
    generated.encode("gbk")
    ast.parse(generated)
    return generated


def normalized(content):
    return TIMESTAMP.sub("# Last modified (Asia/Shanghai): <ignored>", content)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="仅判断现有策略与源模块是否一致，忽略时间戳")
    parser.add_argument("--source-dir", type=Path, default=ROOT / "order_bridge")
    parser.add_argument("--output", type=Path, default=ROOT / "strategies" / "http_order.py")
    args = parser.parse_args(argv)
    zone = dt.timezone(dt.timedelta(hours=8))
    timestamp = dt.datetime.now(zone).strftime("%Y-%m-%d %H:%M:%S")
    try:
        expected = render(args.source_dir, timestamp)
        current = args.output.read_bytes().decode("gbk") if args.output.is_file() else None
    except (OSError, UnicodeError, SyntaxError, ValueError) as exc:
        parser.exit(2, "ORDER build failed: {0}\n".format(exc))
    same = current is not None and normalized(current) == normalized(expected)
    if args.check:
        print("ORDER strategy {0}: {1}".format("current" if same else "stale", args.output))
        return 0 if same else 1
    if same:
        print("ORDER strategy unchanged: {0}".format(args.output))
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(expected.encode("gbk"))
    print("ORDER strategy written: {0}".format(args.output))
    return 0


if __name__ == "__main__":
    sys.exit(main())
