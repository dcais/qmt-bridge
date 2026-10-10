#!/usr/bin/env python3
"""Flatten audited IPC source into readable Python 3.6 compatible GBK strategies."""
import argparse
import ast
import datetime as dt
import io
import re
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMMON = ('service/ipc_bridge/ipc_protocol/wire', 'service/ipc_bridge/ipc_protocol/winpipe', 'service/ipc_bridge/qmt_runtime/executor')
ROLE = {
    'feed': ('service/ipc_bridge/ipc_protocol/feed_contracts', 'service/ipc_bridge/qmt_runtime/feed/api'),
    'order': ('service/ipc_bridge/ipc_protocol/order_common', 'service/ipc_bridge/ipc_protocol/order_queries',
              'service/ipc_bridge/qmt_runtime/order/adapter', 'service/ipc_bridge/qmt_runtime/order/query'),
}
ALLOWED = {'collections', 'time', 'uuid', 'threading', 'ctypes', 'os', 'json',
           'struct', 'datetime', 'math', 'copy', 'hashlib', 're', 'decimal'}
PROJECT_ROOTS = {'service'}
BANNED_CALLS = {'open', '__import__', 'eval', 'exec', 'compile', 'input'}
BANNED_ATTRS = {'read_text', 'write_text', 'read_bytes', 'write_bytes', 'open',
                'connect', 'urlopen', 'socket', 'FileHandler', 'basicConfig'}
TIMESTAMP = re.compile(r'^# Last modified \(Asia/Shanghai\): .*?$', re.MULTILINE)
ENCODING = re.compile(r'^[ \t]*#.*?coding[:=][ \t]*[-\w.]+.*(?:\n|$)', re.MULTILINE)


def modules(role):
    return COMMON + ROLE[role] + ('service/ipc_bridge/qmt_runtime/entrypoint',
                                'service/ipc_bridge/qmt_runtime/' + role + '/entrypoint')


def audit(source, filename, project_imports=False):
    tree = ast.parse(source, filename=filename, feature_version=(3, 6))
    os_names = {'os'}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name.split('.')[0] for alias in node.names]
            os_names.update(alias.asname or alias.name for alias in node.names if alias.name == 'os')
        elif isinstance(node, ast.ImportFrom):
            names = [(node.module or '').split('.')[0]] if not node.level else []
            if node.level and not project_imports:
                raise ValueError('unexpected relative import in ' + filename)
        else:
            names = []
        allowed = ALLOWED | (PROJECT_ROOTS if project_imports else set())
        if set(names) - allowed:
            raise ValueError('unsafe import in {0}: {1}'.format(filename, names))
        if isinstance(node, ast.Call):
            fun = node.func
            if ((isinstance(fun, ast.Name) and fun.id in BANNED_CALLS) or
                    (isinstance(fun, ast.Attribute) and fun.attr in BANNED_ATTRS)):
                raise ValueError('unsafe call in {0}:{1}'.format(filename, node.lineno))
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            if node.value.id in os_names and node.attr != 'name':
                raise ValueError('unsafe os access in ' + filename)
    return tree


def flatten(source, filename, selected):
    tree = audit(source, filename, project_imports=True)
    lines = source.splitlines(keepends=True)
    remove = set()
    package = filename.replace('\\', '/').rsplit('/', 1)[0].split('/')
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level:
            module = '.'.join(package[:len(package) - node.level + 1] +
                              (node.module or '').split('.'))
        else:
            module = node.module or ''
        if node.level or module.split('.')[0] in PROJECT_ROOTS:
            if module not in selected:
                raise ValueError('source import is outside strategy manifest: ' + module)
            if any(alias.asname or alias.name == '*' for alias in node.names):
                raise ValueError('flatten requires explicit unaliased project imports: ' + filename)
            end = getattr(node, 'end_lineno', None)
            if end is None:
                end = next(token.end[0] for token in tokenize.generate_tokens(io.StringIO(source).readline)
                           if token.type == tokenize.NEWLINE and token.start[0] >= node.lineno)
            remove.update(range(node.lineno - 1, end))
    content = ''.join(line for index, line in enumerate(lines) if index not in remove)
    return TIMESTAMP.sub('', ENCODING.sub('', content)).strip(), tree


def render(role, timestamp='2000-01-01 00:00:00'):
    selected = {name.replace('/', '.') for name in modules(role)}
    sections, names = [], {}
    for name in modules(role):
        path = ROOT / (name + '.py')
        content, tree = flatten(path.read_text(encoding='utf-8-sig'), name + '.py', selected)
        check_bindings(ast.parse(content), name, names)
        sections.append('# ---- {0}.py ----\n{1}\n'.format(name, content))
    sections.append('def init(ContextInfo):\n    start_{0}(ContextInfo, globals())\n'.format(role))
    generated = ('# -*- coding: gbk -*-\n# Last modified (Asia/Shanghai): {0}\n'
                 '# Generated by tools/build_ipc_strategies.py; edit source modules instead.\n'
                 '# Parameters: bridge_id="default", account_id="66027616", pipe_name optional,\n'
                 '# schedule_interval_ms=10, schedule_budget_ms=50. Logs are console only.\n\n{1}').format(
                     timestamp, '\n'.join(sections))
    audit(generated, 'ipc_' + role + '.py')
    return generated.encode('gbk')


def check_bindings(tree, module, bindings):
    """Reject shared global names, except repeated identical stdlib imports."""
    def bind(name, signature=None):
        previous = bindings.get(name)
        if previous and previous[0] != module:
            if signature is None or previous[1] != signature:
                raise ValueError('top-level {0} bound in both {1} and {2}'.format(
                    name, previous[0], module))
        bindings[name] = (module, signature)

    def target_names(target):
        if isinstance(target, ast.Name):
            bind(target.id)
        elif isinstance(target, (ast.Tuple, ast.List)):
            for member in target.elts:
                target_names(member)

    def statements(body):
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                bind(node.name)
                continue
            if isinstance(node, ast.Import):
                for alias in node.names:
                    bind(alias.asname or alias.name.split('.')[0], ('import', alias.name))
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    bind(alias.asname or alias.name, ('from', node.module, alias.name))
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    target_names(target)
            elif isinstance(node, (ast.AnnAssign, ast.AugAssign, ast.For)):
                target_names(node.target)
            elif isinstance(node, ast.With):
                for item in node.items:
                    if item.optional_vars is not None:
                        target_names(item.optional_vars)
            for attribute in ('body', 'orelse', 'finalbody'):
                statements(getattr(node, attribute, []))
            for handler in getattr(node, 'handlers', []):
                if handler.name:
                    bind(handler.name)
                statements(handler.body)
    statements(tree.body)


def normalized(content):
    return TIMESTAMP.sub('# Last modified (Asia/Shanghai): <ignored>', content)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Check source parity, ignoring timestamp')
    args = parser.parse_args(argv)
    timestamp = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
    stale = False
    try:
        for role in ('feed', 'order'):
            output = ROOT / 'strategies' / ('ipc_' + role + '.py')
            expected = render(role, timestamp)
            current = output.read_bytes().decode('gbk') if output.is_file() else None
            same = current is not None and normalized(current) == normalized(expected.decode('gbk'))
            if args.check:
                stale = stale or not same
                print('{0}: {1}'.format(output.name, 'current' if same else 'stale'))
            elif not same:
                output.write_bytes(expected)
                print('written ' + output.name)
            else:
                print('unchanged ' + output.name)
    except (OSError, UnicodeError, SyntaxError, ValueError) as exc:
        parser.exit(2, 'IPC build failed: {0}\n'.format(exc))
    return int(stale)


if __name__ == '__main__':
    raise SystemExit(main())
