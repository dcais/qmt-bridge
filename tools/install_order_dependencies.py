#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""安装 HTTP ORDER 的纯 Python PostgreSQL 驱动到隔离 vendor 目录。"""
import argparse
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", type=Path,
                        default=Path.home() / "qmt-bridge" / "vendor")
    parser.add_argument("--wheel-dir", type=Path,
                        help="离线 wheel 目录；使用 --no-index --find-links")
    parser.add_argument("--python36", action="store_true",
                        help="为 QMT Python 3.6 选择 cp36/abi3 wheel（在较新 Python 上执行）")
    parser.add_argument("--dry-run", action="store_true", help="仅打印 pip 命令")
    args = parser.parse_args(argv)
    requirements = ROOT / "requirements-order.txt"
    command = [sys.executable, "-m", "pip", "install", "--target", str(args.target),
               "--only-binary=:all:", "--no-compile", "--no-user"]
    if args.python36:
        command.extend(["--python-version", "3.6", "--implementation", "cp",
                        "--abi", "cp36m", "--abi", "abi3", "--abi", "none",
                        "--platform", "win_amd64", "--platform", "any"])
    if args.wheel_dir:
        if not args.wheel_dir.is_dir():
            parser.error("wheel directory does not exist")
        command.extend(["--no-index", "--find-links", str(args.wheel_dir)])
    command.extend(["-r", str(requirements)])
    print(" ".join(command), flush=True)
    if args.dry_run:
        return 0
    args.target.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PYTHONNOUSERSITE"] = "1"
    return subprocess.call(command, env=env)


if __name__ == "__main__":
    sys.exit(main())
