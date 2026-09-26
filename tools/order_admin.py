#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ORDER schema 与 UNKNOWN 人工管理；此程序从不调用 QMT。"""
import argparse
import getpass
import hashlib
import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from order_bridge.common import OrderError, iso_datetime, public_order  # noqa: E402
from order_bridge.repository import PostgresRepository  # noqa: E402
from order_bridge.state import recompute_order  # noqa: E402
from tools.order_schema import initialize_schema  # noqa: E402


ENV_CONFIG = {"pg_host": "ORDER_PG_HOST", "pg_port": "ORDER_PG_PORT",
              "pg_database": "ORDER_PG_DATABASE", "pg_user": "ORDER_PG_USER",
              "pg_password": "ORDER_PG_PASSWORD", "account_id": "ORDER_ACCOUNT_ID"}


def config_from_args(args):
    config = {}
    if args.config:
        config = json.loads(args.config.read_text(encoding="utf-8-sig"))
        if not isinstance(config, dict):
            raise ValueError("config must be a JSON object")
    if "pg_schema" in config or "PG_SCHEMA" in config or "ORDER_PG_SCHEMA" in os.environ:
        raise ValueError("remove pg_schema / PG_SCHEMA / ORDER_PG_SCHEMA; ORDER uses fixed schema qmt_order, "
                         "select separate pg_database values for simulated and live accounts")
    for key, env_name in ENV_CONFIG.items():
        if os.environ.get(env_name) is not None:
            config[key] = os.environ[env_name]
    if args.account_id:
        config["account_id"] = args.account_id
    required = ("pg_host", "pg_port", "pg_database", "pg_user", "pg_password", "account_id")
    missing = [key for key in required if key not in config or str(config[key]) == ""]
    if missing:
        raise ValueError("missing config keys: " + ", ".join(missing))
    config["pg_port"] = int(config["pg_port"])
    if not 1 <= config["pg_port"] <= 65535:
        raise ValueError("pg_port must be 1..65535")
    config["pg_schema"] = "qmt_order"  # Internal repository setting; not a CLI option.
    return config


def require_unknown(doc):
    if doc.get("submission_status") != "UNKNOWN":
        raise ValueError("order is not in UNKNOWN submission status")


def expected_version(doc, version):
    if int(doc.get("version", -1)) != version:
        raise ValueError("order version changed; inspect the latest record")


def read_evidence(path):
    content = path.read_bytes()
    if not content or len(content) > 1024 * 1024:
        raise ValueError("evidence file must contain 1..1048576 bytes")
    text = content.decode("utf-8-sig")
    try:
        value = json.loads(text)
    except ValueError:
        value = text
    return value, hashlib.sha256(content).hexdigest()


def observed_arguments(args):
    selected = args.observation_id or []
    if not selected or len(set(selected)) != len(selected) or any(value <= 0 for value in selected):
        raise ValueError("observed requires distinct positive --observation-id values")
    order_ids = args.qmt_order_id or []
    task_ids = args.qmt_task_id or []
    if not order_ids and not task_ids:
        raise ValueError("observed requires QMT order/task identifiers from raw observations")
    if len(order_ids) != len(set(order_ids)) or len(task_ids) != len(set(task_ids)):
        raise ValueError("QMT identifiers must be distinct")
    return sorted(selected), sorted(order_ids), sorted(task_ids)


def verify_not_submitted(evidence, doc):
    if not isinstance(evidence, dict) or evidence.get("proof_type") != "pre_call_boundary" or \
            evidence.get("qmt_call_never_started") is not True:
        raise ValueError("not-submitted needs JSON proof_type=pre_call_boundary and qmt_call_never_started=true")
    basis = evidence.get("basis")
    if not isinstance(basis, str) or len(basis.strip()) < 20:
        raise ValueError("not-submitted evidence needs a concrete positive basis")
    lower = basis.lower()
    if any(fragment in lower for fragment in ("no record", "not found", "query empty", "查不到", "无记录", "查询为空")):
        raise ValueError("absence of a QMT record does not prove no submission")
    if doc.get("qmt_orders") or doc.get("qmt_tasks") or doc.get("fills"):
        raise ValueError("existing QMT task/order/fill contradicts not-submitted")


def resolve(repo, args):
    evidence, digest = read_evidence(args.evidence_file)
    if not args.reason.strip():
        raise ValueError("resolution reason is required")
    doc = repo.get_by_id(args.order_id)
    require_unknown(doc)
    expected_version(doc, args.expected_version)
    if args.resolution == "observed":
        obs_ids, order_ids, task_ids = observed_arguments(args)
    else:
        verify_not_submitted(evidence, doc)
        obs_ids, order_ids, task_ids = [], [], []

    audit = {"operator": getpass.getuser(), "reason": args.reason,
             "evidence_sha256": digest, "evidence": evidence,
             "expected_version": args.expected_version, "resolution": args.resolution,
             "observation_ids": obs_ids, "qmt_order_ids": order_ids,
             "qmt_task_ids": task_ids, "at": iso_datetime()}
    if args.resolution == "observed":
        updated = repo.manual_associate(args.order_id, obs_ids, args.expected_version, audit)
        return {"order_id": updated["order_id"], "version": updated["version"],
                "submission_status": updated["submission_status"],
                "resolution": updated.get("resolution", "OBSERVED"), "evidence_sha256": digest}

    def mutate(current):
        require_unknown(current)
        expected_version(current, args.expected_version)
        if args.resolution == "not-submitted":
            verify_not_submitted(evidence, current)
        before = current["submission_status"]
        current["submission_status"] = "CONFIRMED" if args.resolution == "observed" else "REJECTED"
        current["resolution"] = "OBSERVED" if args.resolution == "observed" else "RESOLVED_NOT_SUBMITTED"
        if args.resolution == "not-submitted":
            current["error"] = {"code": "MANUALLY_CONFIRMED_NOT_SUBMITTED", "message": "QMT call never started"}
        current["reconcile_requested"] = False
        entry = dict(audit)
        entry.update(before_submission_status=before,
                     after_submission_status=current["submission_status"])
        current.setdefault("manual_resolutions", []).append(entry)
        recompute_order(current)
    updated = repo.update_order(args.order_id, "MANUAL_UNKNOWN_RESOLUTION", mutate)
    return {"order_id": updated["order_id"], "version": updated["version"],
            "submission_status": updated["submission_status"], "resolution": updated["resolution"],
            "evidence_sha256": digest}


def execute(repo, args):
    if args.command == "schema":
        return initialize_schema(repo) if args.action == "init" else repo.check_schema()
    if args.action == "list":
        if args.limit < 1 or args.limit > 1000:
            raise ValueError("limit must be 1..1000")
        selected, cursor = [], args.cursor
        while len(selected) < args.limit:
            page = repo.list_orders(active=False, limit=1000, cursor=cursor)
            for index, doc in enumerate(page["orders"]):
                if doc.get("submission_status") == "UNKNOWN":
                    selected.append(public_order(doc))
                    if len(selected) == args.limit:
                        more_here = any(row.get("submission_status") == "UNKNOWN"
                                        for row in page["orders"][index + 1:])
                        more = more_here or page["has_more"]
                        return {"orders": selected, "has_more": more,
                                "next_cursor": doc["order_id"] if more else None}
            if not page["has_more"]:
                return {"orders": selected, "has_more": False, "next_cursor": None}
            cursor = page["next_cursor"]
        return {"orders": selected, "has_more": False, "next_cursor": None}
    if args.action == "inspect":
        doc = repo.get_by_id(args.order_id)
        require_unknown(doc)
        return {"order": doc,
                "observations": repo.lookup_observations(args.order_id, include_unassociated=True)}
    if args.action == "reconcile":
        def mutate(doc):
            require_unknown(doc)
            expected_version(doc, args.expected_version)
            doc["reconcile_requested"] = True
        doc = repo.update_order(args.order_id, "MANUAL_RECONCILE_REQUESTED", mutate)
        return {"order_id": doc["order_id"], "version": doc["version"],
                "reconcile_requested": doc["reconcile_requested"]}
    return resolve(repo, args)


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--config", type=Path, help="JSON 配置文件路径")
    root.add_argument("--account-id", help="覆盖配置中的 ORDER_ACCOUNT_ID")
    commands = root.add_subparsers(dest="command")
    commands.required = True  # Python 3.6 的 add_subparsers 尚不接受 required 参数。
    schema = commands.add_parser("schema")
    schema.add_argument("action", choices=("init", "check"))
    unknown = commands.add_parser("unknown")
    actions = unknown.add_subparsers(dest="action")
    actions.required = True
    listing = actions.add_parser("list")
    listing.add_argument("--limit", type=int, default=100)
    listing.add_argument("--cursor")
    inspect = actions.add_parser("inspect")
    inspect.add_argument("--order-id", required=True)
    reconcile = actions.add_parser("reconcile")
    reconcile.add_argument("--order-id", required=True)
    reconcile.add_argument("--expected-version", required=True, type=int)
    resolution = actions.add_parser("resolve")
    resolution.add_argument("--order-id", required=True)
    resolution.add_argument("--expected-version", required=True, type=int)
    resolution.add_argument("--resolution", required=True, choices=("observed", "not-submitted"))
    resolution.add_argument("--reason", required=True)
    resolution.add_argument("--evidence-file", required=True, type=Path)
    resolution.add_argument("--observation-id", action="append", type=int)
    resolution.add_argument("--qmt-order-id", action="append")
    resolution.add_argument("--qmt-task-id", action="append")
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        config = config_from_args(args)
        repo = PostgresRepository(config, config["account_id"])
        result = execute(repo, args)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
        return 0
    except (OSError, UnicodeError, ValueError, OrderError) as exc:
        # 驱动异常由仓库层擦除；配置口令不进入错误文本。
        print("ORDER admin failed: " + str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
