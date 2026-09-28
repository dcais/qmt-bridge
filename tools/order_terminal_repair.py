#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Last modified (Asia/Shanghai): 2026-09-28
"""凭完整证据修复被 RECONCILE_GAP 错误重开的 REJECTED 订单。

默认干运行；不调用 QMT。
"""
import argparse
import copy
import getpass
import hashlib
import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from order_bridge.common import OrderError, iso_datetime  # noqa: E402
from order_bridge.repository import PostgresRepository, repo_json  # noqa: E402
from order_bridge.state import (apply_observation, observation_identifiers,
                                recompute_order, reconcile_pending)  # noqa: E402
from tools.order_admin import config_from_args  # noqa: E402


# 只允许覆盖/运行检查点与派生执行状态变化。
NON_BUSINESS = frozenset((
    "version", "updated_at", "fact_version", "last_reconcile_round",
    "reconcile_round_fact_version", "last_reconcile_attempt_at", "last_reconciled_at",
    "reconcile_due_at", "reconcile_priority", "reconcile_requested", "reconcile_pending",
    "last_reconcile_gap_since", "reconciliation_complete", "sync_status",
    "execution_status", "terminal_facts_fingerprint", "basket_cleanup_eligible"))


def require(condition, message):
    if not condition:
        raise OrderError(409, "TERMINAL_REPAIR_PROOF_FAILED", message)


def business_facts(doc):
    result = {key: copy.deepcopy(value) for key, value in doc.items() if key not in NON_BUSINESS}
    for item in result.get("items", []):
        item.pop("execution_status", None)
    return result


def successful_terminal(doc):
    return (doc.get("submission_status") == "CONFIRMED"
            and doc.get("execution_status") == "REJECTED"
            and doc.get("reconciliation_complete") is True
            and doc.get("sync_status") == "COMPLETE"
            and not doc.get("reconcile_pending") and not doc.get("reconcile_requested")
            and not doc.get("unassociated_evidence")
            and not doc.get("cancel_requested")
            and not doc.get("fills")
            and bool(doc.get("last_reconciled_at"))
            and bool(doc.get("qmt_orders")) and bool(doc.get("qmt_tasks"))
            and all(item.get("execution_status") == "REJECTED" for item in doc.get("items", []))
            and all(row.get("terminal") and row.get("status") == "REJECTED"
                    for row in doc.get("qmt_orders", []))
            and all(row.get("terminal") for row in doc.get("qmt_tasks", []))
            and not any(row.get("kind") in ("CANCEL", "CANCEL_ORDER", "CANCEL_TASK")
                        and row.get("status") in ("CALLING", "RETURNED", "UNKNOWN")
                        for row in doc.get("attempts", [])))


def relevant_observation(doc, kind, raw, order_id):
    if order_id is not None:
        return order_id == doc["order_id"]
    ids = observation_identifiers(kind, raw)
    if ids.get("account_id") and str(ids["account_id"]) != str(doc["account_id"]):
        return False
    marker = ids.get("remark") or raw.get("strategyName")
    if marker and str(marker) != doc["remark"]:
        return False
    for name, collection in (("qmt_order_id", "qmt_orders"), ("qmt_task_id", "qmt_tasks")):
        if ids.get(name) and any(str(row.get(name)) == str(ids[name]) for row in doc[collection]):
            return True
    if marker == doc["remark"]:
        return True
    # 同账户同证券但缺少可区分身份的观测，须保守阻止修复。
    if kind == "error" and not marker:
        raise ValueError("ambiguous newer unassociated error observation")
    if any(ids.get("symbol") == item.get("symbol") and ids.get("side") == item.get("side")
           for item in doc["items"]):
        raise ValueError("ambiguous newer unassociated observation")
    return False


def validate(current, events, observations, checkpoint_seq):
    require(events and events[0][0] == checkpoint_seq, "checkpoint event is missing")
    seq, event_type, occurred_at, checkpoint = events[0]
    require(event_type == "RECONCILE_STATE_CHANGED", "checkpoint is not a successful reconciliation event")
    require(checkpoint.get("order_id") == current.get("order_id") and successful_terminal(checkpoint),
            "checkpoint is not a complete REJECTED terminal order")
    require(current.get("version") is not None and current.get("execution_status") == "INCOMPLETE"
            and current.get("reconciliation_complete") is False
            and current.get("sync_status") == "INCOMPLETE",
            "current order is not the reopened INCOMPLETE state")
    require(current.get("submission_status") == "CONFIRMED"
            and current.get("last_reconciled_at") == checkpoint.get("last_reconciled_at"),
            "successful reconciliation timestamp or submission changed")
    expected = business_facts(checkpoint)
    require(business_facts(current) == expected, "current business facts differ from terminal checkpoint")
    require(any(row[1] == "RECONCILE_GAP" for row in events[1:]), "no intervening RECONCILE_GAP")
    previous_fact_version = checkpoint.get("fact_version", 0)
    for event_seq, kind, _, doc in events[1:]:
        require(kind in ("RECONCILE_GAP", "RECONCILE_STATE_CHANGED"),
                "unexpected intervening event at seq {}".format(event_seq))
        require(business_facts(doc) == expected, "business facts changed at seq {}".format(event_seq))
        expected_fact_version = previous_fact_version + (kind == "RECONCILE_GAP")
        require(doc.get("fact_version") == expected_fact_version,
                "fact version changed outside RECONCILE_GAP at seq {}".format(event_seq))
        previous_fact_version = expected_fact_version
    require(current.get("fact_version") == previous_fact_version,
            "current fact version differs from event history")
    checked = []
    for observation_id, kind, raw, source, observed_at, applied, order_id in observations:
        if not relevant_observation(checkpoint, kind, raw, order_id):
            continue
        trial = copy.deepcopy(checkpoint)
        apply_observation(trial, kind, raw, source, observed_at=iso_datetime(observed_at)
                          if not isinstance(observed_at, str) else observed_at)
        require(business_facts(trial) == expected
                and not trial.get("unassociated_evidence"),
                "newer observation {} changes terminal facts".format(observation_id))
        checked.append(observation_id)
    return {"checkpoint_seq": seq, "checkpoint_at": iso_datetime(occurred_at)
            if not isinstance(occurred_at, str) else occurred_at,
            "checked_observation_ids": checked,
            "intervening_event_seqs": [row[0] for row in events[1:]]}


def repair(repo, args):
    require(args.reason and args.reason.strip(), "reason is required")
    require(args.expected_version >= 0 and args.checkpoint_event_seq > 0,
            "expected version and checkpoint seq must be positive")
    require(not args.apply or args.evidence_out is not None, "--apply requires --evidence-out")

    def transaction(cur):
        if not args.apply:
            cur.execute("SET TRANSACTION READ ONLY")
        current = repo.repo_require(repo.repo_load(cur, args.order_id, lock=args.apply))
        require(current.get("version") == args.expected_version,
                "order version changed; inspect latest record")
        cur.execute("SELECT event_seq,event_type,occurred_at,document FROM " + repo.repo_s +
                    ".order_events WHERE account_type=%s AND account_id=%s AND order_id=%s "
                    "AND event_seq>=%s ORDER BY event_seq",
                    repo.repo_scope + (args.order_id, args.checkpoint_event_seq))
        events = [(row[0], row[1], row[2], repo_json(row[3])) for row in cur.fetchall()]
        require(events and events[0][0] == args.checkpoint_event_seq, "checkpoint event is missing")
        occurred_at = events[0][2]
        cur.execute("SELECT observation_id,kind,raw,source,observed_at,applied,order_id FROM " + repo.repo_s +
                    ".qmt_observations WHERE account_type=%s AND account_id=%s "
                    "AND (order_id=%s OR order_id IS NULL) "
                    "AND observed_at::timestamptz>%s::timestamptz ORDER BY observation_id",
                    repo.repo_scope + (args.order_id, occurred_at))
        observations = [(row[0], row[1], repo_json(row[2]), row[3], row[4], row[5], row[6])
                        for row in cur.fetchall()]
        proof = validate(current, events, observations, args.checkpoint_event_seq)
        result = {"order_id": args.order_id, "mode": "apply" if args.apply else "dry-run",
                  "expected_version": args.expected_version, "proof": proof}
        if not args.apply:
            return result
        receipt = {"captured_at": iso_datetime(), "account_type": repo.account_type,
                   "account_id": repo.account_id, "order_id": args.order_id,
                   "operator": getpass.getuser(), "reason": args.reason.strip(),
                   "before": current, "checkpoint": events[0][3], "events": [
                       {"seq": row[0], "type": row[1], "at": row[2], "document": row[3]}
                       for row in events],
                   "observations": [{"id": row[0], "kind": row[1], "raw": row[2],
                                     "source": row[3], "observed_at": row[4], "applied": row[5],
                                     "order_id": row[6]} for row in observations], "proof": proof}
        payload = json.dumps(receipt, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()
        with args.evidence_out.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        before_state = {key: current.get(key) for key in
                        ("version", "fact_version", "execution_status", "sync_status",
                         "reconciliation_complete", "reconcile_pending", "reconcile_requested")}
        current["reconciliation_complete"] = True
        current["reconcile_requested"] = False
        current["reconcile_priority"] = False
        # 让修复前启动的旧对账轮次失效。
        current["fact_version"] += 1
        recompute_order(current)
        require(current["execution_status"] == "REJECTED" and not reconcile_pending(current)
                and current["sync_status"] == "COMPLETE", "recomputed order is not terminal")
        current["reconcile_pending"] = False
        after_state = {key: current.get(key) for key in before_state}
        after_state["version"] = args.expected_version + 1
        current.setdefault("terminal_repairs", []).append({
            "operator": getpass.getuser(), "reason": args.reason.strip(),
            "checkpoint_event_seq": args.checkpoint_event_seq,
            "before": before_state, "after": after_state,
            "evidence_path": str(args.evidence_out.resolve()), "evidence_sha256": digest,
            "at": iso_datetime()})
        repo.repo_save(cur, current, "MANUAL_TERMINAL_REPAIR", child_fields=("items",))
        result.update(version=current["version"], execution_status=current["execution_status"],
                      evidence_sha256=digest, evidence_path=str(args.evidence_out.resolve()))
        return result

    return repo.repo_run(transaction, mutation=args.apply)


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--config", type=Path, help="JSON configuration; ORDER_* env overrides")
    root.add_argument("--account-id", help="override ORDER_ACCOUNT_ID")
    root.add_argument("--order-id", required=True)
    root.add_argument("--expected-version", required=True, type=int)
    root.add_argument("--checkpoint-event-seq", required=True, type=int)
    root.add_argument("--reason", required=True)
    root.add_argument("--evidence-out", type=Path)
    root.add_argument("--apply", action="store_true")
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        config = config_from_args(args)
        repo = PostgresRepository(config, config["account_id"])
        print(json.dumps(repair(repo, args), ensure_ascii=False, sort_keys=True, default=str))
        return 0
    except (OSError, UnicodeError, ValueError, OrderError) as exc:
        print("ORDER terminal repair failed: " + str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
