#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Last modified (Asia/Shanghai): 2026-10-14
"""凭完整事件证据恢复被 RECONCILE_GAP 重开的已闭合终态订单。

默认干运行；不调用 QMT。
"""
import argparse
import copy
import getpass
import hashlib
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.ipc_bridge.ipc_protocol.order_common import (OrderError, fingerprint,
                                                       iso_datetime, parse_timestamp)  # noqa: E402
from service.ipc_bridge.order.journal_storage import journal_unpack  # noqa: E402
from service.ipc_bridge.order.repository import PostgresRepository, repo_json, repo_timestamp  # noqa: E402
from service.ipc_bridge.order.state import (apply_observation, observation_identifiers,
                                mark_reconciled, reconcile_pending, recompute_order,
                                state_execution_finished, state_reconciliation_facts,
                                is_order_active, state_value, state_fill_cent_precision,
                                state_float_representation_equal, state_nonnegative_decimal)  # noqa: E402
from tools.order_admin import config_from_args  # noqa: E402
from service.ipc_bridge.order.event_correction import correction_source_refs, correction_notice  # noqa: E402


# 只允许覆盖/运行检查点与派生执行状态变化。
NON_BUSINESS = frozenset((
    "version", "updated_at", "fact_version", "last_reconcile_round",
    "reconcile_round_fact_version", "last_reconcile_attempt_at", "last_reconciled_at",
    "reconcile_due_at", "reconcile_priority", "reconcile_requested", "reconcile_pending",
    "last_reconcile_gap_since", "reconciliation_complete", "sync_status",
    "execution_status", "terminal_facts_fingerprint", "basket_cleanup_eligible",
    "cancel_status", "terminal_reconciliation_checkpoint"))
TERMINAL = frozenset(("FILLED", "CANCELLED", "PARTIALLY_CANCELLED", "REJECTED"))


def require(condition, message):
    if not condition:
        raise OrderError(409, "TERMINAL_REPAIR_PROOF_FAILED", message)


def business_facts(doc):
    result = {key: copy.deepcopy(value) for key, value in doc.items() if key not in NON_BUSINESS}
    for item in result.get("items", []):
        item.pop("execution_status", None)
    for request in result.get("cancel_requests", []):
        request.pop("status", None)
    return result


def successful_terminal(doc):
    if (doc.get("execution_status") not in TERMINAL
            or doc.get("reconciliation_complete") is not True
            or doc.get("sync_status") != "COMPLETE"
            or doc.get("reconcile_pending") or doc.get("reconcile_requested")
            or not doc.get("last_reconciled_at") or not state_execution_finished(doc)):
        return False
    if any(row.get("status") not in ("CONFIRMED", "NOT_NEEDED", "REJECTED")
           for row in doc.get("cancel_requests", [])):
        return False
    for attempt in doc.get("attempts", []):
        if attempt.get("kind") not in ("CANCEL", "CANCEL_ORDER", "CANCEL_TASK"):
            continue
        if attempt.get("status") == "CALLING":
            return False
        if attempt.get("status") not in ("RETURNED", "UNKNOWN"):
            continue
        kind = attempt.get("kind")
        if kind == "CANCEL":
            return False
        if not doc.get("cancel_requested") or not any(
                attempt.get("cancel_request_id") in (
                    row.get("cancel_request_id"), row.get("canonical_cancel_request_id"))
                and attempt.get("cancel_request_id")
                for row in doc.get("cancel_requests", [])):
            return False
        field, collection = (("qmt_order_id", "qmt_orders") if kind == "CANCEL_ORDER"
                             else ("qmt_task_id", "qmt_tasks"))
        if not attempt.get("target_id") or not any(
                str(row.get(field)) == str(attempt["target_id"]) and row.get("terminal")
                for row in doc.get(collection, [])):
            return False
    candidate = copy.deepcopy(doc)
    recompute_order(candidate)
    return (candidate.get("execution_status") == doc["execution_status"]
            and state_execution_finished(candidate)
            and not reconcile_pending(candidate)
            and business_facts(candidate) == business_facts(doc))


def validate_internal_checkpoint(doc):
    """An internal proof is checked, but never substitutes for the audited event."""
    if "terminal_reconciliation_checkpoint" not in doc:
        return
    checkpoint = doc["terminal_reconciliation_checkpoint"]
    require(isinstance(checkpoint, dict)
            and checkpoint.get("facts_fingerprint") == state_reconciliation_facts(doc)
            and checkpoint.get("reconciled_at") == doc.get("last_reconciled_at")
            and bool(checkpoint.get("reconciled_at")),
            "internal terminal checkpoint differs from audited facts")


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
            "checkpoint is not a complete terminal order")
    require(current.get("version") is not None and current.get("execution_status") in TERMINAL | {"INCOMPLETE"}
            and current.get("reconciliation_complete") is False
            and current.get("sync_status") == "INCOMPLETE",
            "current order is not the reopened INCOMPLETE state")
    require(current.get("submission_status") == "CONFIRMED"
            and current.get("last_reconciled_at") == checkpoint.get("last_reconciled_at"),
            "successful reconciliation timestamp or submission changed")
    expected = business_facts(checkpoint)
    require(business_facts(current) == expected, "current business facts differ from terminal checkpoint")
    validate_internal_checkpoint(current)
    validate_internal_checkpoint(checkpoint)
    require(any(row[1] == "RECONCILE_GAP" for row in events[1:]), "no intervening RECONCILE_GAP")
    previous_fact_version = checkpoint.get("fact_version", 0)
    for event_seq, kind, _, doc in events[1:]:
        require(kind in ("RECONCILE_GAP", "RECONCILE_STATE_CHANGED"),
                "unexpected intervening event at seq {}".format(event_seq))
        require(business_facts(doc) == expected, "business facts changed at seq {}".format(event_seq))
        validate_internal_checkpoint(doc)
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


def equivalent_facts(doc):
    facts = business_facts(doc)
    facts.pop("unassociated_evidence", None)
    return facts


def require_internal_document(doc):
    # Public ORDER responses omit attempts and child metadata, and cannot prove closure.
    require(all(key in doc for key in ("attempts", "remark", "request_hash", "request",
            "items", "qmt_orders", "qmt_tasks", "fills", "cancel_requests",
            "unassociated_evidence", "resolved_evidence")),
            "complete internal order document is required")


def is_stripped_terminal_observation(kind, raw):
    required = ("m_nTaskId", "m_strRemark", "m_strSource", "m_strOrderStrategyType", "m_strXTTrade")
    return (kind in ("deal", "order") and all(key in raw for key in required)
            and raw.get("m_nTaskId") in (-1, "-1")
            and raw.get("m_strRemark") == ""
            and raw.get("m_strSource") == ""
            and raw.get("m_strOrderStrategyType") == ""
            and raw.get("m_strXTTrade") == "其他终端")


def scoped_order(checkpoint, kind, raw):
    ids = observation_identifiers(kind, raw)
    require(kind in ("order", "deal"), "only order/deal query duplicates are supported")
    stripped = is_stripped_terminal_observation(kind, raw)
    require(ids.get("account_id") == checkpoint["account_id"]
            and (stripped or ids.get("remark") == checkpoint["remark"]),
            "query account or remark differs")
    require(all(ids.get(key) for key in ("trading_day", "market", "qmt_order_id", "symbol", "side")),
            "query identity scope is missing")
    matches = [row for row in checkpoint["qmt_orders"] if all(
        ids[key] == row.get(key) for key in ("trading_day", "market", "qmt_order_id",
                                            "symbol", "side"))]
    require(len(matches) == 1, "query order scope is ambiguous or differs")
    order = matches[0]
    if stripped:
        require(bool(checkpoint.get("remark")) and bool(order.get("qmt_task_id")),
                "stripped observation requires non-empty checkpoint remark and task identity")
    else:
        require(ids.get("qmt_task_id") == order.get("qmt_task_id"), "query task identity differs")
    items = [item for item in checkpoint["items"] if item.get("symbol") == ids["symbol"]
             and item.get("side") == ids["side"]]
    require(len(items) == 1 and items[0]["item_id"] == order.get("item_id"),
            "query item identity is ambiguous or differs")
    return ids, order, stripped


def prove_query_noop(checkpoint, kind, raw, source, observed_at, fill_precision=False):
    ids, order, stripped = scoped_order(checkpoint, kind, raw)
    alias = (raw.get("m_nRef") in (None, "", 0, "0")
             and ids["market"] in ("SH", "SZ")
             and raw.get("m_strOrderRef") == ids["market"] + "_" + ids["qmt_order_id"])
    allowed_callback = ((kind == "order" and alias)
                        or (kind == "deal" and (alias or stripped)))
    require(source == "query" or (source == "callback" and allowed_callback),
            "newer observation is not a query")
    known = order
    if kind == "deal":
        fills = [row for row in checkpoint["fills"] if row.get("fill_key") ==
                 [ids["trading_day"], ids["market"], ids.get("trade_id")]]
        require(len(fills) == 1 and fills[0].get("qmt_order_id") == ids["qmt_order_id"]
                and fills[0].get("item_id") == order.get("item_id"), "fill identity differs")
        known = fills[0]
    known_raw = known.get("raw")
    require(isinstance(known_raw, dict) and known_raw, "checkpoint lacks original raw observation")
    references = (raw.get("m_nRef"), raw.get("m_strOrderRef"))
    native = (known_raw.get("m_nRef"), known_raw.get("m_strOrderRef"))
    missing_ref = raw.get("m_nRef") in (0, "0") and raw.get("m_strOrderRef") == native[1]
    require(references == native or missing_ref or alias,
            "query references are neither original nor derived")
    ignored = {"m_nRef", "m_strOrderRef", "m_nOrderStrategyType"}
    if kind == "order":
        ignored.add("m_strInsertTime")
    if stripped:
        ignored.update(("m_nTaskId", "m_strRemark", "m_strSource", "m_strOrderStrategyType", "m_strXTTrade"))
    amount_fields = {"amount", "filled_amount", "m_dTradeAmount"}
    float_price_fields = {"m_dPrice", "m_dTradedPrice"}
    cent_precision = (fill_precision and kind == "deal"
                      and state_fill_cent_precision(checkpoint, known, raw, source))
    if fill_precision and kind == "deal":
        ignored.update(("m_dCommission", "m_dComssion", "m_dComission"))
    for key in set(raw) | set(known_raw):
        if key in ignored:
            continue
        # Deal commission float representations differ in the original callback
        # and query; permit representation noise only, not a changed fee.
        tolerance = (Decimal("0.001") if key in amount_fields else
                     Decimal("0.000000000001") if key in ("m_dCommission", "m_dComssion") else None)
        if tolerance is None and key in float_price_fields and key in raw and key in known_raw:
            left_dec = state_nonnegative_decimal(raw.get(key))
            right_dec = state_nonnegative_decimal(known_raw.get(key))
            same = (left_dec is not None and right_dec is not None
                    and state_float_representation_equal(left_dec, right_dec))
        elif (tolerance is None and key == "m_nOpType" and kind == "order" and stripped
              and key in raw and key in known_raw):
            same = (raw.get(key) == known_raw.get(key)
                    or raw.get(key) in (2147483647, "2147483647"))
        elif tolerance is None:
            same = raw.get(key) == known_raw.get(key)
        else:
            try:
                left, right = Decimal(str(raw.get(key))), Decimal(str(known_raw.get(key)))
                same = (left.is_finite() and right.is_finite() and left >= 0 and right >= 0
                        and (abs(left - right) <= tolerance
                             or (cent_precision and key in amount_fields
                                 and left == Decimal(str(state_value(
                                     raw, "amount", "filled_amount", "m_dTradeAmount")))
                                 and right == Decimal(str(known["amount"])))))
            except (InvalidOperation, ValueError, TypeError):
                same = False
        require(same, "query raw field {} changes audited facts".format(key))
    trial = copy.deepcopy(checkpoint)
    apply_observation(trial, kind, raw, source, observed_at=observed_at)
    require(trial == checkpoint, "query changes audited terminal facts")
    return ids, order


def prove_equivalent_evidence(checkpoint, evidence, observations, fill_precision=False):
    kind, raw = evidence.get("kind"), evidence.get("raw")
    reason = evidence.get("reason")
    require(isinstance(raw, dict) and (kind, reason) in (
        ("deal", "CONFLICTING_FILL_IDENTITY"), ("order", "CONFLICTING_ORDER_REFERENCE")),
        "evidence is not an approved query conflict")
    require(evidence.get("evidence_id") == fingerprint({"kind": kind, "raw": raw, "reason": reason}),
            "evidence identifier differs from raw facts")
    matching = [row for row in observations if row[1] == kind and row[2] == raw
                and row[3] == evidence.get("source")
                and parse_timestamp(repo_timestamp(row[4])) == parse_timestamp(evidence.get("observed_at"))
                and row[6] == checkpoint["order_id"] and row[5] is True]
    require(len(matching) == 1, "conflict lacks a unique applied raw observation")
    ids, order = prove_query_noop(checkpoint, kind, raw, evidence["source"], evidence["observed_at"],
                                  fill_precision=fill_precision)
    if kind == "deal":
        fills = [row for row in checkpoint["fills"]
                 if row.get("fill_key") == [ids["trading_day"], ids["market"], ids.get("trade_id")]]
        require(len(fills) == 1 and fills[0].get("qmt_order_id") == ids["qmt_order_id"]
                and fills[0].get("item_id") == order.get("item_id"), "fill identity differs")
        try:
            amount = Decimal(str(state_value(raw, "amount", "filled_amount", "m_dTradeAmount")))
            delta = abs(amount - Decimal(fills[0]["amount"]))
            quantity = Decimal(str(state_value(raw, "quantity", "filled_quantity", "m_nVolume")))
            equivalent = (amount.is_finite() and amount >= 0 and delta.is_finite()
                          and (delta <= Decimal("0.001")
                               or (fill_precision and state_fill_cent_precision(
                                   checkpoint, fills[0], raw, evidence["source"])))
                          and quantity == fills[0]["quantity"])
        except (InvalidOperation, ValueError, TypeError):
            equivalent = False
        require(equivalent, "fill economics do not satisfy approved amount rule"
                if fill_precision else "fill economics differ beyond 0.001 yuan")
        rule = ("QUERY_FILL_CALLBACK_QUERY_CENT_PRECISION" if fill_precision
                and delta > Decimal("0.001") else "QUERY_FILL_AMOUNT_ABS_DELTA_LE_0_001")
    else:
        require(raw.get("m_nRef") in (0, "0")
                and raw.get("m_strOrderRef") == ids["market"] + "_" + ids["qmt_order_id"]
                and order.get("native_ref") and order.get("native_order_ref"),
                "query reference is not a derived market/sysid alias")
        rule = "QUERY_DERIVED_ORDER_REFERENCE_SAME_ECONOMICS"
    return {"evidence_id": evidence["evidence_id"], "observation_id": matching[0][0],
            "reason": reason, "rule": rule, "qmt_order_id": ids["qmt_order_id"],
            "scope": {key: ids[key] for key in ("account_id", "trading_day", "market")}}


def validate_unique_order_scope(checkpoint, account_orders):
    checked = []
    for known in checkpoint["qmt_orders"]:
        scope = tuple(known.get(key) for key in ("trading_day", "market", "qmt_order_id"))
        require(all(scope), "checkpoint order scope is missing")
        owners = []
        for doc in account_orders:
            for row in doc.get("qmt_orders", []):
                if row.get("qmt_order_id") != scope[2]:
                    continue
                if all(not row.get(key) or row.get(key) == scope[index]
                       for index, key in enumerate(("trading_day", "market"))):
                    require(row.get("trading_day") and row.get("market"),
                            "order identity collision has incomplete scope")
                    owners.append(doc["order_id"])
        require(owners == [checkpoint["order_id"]], "order identity collision across account orders")
        checked.append({"trading_day": scope[0], "market": scope[1], "qmt_order_id": scope[2],
                        "owner_order_id": owners[0]})
    return checked


def validate_equivalent(current, events, observations, checkpoint_seq, account_orders,
                        fill_precision=False):
    require(events and events[0][0] == checkpoint_seq, "checkpoint event is missing")
    seq, kind, occurred_at, checkpoint = events[0]
    require_internal_document(checkpoint)
    require_internal_document(current)
    require(kind == "RECONCILE_STATE_CHANGED", "checkpoint is not a successful reconciliation event")
    require(checkpoint.get("order_id") == current.get("order_id") and successful_terminal(checkpoint),
            "checkpoint is not a complete terminal order")
    validate_internal_checkpoint(checkpoint)
    require(current.get("sync_status") == "INCOMPLETE" and current.get("reconcile_pending") is True
            and bool(current.get("unassociated_evidence")),
            "current order is not the reopened INCOMPLETE state")
    uniqueness = validate_unique_order_scope(checkpoint, account_orders)
    expected = equivalent_facts(checkpoint)
    previous = checkpoint
    previous_seq = seq
    event_proofs = []
    proofs = {}
    for event_seq, event_kind, _, doc in events[1:]:
        require_internal_document(doc)
        require(event_seq > previous_seq and event_kind in (
            "RECONCILE_GAP", "RECONCILE_STATE_CHANGED", "QMT_OBSERVATION"),
            "unexpected intervening event at seq {}".format(event_seq))
        require(equivalent_facts(doc) == expected,
                "business facts changed at seq {}".format(event_seq))
        require(doc.get("terminal_reconciliation_checkpoint") == checkpoint.get("terminal_reconciliation_checkpoint"),
                "historical internal terminal checkpoint changed")
        old, new = previous["unassociated_evidence"], doc["unassociated_evidence"]
        require(new[:len(old)] == old, "existing evidence changed or disappeared")
        added = new[len(old):]
        require(bool(added) == (event_kind == "QMT_OBSERVATION"),
                "QMT_OBSERVATION must add only approved equivalent evidence")
        fv_delta = 1 if (event_kind == "RECONCILE_GAP" or (
            event_kind == "QMT_OBSERVATION" and any(row.get("source") != "query" for row in added))) else 0
        require(doc.get("fact_version") == previous.get("fact_version", 0) + fv_delta,
                "fact version changed outside approved query/gap events")
        require(doc["version"] > previous["version"], "event document version is not increasing")
        for evidence in added:
            proof = prove_equivalent_evidence(checkpoint, evidence, observations,
                                              fill_precision=fill_precision)
            require(proof["evidence_id"] not in proofs, "duplicate conflict evidence")
            proofs[proof["evidence_id"]] = proof
        event_proofs.append({"event_seq": event_seq, "type": event_kind,
                             "equivalent_business_facts": True,
                             "added_evidence_ids": [row["evidence_id"] for row in added]})
        previous, previous_seq = doc, event_seq
    require(equivalent_facts(current) == expected
            and current["unassociated_evidence"] == previous["unassociated_evidence"]
            and current.get("fact_version") == previous.get("fact_version")
            and current["version"] >= previous["version"]
            and current.get("terminal_reconciliation_checkpoint") == checkpoint.get("terminal_reconciliation_checkpoint"),
            "current differs from audited event history beyond runtime checkpoints")
    require(current["unassociated_evidence"] and len(proofs) == len(current["unassociated_evidence"]),
            "not all current conflicts were proved")
    checked = []
    for observation_id, obs_kind, raw, source, observed_at, applied, order_id in observations:
        if not relevant_observation(checkpoint, obs_kind, raw, order_id):
            # A different owner cannot share this exact scoped QMT order identity.
            ids = observation_identifiers(obs_kind, raw)
            require(not any(all(ids.get(key) == row.get(key) for key in (
                "trading_day", "market", "qmt_order_id")) for row in checkpoint["qmt_orders"]),
                "raw observation is associated with a colliding order")
            continue
        stamp = iso_datetime(observed_at) if not isinstance(observed_at, str) else observed_at
        require(applied is True, "pending unapplied observation {} blocks repair".format(observation_id))
        prove_query_noop(checkpoint, obs_kind, raw, source, stamp, fill_precision=fill_precision)
        checked.append({"observation_id": observation_id, "kind": obs_kind, "source": source,
                        "applied": applied, "order_id": order_id, "equivalent": True,
                        **({"deal_amount_policy": "SCOPED_CALLBACK_QUERY_CENT_PRECISION",
                            "ignored_raw_fields": ["m_dCommission", "m_dComssion", "m_dComission"]}
                           if fill_precision and obs_kind == "deal" else {})})
    return {"checkpoint_seq": seq, "checkpoint_at": str(occurred_at),
            "historical_reconciled_at": checkpoint["last_reconciled_at"],
            "checked_observation_ids": [row["observation_id"] for row in checked],
            "observation_proofs": checked, "evidence_proofs": list(proofs.values()),
            "event_proofs": event_proofs, "unique_order_scopes": uniqueness,
            "intervening_event_seqs": [row[0] for row in events[1:]]}


def repaired_candidate(current, checkpoint, proof, equivalent, fill_precision=False):
    candidate = copy.deepcopy(current)
    if equivalent:
        decisions = {row["evidence_id"]: row for row in proof["evidence_proofs"]}
        for evidence in candidate["unassociated_evidence"]:
            resolved = {
                "evidence_id": evidence["evidence_id"],
                "kind": evidence["kind"],
                "source": evidence["source"],
                "reason": evidence["reason"],
                "observed_at": evidence["observed_at"],
                "raw_sha256": fingerprint(evidence["raw"]),
                "resolution": {
                    "kind": ("MANUAL_FILL_PRECISION_QUERY_OBSERVATION"
                             if fill_precision else "MANUAL_EQUIVALENT_QUERY_OBSERVATION"),
                    "order_id": current["order_id"],
                    "checkpoint_event_seq": proof["checkpoint_seq"],
                    **decisions[evidence["evidence_id"]],
                },
            }
            candidate["resolved_evidence"].append(resolved)
        candidate["unassociated_evidence"] = []
    candidate["reconcile_priority"] = False
    candidate["fact_version"] += 1
    # Reuse the audited historical success time; this is not a new QMT history query.
    mark_reconciled(candidate, now=checkpoint["last_reconciled_at"])
    require(candidate["execution_status"] == checkpoint["execution_status"]
            and candidate["sync_status"] == "COMPLETE" and not reconcile_pending(candidate)
            and not is_order_active(candidate), "recomputed order is not terminal")
    candidate["reconcile_pending"] = False
    return candidate


def repair(repo, args):
    fill_precision = getattr(args, "resolve_fill_precision_observations", False)
    equivalent = getattr(args, "resolve_equivalent_observations", False) or fill_precision
    require(not (fill_precision and getattr(args, "resolve_equivalent_observations", False)),
            "choose only one observation resolution mode")
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
        bases = {}
        events = [(row[0], row[1], repo_timestamp(row[2]), journal_unpack(repo, cur, row[0], row[3], bases))
                  for row in cur.fetchall()]
        require(events and events[0][0] == args.checkpoint_event_seq, "checkpoint event is missing")
        occurred_at = events[0][2]
        ownership_clause = "" if equivalent else "AND (order_id=%s OR order_id IS NULL) "
        ownership_params = () if equivalent else (args.order_id,)
        cur.execute("SELECT observation_id,kind,raw,source,observed_at,applied,order_id FROM " + repo.repo_s +
                    ".qmt_observations WHERE account_type=%s AND account_id=%s " + ownership_clause +
                    "AND observed_at::timestamptz>%s::timestamptz ORDER BY observation_id",
                    repo.repo_scope + ownership_params + (occurred_at,))
        observations = [(row[0], row[1], repo_json(row[2]), row[3], repo_timestamp(row[4]), row[5], row[6])
                        for row in cur.fetchall()]
        if equivalent:
            # repo_run(mutation=True) holds account_runtime FOR UPDATE before any
            # reads; every writer takes that same account lock, so owners cannot race.
            cur.execute("SELECT document FROM " + repo.repo_s +
                        ".orders WHERE account_type=%s AND account_id=%s ORDER BY order_id" +
                        (" FOR UPDATE" if args.apply else ""), repo.repo_scope)
            account_orders = [repo_json(row[0]) for row in cur.fetchall()]
            proof = validate_equivalent(current, events, observations, args.checkpoint_event_seq, account_orders,
                                        fill_precision=fill_precision)
        else:
            proof = validate(current, events, observations, args.checkpoint_event_seq)
        candidate = repaired_candidate(current, events[0][3], proof, equivalent, fill_precision=fill_precision)
        source_refs = correction_source_refs(repo, cur, args.order_id, events=events,
                                             client_order_id=current.get('client_order_id'))
        result = {"order_id": args.order_id, "mode": "apply" if args.apply else "dry-run",
                  "expected_version": args.expected_version, "proof": proof, "source_event_refs": source_refs,
                  "repair_kind": ("fill-precision-query-observations" if fill_precision else
                                  "equivalent-query-observations" if equivalent else "strict-terminal-gap"),
                  "expected_state": {key: candidate.get(key) for key in (
                      "fact_version", "execution_status", "sync_status", "last_reconciled_at",
                      "reconciliation_complete", "reconcile_pending", "reconcile_requested")},
                  "expected_active": is_order_active(candidate)}
        if not args.apply:
            return result
        receipt = {"captured_at": iso_datetime(), "account_type": repo.account_type,
                   "account_id": repo.account_id, "order_id": args.order_id,
                   "operator": getpass.getuser(), "reason": args.reason.strip(), "source_event_refs": source_refs,
                   "before": current, "checkpoint": events[0][3], "events": [
                       {"seq": row[0], "type": row[1], "at": row[2], "document": row[3]}
                       for row in events],
                   "observations": [{"id": row[0], "kind": row[1], "raw": row[2],
                                     "source": row[3], "observed_at": row[4], "applied": row[5],
                                     "order_id": row[6]} for row in observations], "proof": proof,
                   "repair_kind": result["repair_kind"], "expected_after": candidate}
        payload = json.dumps(receipt, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()
        with args.evidence_out.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        before_state = {key: current.get(key) for key in
                        ("version", "fact_version", "execution_status", "sync_status",
                         "reconciliation_complete", "reconcile_pending", "reconcile_requested")}
        current = candidate
        if equivalent:
            for row in current["resolved_evidence"]:
                resolution = row.get("resolution") or {}
                if resolution.get("kind") in ("MANUAL_EQUIVALENT_QUERY_OBSERVATION",
                                               "MANUAL_FILL_PRECISION_QUERY_OBSERVATION"):
                    resolution.update(evidence_path=str(args.evidence_out.resolve()), evidence_sha256=digest)
            # Resolved evidence is part of closure facts; bind its receipt before fingerprinting.
            mark_reconciled(current, now=events[0][3]["last_reconciled_at"])
        after_state = {key: current.get(key) for key in before_state}
        after_state["version"] = args.expected_version + 1
        current.setdefault("terminal_repairs", []).append({
            "operator": getpass.getuser(), "reason": args.reason.strip(),
            "checkpoint_event_seq": args.checkpoint_event_seq, "source_event_refs": source_refs,
            "repair_kind": result["repair_kind"],
            "before": before_state, "after": after_state,
            "evidence_path": str(args.evidence_out.resolve()), "evidence_sha256": digest,
            "at": iso_datetime()})
        current.setdefault('event_corrections_v1', []).append(correction_notice(
            'ORDER_STATE_RECONCILIATION_REPAIR', source_refs, args.reason.strip(), getpass.getuser(),
            dict(reference=str(args.evidence_out.resolve()), sha256=digest)))
        # Recompute may change derived cancel request status as well as item status.
        children = ("items", "cancel_requests") if equivalent else ("items",)
        repo.repo_save(cur, current, "MANUAL_TERMINAL_REPAIR", child_fields=children)
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
    modes = root.add_mutually_exclusive_group()
    modes.add_argument("--resolve-fill-precision-observations", action="store_true",
                       help="audit scoped callback/query deal cent precision; deal commission is raw evidence only")
    modes.add_argument("--resolve-equivalent-observations", action="store_true",
                      help="prove and audit only duplicate query amount/reference conflicts")
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
