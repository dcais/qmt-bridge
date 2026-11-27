#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Safely deduplicate historical noisy qmt_observations while preserving referenced & pending rows."""

import argparse
from dataclasses import dataclass, field
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.ipc_bridge.order.repository import PostgresRepository  # noqa: E402
from tools.order_admin import config_from_args  # noqa: E402
from tools.order_storage_backfill import connect  # noqa: E402


@dataclass
class CompactionPlan:
    total_scanned: int = 0
    kept_first: int = 0
    kept_protected: int = 0
    delete_ids: list[int] = field(default_factory=list)
    hash_updates: list[tuple[int, str]] = field(default_factory=list)


def collect_protected_observation_ids(documents: Any) -> set[int]:
    """Recursively collect every referenced integer observation_id from JSON documents."""
    protected: set[int] = set()
    stack = list(documents)
    while stack:
        current = stack.pop()
        if isinstance(current, str):
            try:
                current = json.loads(current)
            except ValueError:
                continue
        if isinstance(current, dict):
            for key, value in current.items():
                if key == "observation_id" and isinstance(value, int) and not isinstance(value, bool):
                    protected.add(value)
                elif key == "observation_ids" and isinstance(value, list):
                    for item in value:
                        if isinstance(item, int) and not isinstance(item, bool):
                            protected.add(item)
                elif isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return protected


def _coerce_raw(raw: Any) -> Any:
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except ValueError:
            return raw
    return raw


def plan_observation_compaction(rows: Any, protected_ids: set[int]) -> CompactionPlan:
    """Select duplicate qmt_observations to delete while keeping first, protected, and unapplied rows."""
    plan = CompactionPlan()
    seen_norm: set[tuple[str, str, str]] = set()
    kept_hashes_by_scope: dict[tuple[str, str], set[str]] = {}
    normalize_candidates: list[tuple[int, tuple[str, str], str, str | None]] = []

    for row in rows:
        obs_id, account_type, account_id, kind, source, raw, applied, obs_hash = row
        plan.total_scanned += 1
        scope = (account_type, account_id)
        norm_hash, _ = PostgresRepository.observation_dedup_hashes(kind, source, _coerce_raw(raw))
        norm_key = (account_type, account_id, norm_hash)
        is_protected = obs_id in protected_ids or (not applied)

        if norm_key in seen_norm:
            if is_protected:
                plan.kept_protected += 1
                if obs_hash is not None:
                    kept_hashes_by_scope.setdefault(scope, set()).add(obs_hash)
            else:
                plan.delete_ids.append(obs_id)
            continue

        seen_norm.add(norm_key)
        plan.kept_first += 1
        if obs_hash is not None:
            kept_hashes_by_scope.setdefault(scope, set()).add(obs_hash)
        if obs_hash != norm_hash:
            normalize_candidates.append((obs_id, scope, norm_hash, obs_hash))

    for obs_id, scope, norm_hash, old_hash in normalize_candidates:
        scope_hashes = kept_hashes_by_scope.setdefault(scope, set())
        if norm_hash in scope_hashes:
            continue
        if old_hash is not None:
            scope_hashes.discard(old_hash)
        scope_hashes.add(norm_hash)
        plan.hash_updates.append((obs_id, norm_hash))

    return plan


def _fetch_protected_ids(conn, schema: str) -> set[int]:
    protected: set[int] = set()
    with conn.cursor() as cur:
        for table in ("orders", "order_events"):
            cur.execute(
                f"SELECT document FROM {schema}.{table} WHERE document::text LIKE %s",
                ("%observation_id%",),
            )
            protected.update(collect_protected_observation_ids(row[0] for row in cur.fetchall()))
    return protected


def _stream_observations(conn, schema: str, *, account_id: str | None, fetch_size: int = 5000):
    last_id = 0
    where = ["observation_id > %s"]
    base_params: list[Any] = []
    if account_id is not None:
        where.append("account_id = %s")
        base_params.append(account_id)
    sql = (
        f"SELECT observation_id, account_type, account_id, kind, source, raw, applied, observation_hash "
        f"FROM {schema}.qmt_observations "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY observation_id ASC LIMIT %s"
    )
    while True:
        with conn.cursor() as cur:
            cur.execute(sql, (last_id, *base_params, fetch_size))
            batch = cur.fetchall()
        conn.rollback()
        if not batch:
            break
        for row in batch:
            yield row
        last_id = batch[-1][0]


def compact_observations(
    conn,
    *,
    schema: str = '"qmt_order"',
    account_id: str | None = None,
    apply: bool = False,
    batch_size: int = 5000,
    vacuum: bool = False,
) -> dict[str, Any]:
    """Execute dry-run or batched live compaction on qmt_order.qmt_observations."""
    if vacuum and not apply:
        raise ValueError("--vacuum requires --apply")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    protected_ids = _fetch_protected_ids(conn, schema)
    conn.rollback()
    plan = plan_observation_compaction(
        _stream_observations(conn, schema, account_id=account_id),
        protected_ids,
    )

    deleted = 0
    normalized = 0
    vacuum_mode = None
    if apply:
        for start in range(0, len(plan.delete_ids), batch_size):
            batch = plan.delete_ids[start : start + batch_size]
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {schema}.qmt_observations WHERE observation_id = ANY(%s)",
                    (batch,),
                )
                deleted += cur.rowcount
            conn.commit()

        for start in range(0, len(plan.hash_updates), batch_size):
            batch = plan.hash_updates[start : start + batch_size]
            with conn.cursor() as cur:
                for obs_id, norm_hash in batch:
                    cur.execute(
                        f"UPDATE {schema}.qmt_observations SET observation_hash = %s WHERE observation_id = %s",
                        (norm_hash, obs_id),
                    )
                    normalized += cur.rowcount
            conn.commit()

        if vacuum:
            previous_autocommit = conn.autocommit
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    try:
                        cur.execute("SET lock_timeout = '3s'")
                        cur.execute(f"VACUUM FULL ANALYZE {schema}.qmt_observations")
                        vacuum_mode = "VACUUM_FULL"
                    except Exception:
                        cur.execute("SET lock_timeout = '5s'")
                        cur.execute(f"VACUUM ANALYZE {schema}.qmt_observations")
                        vacuum_mode = "VACUUM"
            finally:
                conn.autocommit = previous_autocommit

    return {
        "mode": "apply" if apply else "dry-run",
        "account_id": account_id,
        "protected_referenced_ids": len(protected_ids),
        "total_scanned": plan.total_scanned,
        "kept_first": plan.kept_first,
        "kept_protected": plan.kept_protected,
        "planned_delete": len(plan.delete_ids),
        "planned_hash_updates": len(plan.hash_updates),
        "deleted": deleted,
        "normalized": normalized,
        "vacuum_mode": vacuum_mode,
    }


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--config", type=Path, help="JSON configuration; ORDER_* env overrides")
    root.add_argument("--dotenv", type=Path, default=Path(".env"), help="dotenv path for config")
    root.add_argument("--account-id", help="override ORDER_ACCOUNT_ID")
    root.add_argument("--all-accounts", action="store_true", help="Compact across every account_id")
    mode = root.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Compute compaction counts without writes")
    mode.add_argument("--apply", action="store_true", help="Delete duplicate observations in batches")
    root.add_argument("--batch-size", type=int, default=5000)
    root.add_argument("--vacuum", action="store_true", help="Run VACUUM FULL/VACUUM ANALYZE after --apply")
    return root


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    config = config_from_args(args, dotenv_path=str(args.dotenv))
    account_scope = None if args.all_accounts else config["account_id"]
    conn = connect(config)
    try:
        summary = compact_observations(
            conn,
            schema='"' + config["pg_schema"] + '"',
            account_id=account_scope,
            apply=bool(args.apply),
            batch_size=args.batch_size,
            vacuum=bool(args.vacuum),
        )
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
