# -*- coding: utf-8 -*-
"""Offline tests for tools/order_observation_compaction.py."""
import importlib.util
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service.ipc_bridge.order.repository import PostgresRepository


def _load_tool():
    path = ROOT / "tools" / "order_observation_compaction.py"
    spec = importlib.util.spec_from_file_location("order_observation_compaction", str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ObservationCompactionTests(unittest.TestCase):
    def test_collect_protected_ids_finds_nested_observation_references(self):
        tool = _load_tool()
        docs = [
            {
                "resolved_evidence": [
                    {"resolution": {"observation_id": 101, "nested": {"observation_ids": [102, 103]}}},
                ],
                "flag": True,
                "ignore": {"observation_id": True},
            },
            '{"fills": [{"observation_id": 205}]}',
        ]
        self.assertEqual(tool.collect_protected_observation_ids(docs), {101, 102, 103, 205})

    def test_compaction_keeps_first_protected_and_unapplied_and_avoids_hash_collision(self):
        tool = _load_tool()
        raw_base = {"m_strOrderSysID": "S1", "m_nVolume": 100, "m_nOrderStrategyType": -1}
        raw_noise = {"m_strOrderSysID": "S1", "m_nVolume": 100, "m_nOrderStrategyType": -99}
        norm_hash, hash_1 = PostgresRepository.observation_dedup_hashes("deal", "query", raw_base)
        norm_hash_2, hash_2 = PostgresRepository.observation_dedup_hashes("deal", "query", raw_noise)
        self.assertEqual(norm_hash, norm_hash_2)
        self.assertNotEqual(hash_1, hash_2)

        rows = [
            (1, "STOCK", "66027616", "deal", "query", raw_base, True, hash_1),
            (2, "STOCK", "66027616", "deal", "query", raw_noise, True, hash_2),
            (3, "STOCK", "66027616", "deal", "query", raw_noise, True, hash_2),  # protected below
            (4, "STOCK", "66027616", "deal", "query", raw_noise, False, hash_2),  # unapplied
            (5, "STOCK", "66027616", "deal", "query", raw_noise, True, norm_hash),  # already has norm_hash
        ]
        # Case A: first row can safely normalize because row 5 will be deleted
        plan = tool.plan_observation_compaction(rows, protected_ids={3})
        self.assertEqual(plan.total_scanned, 5)
        self.assertEqual(plan.kept_first, 1)
        self.assertEqual(plan.kept_protected, 2)
        self.assertEqual(plan.delete_ids, [2, 5])
        self.assertEqual(plan.hash_updates, [(1, norm_hash)])

        # Case B: if row 5 is ALSO protected, row 1 must NOT rewrite its hash to norm_hash
        plan_collision_safe = tool.plan_observation_compaction(rows, protected_ids={3, 5})
        self.assertEqual(plan_collision_safe.delete_ids, [2])
        self.assertEqual(plan_collision_safe.hash_updates, [])


if __name__ == "__main__":
    unittest.main()
