# -*- coding: utf-8 -*-
"""QMT 返回对象诊断通过真实异步日志链路；Last modified: 2026-09-28。"""
import tempfile
import unittest
from unittest.mock import Mock

from order_bridge.async_log import AsyncOrderLogger
from order_bridge.common import new_order_document
from order_bridge.runtime import OrderRuntime
from order_bridge.state import apply_observation


class COrderDetail(object):
    m_nVolumeTotalOriginal = 100
    m_strOrderSysID = "1"
    m_zLast = "readable after failed attribute"

    @property
    def m_unreadableBusinessField(self):
        raise TypeError("business field conversion failed")


class TaggedOrderDetail(object):
    m_strAccountID = "account"
    m_strOrderSysID = "1"
    m_strInstrumentID = "511880"
    m_strExchangeID = "SH"
    m_strInsertDate = "20260926"
    m_nOffsetFlag = 48
    m_nOrderStatus = 50
    m_nVolumeTotalOriginal = 100
    m_nVolumeTraded = 0

    @property
    def m_xtTag(self):
        raise TypeError("No to_python converter for boost::shared_ptr<se::CXtOrderTag>")


class ReturnLogTests(unittest.TestCase):
    def check_snapshot(self, fields):
        snapshot = fields["return_snapshot"]
        self.assertEqual(snapshot["object_type"], "COrderDetail")
        self.assertEqual(snapshot["fields_source"], "dir(object)")
        self.assertEqual(snapshot["fields"]["m_nVolumeTotalOriginal"], 100)
        self.assertEqual(snapshot["fields"]["m_strOrderSysID"], "1")
        self.assertEqual(snapshot["fields"]["m_zLast"], "readable after failed attribute")
        self.assertEqual(snapshot["field_errors"]["m_unreadableBusinessField"]["type"], "TypeError")
        self.assertNotIn("m_unreadableBusinessField", snapshot["fields"])
        self.assertFalse(snapshot["truncated"])

    def test_query_failure_logs_readable_fields_without_ingesting_partial_order(self):
        with tempfile.TemporaryDirectory() as directory:
            records = []
            logger = AsyncOrderLogger(directory, sink=records.append)
            repo = Mock()
            runtime = OrderRuntime({"get_trade_detail_data": lambda *args: [COrderDetail()]},
                                   object(), "account", repository=repo, logger=logger)
            bg = runtime.background
            bg.round = {"id": "round-1", "complete": True, "live_complete": True,
                        "waiting": True, "stage": 1, "queries": ['task', 'order', 'deal']}
            bg._enqueue("query", query_kind="order", round_id="round-1")
            self.assertEqual(runtime.tick(max_actions=1), 1)
            result = bg.results.get_nowait()
            bg._merge(result)
            bg.capacity.release()
            logger.start()
            logger.request_stop()
            self.assertTrue(logger.join(2))
            fields = next(record["fields"] for record in records
                          if record["message"] == "QMT reconciliation incomplete")
            self.check_snapshot(fields)
            self.assertEqual(fields["return_type"], "list")
            self.assertEqual(fields["return_count"], 1)
            self.assertEqual(fields["row_index"], 0)
            self.assertFalse(bg.round["complete"])
            repo.ingest_observation.assert_not_called()
            self.assertNotIn("return_snapshot", runtime.health()["last_reconcile_error"])

    def test_callback_failure_logs_readable_fields_without_enqueueing_partial_order(self):
        with tempfile.TemporaryDirectory() as directory:
            records = []
            logger = AsyncOrderLogger(directory, sink=records.append)
            runtime = OrderRuntime({}, object(), "account", repository=Mock(), logger=logger)
            runtime.observe("order", COrderDetail())
            logger.start()
            logger.request_stop()
            self.assertTrue(logger.join(2))
            self.check_snapshot(records[0]["fields"])
            self.assertTrue(runtime.observations.empty())
            self.assertTrue(runtime.observation_gap)

    def test_internal_tag_does_not_block_reconcile_or_order_association(self):
        with tempfile.TemporaryDirectory() as directory:
            records = []
            logger = AsyncOrderLogger(directory, sink=records.append)
            repo = Mock()
            repo.begin_reconcile_batch.return_value = {
                "orders": [], "next_cursor": None, "has_more": False}
            repo.reconcile_round_batch.return_value = {
                "orders": [], "next_cursor": None, "has_more": False}
            repo.reconcile_history_start.return_value = None
            runtime = OrderRuntime(
                {"get_trade_detail_data": lambda account, account_type, kind:
                    [TaggedOrderDetail()] if kind == "order" else []},
                object(), "account", repository=repo, logger=logger)
            bg = runtime.background
            bg.db_ready = True
            for unused in range(20):
                bg._reconcile_step()
                if not bg.queues["query"].empty():
                    runtime.tick(max_actions=1)
                    bg._merge(bg.results.get_nowait())
                    bg.capacity.release()
                if bg.round is None:
                    break
            self.assertIsNone(bg.round)
            self.assertIsNotNone(runtime.last_reconciled_at)
            self.assertIsNone(runtime.health()["last_reconcile_error"])
            self.assertTrue(runtime.recovery_complete)
            self.assertEqual(repo.ingest_observation.call_count, 1)
            kind, raw, source = repo.ingest_observation.call_args[0]
            self.assertEqual((kind, source), ("order", "query"))
            self.assertNotIn("m_xtTag", raw)
            self.assertEqual(raw["m_nVolumeTotalOriginal"], 100)
            doc = new_order_document({
                "client_order_id": "test", "account_id": "account", "order_type": "SINGLE",
                "symbol": "511880.SH", "side": "BUY", "quantity": 100,
                "execution": {"type": "DIRECT"}})
            doc["submission_status"] = "SUBMITTING"
            apply_observation(doc, kind, raw, source)
            self.assertEqual(doc["submission_status"], "CONFIRMED")
            self.assertEqual(doc["qmt_orders"][0]["qmt_order_id"], "1")
            self.assertEqual(doc["qmt_orders"][0]["status"], "WORKING")
            self.assertFalse(doc.get("unassociated_evidence"))
            # 同实例的后续回报继续采集，但不重复写排除提示。
            runtime.observe("order", TaggedOrderDetail())
            self.assertEqual(runtime.observations.get_nowait(), (kind, raw))
            self.assertFalse(runtime.observation_gap)
            logger.start()
            logger.request_stop()
            self.assertTrue(logger.join(2))
            warnings = [row for row in records if row["message"] == "QMT snapshot internal field excluded"]
            self.assertEqual(len(warnings), 1)
            snapshot = warnings[0]["fields"]["return_snapshot"]
            self.assertEqual(snapshot["fields"]["m_strOrderSysID"], "1")
            self.assertEqual(snapshot["fields"]["m_nVolumeTotalOriginal"], 100)
            self.assertIn("m_xtTag", snapshot["skipped_fields"])
            self.assertEqual(snapshot["field_errors"], {})

    def test_callback_with_internal_tag_succeeds_before_any_query(self):
        runtime = OrderRuntime({}, object(), "account", repository=Mock(), logger=Mock())
        runtime.observe("order", TaggedOrderDetail())
        kind, raw = runtime.observations.get_nowait()
        self.assertEqual(kind, "order")
        self.assertEqual(raw["m_strOrderSysID"], "1")
        self.assertNotIn("m_xtTag", raw)
        self.assertFalse(runtime.observation_gap)
        self.assertEqual(runtime.background.overflows, 0)


if __name__ == "__main__":
    unittest.main()
