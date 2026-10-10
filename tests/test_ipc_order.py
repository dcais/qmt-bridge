"""Remote execution keeps durable order ownership and callback ACK boundaries."""
import threading
import time
import unittest
from unittest.mock import Mock

from service.ipc_bridge.ipc_protocol.order_common import OrderError
from service.ipc_bridge.ipc_protocol.order_common import fingerprint
from service.ipc_bridge.order.contracts import normalize_order
from service.ipc_bridge.order.service import OrderService


class IpcOrderTests(unittest.TestCase):
    def service(self):
        repo = Mock()
        repo.check_schema.return_value = {'ready': True, 'schema_version': 3}
        repo.acquire_executor.return_value = {'epoch': 7}
        repo.recover.return_value = {'next_cursor': None, 'has_more': False}
        repo.health.return_value = {'ready': True}
        repo.mark_reconcile_gap.return_value = {'next_cursor': None, 'has_more': False}
        repo.queued_orders.return_value = []
        repo.cancellation_orders.return_value = []
        repo.reconcile_history_start.return_value = None
        repo.begin_reconcile_batch.return_value = {'orders': [], 'next_cursor': None, 'has_more': False}
        repo.reconcile_round_batch.return_value = {'orders': [], 'next_cursor': None, 'has_more': False}
        repo.ingest_observation.return_value = None
        link = Mock(ready=True)
        link.call.return_value = []
        service = OrderService('test', None, {}, link, repository=repo, local_lock=Mock())
        self.addCleanup(self.cleanup, service)
        return service

    def cleanup(self, service):
        if service.worker is not None:
            service.stop()
            self.assertTrue(service.runtime.stopped_event.wait(2))

    def wait(self, predicate):
        end = time.monotonic() + 2
        while not predicate() and time.monotonic() < end:
            time.sleep(.005)
        self.assertTrue(predicate())

    def test_timeout_after_dispatch_is_unknown_and_carries_claim_authority(self):
        service = self.service()
        runtime = service.runtime
        background = runtime.background
        background.db_ready = True
        background.grant = (runtime.instance_id, 7, runtime.clock() + 5)
        background._enqueue('submit', document={}, pending_key=('submit', 'x'))
        service.link.call.side_effect = TimeoutError('reply lost after QMT executed')
        self.assertEqual(runtime.tick(), 1)
        result = background.results.get_nowait()
        self.assertEqual(result['status'], 'UNKNOWN')
        self.assertEqual(service.link.call.call_args.kwargs['metadata']['authority'], [runtime.instance_id, 7])
        self.assertEqual(service.link.call.call_args.kwargs['metadata']['request_id'], result['token'])
        self.assertEqual(runtime.tick(), 0)
        self.assertEqual(service.link.call.call_count, 1)

    def test_acceptance_and_dispatch_wait_for_repository_operations(self):
        service = self.service()
        runtime = service.runtime
        background = runtime.background
        runtime.initialized = runtime.recovery_complete = True
        background.db_ready = True
        background.grant = (runtime.instance_id, 7, runtime.clock() + 5)
        request = dict(client_order_id='one', account_id='test', order_type='SINGLE',
                       sizing_type='QUANTITY', symbol='600000.SH', side='BUY', quantity=100,
                       price_type='QUOTE', quote_type='LATEST', execution={'type': 'DIRECT'})
        runtime.repo.get_order.side_effect = OrderError(404, 'NOT_FOUND', 'not found')
        document = dict(order_id='id', client_order_id='one', request=request,
                        preparation_complete=True, attempts=[{'attempt_id': 'claimed-attempt'}])
        runtime.repo.accept_order.return_value = (True, document)
        self.assertEqual(service.handle('submit_order', request, 'POST')[0], 202)
        runtime.repo.accept_order.assert_called_once()
        service.link.call.assert_not_called()
        runtime.repo.queued_orders.return_value = [document]
        runtime.repo.claim_submission.side_effect = RuntimeError('commit failed')
        with self.assertRaises(RuntimeError):
            background._dispatch()
        self.assertTrue(background.queues['submit'].empty())
        service.link.call.assert_not_called()
        runtime.repo.claim_submission.side_effect = None
        runtime.repo.claim_submission.return_value = (True, document)
        background._dispatch()
        service.link.call.assert_not_called()
        runtime.tick()
        self.assertEqual(service.link.call.call_args.args[0], 'order.submit')

    def test_peer_gap_blocks_new_acceptance_but_preserves_idempotent_replay(self):
        service = self.service()
        runtime = service.runtime
        runtime.initialized = runtime.recovery_complete = True
        runtime.background.db_ready = True
        runtime.background.grant = (runtime.instance_id, 7, runtime.clock() + 5)
        service.link.execution_ready = False
        request = dict(client_order_id='one', account_id='test', order_type='SINGLE',
                       sizing_type='QUANTITY', symbol='600000.SH', side='BUY', quantity=100,
                       price_type='QUOTE', quote_type='LATEST', execution={'type': 'DIRECT'})
        runtime.repo.get_order.side_effect = OrderError(404, 'NOT_FOUND', 'not found')
        with self.assertRaises(OrderError) as caught:
            service.handle('submit_order', request, 'POST')
        self.assertEqual(caught.exception.code, 'EXECUTOR_NOT_READY')
        runtime.repo.accept_order.assert_not_called()
        runtime.repo.get_order.side_effect = None
        runtime.repo.get_order.return_value = dict(order_id='id', client_order_id='one',
            request_hash=fingerprint(normalize_order(request, 'test')))
        status, result = service.handle('submit_order', request, 'POST')
        self.assertEqual(status, 200)
        self.assertTrue(result['replayed'])
        self.assertFalse(service.health()['accepting_orders'])

    def test_callback_does_not_ack_until_commit_and_retries_persistence_failure(self):
        service = self.service()
        entered, release, acknowledged = threading.Event(), threading.Event(), threading.Event()
        calls = []
        def ingest(*args):
            calls.append(args)
            entered.set()
            release.wait(2)
            if len(calls) == 1:
                raise RuntimeError('commit unavailable')
            return None
        service.runtime.repo.ingest_observation.side_effect = ingest
        service.start()
        self.wait(lambda: service.runtime.initialized)
        thread = threading.Thread(target=lambda: (service.observe_remote('order', {'x': 1}, 'boot', 1), acknowledged.set()))
        thread.start()
        self.assertTrue(entered.wait(1))
        self.assertFalse(acknowledged.is_set())
        release.set()
        self.assertTrue(acknowledged.wait(2))
        thread.join(1)
        self.assertEqual(len(calls), 2)

    def test_gap_blocks_recovery_and_advances_generation(self):
        service = self.service()
        service.runtime.recovery_complete = True
        service.report_gap('boot changed')
        self.assertFalse(service.runtime.recovery_complete)
        self.assertTrue(service.runtime.observation_gap)
        self.assertEqual(service.runtime.background.fact_generation, 1)

    def test_authorization_lease_does_not_extend_database_grant(self):
        service = self.service()
        runtime = service.runtime
        runtime.clock = lambda: 100.0
        runtime.background.db_ready = True
        runtime.background.grant = (runtime.instance_id, 7, 100.2)
        lease = service.authorize_lease()
        self.assertEqual(lease['authority'], [runtime.instance_id, 7])
        self.assertAlmostEqual(lease['remaining_seconds'], .2)
        runtime.clock = lambda: 100.3
        self.assertIsNone(service.authorize_lease())

    def test_queries_normalize_bound_account_and_preserve_evidence(self):
        service = self.service()
        service.link.call.return_value = {'m_dAvailable': 12}
        service.runtime.repo.evidence_marker.return_value = None
        status, result = service.handle('account_evidence_v1', {}, 'GET')
        self.assertEqual(status, 200)
        self.assertEqual(result['account_id'], 'test')
        self.assertEqual(result['assets']['available']['value'], 12)
        self.assertEqual(service.link.call.call_args.args[1], {'request': {
            'method': 'account', 'params': {'accountId': 'test', 'accountType': 'STOCK'}}})
        with self.assertRaises(OrderError):
            service.handle('account_evidence_v1', {}, 'POST')


if __name__ == '__main__':
    unittest.main()
