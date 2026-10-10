"""External durable order service; the link carries only QMT operations and facts."""
import queue
import threading

from .api_contracts import METHODS, normalize_request
from .common import OrderError, copy_json, iso_datetime
from .evidence import evidence_sample
from .runtime import OrderRuntime


class RemoteQmtAdapter:
    def __init__(self, link, timeout=10):
        self.link = link
        self.timeout = timeout
        self.apis = {}
        self.log_context = {}
        self.authority = None

    def available(self, name):
        return bool(self.apis.get(name))

    @property
    def execution_ready(self):
        return bool(getattr(self.link, 'execution_ready', self.link.ready))

    def refresh_capabilities(self):
        self.apis = self.link.call('order.capabilities', {}, timeout=self.timeout)

    def _call(self, method, params, side_effect=False):
        metadata = dict(self.log_context)
        if metadata.get('directive_id'):
            metadata['request_id'] = metadata['directive_id']
        if side_effect:
            if not self.authority:
                raise OrderError(503, 'EXECUTOR_NOT_READY', 'executor authority is unavailable')
            metadata['authority'] = list(self.authority)
        # Any transport exception after dispatch remains UNKNOWN in the durable runtime.
        return self.link.call(method, params, timeout=self.timeout, metadata=metadata)

    def submit(self, document):
        return self._call('order.submit', {'document': document}, True)

    def cancel_action(self, action):
        return self._call('order.cancel_action', {'action': action}, True)

    def prepare_step(self, document, stage='RESOLVE', payload=None):
        return self._call('order.prepare_step', {'document': document, 'stage': stage,
                                                'payload': payload}, True)

    def query(self, kind):
        return self._call('order.query', {'kind': kind})

    def snapshot(self, value):
        return copy_json(value)


class OrderService:
    def __init__(self, account_id, pg_config, settings, link, logger=None,
                 repository=None, local_lock=None):
        self.link = link
        self.adapter = RemoteQmtAdapter(link, (settings or {}).get('request_timeout_seconds', 10))
        self.runtime = OrderRuntime({}, None, account_id, pg_config, logger,
                                    repository=repository, local_lock=local_lock,
                                    settings=settings, adapter=self.adapter)
        self.worker = None

    def start(self):
        if self.worker is not None:
            return
        self.runtime.initialize()
        self.worker = threading.Thread(target=self._run, name='order-ipc-dispatch', daemon=True)
        self.worker.start()

    def _run(self):
        while not self.runtime.stop_event.is_set():
            if self.link.ready:
                self.runtime.tick()
            self.runtime.stop_event.wait(.01)

    def stop(self):
        self.runtime.stop()

    def authorize(self):
        lease = self.authorize_lease()
        return lease['authority'] if lease else None

    def authorize_lease(self):
        background = self.runtime.background
        grant = background.grant
        if grant and background.authorized() and background.grant is grant:
            remaining = max(0, grant[2] - self.runtime.clock())
            if remaining:
                return dict(authority=list(grant[:2]), remaining_seconds=remaining)
        return None

    def report_gap(self, *args, **kwargs):
        runtime = self.runtime
        runtime.observation_gap = True
        runtime.recovery_complete = False
        runtime.history_coverage_complete = False
        runtime.background.fact_generation += 1
        runtime.background.next_round = 0

    def observe_remote(self, kind, payload, boot_id=None, seq=None):
        """Return only after the existing repository transaction commits (ACK boundary)."""
        runtime = self.runtime
        if kind not in ('order', 'deal', 'task', 'error'):
            raise OrderError(400, 'INVALID_OBSERVATION', 'unsupported observation kind')
        runtime._runtime_require_store()
        committed = threading.Event()
        try:
            runtime.observations.put_nowait((kind, copy_json(payload), committed))
        except queue.Full:
            self.report_gap()
            raise OrderError(503, 'OBSERVATION_QUEUE_FULL', 'callback persistence queue is full')
        while not committed.wait(.05):
            if runtime.stopped_event.is_set():
                raise OrderError(503, 'ORDER_STOPPED', 'observation was not committed')

    def health(self):
        result = self.runtime.health()
        result['ipc_ready'] = bool(self.link.ready)
        result['accepting_orders'] = result['accepting_orders'] and self.adapter.execution_ready
        return result

    def _marker(self, params):
        runtime = self.runtime
        if runtime.repo is None or params['accountId'] != runtime.account_id or params['accountType'] != 'STOCK':
            return None
        try:
            return runtime.repo.evidence_marker()
        except Exception:
            return None

    def handle(self, method, params, verb):
        if method == 'health':
            self.runtime.handle(method, params, verb)
            return 200, self.health()
        if method == 'capabilities' and self.link.ready:
            self.adapter.refresh_capabilities()
        if method not in METHODS:
            return self.runtime.handle(method, params, verb)
        evidence = method in ('account_evidence_v1', 'positions_evidence_v1')
        if evidence and verb != 'GET':
            raise OrderError(405, 'METHOD_NOT_ALLOWED', 'evidence queries require GET')
        request = normalize_request(method, params, allow_algo_string=verb == 'GET',
                                    account_id=self.runtime.account_id)
        before = self._marker(request['params']) if evidence else None
        started = iso_datetime()
        if evidence:
            request['method'] = method.replace('_evidence_v1', '')
        raw = self.link.call('order.query', {'request': request}, timeout=self.adapter.timeout)
        if not evidence:
            return 200, raw
        result = evidence_sample(request['method'], request['params']['accountId'],
                                 request['params']['accountType'], raw, started, iso_datetime())
        after = self._marker(request['params'])
        result['event_watermark'] = dict(before=before, after=after)
        if before and after and all(before[key] == after[key] for key in
                                    ('stream_id', 'executor_epoch', 'executor_instance')) and before['high_watermark'] <= after['high_watermark']:
            result['event_relation'] = 'non_atomic_bracket'
        self.runtime.record_evidence_sample(result)
        return 200, result
