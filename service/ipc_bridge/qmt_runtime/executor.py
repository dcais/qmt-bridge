# -*- coding: utf-8 -*-
"""QMT-only in-memory IPC executor; no persistence or worker threads."""
import collections
import json
import time
import math
import uuid
import threading

from service.ipc_bridge.ipc_protocol.winpipe import PipeEndpoint


def console_log(level, message, **fields):
    """Bounded metadata only; never serialize request/response or native objects."""
    allowed = ('request_id', 'method', 'elapsed_ms', 'code', 'qmt_method', 'qmt_call_id',
               'return_type', 'return_count', 'error_code', 'error_type', 'stage',
               'attempt_id', 'order_id', 'client_order_id')
    try:
        record = {'level': str(level)[:16], 'message': str(message)[:128]}
        for key in allowed:
            value = fields.get(key)
            if type(value) is str:
                record[key] = value[:256]
            elif type(value) in (int, bool):
                record[key] = value
            elif type(value) is float and math.isfinite(value):
                record[key] = round(value, 3)
        print(json.dumps(record, ensure_ascii=True, allow_nan=False))
    except Exception:
        pass


class DeferredResult(object):
    def __init__(self, step):
        self.step = step


class QmtRuntime(object):
    def __init__(self, role, bridge_id, account_id, pipe_name, execute,
                 endpoint_factory=PipeEndpoint, clock=time.monotonic,
                 event_limit=4096, request_limit=100000, budget_ms=50):
        self.role, self.bridge_id, self.account_id = role, bridge_id, account_id
        self.pipe_name, self.execute = pipe_name, execute
        self.endpoint_factory, self.clock = endpoint_factory, clock
        self.event_limit, self.request_limit = event_limit, request_limit
        self.budget = budget_ms / 1000.0
        self.boot_id = str(uuid.uuid4())
        self.endpoint = None
        self.session_id = None
        self.hello_sent = False
        self.closed = False
        self.gap = False
        self.seq = 0
        self.events = collections.deque()
        self.sent_seq = 0
        self.seen = set()
        self.pending_ids = set()
        self.request_context = {}
        self.inbox = collections.deque()
        self.jobs = collections.deque()
        self.authority = None
        self.authority_until = 0
        self.challenge_id = None
        self.challenge_until = 0
        self.challenge_started = 0
        self.next_challenge = 0
        self.next_heartbeat = 0
        self.guard = threading.Lock()
        self.tick_guard = threading.Lock()
        self.capabilities = {'protocol': 1, 'callbacks': role == 'order'}

    def _send(self, value):
        value.setdefault('boot_id', self.boot_id)
        value.setdefault('session_id', self.session_id)
        self.endpoint.send(value)

    def disconnect(self):
        if self.endpoint is not None:
            self.endpoint.close()
        self.endpoint = None
        self.session_id = None
        self.hello_sent = False
        self.authority = None
        self.authority_until = 0
        self.challenge_id = None
        self.inbox.clear()
        self.jobs.clear()
        self.pending_ids.clear()
        self.sent_seq = 0
        self.next_heartbeat = self.next_challenge = 0

    def close(self):
        self.closed = True
        if self.tick_guard.acquire(False):
            try:
                self.disconnect()
            finally:
                self.tick_guard.release()
        # An active tick owns native OVERLAPPED memory until its finally block.

    def observe(self, event, payload):
        with self.guard:
            self.seq += 1
            if len(self.events) >= self.event_limit:
                self.gap = True
                return
            self.events.append({'type': 'event', 'seq': self.seq,
                                'event': event, 'payload': payload})

    def _request(self, frame):
        request_id = frame.get('request_id')
        if not isinstance(request_id, str) or not request_id:
            return
        result = {'type': 'result', 'request_id': request_id}
        started = self.clock()
        method = frame.get('method')
        console_log('INFO', 'IPC request started', request_id=request_id, method=method, elapsed_ms=0)
        try:
            if request_id in self.pending_ids:
                raise RuntimeError('DUPLICATE_REQUEST')
            if method in ('order.submit', 'order.prepare_step', 'order.cancel_action'):
                if request_id in self.seen:
                    raise RuntimeError('DUPLICATE_REQUEST')
                if len(self.seen) >= self.request_limit:
                    raise RuntimeError('REQUEST_CAPACITY_EXHAUSTED')
                self.seen.add(request_id)
            if not isinstance(frame.get('expires_at'), (int, float)) or time.time() >= frame['expires_at']:
                raise RuntimeError('REQUEST_EXPIRED')
            method, params = frame.get('method'), frame.get('params', {})
            if not isinstance(method, str) or not isinstance(params, dict):
                raise RuntimeError('INVALID_REQUEST')
            if method in ('order.submit', 'order.prepare_step', 'order.cancel_action'):
                if (self.gap or self.authority is None or
                        self.clock() >= self.authority_until or
                        frame.get('authority') != self.authority):
                    raise RuntimeError('AUTHORITY_UNAVAILABLE')
            if self.closed:
                raise RuntimeError('STRATEGY_STOPPED')
            self.request_context = {'request_id': request_id, 'method': method}
            payload = self.execute(method, params)
            if isinstance(payload, DeferredResult):
                if len(self.jobs) >= 128:
                    raise RuntimeError('BATCH_CAPACITY_EXHAUSTED')
                self.pending_ids.add(request_id)
                self.jobs.append((result, payload, frame['expires_at'], method, started))
                return
            result.update(status=200, payload=payload)
        except Exception as exc:
            code = getattr(exc, 'code', str(exc) if isinstance(exc, RuntimeError) else 'QMT_ERROR')
            result.update(status=getattr(exc, 'status', 503), code=code,
                          message=str(exc))
        self._log_result(result, method, started)
        self._send(result)

    def _log_result(self, result, method, started):
        failed = result.get('status', 500) >= 400
        console_log('ERROR' if failed else 'INFO',
                    'IPC request failed' if failed else 'IPC request finished',
                    request_id=result['request_id'], method=method, code=result.get('code'),
                    elapsed_ms=(self.clock() - started) * 1000)

    def _step_job(self):
        if self.closed or not self.jobs:
            return
        result, job, deadline, method, started = self.jobs.popleft()
        try:
            if time.time() >= deadline:
                raise RuntimeError('REQUEST_EXPIRED')
            done, payload = job.step()
            if not done:
                self.jobs.append((result, job, deadline, method, started))
                return
            result.update(status=200, payload=payload)
        except Exception as exc:
            result.update(status=getattr(exc, 'status', 503),
                          code=getattr(exc, 'code', 'QMT_ERROR'), message=str(exc))
        self.pending_ids.discard(result['request_id'])
        self._log_result(result, method, started)
        self._send(result)

    def _frame(self, frame):
        kind = frame.get('type')
        if kind == 'welcome':
            if (frame.get('protocol') != 1 or frame.get('role') != self.role or
                    frame.get('bridge_id') != self.bridge_id or
                    frame.get('account_id') != self.account_id or
                    not isinstance(frame.get('session_id'), str)):
                raise RuntimeError('INVALID_WELCOME')
            if self.session_id is not None:
                raise RuntimeError('DUPLICATE_WELCOME')
            self.session_id = frame['session_id']
            return
        if not self.session_id or frame.get('session_id') != self.session_id:
            return
        if kind == 'authority_grant':
            grant = frame.get('authority')
            remaining = frame.get('remaining_seconds')
            if (self.challenge_id is not None and
                    frame.get('challenge_id') == self.challenge_id and
                    self.clock() < self.challenge_until and
                    isinstance(grant, list) and len(grant) == 2 and
                    isinstance(remaining, (int, float)) and not isinstance(remaining, bool) and
                    math.isfinite(remaining) and remaining > 0):
                deadline = min(self.challenge_until, self.challenge_started + min(remaining, 0.5))
                if self.clock() >= deadline:
                    return
                self.authority, self.authority_until = grant, deadline
                self.challenge_id = None
        elif kind == 'ack' and frame.get('boot_id') == self.boot_id:
            seq = frame.get('seq')
            if isinstance(seq, int) and not isinstance(seq, bool) and seq <= self.sent_seq:
                with self.guard:
                    while self.events and self.events[0]['seq'] <= seq:
                        self.events.popleft()
        elif kind == 'request':
            self._request(frame)

    def tick(self):
        if self.closed or not self.tick_guard.acquire(False):
            return
        try:
            if not self.closed:
                self._tick()
        except Exception as exc:
            print('IPC disconnected: {0}: {1}'.format(type(exc).__name__, exc))
            self.disconnect()
        finally:
            try:
                if self.closed:
                    self.disconnect()
            finally:
                self.tick_guard.release()

    def _tick(self):
        started = self.clock()
        if self.endpoint is None:
            self.endpoint = self.endpoint_factory(self.pipe_name, server=False)
        frames = self.endpoint.poll()
        if self.closed or not self.endpoint.connected:
            return
        if not self.hello_sent:
            self._send({'type': 'hello', 'protocol': 1, 'role': self.role,
                        'bridge_id': self.bridge_id, 'account_id': self.account_id,
                        'capabilities': self.capabilities})
            self.hello_sent = True
        if len(self.inbox) + len(frames) > 4096:
            raise RuntimeError('INBOX_OVERFLOW')
        self.inbox.extend(frames)
        while not self.closed and self.inbox and self.clock() - started < self.budget:
            self._frame(self.inbox.popleft())
        if self.closed or not self.session_id:
            return
        if self.clock() - started < self.budget:
            self._step_job()
        now = self.clock()
        if self.role == 'order' and now >= self.next_challenge:
            self.challenge_id = str(uuid.uuid4())
            self.challenge_started = now
            self.challenge_until = now + 0.5
            self.next_challenge = now + 0.15
            self._send({'type': 'authority_challenge', 'challenge_id': self.challenge_id})
        if now >= self.next_heartbeat:
            self._send({'type': 'heartbeat', 'ready': True, 'authority_ready': self.authority is not None and now < self.authority_until and not self.gap, 'qmt_ms': (self.clock() - started) * 1000,
                        'gap': self.gap, 'capabilities': self.capabilities})
            if self.gap:
                self._send({'type': 'event_gap', 'seq': self.seq})
            self.next_heartbeat = now + 0.25
        with self.guard:
            pending = []
            for event in self.events:
                if event['seq'] > self.sent_seq:
                    pending.append(dict(event))
                    if len(pending) == 64:
                        break
        for event in pending:
            if self.clock() - started >= self.budget:
                break
            self._send(event)
            self.sent_seq = event['seq']
