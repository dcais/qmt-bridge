# -*- coding: utf-8 -*-
"""ORDER 后台事务与短期授权；Last modified: 2026-09-26。

队列只承载普通快照。容量令牌覆盖排队、QMT 在途和待持久结果整个生命期。
"""
import datetime as dt
import queue
import threading
import time
import uuid

from .common import copy_json, iso_datetime, parse_timestamp, utc_now
from .state import pending_cancellations, recompute_order, observation_identifiers


class OrderBackground(object):
    def __init__(self, runtime):
        self.r = runtime
        self.queues = {kind: queue.Queue(128) for kind in ('cancel', 'submit', 'prepare', 'query')}
        self.results = queue.Queue(128)
        self.capacity = threading.BoundedSemaphore(128)
        self.grant = None
        self.db_ready = False
        self.schema_ready = threading.Event()
        self.authority_stop = threading.Event()
        self.authority_done = threading.Event()
        self.started = False
        self.stopped = False
        self.pending = set()
        self.consumed = set()
        self.fact_generation = 0
        self.overflows = 0
        self.rotation = 0
        self.sample = {}
        self.sampled_at = None
        self.tick_ms = 0
        self.qmt_ms = 0
        self.round = None
        self.next_round = 0
        self.cursors = {'submit': None, 'cancel': None}
        self.last_sample = 0
        self.gap_cursor = None
        self.gap_stamp = None
        self.gap_generation = -1
        self.handled_gap_generation = 0
        self.db_thread = None
        self.idle = threading.Event()
        self.idle.set()
        self.logged_versions = {}
        self.busy = False

    def _log(self, level, message, **fields):
        try:
            self.r.logger(level, message, **fields)
        except Exception:
            pass

    def start(self):
        if self.started or self.r.repo is None:
            if self.r.stop_event.is_set() and self.r.repo is None:
                self.stopped = self.r.stopped = True
                self.r.stopped_event.set()
            return
        self.started = True
        self.r.stopped = False
        self.db_thread = threading.Thread(target=self._work, name='order-database', daemon=True)
        self.db_thread.start()

    def authorized(self, directive=None, require_database=True):
        grant = self.grant
        return bool(grant and (not require_database or (self.db_ready and self.sample.get('ready', True))) and not self.r.stop_event.is_set()
                    and self.r.clock() < grant[2]
                    and (directive is None or directive.get('authority') == grant[:2]))

    def _authority(self):
        r = self.r
        try:
            while not self.authority_stop.is_set():
                if not self.schema_ready.wait(.05):
                    continue
                try:
                    if self.grant is None:
                        result = r.repo.acquire_executor(r.instance_id, r.host_id)
                        identity = (r.instance_id, result['epoch'])
                    # 发放期限从检查开始算；阻塞 SQL 不得延长旧授权。
                    began = r.clock()
                    r.repo.check_executor()
                    self.grant = identity + (began + .75,)
                except Exception as exc:
                    self.grant = None
                    r.last_error = getattr(exc, 'code', 'EXECUTOR_LOCK_LOST')
                    self._log('ERROR', 'Order authority lost', error_code=r.last_error)
                    break
                self.authority_stop.wait(.15)
        finally:
            self.grant = None
            try:
                r.repo.close()
            except Exception:
                r.last_error = 'PERSISTENCE_CLOSE_ERROR'
            finally:
                self.authority_done.set()

    def _enqueue(self, kind, **data):
        if not self.capacity.acquire(False):
            return False
        data.update(kind=kind, token=uuid.uuid4().hex,
                    authority=self.grant[:2] if self.grant else None)
        try:
            self.queues[kind].put_nowait(data)
            return True
        except queue.Full:
            self.capacity.release()
            return False

    def tick(self, deadline=None, budget=None, max_actions=None):
        r = self.r
        began = r.clock()
        r.last_tick = began
        if r.repo is None or r.stop_event.is_set() or not r.tick_lock.acquire(False):
            return 0
        try:
            if self.busy or r.stop_event.is_set():
                return 0
            self.busy = True
            self.idle.clear()
        finally:
            r.tick_lock.release()
        if deadline is None:
            deadline = began + r.settings['schedule_budget_ms'] / 1000.0
        budget = budget if budget is not None else {'submit': 0, 'cancel': 0}
        count = 0
        try:
            empty = 0
            kinds = ('cancel', 'submit', 'prepare', 'query')
            while r.clock() < deadline and (max_actions is None or count < max_actions):
                kind = kinds[self.rotation]
                self.rotation = (self.rotation + 1) % len(kinds)
                if kind != 'query' and not self.db_ready:
                    empty += 1
                    if empty >= 4:
                        break
                    continue
                if kind in ('cancel', 'submit') and budget.get(kind, 0) >= r.settings[kind + '_batch_size']:
                    empty += 1
                    if empty >= 4:
                        break
                    continue
                try:
                    directive = self.queues[kind].get_nowait()
                except queue.Empty:
                    empty += 1
                    if empty >= 4:
                        break
                    continue
                empty = 0
                result = dict(directive, status='ABORTED_NO_CALL')
                token = directive['token']
                if token in self.consumed:
                    # 指令去重由 attempt 生命周期控制；重复令牌不产生第二个结果。
                    continue
                self.consumed.add(token)
                call_start = r.clock()
                try:
                    if not r.stop_event.is_set() and (kind == 'query' or self.authorized(directive, require_database=False)):
                        if kind != 'query' and not self.db_ready:
                            self.consumed.discard(token)
                            self.queues[kind].put_nowait(directive)
                            break
                        if kind == 'submit':
                            document = directive['document']
                            expired = False
                            try:
                                if document.get('request'):
                                    r._runtime_smart_window(document['request'])
                                expired = bool(document.get('submit_before') and utc_now() >= parse_timestamp(document['submit_before']))
                            except Exception:
                                expired = True
                            if expired:
                                result.update(status='ABORTED_NO_CALL', error={'code': 'ORDER_EXPIRED', 'message': 'Order expired before QMT call'})
                                self.results.put_nowait(result)
                                count += 1
                                continue
                            budget[kind] = budget.get(kind, 0) + 1
                            result['value'] = r.adapter.snapshot(r.adapter.submit(directive['document']))
                        elif kind == 'cancel':
                            budget[kind] = budget.get(kind, 0) + 1
                            result['value'] = r.adapter.cancel_action(directive['action'])
                        elif kind == 'prepare':
                            result['value'] = r.adapter.prepare_step(directive['document'], directive['stage'])
                        else:
                            result['value'] = r.adapter.query(directive['query_kind'], *directive.get('dates', ()))
                        result['status'] = 'RETURNED'
                except Exception as exc:
                    result.update(status='UNKNOWN', error={'code': getattr(exc, 'code', 'QMT_ERROR'),
                                  'message': str(exc)[:4096], 'type': type(exc).__name__})
                self.qmt_ms = (r.clock() - call_start) * 1000
                # 预留令牌保证队列不会满，QMT 线程从不等待消费者。
                self.results.put_nowait(result)
                count += 1
            return count
        finally:
            self.tick_ms = (r.clock() - began) * 1000
            self.busy = False
            self.idle.set()

    def _work(self):
        r = self.r
        authority = threading.Thread(target=self._authority, name='order-authority', daemon=True)
        authority.start()
        retained = None
        initialized = False
        recovery_cursor = None
        setup_done = False
        recovery_done = False
        startup_gap_cursor = None
        startup_stamp = iso_datetime()
        try:
            while True:
                if r.stop_event.is_set():
                    # 仅后台等待已进入的 QMT 调用；本机锁继续持有。
                    with r.admission_lock:
                        pass
                    self.idle.wait()
                    external = getattr(r, 'external_idle', None)
                    if external is not None:
                        external.wait()
                    for source in self.queues.values():
                        while True:
                            try:
                                self.results.put_nowait(dict(source.get_nowait(), status='ABORTED_NO_CALL'))
                            except queue.Empty:
                                break
                    if retained is None and self.results.empty() and r.observations.empty():
                        break
                try:
                    if not initialized and not r.stop_event.is_set():
                        if not setup_done:
                            if r.local_lock:
                                r.local_lock.acquire()
                            self.sample = r.repo.check_schema()
                            r.repo.ensure_account_runtime()
                            self.schema_ready.set()
                            setup_done = True
                        if self.grant is None:
                            r.stop_event.wait(.02)
                            continue
                        if not recovery_done:
                            page = r.repo.recover(limit=r.settings['reconcile_batch_size'],
                                                  cursor=recovery_cursor, authority=self.grant[:2])
                            recovery_cursor = page['next_cursor']
                            if page['has_more']:
                                continue
                            recovery_done = True
                        gap = r.repo.mark_reconcile_gap(startup_stamp, limit=r.settings['reconcile_batch_size'],
                                                        cursor=startup_gap_cursor)
                        startup_gap_cursor = gap['next_cursor']
                        if gap['has_more']:
                            continue
                        initialized = r.initialized = True
                        self.db_ready = True
                        self._log('INFO', 'Order executor recovering', account_id=r.account_id, schema_version=self.sample.get('schema_version'))
                    if retained is None:
                        try:
                            retained = ('result', self.results.get_nowait())
                        except queue.Empty:
                            try:
                                retained = ('observation', r.observations.get_nowait())
                            except queue.Empty:
                                pass
                    if retained:
                        if retained[0] == 'result':
                            self.db_ready = False
                            self._merge(retained[1])
                            self.capacity.release()
                            self.consumed.discard(retained[1]['token'])
                        else:
                            self.db_ready = False
                            document = r.repo.ingest_observation(retained[1][0], retained[1][1], 'callback')
                            if document and self.logged_versions.get(document['order_id']) != document.get('version'):
                                self.logged_versions[document['order_id']] = document.get('version')
                                if len(self.logged_versions) > 1024:
                                    self.logged_versions.pop(next(iter(self.logged_versions)))
                                identifiers = observation_identifiers(retained[1][0], retained[1][1])
                                self._log('INFO', 'QMT callback recorded', order_id=document['order_id'],
                                          client_order_id=document.get('client_order_id'), order_version=document.get('version'),
                                          qmt_order_id=identifiers.get('qmt_order_id'), qmt_task_id=identifiers.get('qmt_task_id'))
                        was_result = retained[0] == 'result'
                        retained = None
                        self.db_ready = initialized
                        if was_result:
                            continue
                    if not r.stop_event.is_set() and initialized:
                        self._service()
                    r.stop_event.wait(.005)
                except _ContinueMerge:
                    self.db_ready = True
                    if not r.stop_event.is_set():
                        try:
                            self._service()
                        except Exception as exc:
                            self.db_ready = False
                            r.last_error = getattr(exc, 'code', 'PERSISTENCE_UNAVAILABLE')
                    continue
                except Exception as exc:
                    self.db_ready = False
                    code = getattr(exc, 'code', 'PERSISTENCE_UNAVAILABLE')
                    if r.last_error != code:
                        self._log('ERROR', 'Order persistence suspended', error_code=code)
                    r.last_error = code
                    # retained 不移除；提交结果不明可幂等重写结果，认领异常则没有指令。
                    time.sleep(.05)
        finally:
            self.db_ready = False
            self.authority_stop.set()
            self.authority_done.wait()
            if r.local_lock:
                r.local_lock.release()
            r.initialized = r.recovery_complete = False
            self.stopped = r.stopped = True
            r.stopped_event.set()

    def _service(self):
        """每个有界归并批次后提供一次后台派发/对账服务机会。"""
        r = self.r
        r.repo.replay_pending_observations(limit=r.settings['reconcile_batch_size'])
        if (r.observation_gap or self.fact_generation != self.handled_gap_generation) and self.gap_stamp is None:
            self.gap_stamp = iso_datetime()
            self.gap_generation = self.fact_generation
        if self.gap_stamp is not None:
            gap = r.repo.mark_reconcile_gap(self.gap_stamp, limit=r.settings['reconcile_batch_size'], cursor=self.gap_cursor)
            self.gap_cursor = gap['next_cursor']
            if not gap['has_more']:
                self.gap_stamp = None
                self.handled_gap_generation = self.gap_generation
                r.observation_gap = self.handled_gap_generation != self.fact_generation
            else:
                return
        self._reconcile_step()
        if r.recovery_complete and self.authorized():
            self._dispatch()
        if r.clock() - self.last_sample >= 1:
            self.sample = r.repo.health()
            self.db_ready = True
            self.sampled_at = iso_datetime()
            self.last_sample = r.clock()

    def _dispatch(self):
        r = self.r
        # 同时只认领一笔提交；预取/准备不把整批订单变成 SUBMITTING。
        if not any(key[0] == 'submit' for key in self.pending):
            rows = r.repo.queued_orders(limit=r.settings['submit_batch_size'], cursor=self.cursors['submit'])
            self.cursors['submit'] = rows[-1]['order_id'] if rows else None
            for document in rows:
                key = ('submit', document['order_id'])
                if key in self.pending:
                    continue
                try:
                    r._runtime_smart_window(document['request'])
                except Exception as exc:
                    def reject(row):
                        if row['submission_status'] == 'QUEUED':
                            row.update(submission_status='EXPIRED', error={'code': exc.code, 'message': exc.message})
                            recompute_order(row)
                    r.repo.update_order(document['order_id'], 'ORDER_EXPIRED', reject)
                    continue
                stage = document.get('preparation_stage', 'RESOLVE')
                if not document.get('preparation_complete'):
                    if self._enqueue('prepare', document=document, stage=stage, pending_key=key):
                        self.pending.add(key)
                    break
                if not self.capacity.acquire(False):
                    break
                try:
                    grant = self.grant
                    claimed, prepared = r.repo.claim_submission(document['order_id'], authority=grant[:2])
                    if claimed:
                        self.pending.add(key)
                        attempt = prepared['attempts'][-1]
                        self.queues['submit'].put_nowait(dict(kind='submit', token=attempt['attempt_id'],
                            authority=grant[:2], document=prepared, pending_key=key))
                    else:
                        self.capacity.release()
                except Exception:
                    self.capacity.release()
                    raise
                break
        rows = r.repo.cancellation_orders(limit=r.settings['cancel_batch_size'], cursor=self.cursors['cancel'])
        self.cursors['cancel'] = rows[-1]['order_id'] if rows else None
        for document in rows:
            key = ('cancel', document['order_id'])
            if key in self.pending:
                continue
            actions = pending_cancellations(document)
            if not actions or not self.capacity.acquire(False):
                continue
            try:
                grant = self.grant
                action = dict(actions[0], cancel_request_id=document['active_cancel_request_id'])
                claimed, prepared = r.repo.claim_cancel(document['order_id'], action, authority=grant[:2])
                if claimed:
                    action = prepared['attempts'][-1]
                    self.pending.add(key)
                    self.queues['cancel'].put_nowait(dict(kind='cancel', token=action['attempt_id'],
                        authority=grant[:2], document=prepared, action=action, pending_key=key))
                else:
                    self.capacity.release()
            except Exception:
                self.capacity.release()
                raise

    def _merge(self, result):
        r = self.r
        kind = result['kind']
        if kind == 'query':
            if result['status'] == 'RETURNED':
                # 每条原始事实独立事务，游标保留于结果上以便故障重试。
                index = result.get('merge_index', 0)
                values = result['value']
                for raw in values[index:index + r.settings['reconcile_batch_size']]:
                    r.repo.ingest_observation(result['query_kind'], raw, 'history' if result.get('dates') else 'query')
                    index += 1
                    result['merge_index'] = index
                if index < len(values):
                    raise _ContinueMerge()
            else:
                self.round['complete'] = False
                self._log('WARNING', 'QMT reconciliation incomplete', kind=result['query_kind'], error_code=(result.get('error') or {}).get('code', result['status']))
                if not result.get('dates'):
                    self.round['live_complete'] = False
            self.round['waiting'] = False
            self.round['stage'] += 1
            return
        doc = result['document']
        status = result['status']
        def update(row):
            if kind == 'prepare':
                if row['submission_status'] != 'QUEUED':
                    return
                if status == 'RETURNED':
                    value = result['value']
                    row.update(copy_json(value['updates']))
                    row['preparation_stage'] = value['stage']
                    row['preparation_complete'] = value['stage'] is None
                    if value['stage'] is None and row.get('order_type') == 'BASKET':
                        row['basket_state'] = 'VERIFIED'
                elif status == 'UNKNOWN':
                    row.update(submission_status='REJECTED', error=result.get('error'))
                    recompute_order(row)
                return
            for attempt in row['attempts']:
                if attempt.get('attempt_id') == result['token']:
                    attempt.update(status=status, returned_at=iso_datetime(), return_value=result.get('value'), error=result.get('error'))
            if kind == 'submit' and status != 'RETURNED':
                # 认领后不回 QUEUED；无调用证明也保守留给恢复/对账。
                if row['submission_status'] == 'SUBMITTING':
                    if status == 'ABORTED_NO_CALL' and (result.get('error') or {}).get('code') == 'ORDER_EXPIRED':
                        row.update(submission_status='EXPIRED', error=result['error'])
                    else:
                        row.update(submission_status='UNKNOWN', execution_status='UNKNOWN')
            if kind == 'cancel' and status == 'RETURNED' and result.get('value') is not True:
                for attempt in row['attempts']:
                    if attempt.get('attempt_id') == result['token']:
                        attempt['status'] = 'REJECTED'
            recompute_order(row)
        r.repo.update_order(doc['order_id'], kind.upper() + '_CALL_' + status, update)
        self.pending.discard(result['pending_key'])
        self._log('INFO' if status == 'RETURNED' else 'WARNING', 'QMT execution stage persisted',
                  stage=kind, outcome=status, order_id=doc['order_id'], client_order_id=doc.get('client_order_id'),
                  attempt_id=result['token'], cancel_request_id=(result.get('action') or {}).get('cancel_request_id'),
                  error_code=(result.get('error') or {}).get('code'))

    def _reconcile_step(self):
        r = self.r
        if self.round is None:
            if r.clock() < self.next_round:
                return
            self.round = dict(id=uuid.uuid4().hex, stage=0, waiting=False, complete=True,
                              generation=self.fact_generation, cursor=None, frozen=False, live_complete=True)
        current = self.round
        if not current['frozen']:
            batch = r.repo.begin_reconcile_batch(limit=r.settings['reconcile_batch_size'],
                                                 round_id=current['id'], cursor=current['cursor'])
            current['cursor'] = batch['next_cursor']
            if batch['has_more']:
                return
            current['frozen'] = True
            current['cursor'] = None
            today = utc_now().astimezone(dt.timezone(dt.timedelta(hours=8))).date()
            earliest = r.repo.reconcile_history_start()
            dates = ()
            if earliest:
                start = parse_timestamp(earliest).astimezone(dt.timezone(dt.timedelta(hours=8))).date()
                if start < today:
                    dates = (start.strftime('%Y%m%d'), (today - dt.timedelta(days=1)).strftime('%Y%m%d'))
            self.round['queries'] = [('task', ()), ('order', ()), ('deal', ())]
            if dates:
                self.round['queries'].extend([('order', dates), ('deal', dates)])
        if current['waiting']:
            return
        if current['stage'] < len(current['queries']):
            kind, dates = current['queries'][current['stage']]
            if self._enqueue('query', query_kind=kind, dates=dates):
                current['waiting'] = True
            return
        batch = r.repo.reconcile_round_batch(current['id'], limit=r.settings['reconcile_batch_size'],
                                             cursor=current['cursor'])
        complete = current['complete'] and current['generation'] == self.fact_generation
        stamp = iso_datetime()
        for document in batch['orders']:
            if document['submission_status'] == 'SUBMITTING':
                attempts = [a for a in document['attempts'] if a.get('kind') == 'SUBMIT']
                started = attempts[-1].get('created_at', document['updated_at']) if attempts else document['updated_at']
                if (parse_timestamp(stamp) - parse_timestamp(started)).total_seconds() >= r.confirmation_timeout:
                    def unknown(row):
                        if row['submission_status'] == 'SUBMITTING':
                            row.update(submission_status='UNKNOWN', execution_status='UNKNOWN',
                                       error={'code': 'SUBMISSION_OUTCOME_UNKNOWN', 'message': 'QMT acknowledgement is not yet associated'})
                    document = r.repo.update_order(document['order_id'], 'SUBMISSION_UNKNOWN', unknown)
            r.repo.finish_reconcile(document['order_id'], document.get('reconcile_round_fact_version', -1), complete, stamp)
        current['cursor'] = batch['next_cursor']
        if batch['has_more']:
            return
        if current['live_complete']:
            r.recovery_complete = True
        if complete:
            r.last_reconciled_at = stamp
        r.history_coverage_complete = current['complete']
        self.round = None
        self.next_round = r.clock() + 1

    def health(self):
        r = self.r
        grant = self.grant
        alive = r.last_tick is not None and r.clock() - r.last_tick < 5
        return dict(http_running=not r.stop_event.is_set(), database_available=self.db_ready and self.sample.get('ready', True),
                    trading_configured=r.repo is not None, scheduler_alive=alive,
                    recovery_complete=r.recovery_complete, history_coverage_complete=r.history_coverage_complete,
                    accepting_orders=bool(self.authorized() and r.recovery_complete and alive),
                    executor_owned=bool(grant and r.clock() < grant[2]), schema_version=self.sample.get('schema_version'),
                    unknown_order_count=self.sample.get('unknown_order_count'), pending_count=self.sample.get('pending_count'),
                    account_id=r.account_id, last_reconciled_at=r.last_reconciled_at, error_code=r.last_error,
                    observation_gap=r.observation_gap, sampled_at=self.sampled_at,
                    lifecycle=('STOPPED' if self.stopped else 'STOPPING' if r.stop_event.is_set() else 'RUNNING' if r.recovery_complete else 'RECOVERING' if r.initialized else 'STARTING'),
                    state='STOPPED' if self.stopped else ('STOPPING' if r.stop_event.is_set() else 'RUNNING'),
                    queues=dict([(k, v.qsize()) for k, v in self.queues.items()] + [('results', self.results.qsize()), ('observations', r.observations.qsize())]),
                    overflow_count=self.overflows, tick_ms=self.tick_ms, qmt_ms=self.qmt_ms,
                    authority_remaining_ms=max(0, (grant[2] - r.clock()) * 1000) if grant else 0)


class _ContinueMerge(Exception):
    """保留快照游标并向其他后台工作让出，不丢弃未合并记录。"""
