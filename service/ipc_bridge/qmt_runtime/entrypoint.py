# -*- coding: utf-8 -*-
"""QMT callbacks and scheduler. Only the schedule callback dispatches APIs."""
import datetime as dt
import math
import re
from .executor import QmtRuntime, DeferredResult, console_log

_RUNTIME = None
_TIMER = None
_ADAPTER = None


def _setting(apis, context, name, default):
    return apis.get(name, apis.get(name.upper(), getattr(context, name, default)))


def _account_id(value):
    if isinstance(value, str) and value.strip():
        return value.strip()
    if type(value) is int and value > 0:
        return str(value)
    if type(value) is float and math.isfinite(value) and value.is_integer() and 0 < value < 2 ** 53:
        return str(int(value))
    raise ValueError('account_id must be nonempty string or exact positive integer')


def _positive_integer(value, name):
    if type(value) is str and re.match(r'^[0-9]+$', value.strip()):
        value = int(value.strip())
    if type(value) is float and math.isfinite(value) and value.is_integer():
        value = int(value)
    if type(value) is not int or value < 1:
        raise ValueError(name + ' must be a positive integer')
    return value


def start(context, apis, role, executor_factory):
    global _RUNTIME, _TIMER, _ADAPTER
    if _RUNTIME is not None:
        raise RuntimeError('IPC strategy already running')
    bridge_id = _setting(apis, context, 'bridge_id', 'default')
    if not isinstance(bridge_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', bridge_id):
        raise ValueError('invalid bridge_id')
    account_id = _account_id(_setting(apis, context, 'account_id', '66027616'))
    pipe_name = str(_setting(apis, context, 'pipe_name',
                            '\\\\.\\pipe\\qmt-bridge.{0}.{1}'.format(bridge_id, role)))
    interval = _positive_integer(_setting(apis, context, 'schedule_interval_ms', 10), 'schedule_interval_ms')
    budget = _positive_integer(_setting(apis, context, 'schedule_budget_ms', 50), 'schedule_budget_ms')
    if interval < 1 or budget < 1 or not bridge_id:
        raise ValueError('invalid IPC settings')
    if role == 'order':
        context.set_account(account_id)
    execute, _ADAPTER = executor_factory(context, apis, account_id,
                                         lambda: runtime.request_context)
    runtime = QmtRuntime(role, bridge_id, account_id, pipe_name, execute, budget_ms=budget)
    _RUNTIME = runtime
    try:
        _TIMER = context.schedule_run(process_ipc_requests, '20200101000000', -1,
                                      dt.timedelta(milliseconds=interval), 'ipc_' + role + '_timer')
    except Exception:
        _RUNTIME.close()
        _RUNTIME = None
        raise
    print('IPC_{0} started bridge={1} account={2}'.format(role.upper(), bridge_id, account_id))


def process_ipc_requests(ContextInfo):
    runtime = _RUNTIME
    if runtime is not None:
        runtime.tick()


def stop(ContextInfo):
    global _RUNTIME, _TIMER, _ADAPTER
    runtime, timer = _RUNTIME, _TIMER
    _RUNTIME = _TIMER = _ADAPTER = None
    if runtime is not None:
        runtime.closed = True
    try:
        if timer is not None:
            ContextInfo.cancel_schedule_run(timer)
    finally:
        if runtime is not None:
            runtime.close()


def handlebar(ContextInfo):
    pass


def _callback(event, value):
    runtime, adapter = _RUNTIME, _ADAPTER
    if runtime is None or adapter is None:
        return
    try:
        runtime.observe(event, adapter.snapshot(value))
    except Exception as exc:
        runtime.gap = True
        print('IPC callback snapshot failed: {0}'.format(exc))


def order_callback(ContextInfo, orderInfo):
    _callback('order', orderInfo)


def deal_callback(ContextInfo, dealInfo):
    _callback('deal', dealInfo)


def task_callback(ContextInfo, taskInfo):
    _callback('task', taskInfo)


def orderError_callback(ContextInfo, passOrderInfo, msg):
    runtime, adapter = _RUNTIME, _ADAPTER
    if runtime is None or adapter is None:
        return
    try:
        payload = adapter.snapshot(passOrderInfo)
        if not isinstance(payload, dict):
            raise ValueError('error callback must snapshot to an object')
        payload['error_message'] = str(msg)
        runtime.observe('error', payload)
    except Exception as exc:
        runtime.gap = True
        print('IPC error snapshot failed: {0}'.format(exc))


def account_callback(ContextInfo, accountInfo):
    pass
