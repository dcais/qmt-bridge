"""Exercise the delivered standalone code with fake QMT APIs and pipe frames."""
import ast
import shutil
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tools import build_ipc_strategies as builder
from tests.test_ipc_qmt import Endpoint


def strategy(role):
    namespace = {}
    # Execution here is a test harness; delivered strategies contain no dynamic execution.
    exec(compile(builder.render(role), '<generated ipc_' + role + '>', 'exec'), namespace)
    real_runtime = namespace['QmtRuntime']
    def runtime(*args, **kwargs):
        kwargs['endpoint_factory'] = Endpoint
        return real_runtime(*args, **kwargs)
    namespace.update(QmtRuntime=runtime, account_id='000123', bridge_id='test')
    context = SimpleNamespace(set_account=Mock(), schedule_run=Mock(return_value='timer'),
                              cancel_schedule_run=Mock())
    return namespace, context


def welcome(namespace, context, role):
    namespace['process_ipc_requests'](context)
    runtime = namespace['_RUNTIME']
    endpoint = runtime.endpoint
    endpoint.frames.append(dict(type='welcome', protocol=1, role=role,
                                bridge_id='test', account_id='000123', session_id='session'))
    namespace['process_ipc_requests'](context)
    return runtime, endpoint


def enqueue(endpoint, method, params, request_id='request', **metadata):
    endpoint.frames.append(dict(type='request', session_id='session', request_id=request_id,
                                method=method, params=params, expires_at=time.time() + 10, **metadata))


def result(endpoint, request_id='request'):
    return next(frame for frame in endpoint.sent
                if frame['type'] == 'result' and frame['request_id'] == request_id)


def test_generated_feed_uses_injected_globals_and_rebinds_on_restart():
    namespace, context = strategy('feed')
    sector = Mock(return_value=[['行业'], ['node']])
    download = Mock()
    namespace.update(get_sector_list=sector, download_history_data=download)
    namespace['init'](context)
    assert not sector.called and not download.called
    runtime, endpoint = welcome(namespace, context, 'feed')
    enqueue(endpoint, 'get_sector_list', {})
    assert not sector.called
    namespace['process_ipc_requests'](context)
    assert result(endpoint)['payload'] == [['行业'], ['node']]
    sector.assert_called_once_with('')
    enqueue(endpoint, 'download_history_data', {'stockcode': '600000.SH', 'period': '1d'}, 'download')
    namespace['process_ipc_requests'](context)
    download.assert_called_once_with('600000.SH', '1d', '', '')
    assert result(endpoint, 'download')['status'] == 200
    context.schedule_run.assert_called_once()
    namespace['stop'](context)
    assert runtime.closed and not endpoint.connected
    context.cancel_schedule_run.assert_called_once_with('timer')
    namespace.pop('get_sector_list')
    namespace['init'](context)
    _, endpoint = welcome(namespace, context, 'feed')
    enqueue(endpoint, 'get_sector_list', {})
    namespace['process_ipc_requests'](context)
    assert result(endpoint)['code'] == 'QMT_API_UNAVAILABLE'
    namespace['stop'](context)


def test_generated_feed_batch_advances_one_stock_per_schedule_tick():
    namespace, context = strategy('feed')
    context.get_instrument_detail = Mock(return_value={'InstrumentName': 'sample'})
    namespace['init'](context)
    _, endpoint = welcome(namespace, context, 'feed')
    enqueue(endpoint, 'get_instrument_details', {'stock_code': ['600000.SH', '000001.SZ']})
    namespace['process_ipc_requests'](context)
    assert context.get_instrument_detail.call_count == 1
    assert not any(frame['type'] == 'result' for frame in endpoint.sent)
    namespace['process_ipc_requests'](context)
    assert context.get_instrument_detail.call_count == 2
    assert result(endpoint)['status'] == 200
    namespace['stop'](context)


def test_generated_order_calls_native_and_snapshots_callbacks_on_schedule():
    namespace, context = strategy('order')
    native = Mock(return_value=0)
    query = Mock(return_value=[])
    namespace.update(passorder=native, get_trade_detail_data=query)
    namespace['init'](context)
    context.set_account.assert_called_once_with('000123')
    runtime, endpoint = welcome(namespace, context, 'order')
    request = dict(client_order_id='client', account_id='000123', order_type='SINGLE',
                   sizing_type='QUANTITY', symbol='600000.SH', side='BUY', quantity=100,
                   execution={'type': 'DIRECT'}, price_type='LIMIT', limit_price='10')
    document = namespace['new_order_document'](request)
    document.update(namespace['_ADAPTER'].prepare_step(document)['updates'])
    # A valid grant is tied to the actual challenge emitted by the generated runtime.
    endpoint.frames.append(dict(type='authority_grant', session_id='session',
                                challenge_id=runtime.challenge_id,
                                authority=['owner', 1], remaining_seconds=0.5))
    enqueue(endpoint, 'order.submit', {'document': document}, authority=['owner', 1])
    assert not native.called
    namespace['process_ipc_requests'](context)
    assert result(endpoint)['status'] == 200
    native.assert_called_once()
    assert native.call_args.args[2] == '000123'
    assert native.call_args.args[-1] is context
    enqueue(endpoint, 'order.query', {'kind': 'order'}, 'query')
    namespace['process_ipc_requests'](context)
    query.assert_called_once_with('000123', 'STOCK', 'order')
    assert result(endpoint, 'query')['payload'] == []
    query.reset_mock()
    query.return_value = [SimpleNamespace(m_dBalance=123.5, m_strAccountID='000123')]
    enqueue(endpoint, 'order.query', {'request': {'method': 'account', 'params': {
        'accountId': '000123', 'accountType': 'STOCK'}}}, 'account')
    namespace['process_ipc_requests'](context)
    query.assert_called_once_with('000123', 'STOCK', 'account')
    assert result(endpoint, 'account')['payload'] == {
        'm_dBalance': 123.5, 'm_strAccountID': '000123'}
    value = SimpleNamespace(m_strOrderSysID='native-order', m_strRemark=document['remark'])
    for callback in ('order_callback', 'deal_callback', 'task_callback'):
        namespace[callback](context, value)
    namespace['orderError_callback'](context, value, 'rejected')
    assert not any(frame['type'] == 'event' for frame in endpoint.sent)
    namespace['process_ipc_requests'](context)
    events = [frame for frame in endpoint.sent if frame['type'] == 'event']
    assert [frame['event'] for frame in events] == ['order', 'deal', 'task', 'error']
    assert events[0]['payload']['m_strOrderSysID'] == 'native-order'
    assert events[-1]['payload']['error_message'] == 'rejected'
    namespace['stop'](context)
    assert runtime.closed and not endpoint.connected


def test_build_ignores_timestamp_and_preserves_noop_bytes_and_mtime(tmp_path, monkeypatch):
    for package in ('service/ipc_bridge/ipc_protocol', 'service/ipc_bridge/qmt_runtime'):
        shutil.copytree(builder.ROOT / package, tmp_path / package)
    (tmp_path / 'strategies').mkdir()
    monkeypatch.setattr(builder, 'ROOT', tmp_path)
    assert builder.main([]) == 0
    paths = [tmp_path / 'strategies' / ('ipc_' + role + '.py') for role in ('feed', 'order')]
    for path in paths:
        text = path.read_bytes().decode('gbk')
        path.write_bytes(builder.TIMESTAMP.sub('# Last modified (Asia/Shanghai): 1999-01-01 00:00:00', text).encode('gbk'))
    before = [(path.read_bytes(), path.stat().st_mtime_ns) for path in paths]
    assert builder.main(['--check']) == 0
    assert builder.main([]) == 0
    assert before == [(path.read_bytes(), path.stat().st_mtime_ns) for path in paths]
    paths[0].write_bytes(paths[0].read_bytes() + b'\n# stale\n')
    stale = paths[0].read_bytes()
    assert builder.main(['--check']) == 1
    assert paths[0].read_bytes() == stale
    assert builder.main([]) == 0
    assert builder.main(['--check']) == 0


@pytest.mark.parametrize('first,second', [
    ('LIMIT = 1', 'LIMIT = 2'),
    ('import math as helper', 'import json as helper'),
    ('if True:\n    LIMIT = 1', 'def LIMIT():\n    pass'),
])
def test_flatten_rejects_conflicting_module_globals(first, second):
    bindings = {}
    builder.check_bindings(ast.parse(first), 'first', bindings)
    with pytest.raises(ValueError, match='top-level'):
        builder.check_bindings(ast.parse(second), 'second', bindings)


def test_external_runtime_requires_explicit_adapter():
    from service.ipc_bridge.order.runtime import OrderRuntime
    with pytest.raises(ValueError, match='explicit IPC adapter'):
        OrderRuntime({}, None, 'account')
