"""Order executor and QMT strategy entrypoint."""
import datetime as dt
from service.ipc_bridge.ipc_protocol.order_common import OrderError, parse_timestamp
from ..entrypoint import start
from ..executor import console_log
from .adapter import QmtAdapter
from .query import dispatch_request


def _submit_document(adapter, document, now=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    request = document.get('request') or {}
    deadline = document.get('submit_before') or request.get('submit_before')
    if deadline and now >= parse_timestamp(deadline):
        raise OrderError(422, 'ORDER_EXPIRED', 'Order expired before QMT call; no native call made')
    execution = request.get('execution') or {}
    if execution.get('type') == 'SMART':
        zone = dt.timezone(dt.timedelta(hours=8))
        today = now.astimezone(zone).date()
        start = parse_timestamp(execution['start_at']).astimezone(zone)
        end = parse_timestamp(execution['end_at']).astimezone(zone)
        if end <= now:
            raise OrderError(422, 'SMART_WINDOW_EXPIRED', 'SMART window ended before QMT call; no native call made')
        if start.date() != today or end.date() != today:
            raise OrderError(422, 'SMART_DATE_UNSUPPORTED', 'SMART times must use current Shanghai day; no native call made')
    return adapter.submit(document)


def create_order_executor(context, apis, account_id, request_context):
    def native_log(level, message, **fields):
        fields.update(request_context())
        console_log(level, message, **fields)
    adapter = QmtAdapter(apis, context, account_id, logger=native_log)
    def execute(method, params):
        if method == 'order.submit':
            return _submit_document(adapter, params['document'])
        if method == 'order.prepare_step':
            return adapter.prepare_step(params['document'], params.get('stage', 'RESOLVE'), params.get('payload'))
        if method == 'order.cancel_action':
            return adapter.cancel_action(params['action'])
        if method == 'order.capabilities':
            return {name: callable(apis.get(name)) for name in
                    ('passorder', 'algo_passorder', 'smart_algo_passorder', 'cancel',
                     'cancel_task', 'set_basket', 'get_basket', 'get_history_trade_detail_data')}
        if method == 'order.query' and 'kind' in params:
            return adapter.query(params['kind'])
        request = params.get('request') if method == 'order.query' else {'method': method, 'params': params}
        return dispatch_request(context, request, apis, account_id, logger=native_log)
    return execute, adapter


def start_order(context, apis):
    start(context, apis, 'order', create_order_executor)
