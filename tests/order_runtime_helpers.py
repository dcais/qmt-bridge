"""Explicit native adapter injection for historical unit-test fixtures only."""
from service.ipc_bridge.order.runtime import OrderRuntime as ServiceOrderRuntime
from service.ipc_bridge.qmt_runtime.order.adapter import QmtAdapter


def OrderRuntime(apis, context, account_id, *args, **kwargs):
    kwargs.setdefault('adapter', QmtAdapter(apis, context, account_id, logger=kwargs.get('logger')))
    return ServiceOrderRuntime(apis, context, account_id, *args, **kwargs)
