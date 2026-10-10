# Frozen compatibility fixtures

`legacy_http_feed.py` and `legacy_http_order.py` are byte-identical snapshots of the
retained HTTP strategies. They are compatibility oracles for offline tests only.
Do not import them from production modules, deploy them to QMT, or regenerate them.

The deployable HTTP strategies remain in `strategies/http_feed.py` and
`strategies/http_order.py`. The additional IPC QMT strategies are `strategies/ipc_feed.py` and `strategies/ipc_order.py`,
generated as readable Python from `service/ipc_bridge/qmt_runtime` and `service/ipc_bridge/ipc_protocol` by
`tools/build_ipc_strategies.py`. External HTTP, persistence and file logging stay
in `service/ipc_bridge` and are excluded from the QMT strategies.
