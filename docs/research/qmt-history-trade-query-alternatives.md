# QMT 历史委托与成交查询：已核实的接口边界

Last modified: 2026-09-28

本记录只比对迅投官方文档与本机安装包的静态代码；未连接 QMT、查询账户或验证返回数据。

## 内置策略 API

- 官方仍列出 `get_history_trade_detail_data(accountID, strAccountType, strDatatype, strStratDate, strEndDate)`，数据类型包含 `POSITION`、`ORDER`、`DEAL`，示例使用 `YYYYMMDD` 日期。文档同时写了“返回元组”和“返回 list”，须以目标客户端实测确定结果结构。[官方交易函数文档：历史交易明细](https://dict.thinktrader.net/innerApi/trading_function.html)
- 本机 `C:\国金QMT交易端模拟\bin.x64\Lib\site-packages\xtquant\qmttools\functions.py:283` 定义 `get_trade_detail_data`（通过 `callFormula(..., 'gettradedetail', ...)`），没有 `get_history_trade_detail_data` 或 `get_value_by_order_id` 定义。这说明**这份 Python 包未暴露这两个函数**，不能推出所有 QMT 版本均已删除。内置策略运行环境可能另行注入函数，尚未验证。来源：上述本机 `functions.py` 全文静态搜索。
- `get_trade_detail_data(accountID, strAccountType, strDatatype[, strategyName])` 的官方参数中没有日期，支持 `ORDER`、`DEAL` 等类型及委托/成交的策略名过滤。它可用于读取交易明细，但官方这一节没有承诺任意历史日期范围；不能直接当作历史区间接口。[官方交易函数文档：交易明细](https://dict.thinktrader.net/innerApi/trading_function.html)
- `get_value_by_order_id(orderId, accountID, strAccountType, strDatatype)` 官方说明是按已知委托号取 `ORDER` 或 `DEAL` 对象，没有日期区间参数；只能作为逐单查询候选，不能代替历史列表扫描。[官方交易函数文档：按委托号查询](https://dict.thinktrader.net/innerApi/trading_function.html)
- 本机 `C:\国金QMT交易端模拟\python\_PyContextInfo.py:136` 还有 `ContextInfo.get_tradedatafromerds(accounttype, accountid, startdate, enddate)`，但第 137 行仅转发给 `self.context`。目前没有核实到它的官方公开契约、返回结构或本机底层实现；它只是待验证的候选，不能据此认定为历史 `ORDER`/`DEAL` 替代接口。来源：上述本机 `_PyContextInfo.py:136-137`；对照[官方交易函数文档](https://dict.thinktrader.net/innerApi/trading_function.html)。

## MiniQMT XtQuant API

- 官方 `query_stock_orders(account, cancelable_only=False)` 和 `query_stock_trades(account)` 明确分别返回**当日**所有委托、当日所有成交；均无日期区间参数。`None` 可能是查询失败，也可能是当日列表为空。[官方 XtQuant 文档：委托/成交查询](https://dict.thinktrader.net/nativeApi/xttrader.html#委托查询)
- 本机 `C:\国金QMT交易端模拟\bin.x64\Lib\site-packages\xtquant\xttrader.py:627` 提供 `query_stock_order(account, order_id)`，使用 `QueryStockOrdersReq.m_nOrderID` 查询单个委托；第 647、682 行分别提供当日委托、成交列表接口。官方当前页面明确写了列表接口，未找到单笔 `query_stock_order` 的独立说明；本机单笔接口同样不带日期范围。来源：上述本机 `xttrader.py:627-697`；[官方 XtQuant 文档：查询接口](https://dict.thinktrader.net/nativeApi/xttrader.html#股票查询接口)。
- 官方还列出 `export_data(account, result_path, data_type, start_time=None, end_time=None, user_param={})` 及基于导出文件读取的 `query_data(...)`，示例数据类型为 `deal`。这是官方文档中**有起止时间参数**的另一条查询/导出路径，但文档未给出日期格式、支持的数据类型全集及目标柜台的可用范围，不能仅凭签名保证覆盖历史 `ORDER`/`DEAL`。[官方 XtQuant 文档：通用数据导出与查询](https://dict.thinktrader.net/nativeApi/xttrader.html#通用数据导出)
- 本机 `C:\国金QMT交易端模拟\bin.x64\Lib\site-packages\xtquant\xttrader.py` 没有 `export_data` 或 `query_data` 定义，因此上述官方路径也不能直接套用于这份安装包。若需要历史区间数据，应先确认目标安装包/客户端对相应接口的暴露与账户权限，再做只读小范围查询及柜台记录对账。来源：上述本机 `xttrader.py` 全文静态搜索；[官方 XtQuant 文档：通用数据导出与查询](https://dict.thinktrader.net/nativeApi/xttrader.html#通用数据导出)。

目前未确认这份安装包有可用的、等价于按日期范围取历史委托与成交的接口。需要持续留存当日查询和委托/成交回调的原始事实；历史缺口须用有来源的柜台记录或经过验证的接口补齐，不能把当日空结果记作历史完整。此为基于上述接口范围的实施推论，未做运行验证。
