# QMT 龙虎榜 API 研究

## 结论

`ContextInfo.get_longhubang(stock_list, startTime, endTime)` 是 QMT 官方行情函数里的“龙虎榜”查询接口。
它接收股票列表、起始日期、结束日期，返回 `pandas.DataFrame`。返回列包含：

- `stockCode`
- `stockName`
- `date`
- `reason`
- `close`
- `SpreadRate`
- `TurnoverVolume`
- `Turnover_Amount`
- `buyTraderBooth`
- `sellTraderBooth`

`buyTraderBooth` / `sellTraderBooth` 本身还是嵌套 `pandas.DataFrame`，字段包含：

- `traderName`
- `buyAmount`
- `buyPercent`
- `sellAmount`
- `sellPercent`
- `totalAmount`
- `rank`
- `direction`

## 官方文档证据

### 本地 KB 原始块

- `C:\Users\dying\git\home\qmt-explore\data\chunks.jsonl:58`
  - 命中 `chunk_id = data_function:contextinfo-get-longhubang-获取龙虎榜数据:0:0`
  - 直接给出原型：`ContextInfo.get_longhubang(stock_list, startTime, endTime)`
  - 参数：
    - `stock_list`: `list`，示例 `['600000.SH', '600036.SH']`
    - `startTime`: `str`，示例 `'20170101'`
    - `endTime`: `str`，示例 `'20180101'`
  - 返回：
    - `pandas.DataFrame`
    - 字段如上所列
  - 嵌套字段：
    - `buyTraderBooth` / `sellTraderBooth` 包含营业部明细字段，如 `traderName`、`buyAmount`、`sellAmount`、`rank` 等

### 本地 KB 原始 HTML

- `C:\Users\dying\git\home\qmt-explore\data\raw\data_function.json:1`
  - 来源 URL 为 `https://dict.thinktrader.net/innerApi/data_function.html`
  - HTML 中有 `ContextInfo.get_longhubang - 获取龙虎榜数据` 的官方锚点
  - 这条记录的抓取时间为 `2026-07-10T14:08:15.344267+00:00`
  - `last_modified` 头为 `Tue, 07 Jul 2026 06:09:04 GMT`

### 官方在线页

- `https://dict.thinktrader.net/innerApi/data_function.html#contextinfo-get-longhubang-获取龙虎榜数据`
  - 与本地 KB 里的 HTML 记录对应

### 本机 QMT 版本差异

- `C:\国金QMT交易端模拟\python\_PyContextInfo.py:266-304`
  - 本机包装器签名是
    `get_longhubang(stock_list=[], startTime='', endTime='', count=-1)`。
  - 传入日期字符串时仍按官方文档的日期区间调用，包装器会把 `count` 重置为
    `-1`；当前 FEED 使用的正是这个稳定的三参数日期区间形式。
  - 包装器明确构造上述 10 个顶层字段，并把买卖席位转换为嵌套 DataFrame。

### 数据准备

- `C:\Users\dying\git\home\qmt-explore\data\chunks.jsonl:760`
  - QMT 客户端手册第 443 页把“龙虎榜数据”列为“补充数据”可下载的数据类型。
- `C:\Users\dying\git\home\qmt-explore\data\chunks.jsonl:769`
  - 第 452 页说明客户端龙虎榜页面展示代码、名称、上榜日、收盘价、涨跌幅、
    上榜原因，以及买入/卖出金额最大的前 5 名营业部。

因此，如果接口返回空表，应先检查目标日期是否确实有上榜记录，并在 QMT
数据管理中确认已经补充下载“龙虎榜数据”。后一句是根据客户端数据管理说明
给出的排查建议，不是官方 API 页面承诺的自动下载行为。

## 桥接实现检查

### 入口和调用

- `C:\Users\dying\git\home\qmt-bridge\strategies\http_feed.py:1360-1427`
  - `handle_get_longhubang(ContextInfo, params)` 只允许 `stock_list`、`startTime`、`endTime`
  - 会拒绝未知参数
  - 会校验日期顺序和范围
  - 最终直接调用 `ContextInfo.get_longhubang(stock_list, start_time, end_time)`
  - 返回结果再经 `qmt_table_to_json(...)` 序列化

### 约束与限制

- `C:\Users\dying\git\home\qmt-bridge\strategies\http_feed.py:47-53`
  - 桥接层给龙虎榜查询加了本地保护上限：
    - `MAX_LONGHUBANG_STOCKS = 20`
    - `MAX_LONGHUBANG_DATE_DAYS = 3660`
    - `MAX_LONGHUBANG_STOCK_DAYS = 3660`
    - `MAX_LONGHUBANG_ROWS = 1000`
    - `MAX_LONGHUBANG_COLUMNS = 32`
    - `MAX_LONGHUBANG_CELLS = 20000`
    - `MAX_LONGHUBANG_JSON_VALUES = 50000`

### 行为验证

- `C:\Users\dying\git\home\qmt-bridge\tests\test_http_feed.py:2306-2451`
  - 覆盖了：
    - 嵌套营业部表序列化
    - 非法参数拒绝
    - 非法 QMT 返回值拒绝
    - 与历史类接口同一轮次最多处理一个任务

## 一致性判断

`qmt-bridge` 的 `get_longhubang` 实现与官方文档的核心签名是一致的：

- 都使用 `stock_list + startTime + endTime`
- 都把结果当作表格返回
- 桥接层没有重命名或重排官方字段

桥接层额外加入了本地安全/资源上限，这是实现约束，不是官方 API 语义的一部分。

## 需要注意

- 官方文档给出的返回是 `pandas.DataFrame`，并说明 `buyTraderBooth` 和 `sellTraderBooth` 是嵌套表。
- `qmt-bridge` 侧只是做 JSON 安全序列化和输入限制，不应把这些限制误读成 QMT 原生 API 的限制。
- 这次研究里，`python -m qmt_kb search` 在当前 PowerShell 会话中不可用，因此证据直接来自本地 KB 导出的 `chunks.jsonl` 与 `data/raw/data_function.json`。

## 证据清单

- `C:\Users\dying\git\home\qmt-explore\data\chunks.jsonl:58`
- `C:\Users\dying\git\home\qmt-explore\data\raw\data_function.json:1`
- `C:\Users\dying\git\home\qmt-explore\data\chunks.jsonl:760`
- `C:\Users\dying\git\home\qmt-explore\data\chunks.jsonl:769`
- `C:\国金QMT交易端模拟\python\_PyContextInfo.py:266-304`
- `C:\Users\dying\git\home\qmt-bridge\strategies\http_feed.py:47-53`
- `C:\Users\dying\git\home\qmt-bridge\strategies\http_feed.py:1360-1427`
- `C:\Users\dying\git\home\qmt-bridge\tests\test_http_feed.py:2306-2451`
- `https://dict.thinktrader.net/innerApi/data_function.html#contextinfo-get-longhubang-获取龙虎榜数据`
