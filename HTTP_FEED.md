# QMT HTTP Feed

策略文件：

`strategies/http_feed.py`

该策略在本机启动一个零第三方依赖的通用 HTTP 服务：

```text
http://127.0.0.1:1688/{method}
```

HTTP 层不处理具体 QMT 业务。每个请求都会被规范化成一个 JSON 任务：

```json
{
  "method": "account",
  "params": {}
}
```

其中：

- `method` 来自 URL 的第一个路径段。
- GET 请求的 `params` 来自 query string。
- POST 请求的 `params` 来自 JSON object 请求体。
- HTTP 服务只支持 GET 和 POST；其他 HTTP verb 不进入任务队列。

任务进入有界队列后，由 `schedule_run` 调用 `process_http_requests` 拉取，
再交给 `dispatch_request` 按 `method` 分支处理。当前实现了 `account` 和
`get_stock_list_in_sector`、`get_sector_list`、`get_trading_dates`
、`get_instrument_detail`、`get_market_data_ex` 和
`get_financial_data` 分支。

## Account demo

默认查询账户：

```text
GET http://127.0.0.1:1688/account
```

成功时直接返回账户对象的全部 `m_` 字段：

```json
{
  "m_accountID": "66027616",
  "m_available": 1234.5
}
```

也可以通过 GET 参数指定账户：

```text
GET /account?accountId=66027616&accountType=STOCK
```

或者发送 POST JSON：

```http
POST /account
Content-Type: application/json

{
  "accountId": "66027616",
  "accountType": "STOCK"
}
```

## 板块成分股

method 名称与 QMT API 保持一致：

```text
GET /get_stock_list_in_sector?sectorname=沪深300
```

内部调用：

```python
ContextInfo.get_stock_list_in_sector("沪深300")
```

返回值是成分股代码数组，代码格式为 `stockcode.market`：

```json
[
  "600000.SH",
  "000001.SZ"
]
```

PowerShell 中建议让 curl 对中文板块名执行 URL 编码：

```powershell
curl.exe --get `
  --data-urlencode "sectorname=沪深300" `
  "http://127.0.0.1:1688/get_stock_list_in_sector"
```

也支持可选的实时数据毫秒时间戳：

```http
POST /get_stock_list_in_sector
Content-Type: application/json

{
  "sectorname": "沪深300",
  "realtime": 1720000000000
}
```

`sectorname` 必须是客户端左侧板块列表中的板块名，包括自定义板块。
`realtime` 省略时调用单参数形式。

## 板块目录

查询顶层板块目录：

```powershell
curl.exe "http://127.0.0.1:1688/get_sector_list"
```

等价于在 QMT 策略线程中调用：

```python
get_sector_list("")
```

查询指定目录节点：

```powershell
curl.exe --get `
  --data-urlencode "node=我的" `
  "http://127.0.0.1:1688/get_sector_list"
```

返回数组的第一项是当前节点下的板块名，第二项是子目录节点名：

```json
[
  ["我的自选", "龙头", "卖出篮子"],
  ["新建分类1"]
]
```

`node` 必须是字符串；省略时默认为空字符串，即顶层目录。接口直接返回
QMT `get_sector_list(node)` 的二维数组结果。

## 交易日

查询指定股票最近 30 个日线交易日：

```powershell
curl.exe --get `
  --data-urlencode "stockcode=600000.SH" `
  --data-urlencode "count=30" `
  --data-urlencode "period=1d" `
  "http://127.0.0.1:1688/get_trading_dates"
```

指定日期范围：

```powershell
curl.exe --get `
  --data-urlencode "stockcode=600000.SH" `
  --data-urlencode "start_date=20260701" `
  --data-urlencode "end_date=20260731" `
  --data-urlencode "count=30" `
  --data-urlencode "period=1d" `
  "http://127.0.0.1:1688/get_trading_dates"
```

返回值为字符串数组：

```json
[
  "20260701",
  "20260702",
  "20260703"
]
```

参数与 QMT `ContextInfo.get_trading_dates` 保持一致：

- `count`：必填，整数，范围为 1 到 10000。
- `stockcode`：可选，默认空字符串，表示当前图代码。
- `start_date`、`end_date`：可选，格式为 `YYYYMMDD` 或
  `YYYYMMDDHHMMSS`。
- `period`：可选，默认 `1d`；支持 `1d`、`1m`、`3m`、`5m`、
  `15m`、`30m`、`1h`、`1w`、`1mon`、`1q`、`1hy`、`1y`。

日线返回 `YYYYMMDD`；其他周期返回 `YYYYMMDDHHMMSS`。FEED 对
`count` 设置上限，避免单个 HTTP 请求在 QMT 策略线程中产生无界工作量。

## 行情数据

查询浦发银行最近 10 根日线：

```powershell
curl.exe --get `
  --data-urlencode "fields=open" `
  --data-urlencode "fields=high" `
  --data-urlencode "fields=low" `
  --data-urlencode "fields=close" `
  --data-urlencode "stock_code=600000.SH" `
  --data-urlencode "period=1d" `
  --data-urlencode "count=10" `
  --data-urlencode "subscribe=false" `
  "http://127.0.0.1:1688/get_market_data_ex"
```

也可以使用 POST JSON 查询多个合约：

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d '{"fields":["close","volume"],"stock_code":["600000.SH","000001.SZ"],"period":"1m","start_time":"20260701093000","end_time":"20260701150000","count":100,"dividend_type":"none","fill_data":true,"subscribe":false}' `
  "http://127.0.0.1:1688/get_market_data_ex"
```

返回值按股票代码分组，每个 QMT DataFrame 规范化为 JSON：

```json
{
  "600000.SH": {
    "index": ["20260701093000", "20260701093100"],
    "columns": ["close", "volume"],
    "data": [
      [10.10, 1200],
      [10.12, 1800]
    ]
  }
}
```

参数与 QMT `ContextInfo.get_market_data_ex` 对应：

- `fields`：必填，字符串数组，至少 1 项、最多 32 项。FEED 不接受空数组
  的“全部字段”模式，避免 QMT 返回规模无法在调用前准确估算。
- `stock_code`：必填，`stock.market` 字符串或数组，最多 20 项。
- `period`：可选，默认 `follow`；由当前 QMT 客户端校验具体周期。
- `start_time`、`end_time`：可选，格式为 `YYYYMMDD` 或
  `YYYYMMDDHHMMSS`。
- `count`：可选，FEED 默认 1，范围为 1 到 1000。FEED 不接受 QMT 的
  `-1` 全量模式，避免 HTTP 请求把全部历史加载到策略线程。
- `dividend_type`：可选，支持 `follow`、`none`、`front`、`back`、
  `front_ratio`、`back_ratio`。
- `fill_data`：可选，默认 `true`。
- `subscribe`：可选，但只能为 `false`。`get_market_data_ex` 的自动订阅
  没有可供 HTTP 层管理的订阅 ID，只能在策略停止时统一释放，因此 FEED
  拒绝 `true`，避免重复请求耗尽订阅额度。

请求按股票数、字段数和 `count` 估算出的数据单元不能超过 20000。
返回结果同样最多 20000 个表格单元；Level-2 档位等嵌套数据转换后最多
50000 个 JSON 值。返回值中的 numpy 数组会转换为 JSON 数组，`NaN`、
正负无穷会转换为 `null`。历史数据应先在 QMT 数据管理中下载；
`subscribe=false` 不会补取尚未下载的数据。

## 合约详细信息

获取合约基本信息：

```powershell
curl.exe --get `
  --data-urlencode "stockcode=600000.SH" `
  "http://127.0.0.1:1688/get_instrument_detail"
```

获取全部字段：

```powershell
curl.exe --get `
  --data-urlencode "stockcode=600000.SH" `
  --data-urlencode "iscomplete=true" `
  "http://127.0.0.1:1688/get_instrument_detail"
```

返回 QMT `ContextInfo.get_instrument_detail` 的字典：

```json
{
  "ExchangeID": "SH",
  "InstrumentID": "600000",
  "InstrumentName": "浦发银行"
}
```

`stockcode` 必填，必须使用 `stock.market` 格式。`iscomplete` 可选，
默认 `false`；GET 参数接受 `true`、`false`，POST JSON 接受布尔值。
返回字段由 QMT 客户端版本和 `iscomplete` 决定，上面的 JSON 仅是示例，
调用方不应把它当作固定字段集合。

该分支要求客户端支持新版 `ContextInfo.get_instrument_detail`。旧版客户端
只有 `ContextInfo.get_instrumentdetail`，且不支持 `iscomplete`；应升级
QMT 客户端后再使用本接口。

## 财务数据

接口支持 QMT `ContextInfo.get_financial_data` 的区间查询和单根 K 线查询。
区间查询是默认模式，建议使用 POST JSON：

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d '{"mode":"range","fieldList":["ASHAREBALANCESHEET.fix_assets"],"stockList":["600000.SH"],"startDate":"20260701","endDate":"20260731","report_type":"announce_time"}' `
  "http://127.0.0.1:1688/get_financial_data"
```

参数：

- `fieldList`：必填，财务字段数组，最多 16 项。
- `stockList`：必填，`stock.market` 格式的股票数组，最多 20 项。
- `startDate`、`endDate`：必填，格式为 `YYYYMMDD`。
- `report_type`：可选，默认 `announce_time`；也可传 `report_time`。

区间、字段数和股票数估算出的数据单元不能超过 20000，避免单次同步查询
在 QMT 策略线程中产生无界工作量。QMT 可能返回 Series、DataFrame 或
旧版 pandas Panel，FEED 会将其规范化为 JSON。例如 DataFrame：

```json
{
  "type": "dataframe",
  "index": ["20260701", "20260702"],
  "columns": ["fix_assets"],
  "data": [[100.5], [null]]
}
```

Panel 返回 `items`、`major_axis`、`minor_axis` 和三维 `data`；Series
返回 `index` 和一维 `data`。`NaN`、正负无穷会转换为 JSON `null`。

查询某根 K 线位置对应的单个财务值：

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d '{"mode":"bar","tabname":"ASHAREBALANCESHEET","colname":"fix_assets","market":"SH","code":"600000","barpos":12}' `
  "http://127.0.0.1:1688/get_financial_data"
```

`barpos` 必须是 0 到 10000000 的整数，成功时直接返回数值或 `null`。
该模式也接受 `report_type`。FEED 始终按 QMT 官方示例将 `barpos` 作为
第 5 个位置参数；显式提供 `report_type` 时通过同名关键字参数传入，
避免依赖官方原型与示例中不一致的位置参数顺序。

使用前必须先在 QMT 数据管理中下载对应财务数据。回测和历史研究通常应使用
默认的 `announce_time`，避免读取当时尚未披露的财报；只有明确需要按报告期
取数时才使用 `report_time`。

## QMT 线程边界

1. HTTP 请求在线程化 HTTP 服务中接收。
2. HTTP 请求线程使用 `put_nowait()` 放入任务。
3. HTTP 请求线程等待任务 `Event`，不阻塞 QMT 策略线程。
4. `schedule_run` 每 10 毫秒调用一次 `process_http_requests`。
5. QMT 回调使用 `get_nowait()`，每轮最多接受 10 个任务；行情查询每轮
   最多执行 1 个。
6. `dispatch_request` 执行对应的极短 QMT 操作后设置结果和 `Event`。

QMT 策略线程不等待队列、HTTP 连接或 HTTP 服务线程。当前各业务分支会
同步执行一次对应的 QMT API，包括 `get_trade_detail_data`、
`get_stock_list_in_sector`、`get_sector_list` 和 `get_trading_dates`；
`get_instrument_detail`、`get_market_data_ex` 和 `get_financial_data`
同样为同步调用。这些调用必须保持极短。
策略会在以下字段中记录最近和历史最长处理耗时，便于在 QMT 中观察：

```python
_FEED_STATE.last_dispatch_seconds
_FEED_STATE.max_dispatch_seconds
```

50 毫秒调度预算只控制是否开始处理下一项，不能中断已经开始的 QMT API。
不能保证快速完成的操作不应加入同步分支，应改成发起异步操作或返回缓存快照。

`stop()` 只发送停止信号、取消定时任务并唤醒排队请求，不执行
`thread.join()` 或阻塞式 HTTP 关闭。

`Queue`、`Event`、`Thread` 和 `HTTPServer` 等运行时对象只保存在策略模块
全局 `_FEED_STATE` 中，不能挂到 `ContextInfo`。QMT 会对 `ContextInfo`
执行 `deepcopy`，线程锁对象无法 pickle。

## 增加新 method

在 `dispatch_request` 中增加白名单分支：

```python
def dispatch_request(ContextInfo, request):
    method = request.get("method")
    params = request.get("params")

    if method == "account":
        return handle_account(ContextInfo, params)
    if method == "get_stock_list_in_sector":
        return handle_get_stock_list_in_sector(ContextInfo, params)
    if method == "get_sector_list":
        return handle_get_sector_list(ContextInfo, params)
    if method == "get_trading_dates":
        return handle_get_trading_dates(ContextInfo, params)
    if method == "get_instrument_detail":
        return handle_get_instrument_detail(ContextInfo, params)
    if method == "get_market_data_ex":
        return handle_get_market_data_ex(ContextInfo, params)
    if method == "get_financial_data":
        return handle_get_financial_data(ContextInfo, params)

    raise FeedError(404, "METHOD_NOT_FOUND", "unsupported method")
```

HTTP 层无需增加新的 Handler。

## 启动

在 QMT 中加载并运行 `strategies/http_feed.py`。默认配置：

```python
ACCOUNT_ID = "66027616"
ACCOUNT_TYPE = "STOCK"
HTTP_HOST = "127.0.0.1"
HTTP_PORT = 1688
```

启动成功后日志显示：

```text
QMT HTTP feed listening on http://127.0.0.1:1688/{method}
```

PowerShell 请求：

```powershell
Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:1688/account'
```

## HTTP 状态

- `200`：method 执行成功。
- `400`：参数或 JSON 不合法。
- `404`：method 不存在或账户不存在。
- `413`：POST JSON 超过 1 MiB。
- `429`：请求队列已满。
- `500`：未处理的 QMT 异常。
- `503`：策略正在停止。
- `504`：请求在规定时间内未被 QMT 调度处理。

服务只监听 `127.0.0.1`。端口 `1688` 被占用时初始化会明确失败，不会
静默切换端口。
