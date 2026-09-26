# QMT HTTP Feed

策略文件：

`strategies/http_feed.py`

该策略在本机启动一个零第三方依赖的通用 HTTP 服务：

```text
http://127.0.0.1:1688/{method}
```

除下载任务和状态查询外，HTTP 层不处理具体 QMT 业务。普通请求会被规范化成
一个 JSON 任务：

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

任务进入有界队列后，由 `schedule_run` 调用 `process_http_requests` 拉取。
单项请求交给 `dispatch_request` 按 `method` 分支处理，批量请求则由
`process_batch_request` 跨多个 tick 增量推进。当前实现了 `account` 和
`get_stock_list_in_sector`、`get_sector_list`、`get_trading_dates`
、`get_instrument_detail`、`get_divid_factors`、`get_weight_in_index`
、`get_instrument_details`、`get_divid_factors_batch`、`get_weights_in_index`
、`get_full_tick`、`get_his_index_data`、`get_his_contract_list`、`get_longhubang`
、`get_market_data_ex`、`get_financial_data`、`download_history_data`
和 `get_download_status` 分支。

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
  -d '{"fields":["close","volume"],"stock_code":["600000.SH","000001.SZ"],"period":"1m","start_time":"20260701093000","end_time":"20260701150000","count":100,"dividend_type":"none","fill_data":false,"subscribe":false}' `
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

- `fields`：可选，字符串数组，最多 32 项。省略或传空数组时，FEED 不会
  把空数组直接交给 QMT，而是展开为固定字段集：
  - 普通 K 线：`time`、`open`、`high`、`low`、`close`、`volume`、
    `amount`、`settle`、`openInterest`、`preClose`、`suspendFlag`。
  - `tick`：`time`、`lastPrice`、`lastClose`、`open`、`high`、`low`、
    `close`、`volume`、`amount`、`settle`、`openInterest`、
    `stockStatus`。
  - Level-2 周期：按 `l2quote`、`l2quoteaux`、`l2order`、
    `l2transaction`、`l2transactioncount`、`l2orderqueue` 分别展开
    为一组常用且有界的字段。
  - `period=follow` 时使用本次 schedule 回调传入的
    `ContextInfo.period` 选择字段集，不缓存 `ContextInfo`。当前周期
    无法识别时返回 `400 INVALID_PARAMS`；显式传入非空 `fields` 不受
    此限制。
- `stock_code`：必填，`stock.market` 字符串或数组，最多 20 项。
- `period`：可选，默认 `follow`；由当前 QMT 客户端校验具体周期。
- `start_time`、`end_time`：可选，格式为 `YYYYMMDD` 或
  `YYYYMMDDHHMMSS`。
- `count`：可选，FEED 默认 1，范围为 1 到 1000。FEED 不接受 QMT 的
  `-1` 全量模式，避免 HTTP 请求把全部历史加载到策略线程。
- `dividend_type`：可选，支持 `follow`、`none`、`front`、`back`、
  `front_ratio`、`back_ratio`。
- `fill_data`：可选，默认 `false`，避免把停牌或缺失历史数据自动填充成
  看似真实的行情；确实需要 QMT 填充时可显式传 `true`。
- `subscribe`：可选，但只能为 `false`。`get_market_data_ex` 的自动订阅
  没有可供 HTTP 层管理的订阅 ID，只能在策略停止时统一释放，因此 FEED
  拒绝 `true`，避免重复请求耗尽订阅额度。

请求按股票数、字段数和 `count` 估算出的数据单元不能超过 20000。
返回结果同样最多 20000 个表格单元；Level-2 档位等嵌套数据转换后最多
50000 个 JSON 值。返回值中的 numpy 数组会转换为 JSON 数组，`NaN`、
正负无穷会转换为 `null`。历史数据应先在 QMT 数据管理中下载；
`subscribe=false` 不会补取尚未下载的数据。

## 下载历史行情

FEED 支持通过 QMT 内置 `download_history_data` 发起单证券历史行情下载。
启动下载只能使用 POST：

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d '{"stockcode":"600000.SH","period":"1m","startTime":"20260701093000","endTime":"20260701150000"}' `
  "http://127.0.0.1:1688/download_history_data"
```

成功时立即返回 `202` 和任务号：

```json
{
  "task_id": "download-4a6d2f6d9d8a45ad8d03d5baf9e4d6a1",
  "status": "queued",
  "stockcode": "600000.SH",
  "period": "1m",
  "startTime": "20260701093000",
  "endTime": "20260701150000",
  "created_at": 1782871200.0,
  "updated_at": 1782871200.0,
  "message": "queued"
}
```

参数：

- `stockcode`：必填，单个 `stock.market` 格式证券代码。
- `period`：必填，只接受 `tick`、`1m`、`5m`、`1d`。
- `startTime`、`endTime`：可选，空字符串表示不限定；非空时必须是
  `YYYYMMDD` 或 `YYYYMMDDHHMMSS`，且开始时间不能晚于结束时间。
- `incrementally`：可选布尔值或 `null`。只有显式传入时才会作为
  `incrementally=` 关键字参数传给 QMT；如果当前 QMT 不支持该参数，
  任务会变为 `failed`，FEED 不做兼容性重试。

下载进度通过任务快照查询：

```powershell
curl.exe --get `
  --data-urlencode "task_id=download-4a6d2f6d9d8a45ad8d03d5baf9e4d6a1" `
  "http://127.0.0.1:1688/get_download_status"
```

`get_download_status` 也接受 POST JSON：

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d '{"task_id":"download-4a6d2f6d9d8a45ad8d03d5baf9e4d6a1"}' `
  "http://127.0.0.1:1688/get_download_status"
```

状态只保存在进程内存中，重启策略后会丢失。`status` 取值：

- `queued`：已进入 FEED 队列，尚未进入 QMT 调度回调。
- `running`：本次 schedule 回调已经开始执行 `download_history_data`。
- `completed`：`download_history_data` 函数已经返回；这只说明 QMT API
  调用结束，不证明本地历史数据覆盖完整。
- `failed`：参数进入队列后执行失败，`error.code` 和 `error.message`
  会保留 QMT 异常信息。

单证券下载没有来自 QMT 的真实百分比进度，FEED 不伪造百分比。
`get_download_status` 直接读取线程安全内存快照，不进入 QMT 请求队列；
因此即使某个下载正在 QMT 策略线程里同步执行，HTTP 状态轮询也不会排队等
下一次 QMT 调度。内存任务表最多保留 1000 个任务；满时会优先淘汰已经结束
的任务，若全是未结束任务则返回 `429 DOWNLOAD_TASKS_FULL`。

`download_history_data` 是 QMT 同步调用，FEED 无法从 HTTP 线程强制中断。
调用期间它会占用本策略的 QMT 请求处理路径，其他需要 QMT 的 FEED 查询会
等到该调用返回后再处理。

停止与请求入队通过同一把短锁协调。停止先于下载入队时返回
`503 FEED_STOPPING`，并移除尚未受理的任务；不会在队列清空后留下无人执行的新任务。

## 最新全推 Tick

查询浦发银行的最新全推快照：

```powershell
curl.exe --get `
  --data-urlencode "stock_code=600000.SH" `
  "http://127.0.0.1:1688/get_full_tick"
```

也可以使用 POST JSON 一次查询多个合约：

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d '{"stock_code":["600000.SH","000001.SZ"]}' `
  "http://127.0.0.1:1688/get_full_tick"
```

`stock_code` 必填，接受一个 `stock.market` 字符串或字符串数组。虽然 QMT
全推数据本身没有品种数量限制，FEED 为保护策略线程把单次请求限制为最多
20 个合约。接口在本次 schedule 回调传入的 `ContextInfo` 上调用
`ContextInfo.get_full_tick(stock_code)`，不缓存 `ContextInfo`。

返回对象以合约代码为 key，每个值是该合约的最新 Tick 字典。例如：

```json
{
  "600000.SH": {
    "time": 1782871200000,
    "lastPrice": 10.1,
    "lastClose": 10.0,
    "amount": 312345678.0,
    "volume": 123456,
    "askPrice": [10.11, 10.12, 10.13, 10.14, 10.15],
    "bidPrice": [10.10, 10.09, 10.08, 10.07, 10.06]
  }
}
```

具体字段随 QMT 客户端和行情权限变化，调用方不应假设所有字段都存在。
numpy 数组会转换为 JSON 数组，`NaN`、正负无穷会转换为 `null`；单次
响应最多转换 50000 个 JSON 值。该接口只读取客户端缓存的最新全推快照，
不能查询历史 Tick，也不需要建立订阅。若没有五档盘口，请检查客户端的
全推行情级别。

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

不同 QMT 客户端的 `ContextInfo.get_instrument_detail` 签名不一致。FEED 会先按
新版形式调用 `get_instrument_detail(stockcode, iscomplete)`；如果客户端明确
报告位置参数数量不匹配，则自动退回本机兼容形式
`get_instrument_detail(stockcode)`。单参数客户端会忽略请求中的 `iscomplete`，
但仍返回该客户端能够提供的名称、上市日期、证券类型等实际字段。函数内部
真正抛出的 `TypeError` 不会触发兼容回退。

## 除权除息与复权因子

查询浦发银行的全部除权除息日及复权因子：

```powershell
curl.exe --get `
  --data-urlencode "stockcode=600000.SH" `
  "http://127.0.0.1:1688/get_divid_factors"
```

`stockcode` 必填，必须使用 `stock.market` 格式。接口在当前 schedule 回调
传入的 `ContextInfo` 上调用 `ContextInfo.get_divid_factors`，不缓存
`ContextInfo`。

返回对象的 key 是除权除息日的 Unix 毫秒时间戳。JSON 对象只能使用字符串
作为 key，因此 QMT 返回的整数时间戳会转换为十进制字符串。每条长度为 7
的数组依次表示：每股红利、每股送股、每股转增、配股、配股价、是否股改、
复权系数。例如：

```json
{
  "1689868800000": [0.32, 0.0, 0.0, 0.0, 0.0, 0, 1.04507]
}
```

无记录时返回空对象 `{}`。FEED 最多接受 1000 条记录，并校验时间戳、
数组长度和数值类型，避免异常 QMT 返回值进入 HTTP JSON 响应。

## 指数成分权重

查询万科 A 在沪深 300 指数中的绝对权重：

```powershell
curl.exe --get `
  --data-urlencode "indexcode=000300.SH" `
  --data-urlencode "stockcode=000002.SZ" `
  "http://127.0.0.1:1688/get_weight_in_index"
```

`indexcode` 和 `stockcode` 都是必填参数，必须使用 `stock.market` 格式。
接口调用当前 schedule 回调传入的
`ContextInfo.get_weight_in_index(indexcode, stockcode)`，不缓存
`ContextInfo`。

成功时直接返回一个有限浮点数，单位为 `%`。例如：

```json
0.438
```

表示该股票的绝对权重为 `0.438%`。QMT 接口没有日期参数，FEED 也不接受
额外的日期字段。

## 批量基础数据接口

以下三个接口接受 1 到 20 个不重复的 `stock.market` 代码：

```text
POST /get_instrument_details
POST /get_divid_factors_batch
POST /get_weights_in_index
```

批量获取合约基本信息：

```powershell
$body = @{
  stock_code = @("000063.SZ", "600000.SH")
  iscomplete = $true
} | ConvertTo-Json -Compress

Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:1688/get_instrument_details" `
  -ContentType "application/json; charset=utf-8" `
  -Body $body
```

批量接口与单项接口使用相同的 QMT 签名兼容规则；在只支持单参数调用的客户端
上，`iscomplete` 同样属于尽力而为参数，客户端能够返回哪些字段以实际结果为准。

批量获取复权因子：

```powershell
$body = @{
  stock_code = @("000063.SZ", "600000.SH")
} | ConvertTo-Json -Compress

Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:1688/get_divid_factors_batch" `
  -ContentType "application/json; charset=utf-8" `
  -Body $body
```

批量获取股票在同一指数中的权重：

```powershell
$body = @{
  indexcode = "000300.SH"
  stock_code = @("000002.SZ", "600000.SH")
} | ConvertTo-Json -Compress

Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:1688/get_weights_in_index" `
  -ContentType "application/json; charset=utf-8" `
  -Body $body
```

三个接口统一返回：

```json
{
  "results": {
    "000002.SZ": 0.438
  },
  "errors": {
    "600000.SH": {
      "code": "INVALID_QMT_RESULT",
      "message": "get_weight_in_index did not return a number"
    }
  }
}
```

单只股票失败不会中止其他股票，HTTP 状态仍为 `200`，调用方应同时检查
`results` 和 `errors`。参数整体非法时返回 `400`。批量状态只保存在模块级
`RequestJob` 中，不挂到 `ContextInfo`。

`get_divid_factors_batch` 每只股票最多保留 1000 条记录，整个批次最多保留
20000 条记录；超过批次总上限的股票会进入 `errors`，错误码为
`RESULT_LIMIT_EXCEEDED`。

`get_instrument_details` 整个批次最多转换 50000 个 JSON 值；超过总上限的
股票同样进入 `errors`，错误码为 `RESULT_LIMIT_EXCEEDED`。

批次不会在一个 schedule 回调里循环调用 QMT。每个 tick 只处理一只股票，
未完成任务保留到下一个 tick，并使用该 tick 新传入的 `ContextInfo`。批次执行期间
若策略停止，等待中的 HTTP 请求立即返回 `503`。活动批次会独占后续 tick，最多
连续占用 20 个 tick；这保证同一批次按输入顺序完成，但该批次完成前，队列中的
后续请求不会执行。

## 已退市合约列表

查询上交所期权市场的已退市合约：

```powershell
curl.exe --get `
  --data-urlencode "market=SHO" `
  "http://127.0.0.1:1688/get_his_contract_list"
```

`market` 必填，FEED 会转换为大写，仅允许 1 到 16 个 ASCII 字母或数字。
QMT 官方示例包括 `SH`、`SZ`、`SHO`、`SZO`、`IF`。成功时直接返回合约代码
JSON 列表，例如：

```json
["10000001.SHO", "10000002.SHO"]
```

接口在当前 schedule 回调传入的 `ContextInfo` 上调用
`ContextInfo.get_his_contract_list(market)`，不缓存 `ContextInfo`；每个 tick
最多处理一个此类请求，返回列表最多 50000 项。

该 API 读取本地的过期合约列表。首次使用或返回空列表时，应在 QMT
“行情/数据管理”的智能下载或补充数据中下载“过期合约列表”，重新加载策略后
再调用。

## 历史指数数据

查询沪深 300 的历史指数权重数据：

```powershell
curl.exe --get `
  --data-urlencode "stockcode=000300.SH" `
  "http://127.0.0.1:1688/get_his_index_data"
```

`stockcode` 必填，必须使用 `stock.market` 格式。接口在当前 schedule 回调传入的
`ContextInfo` 上调用 `ContextInfo.get_his_index_data(stockcode)`，不缓存
`ContextInfo`。当前安装的 QMT Python 包装器将该调用转发到历史指数权重查询，
但当前在线文档没有给出稳定的返回字段定义，因此 FEED 不假定固定业务字段，
只将字典、数组或二维表格结果规范化为有界 JSON。非有限浮点数转换为 `null`。

该查询每个 schedule tick 最多执行一个，返回结果最多包含 50000 个 JSON 值。
实际数据范围和可用性取决于本机 QMT 版本及已下载的数据。

## 龙虎榜

查询万科 A 在指定日期范围内的龙虎榜数据：

```powershell
curl.exe --get `
  --data-urlencode "stock_list=000002.SZ" `
  --data-urlencode "startTime=20260701" `
  --data-urlencode "endTime=20260731" `
  "http://127.0.0.1:1688/get_longhubang"
```

多个股票建议使用 POST JSON，也可在 GET 中重复 `stock_list` 参数：

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d '{"stock_list":["000002.SZ","600000.SH"],"startTime":"20260701","endTime":"20260731"}' `
  "http://127.0.0.1:1688/get_longhubang"
```

参数与 [QMT `ContextInfo.get_longhubang` 官方文档](https://dict.thinktrader.net/innerApi/data_function.html#contextinfo-get-longhubang-获取龙虎榜数据)
一致：

- `stock_list`：1 到 20 个 `stock.market` 格式的股票代码。
- `startTime`、`endTime`：必填，格式为 `YYYYMMDD`，开始日期不能晚于结束日期。
- 单次查询跨度最多 3660 天。
- 股票数量乘以自然日数量不能超过 3660；例如单股可查约十年，20 股最多查约半年。

成功结果统一为二维表格 JSON：

```json
{
  "index": [0],
  "columns": ["stockCode", "date", "close", "buyTraderBooth", "sellTraderBooth"],
  "data": [["000002.SZ", "2026-07-01T00:00:00", 12.5, {"index": [], "columns": [], "data": []}, {"index": [], "columns": [], "data": []}]]
}
```

`buyTraderBooth` 和 `sellTraderBooth` 原本也是 DataFrame，FEED 会递归转换成同样的
`index`、`columns`、`data` 结构。顶层最多返回 1000 行、32 列和 20000 个单元格，
整个结果最多包含 50000 个 JSON 值；该查询每个 schedule tick 最多执行一个。

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
5. QMT 回调使用 `get_nowait()`，每轮最多接受 10 个任务；全推 Tick、
   K 线行情、除权因子、历史指数数据、已退市合约列表或龙虎榜查询每轮最多执行 1 个。
   三个批量基础数据接口同样每轮只处理批次中的 1 只股票。
6. `dispatch_request` 执行对应的极短 QMT 操作后设置结果和 `Event`。

QMT 策略线程不等待队列、HTTP 连接或 HTTP 服务线程。当前各业务分支会
同步执行一次对应的 QMT API，包括 `get_trade_detail_data`、
`get_stock_list_in_sector`、`get_sector_list` 和 `get_trading_dates`；
`get_instrument_detail`、`get_divid_factors`、`get_weight_in_index`
、`get_full_tick`、`get_his_index_data`、`get_his_contract_list`、`get_longhubang`
、`get_market_data_ex`、`get_financial_data` 和 `download_history_data`
同样为同步调用。其中 `download_history_data` 可能明显慢于普通查询，会阻塞
后续需要 QMT 的 FEED 请求，直到 QMT 函数返回。
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
    if method == "get_divid_factors":
        return handle_get_divid_factors(ContextInfo, params)
    if method == "get_weight_in_index":
        return handle_get_weight_in_index(ContextInfo, params)
    if method == "get_full_tick":
        return handle_get_full_tick(ContextInfo, params)
    if method == "get_his_index_data":
        return handle_get_his_index_data(ContextInfo, params)
    if method == "get_his_contract_list":
        return handle_get_his_contract_list(ContextInfo, params)
    if method == "get_longhubang":
        return handle_get_longhubang(ContextInfo, params)
    if method == "get_market_data_ex":
        return handle_get_market_data_ex(ContextInfo, params)
    if method == "get_financial_data":
        return handle_get_financial_data(ContextInfo, params)

    raise FeedError(404, "METHOD_NOT_FOUND", "unsupported method")
```

普通同步接口无需增加新的 HTTP Handler。`download_history_data` 是异步
`202` 任务接口，由 HTTP 层先创建任务并入队，再由 schedule 回调执行。

批量方法不在 `dispatch_request` 中循环执行，而是由 `process_batch_request`
跨 schedule tick 增量推进：

```python
BATCH_METHODS = {
    "get_instrument_details",
    "get_divid_factors_batch",
    "get_weights_in_index",
}
```

## 日志

通用方法 `log_message(level, message, **fields)` 同时输出到 QMT console
和 UTF-8 日志文件，格式为 `YYYY-MM-DD HH:mm:ss [LEVEL] message {fields}`，
时间使用北京时间（Asia/Shanghai）。

默认目录为 `%USERPROFILE%\qmt-bridge\logs`，可修改脚本顶部的
`LOG_DIRECTORY`；文件按日期命名为 `feed-YYYY-MM-DD.log`，以追加方式写入。
日志文件不会自动清理。

启动服务以及下载开始、完成时输出 INFO；下载异常输出 ERROR。下载日志包含
`task_id`、`stockcode`、`period`、`startTime`、`endTime`，显式传入时也记录
`incrementally`。完成表示 QMT 函数已返回，不代表行情覆盖已验证。
文件写入失败时尝试向 console 输出 ERROR，不改变下载任务结果。

`LOG_LEVEL` 默认 `INFO`，可设置 `DEBUG`、`INFO`、`WARNING`、`ERROR`，
同时控制 console 与文件的最低输出级别。成功的 `get_download_status` 轮询
使用 DEBUG，默认不输出；失败响应仍按 WARNING/ERROR 记录。

GET/POST 请求日志包含 `request_id`、HTTP 方法、接口名和白名单参数摘要。
数组仅记录数量及前 3 项，每项最多 64 字符，字符串最多 128 字符；
不记录完整请求体、账户响应或行情响应。响应日志包含状态码、耗时（毫秒）、
响应字节数，下载响应另含 `task_id`。同一下载的执行日志关联该 `request_id`。
请求被拒绝、排队超时、客户端断开记为 WARNING；QMT 执行异常记为 ERROR。
超时响应记录队列长度，策略停止记录排队请求数与未完成下载数。

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
- `202`：异步下载任务已创建。
- `400`：参数或 JSON 不合法。
- `404`：method 不存在或账户不存在。
- `405`：方法不允许，例如使用 GET 启动下载。
- `413`：POST JSON 超过 1 MiB。
- `429`：请求队列已满。
- `500`：未处理的 QMT 异常。
- `503`：策略正在停止。
- `504`：请求在规定时间内未被 QMT 调度处理。

服务只监听 `127.0.0.1`。端口 `1688` 被占用时初始化会明确失败，不会
静默切换端口。
