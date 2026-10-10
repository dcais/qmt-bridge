# QMT IPC Feed

Last modified: 2026-10-10

Feed 的 HTTP 接口由仓库外部进程 `service.ipc_bridge` 提供，统一地址为
`http://127.0.0.1:8888/{method}`。在 QMT 中运行生成的 GBK 单文件
`strategies/ipc_feed.py`；QMT 仅通过 Windows named pipe 接收请求，在
`schedule_run` 回调中调用 QMT API 并返回普通 JSON 数据。QMT 策略不读取应用源文件、
不监听 HTTP、不连接 PostgreSQL，也不写日志文件；诊断只打印 console。

从仓库根目录安装外部服务依赖、配置 `.env` 并启动：

```powershell
python -m pip install -r requirements-service.txt
Copy-Item .env.example .env
python tools/build_ipc_strategies.py
python tools/build_ipc_strategies.py --check
python -m service.ipc_bridge
```

已有 `.env` 时直接编辑。环境变量覆盖 `.env`；默认端口由
`QMT_BRIDGE_PORT=8888` 指定。QMT 参数 `bridge_id` 须与外部
`QMT_BRIDGE_ID` 一致；默认均为 `default`。完整配置见 [README](README.md)。
构建器从 UTF-8 源模块生成 Python 3.6 兼容的 GBK 策略，`--check` 只验证产物一致性。
原 HTTP 策略保留在 `strategies/http_feed.py` 与 `strategies/http_order.py`，部署说明见 `HTTP_FEED.md` 和 `HTTP_ORDER.md`。

GET 参数来自 query string，单项保持字符串、重复键形成列表；POST 使用 UTF-8 JSON
object。Feed 方法由 `service/ipc_bridge/feed/requests.py` 的 `FEED_METHODS` 白名单
和 QMT 模块 `feed/qmt_api.py` 的 dispatcher共同限制。三个批量接口逐项推进，
保持 `results` / `errors` 结构。下载任务由外部 `FeedService` 管理。

`/account`、`/positions` 和 `/health` 统一由 ORDER 服务提供，详情见
[IPC_ORDER.md](IPC_ORDER.md)。Feed 不另行提供账户或健康响应。

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
  "http://127.0.0.1:8888/get_stock_list_in_sector"
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
curl.exe "http://127.0.0.1:8888/get_sector_list"
```

等价于在 QMT 策略线程中调用：

```python
get_sector_list("")
```

查询指定目录节点：

```powershell
curl.exe --get `
  --data-urlencode "node=我的" `
  "http://127.0.0.1:8888/get_sector_list"
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
  "http://127.0.0.1:8888/get_trading_dates"
```

指定日期范围：

```powershell
curl.exe --get `
  --data-urlencode "stockcode=600000.SH" `
  --data-urlencode "start_date=20260701" `
  --data-urlencode "end_date=20260731" `
  --data-urlencode "count=30" `
  --data-urlencode "period=1d" `
  "http://127.0.0.1:8888/get_trading_dates"
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
  "http://127.0.0.1:8888/get_market_data_ex"
```

也可以使用 POST JSON 查询多个合约：

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d '{"fields":["close","volume"],"stock_code":["600000.SH","000001.SZ"],"period":"1m","start_time":"20260701093000","end_time":"20260701150000","count":100,"dividend_type":"none","fill_data":false,"subscribe":false}' `
  "http://127.0.0.1:8888/get_market_data_ex"
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
  "http://127.0.0.1:8888/download_history_data"
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
  "http://127.0.0.1:8888/get_download_status"
```

`get_download_status` 也接受 POST JSON：

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d '{"task_id":"download-4a6d2f6d9d8a45ad8d03d5baf9e4d6a1"}' `
  "http://127.0.0.1:8888/get_download_status"
```

状态只保存在进程内存中，重启外部 bridge 服务后会丢失。`status` 取值：

- `queued`：已进入外部下载队列，尚未发起 IPC 请求。
- `running`：外部服务已开始等待 IPC 下载调用；QMT 在 schedule 回调执行原生函数。
- `completed`：`download_history_data` 函数已经返回；这只说明 QMT API
  调用结束，不证明本地历史数据覆盖完整。
- `failed`：执行异常、IPC 超时或服务停止，`error.code` 和 `error.message`
  保存原因。IPC 超时不证明 QMT 调用已停止；服务不会自动重试下载。

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
  "http://127.0.0.1:8888/get_full_tick"
```

也可以使用 POST JSON 一次查询多个合约：

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d '{"stock_code":["600000.SH","000001.SZ"]}' `
  "http://127.0.0.1:8888/get_full_tick"
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
  "http://127.0.0.1:8888/get_instrument_detail"
```

获取全部字段：

```powershell
curl.exe --get `
  --data-urlencode "stockcode=600000.SH" `
  --data-urlencode "iscomplete=true" `
  "http://127.0.0.1:8888/get_instrument_detail"
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
  "http://127.0.0.1:8888/get_divid_factors"
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
  "http://127.0.0.1:8888/get_weight_in_index"
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
  -Uri "http://127.0.0.1:8888/get_instrument_details" `
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
  -Uri "http://127.0.0.1:8888/get_divid_factors_batch" `
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
  -Uri "http://127.0.0.1:8888/get_weights_in_index" `
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
  "http://127.0.0.1:8888/get_his_contract_list"
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
  "http://127.0.0.1:8888/get_his_index_data"
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
  "http://127.0.0.1:8888/get_longhubang"
```

多个股票建议使用 POST JSON，也可在 GET 中重复 `stock_list` 参数：

```powershell
curl.exe -X POST `
  -H "Content-Type: application/json" `
  -d '{"stock_list":["000002.SZ","600000.SH"],"startTime":"20260701","endTime":"20260731"}' `
  "http://127.0.0.1:8888/get_longhubang"
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
  "http://127.0.0.1:8888/get_financial_data"
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
  "http://127.0.0.1:8888/get_financial_data"
```

`barpos` 必须是 0 到 10000000 的整数，成功时直接返回数值或 `null`。
该模式也接受 `report_type`。FEED 始终按 QMT 官方示例将 `barpos` 作为
第 5 个位置参数；显式提供 `report_type` 时通过同名关键字参数传入，
避免依赖官方原型与示例中不一致的位置参数顺序。

使用前必须先在 QMT 数据管理中下载对应财务数据。回测和历史研究通常应使用
默认的 `announce_time`，避免读取当时尚未披露的财报；只有明确需要按报告期
取数时才使用 `report_time`。

## QMT 线程边界

外部 HTTP 线程处理 JSON、请求校验和下载任务缓存，通过 IPC 等待 QMT 结果。
QMT IPC 读写使用非阻塞 Windows API，由 `schedule_run` 默认每 10 毫秒调用
`process_ipc_requests`；QMT API 只在该调度回调执行。批量查询每次推进一只股票。
默认 `schedule_budget_ms=50` 只控制下一项是否开始，不能中断已经开始的原生调用。

普通请求等待时间由外部 `QMT_BRIDGE_REQUEST_TIMEOUT` 指定，默认 10 秒。
下载后台调用使用 `QMT_BRIDGE_DOWNLOAD_TIMEOUT`，默认 120 秒；超时后任务标记
`failed`，原生调用可能继续，不自动重发。下载任务不持久化，重启外部服务后旧 task ID
返回 `DOWNLOAD_TASK_NOT_FOUND`。`completed` 只证明 QMT 函数返回，不证明行情覆盖完整。

运行时队列、锁和 IPC 对象保存在模块级状态，不挂到 QMT 会 deepcopy 的 `ContextInfo`。
停止 QMT 策略会取消定时任务并关闭 IPC；外部 HTTP 和任务缓存由外部服务生命周期管理。

## 增加新 method

在 `service/ipc_bridge/feed/qmt_api.py` 增加有界参数校验、结果序列化和明确的
`dispatch_request` 分支，并更新 `feed/requests.py` 的白名单。普通请求继续使用现有
HTTP 与 IPC 通道，不开放任意函数名、表达式或脚本执行。修改源模块后重新生成并检查：

```powershell
python tools/build_ipc_strategies.py
python tools/build_ipc_strategies.py --check
```

## 日志与健康

外部服务负责 UTF-8 文件日志和外部 console 输出，目录由
`QMT_BRIDGE_LOG_DIRECTORY` 指定，默认 `var/log/ipc_bridge`；级别由
`QMT_BRIDGE_LOG_LEVEL` 指定。QMT 端仅打印 console，不读取或写入日志文件。
请求日志记录 method、状态、耗时和 request ID，不记录行情或账户响应正文。

```powershell
Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8888/health'
```

统一健康接口只接受无参数 GET；返回 ORDER 状态及 `feed`、`order` IPC 连接状态、
`bridge` 实例状态和外部 `logging` 计数。`logging.dropped` 为累计丢弃数量，
`write_errors` 为后台写入错误数量，空队列不能证明从未丢日志。
HTTP 200 只表示读到了健康快照，不能证明 IPC 可用、交易可用或行情覆盖完整。

## HTTP 状态

- `200`：method 执行成功。
- `202`：外部进程创建异步下载任务。
- `400`：参数或 JSON 不合法。
- `404`：method 或下载任务不存在。
- `405`：方法不允许，例如使用 GET 启动下载。
- `413`：POST JSON 超过 1 MiB。
- `429`：请求或下载任务容量已满。
- `500`：QMT 异常。
- `503`：服务停止或 IPC 未就绪。
- `504`：等待 QMT 结果超时；已开始的调用可能继续。

默认绑定 `127.0.0.1:8888`，端口占用时启动失败，不自动切换端口。
