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
`get_stock_list_in_sector`、`get_sector_list` 分支。

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

## QMT 线程边界

1. HTTP 请求在线程化 HTTP 服务中接收。
2. HTTP 请求线程使用 `put_nowait()` 放入任务。
3. HTTP 请求线程等待任务 `Event`，不阻塞 QMT 策略线程。
4. `schedule_run` 每 10 毫秒调用一次 `process_http_requests`。
5. QMT 回调使用 `get_nowait()`，每轮最多接受 10 个任务。
6. `dispatch_request` 执行对应的极短 QMT 操作后设置结果和 `Event`。

QMT 策略线程不等待队列、HTTP 连接或 HTTP 服务线程。当前各业务分支会
同步执行一次对应的 QMT API，包括 `get_trade_detail_data`、
`get_stock_list_in_sector` 和 `get_sector_list`；这些调用必须保持极短。
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
