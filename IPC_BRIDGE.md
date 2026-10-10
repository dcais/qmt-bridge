# IPC Bridge 接口与部署说明

更新日期：2026-10-10。

`ipc_bridge` 是外部 Python FastAPI 服务，统一接收 Feed 和 Order HTTP 请求。
QMT 原生方法在 QMT 内的 `IPC_FEED`、`IPC_ORDER` 策略执行；数据库、订单状态、
幂等、恢复、对账及文件日志由外部服务负责。当前使用 Windows 命名管道。
原 `HTTP_FEED`、`HTTP_ORDER` 策略继续保留，是独立部署方式。

## 1. 启动与环境

外部 Python 要求 3.10+。在仓库根目录安装 `requirements-service.txt`，使用项目 `.venv`。
配置文件位于 `service/ipc_bridge/`：

- `.env.staging`：staging 环境，参考 `.env.staging.example`。
- `.env.prd`：prd 环境，参考 `.env.prd.example`。
- 真实环境文件被 Git 忽略；模板账户留空，需要填写。

```powershell
.\start_ipc_bridge.cmd staging
.\start_ipc_bridge.cmd prd
```

每条命令分别启动一个前台进程，按 Ctrl+C 停止。脚本使用项目虚拟环境，固定工作目录
为仓库根目录。它不会自动启动 QMT 策略，也不会初始化或迁移数据库。

直接启动等价命令：

```powershell
.\.venv\Scripts\python.exe -m service.ipc_bridge --env-file service/ipc_bridge/.env.staging
```

显式指定的文件不存在则报错，只读取选中的文件，不叠加默认 `.env`。
进程环境变量优先于文件；不传 `--env-file` 时读取工作目录下的 `.env`。

| 配置 | 含义 |
| --- | --- |
| `QMT_BRIDGE_HOST` | 默认 `127.0.0.1`；`0.0.0.0` 监听所有本机 IPv4 网卡 |
| `QMT_BRIDGE_PORT` | HTTP 端口，默认 `8888`；下方示例使用 staging 的 `7777` |
| `QMT_BRIDGE_ID` | 桥名，用于确定管道名称，默认 `default` |
| `QMT_BRIDGE_ACCOUNT_ID` | 策略绑定账户，建议字符串形式保留前导零 |
| `QMT_BRIDGE_PG_HOST/PORT/DATABASE/USER/PASSWORD` | 外部 PostgreSQL 配置，固定 schema 为 `qmt_order` |
| `QMT_BRIDGE_REQUEST_TIMEOUT` | 普通 IPC 请求等待秒数，默认 `10` |
| `QMT_BRIDGE_DOWNLOAD_TIMEOUT` | 下载开始投递 IPC 后等待秒数，默认 `120` |
| `QMT_BRIDGE_LOG_DIRECTORY` | 日志目录；相对路径以启动工作目录为基准 |

数据库名、用户、密码三项必须同时设置；全部为空时只启用查询模式。
开启持久交易前须准备兼容的数据库结构，见 [IPC_ORDER.md](IPC_ORDER.md)。
不同环境使用各自端口、桥名、账户及数据库。单个 bridge 只启动一个 worker。
当前 HTTP 无认证，开放局域网监听时应限制到可信客户端。

启动顺序：外部 bridge → QMT 的 IPC_FEED 与 IPC_ORDER → 检查 `/health` → 客户端。
两个 QMT 策略的 `bridge_id`、`account_id` 必须与选定环境一致，可在 QMT 参数设置中填写。
策略参数修改后停止并重新运行。切换方案前停止同一账户原有的订单执行器。

## 2. HTTP 约定

统一地址 `http://主机:端口/{method}`，没有 `/feed`、`/order` 前缀。
`/order` 本身是查询单个订单的方法，不是路由前缀。
成功时直接返回 JSON，无额外 `data` 包装；错误格式为：

```json
{"error":{"code":"INVALID_PARAMS","message":"错误说明"}}
```

GET 使用 query 参数；POST 使用 UTF-8 JSON object，请求体最大 1 MiB。
GET 重复参数可表示数组。POST Feed 以 JSON 为准，不合并 query；POST Order 不接受 query。
目前不提供 `/docs`、`/redoc` 或 `/openapi.json`。

| HTTP 状态 | 含义 |
| --- | --- |
| 200 | 查询成功，或交易命令同幂等键、同内容重放 |
| 202 | 下载进入内存队列，或交易命令已持久化受理；不代表原生操作成功 |
| 400 / 404 / 405 / 413 | 参数无效 / 方法不存在 / HTTP 动词不支持 / 请求体过大 |
| 409 | 幂等键相同但内容冲突等业务冲突 |
| 422 | 业务校验失败等不可执行请求 |
| 429 | 有界队列或并发受理容量已满 |
| 501 | 当前不支持的能力，例如历史交易查询 |
| 503 / 504 | 执行端或数据库不可用等 / 等待 QMT 结果超时 |

## 3. 接口索引

详细参数、字段限制和响应结构分别见 [IPC_FEED.md](IPC_FEED.md) 与
[IPC_ORDER.md](IPC_ORDER.md)。下列名称均直接拼在根路径。

| 分组 | 方法 | 动词 |
| --- | --- | --- |
| 健康 | `health` | GET，无参数 |
| 板块与日历 | `get_sector_list`、`get_stock_list_in_sector`、`get_trading_dates` | GET / POST |
| 证券资料 | `get_instrument_detail`、`get_instrument_details` | GET / POST |
| 复权及权重 | `get_divid_factors`、`get_divid_factors_batch`、`get_weight_in_index`、`get_weights_in_index` | GET / POST |
| 行情 | `get_full_tick`、`get_market_data_ex` | GET / POST |
| 历史资料 | `get_his_index_data`、`get_his_contract_list`、`get_longhubang` | GET / POST |
| 财务 | `get_financial_data` | GET / POST |
| 下载 | `download_history_data` | POST |
| 下载状态 | `get_download_status` | GET / POST |
| 账户与持仓 | `account`、`positions`、`account_evidence_v1`、`positions_evidence_v1` | GET / POST |
| 算法配置 | `get_smart_algo_param` | GET / POST |
| 历史能力 | `history_capabilities`、`get_history_trade_detail_data` | GET；历史查询当前不支持 |
| 交易命令 | `submit_order`、`cancel_order` | POST |
| 订单查询 | `order`、`orders`、`order_events`、`capabilities` | GET |
| 交易证据 | `order_events_evidence_v1`、`recovery_evidence_v1`、`order_fee_evidence_v1` | GET |

重名接口使用统一语义：`account` 使用 ORDER 账户对象的原生 `m_` 字段；
`health` 使用 ORDER 顶层状态，并增加 `feed`、`order`、`bridge` 及外部日志状态。
查询账户参数为 `accountId`，策略配置与交易命令字段为 `account_id`，不要混淆。

## 4. Windows curl 示例

PowerShell 使用 `curl.exe`，避免旧版本 PowerShell 的 `curl` 别名。
同机使用 `127.0.0.1`；远程调用替换为 bridge 所在主机的可达 IP。

```powershell
# 健康、账户、持仓（只读）
curl.exe "http://127.0.0.1:7777/health"
curl.exe "http://127.0.0.1:7777/account"
curl.exe "http://127.0.0.1:7777/positions"

# 中兴通讯最近 100 根日线，不复权
curl.exe -G "http://127.0.0.1:7777/get_market_data_ex" --data-urlencode "stock_code=000063.SZ" --data-urlencode "period=1d" --data-urlencode "count=100" --data-urlencode "dividend_type=none" --data-urlencode "subscribe=false"

# 下载指定区间日线
$body = '{"stockcode":"000063.SZ","period":"1d","startTime":"20260101","endTime":"20261009"}'
$body | curl.exe -X POST "http://127.0.0.1:7777/download_history_data" -H "Content-Type: application/json" --data-binary "@-"

# 替换返回的 task_id 后查询状态
curl.exe -G "http://127.0.0.1:7777/get_download_status" --data-urlencode "task_id=返回的task_id"
```

日线 `period=1d`；`dividend_type=front` 表示前复权，`none` 表示不复权。
行情查询读取 QMT 已有数据，`subscribe=false` 不自动补下载。
下载状态为 `queued/running/completed/failed`；`completed` 只表示原生函数返回，
不证明指定区间覆盖完整，需回读检查。下载任务保存在外部进程内存中，重启后不保留。
交易命令输入及幂等键示例见 [IPC_ORDER.md](IPC_ORDER.md)，不要把示例当作连通性测试执行。

## 5. 健康与交易状态

`GET /health` 返回 200 只代表成功取得状态快照。应进一步检查：

- `bridge.id` 是否为目标环境，`bridge.lifecycle` 是否为 `RUNNING`。
- `feed.ready`、`order.ready` 是否为 true。
- 交易需要 `database_available`、`executor_owned`、`recovery_complete`、`accepting_orders` 等状态满足要求。
- `feed/order.pending_requests` 为正在等待结果的 IPC 请求数。
- `logging.dropped`、`logging.write_errors` 为外部日志丢弃与写入失败计数。

HTTP 202 的交易受理是数据库事务提交，不是 QMT 已接单或已成交。
重试须沿用原 `client_order_id` / `cancel_request_id`；同键不同内容报冲突。
原生调用后断线或超时可能已经产生操作，不确定状态依靠订单、任务和成交事实对账，
不会自动重新下单。当前对账只覆盖当日，不保证补齐跨日历史。

## 6. 排队、超时与日志

Feed、Order 各有独立管道和发送队列，每个通道最多 128 个等待结果的请求，
发送队列容量 256（也包括控制消息）。容量满时拒绝，不能视为无限队列。
同一通道按实际入队顺序发送，以 request_id 匹配结果；批处理及业务调度意味着
完成顺序不保证与 HTTP 到达顺序一致。交易命令先持久化，之后由订单调度器投递 IPC。

QMT 原生调用在各策略的定时回调中串行执行。`schedule_interval_ms` 默认 10，
`schedule_budget_ms` 默认 50；预算用于控制下一项是否开始，不会打断已开始的原生调用。
慢调用会阻塞同策略后续请求，两个独立通道也不保证 QMT 内部资源完全互不影响。

普通 IPC 超时默认 10 秒，包含 IPC 排队和执行等待，不包括所有 HTTP 入站调度时间。
未开始就过期的请求不会执行；已经进入原生调用的请求不能靠超时强制取消。
下载有独立后台队列，最多 64 个等待任务、一个下载 worker；默认 120 秒从投递 IPC 起算。

外部业务日志同时输出 console 与 UTF-8 文件 `order-YYYY-MM-DD.log`（上海日期）。
模板分别使用 `var/log/ipc_bridge/staging` 和 `var/log/ipc_bridge/prd`。
文件名沿用 ORDER 日志命名，但也包含 bridge 请求等业务日志；Uvicorn 自身日志输出 console。
QMT 两个策略只输出 console，不写应用文件、不连接数据库、不监听 HTTP。

## 7. 实现位置与构建

```text
service/ipc_bridge/
├── app.py                  # HTTP 路由
├── config.py               # 外部配置
├── feed/                   # 行情业务及 QMT 调用源码
├── order/                  # 订单业务、持久化及 QMT 调用源码
├── qmt/                    # QMT 入口与执行器
└── transport/
    ├── pipe/               # 当前命名管道
    └── smem/               # 预留共享内存，未实现、不可切换
```

```powershell
python tools/build_ipc_strategies.py
python tools/build_ipc_strategies.py --check
```

生成 `strategies/ipc_feed.py` 与 `strategies/ipc_order.py`，为 GBK 单文件。
QMT 所需源模块内嵌于策略并在内存加载，不在运行时读取项目源文件。
修改相关源模块后须重新构建并更新 QMT 中的策略；不要直接修改生成文件长期维护。
