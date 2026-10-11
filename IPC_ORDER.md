# QMT IPC ORDER bridge

Last modified: 2026-10-11

ORDER 与 Feed 共用外部进程 `service.ipc_bridge` 和根路径
`http://127.0.0.1:8888/{method}`。账户、持仓、算法配置、健康和持久订单 API
统一由 ORDER 服务提供；QMT 中运行 `strategies/ipc_order.py`。
外部服务管理 HTTP、PostgreSQL、幂等、恢复、对账与文件日志；QMT 策略只做
IPC、调度、原生 QMT 调用、回报快照与 console 输出，不读取源文件、不连接数据库、
不监听 HTTP、不写日志文件。

## 启动与配置

从仓库根目录运行：

```powershell
python -m pip install -r requirements-service.txt
Copy-Item .env.example .env
python tools/build_ipc_strategies.py
python tools/build_ipc_strategies.py --check
python -m service.ipc_bridge
```

已有 `.env` 时直接编辑。外部环境变量覆盖 `.env`，所有外部设置使用
`QMT_BRIDGE_` 前缀；数据库凭据仅放在外部配置。配置示例和默认值见
[README](README.md)。统一端口通过 `QMT_BRIDGE_PORT` 配置，默认 8888。

将生成的 GBK 单文件 `strategies/ipc_order.py` 导入 QMT，设置 `bridge_id`
与外部 `QMT_BRIDGE_ID` 一致，`account_id` 与 `QMT_BRIDGE_ACCOUNT_ID` 一致。
默认桥名为 `default`、账户为 `66027616`。`schedule_interval_ms` 默认 10，
`schedule_budget_ms` 默认 50；可选 `pipe_name` 仅在双方约定相同管道名时使用。
QMT 参数变化需停止并重启策略；QMT 参数面板不再配置 HTTP 端口、PG 凭据或文件日志。
外部服务应使用单 worker，避免重复创建管道实例和执行器。

原 HTTP ORDER 策略和 `order_bridge` 源码继续保留，使用 `tools/build_order_strategy.py` 构建，部署见 `HTTP_ORDER.md`。
IPC 原生调用与查询源模块在 `service/ipc_bridge/qmt_runtime/order`，共享纯契约在 `service/ipc_bridge/ipc_protocol`，
持久化、对账和业务源模块在 `service/ipc_bridge/order`。构建器直接合并 QMT 所需安全源码，
生成按文件分节的可读 GBK 策略；时间戳仅在内容变更时更新，`--check` 忽略时间戳。
持久化与 HTTP 模块只运行在外部 Python 环境。

对账每轮只调用 `get_trade_detail_data(account_id, "STOCK", "task"/"order"/"deal")`，不调用历史交易查询函数。
`/health` 的 `reconcile_query_scope="CURRENT_DAY"`、`history_query_available=false` 明确当前范围；
`last_reconciled_at` 表示最近成功查询或订单原成功对账时间，不证明跨日历史完整。
已完整对账的终态订单跨日复用持久结案事实，正常重启和普通回报缺口都不会将其批量重开；
新的成交、委托、撤单变化或引用冲突仍须核验。未结案或证据不完整的旧单保持待核对，不自动重发。
`history_coverage_complete` 仍如实提示历史查询覆盖情况，不是所有订单共同的完成开关。
TradeNest 继续检查逐笔未完成请求及账户资金、持仓；不因接口仅支持当日查询而要求全历史证明，
也不因某笔显示 FILLED 就忽略其尚未解决的关联冲突。

## 请求及返回约定

- GET：参数来自 query string。
- POST：请求体为 UTF-8 JSON object，最大 1 MiB。
- 查询成功及幂等重放：HTTP 200；新交易命令持久受理：HTTP 202。直接返回数据，不增加外层包装。
- 错误：`{"error":{"code":"...","message":"..."}}`。
- 未知 method 返回 404；未知或无效参数返回 400；不支持的 HTTP verb 返回 405。
- 账户和持仓保留 QMT 对象的全部 `m_` 字段；字段读取失败报错。非有限数值转成 JSON `null`。

## 账户信息 `/account`

```http
GET /account?accountId=66027616&accountType=STOCK
```

或：

```http
POST /account
Content-Type: application/json

{"accountId":"66027616","accountType":"STOCK"}
```

两个参数均可省略。`accountId` 必须是非空字符串；`accountType` 规范为大写，支持
`STOCK`、`CREDIT`、`FUTURE`、`HUGANGTONG`、`SHENGANGTONG`、`STOCK_OPTION`。
具体账户能否查询仍取决于本机 QMT 登录和权限。

执行 `get_trade_detail_data(accountId, accountType, "account")`，返回第一条账户对象的
`m_` 字段字典。无记录返回 HTTP 404 / `ACCOUNT_NOT_FOUND`。

## 持仓信息 `/positions`

```http
GET /positions?accountId=66027616&accountType=STOCK
```

同样支持 POST JSON，账户参数规则与 `/account` 一致。
执行 `get_trade_detail_data(accountId, accountType, "position")`，返回持仓字典列表。
原样保留每条持仓，不按证券合并，不根据数量过滤。

QMT 返回空列表时响应 `[]`；这只表示该次查询没有记录，不能独立证明账户已登录且查询完整。
账户／持仓接口返回 `None` 或非列表类型时，响应 500 / `INVALID_QMT_RESULT`。

## 智能算法配置 `/get_smart_algo_param`

查询全部有权限的算法配置：

```http
GET /get_smart_algo_param
```

```http
POST /get_smart_algo_param
Content-Type: application/json

{"algoList":[]}
```

查询指定算法：

```http
POST /get_smart_algo_param
Content-Type: application/json

{"algoList":["VWAP","TWAP"]}
```

GET 可使用 `?algoList=VWAP` 或 `?algoList=VWAP&algoList=TWAP`。
参数省略或为空表示 `[]`。执行全局 QMT 函数 `get_smart_algo_param(algoList)`，
返回算法名到参数定义列表的字典，保留参数名称、范围、默认值、单位等原始字段。
本机没有该函数时返回 HTTP 501 / `API_UNAVAILABLE`；函数调用异常返回 500 / `QMT_ERROR`。
此查询不会创建算法订单；返回的配置不代表已验证算法实盘执行能力。

## 调度、超时和日志

外部 HTTP 线程执行 PostgreSQL 持久受理、幂等和查询；事务提交后新交易命令返回 202。
外部后台线程负责候选读取、最终认领、回报归并、状态计算、恢复及对账；执行权线程用
专用连接持有并核验 PostgreSQL advisory lock，通过短期 IPC 授权允许 QMT 交易调用。
QMT 调度线程执行下单、撤单、查询、算法配置、篮子操作和原生对象快照。
QMT 不执行 SQL；授权过期、连接中断、回报缺口或停止会阻止后续交易调用。

PG 与 QMT 没有原子事务，派发后结果不明保持 `UNKNOWN`，不得自动重发。
回报通过 IPC 返回外部服务，外部持久化事务成功后才确认回报；无法证明连续性时记录缺口
并通过现有对账恢复。队列及回报缓存有界，溢出不会伪装成已持久化。

普通查询外部等待默认 10 秒，由 `QMT_BRIDGE_REQUEST_TIMEOUT` 配置。
QMT 定时调度默认 10ms，预算默认 50ms；预算不能强制中断已经开始的同步 QMT 调用。
外部批量设置为 `QMT_BRIDGE_SUBMIT_BATCH_SIZE`、`QMT_BRIDGE_CANCEL_BATCH_SIZE`、
`QMT_BRIDGE_RECONCILE_BATCH_SIZE`、`QMT_BRIDGE_SCHEDULE_BUDGET_MS` 和
`QMT_BRIDGE_RECONCILE_INTERVAL_SECONDS`；后三项默认分别为 100、50、30。

- 请求容量满：429。
- 等待超时：504；已开始的原生调用可能继续。
- 外部服务停止或 IPC 未就绪：503。

外部服务将有界日志快照异步写到外部 console 和
`QMT_BRIDGE_LOG_DIRECTORY`（默认 `var/log/ipc_bridge`）中的 UTF-8 文件，
按 `QMT_BRIDGE_LOG_LEVEL` 过滤。QMT 端只打印 console，不写日志文件。
请求日志保留 request ID、method、状态及耗时，不记录账户、持仓或算法配置响应正文。
日志队列丢弃和后台写入错误分别通过统一 `/health` 的 `logging` 字段采样。
原生调用正常返回只表示调用结束，订单及撤单结果仍按回报和对账确认。

## 新增 ORDER 写入前准备

PostgreSQL 驱动安装在外部 Python 环境，依赖由 `requirements-service.txt`
锁定；QMT 解释器不需要安装 PostgreSQL 驱动。当前单文件策略生成与检查使用
`tools/build_ipc_strategies.py`，不再使用旧 HTTP 策略构建器。

源模块为 UTF-8，QMT 策略输出为实际 GBK 字节。只在构建器检查通过后导入 QMT。外部运行配置指定 PostgreSQL host、port、database、user、password；模拟盘与实盘使用不同数据库，例如 `paper`、`live`。每个数据库内部固定使用 `qmt_order` schema，不再接受自定义 `pg_schema`；表中不设 `namespace_id`。管理工具的 schema init/check 只需数据库配置，例如 `config.json`：

```json
{"pg_host":"127.0.0.1","pg_port":5432,"pg_database":"paper","pg_user":"order_service","pg_password":"<secret>"}
```

外部服务使用 `.env` 中的 `QMT_BRIDGE_PG_HOST`、`QMT_BRIDGE_PG_PORT`、`QMT_BRIDGE_PG_DATABASE`、`QMT_BRIDGE_PG_USER`、`QMT_BRIDGE_PG_PASSWORD`；管理 CLI 也读取这些变量。CLI 配置优先级为默认主机/端口、`--config` JSON、合并后的 `.env` / 进程环境、显式 `--account-id`；进程环境覆盖 `.env`，同一字段的 `QMT_BRIDGE_*` 优先于兼容的 `ORDER_*`。人工修改命令没有默认账户，必须明确指定。数据库须事先创建。初始化 DDL 统一存放在 `sql/order_init.sql`，不创建外键，表间关联由应用层维护，同时保留主键、幂等唯一约束和索引。以下命令只对所指定的**目标隔离数据库**执行：

```powershell
python tools/order_admin.py --config config.json schema init
python tools/order_admin.py --config config.json schema check
```

当前结构版本为 3，只维护一份当前初始化 SQL。`schema init` 仅用于全新或已兼容版本 3 的库，重复执行保留已有数据、首次创建时间和账户记录；`schema check` 只读检查结构版本、列、生成表达式及必要约束和索引。版本 2 或损坏的结构在执行初始化 DDL 前被拒绝，不会自动重建、清理或升级。外部服务启动也只检查结构，不执行 DDL。

已停写的版本 2 业务库使用专用的一次性离线工具；这不是 `schema init` 或外部服务启动的一部分。先对目标库逐库运行只读预检，完成备份并确认备份文件存在后，才显式执行迁移。工具要求 `--apply` 同时提供非空的现有 `--backup` 文件；成功提交后再次运行只读预检只报告已是版本 3，不重复回填：

```powershell
python tools/order_storage_backfill.py --config config.json
python tools/order_storage_backfill.py --config config.json --apply --backup "C:\backup\order-v2.dump"
python tools/order_admin.py --config config.json schema check
```

将示例备份路径替换为该库实际 `pg_dump` 备份文件。每个库的转换在一次事务内完成：保留可证明的历史创建时间，无法证明的记录统一使用该库本次迁移的 `migration_at`；当前订单文档与六张子表同步补齐稳定 UUID 和时间，历史事件快照、原始观察及交易业务标识不改。失败在提交前整体回滚；若提交结果不确定，先重新连接只读核查，不盲目重试。上述命令是操作入口说明，不表示任何业务库已经迁移或启动验证。

版本 3 的 11 张表均有不可由常规保存改写的 `created_at timestamptz`。六张订单子表使用内部 UUID `record_id`，并从 JSON 文档生成可查询的业务字段；`order_items` 的 `item_id`、`execution_attempts` 的 `attempt_id` 在订单内唯一，`cancel_requests` 的 `cancel_request_id` 在账户内唯一，且三者不得为空。任务、委托、成交的 QMT 编号及交易日、市场等可按显式列查询；QMT 编号不被当作跨订单全局唯一键。

QMT 策略不包含 DDL；外部后台启动先检查固定 schema 内的关键表、字段和当前结构版本，再按外部配置的 `account_id` 幂等创建缺失的 `account_runtime` 行；已存在行的 `event_seq`、执行主机和代次保持不变。缺少表或字段时报告 `SCHEMA_NOT_READY`，版本不兼容时报告 `SCHEMA_VERSION_MISMATCH`；启动不会自动建表或升级。口令不在 CLI 输出中打印；不要把含口令的配置文件提交到仓库。数据库连接只在外部 `.env` 配置；QMT 只配置桥名和交易账户。`schema init/check` 只需数据库配置，`unknown` 人工管理命令仍须指定账户，可在配置文件添加 `"account_id":"<account>"`，或传 `--account-id <account>` / 设置 `QMT_BRIDGE_ACCOUNT_ID`。外部三项 `QMT_BRIDGE_PG_DATABASE`、`QMT_BRIDGE_PG_USER`、`QMT_BRIDGE_PG_PASSWORD` 全部未设置时只启用旧查询，写入接口返回 `TRADING_NOT_CONFIGURED`。旧配置中的 `pg_schema` / `PG_SCHEMA` / `ORDER_PG_SCHEMA` / `QMT_BRIDGE_PG_SCHEMA` 请移除，配置检查会明确拒绝它们。

## 订单输入

新增写命令只接受 POST UTF-8 JSON；`GET /submit_order` 和 `GET /cancel_order` 返回 405。`POST /submit_order` 至少包含 `client_order_id`、`account_id`、`order_type`、`sizing_type`、`execution`、`price_type`。`client_order_id` 是调用方持久幂等键。账户必须与外部配置及 QMT 策略绑定的账户一致。`strategy_id` 是可选非空字符串。`submit_before` 可选，若给出必须是带时区的 ISO 8601 时间。订单一旦被受理即冻结标准化请求；同键同内容重试返回原单，同键不同内容返回 409 / `IDEMPOTENCY_CONFLICT`。HTTP `202` 只表示 PostgreSQL 已受理。

单票例子（数量为股或 ETF 份，不乘 100）：

```http
POST /submit_order
Content-Type: application/json

{"client_order_id":"buy-20260926-001","account_id":"<account>","order_type":"SINGLE","sizing_type":"QUANTITY","symbol":"600000.SH","side":"BUY","quantity":100,"execution":{"type":"DIRECT"},"price_type":"LIMIT","limit_price":"10.25"}
```

单票 `sizing_type=QUANTITY` 须给 `quantity`，`sizing_type=AMOUNT` 须给人民币十进制字符串 `amount`；当前仅股票单票允许金额，ETF 金额单被拒绝。`price_type=QUOTE` 使用 `quote_type`（`LATEST`、`OWN_BEST`、`OPPONENT_BEST`、`FAR_LIMIT`）。`price_type=MARKET` 在 DIRECT/SLICED 中使用交易所适用的业务枚举 `market_type`，如 `BEST5_IOC`；不接受 QMT 数字代码。SH/BJ 须指定十进制字符串 `protection_price`，SZ 不接受此字段。`LIMIT` 不支持篮子；`MARKET` 篮子仅允许 SMART，SMART 不支持 `QUOTE`。这些是 bridge 当前合同，不代表交易所规则。非法或不支持组合返回 400/422 的 `INVALID_ORDER`。

MARKET 枚举：SH/BJ 为 `BEST5_IOC`、`BEST5_TO_LIMIT`、`OPPONENT_BEST`、`OWN_BEST`；SZ 为 `BEST5_IOC`、`OPPONENT_BEST`、`OWN_BEST`、`IOC`、`FOK`。SH/BJ `protection_price` 允许十进制字符串零到 9999，具体 QMT/交易所适用性仍需模拟盘核对。

原生篮子例子：

```json
{"client_order_id":"basket-20260926-001","account_id":"<account>","order_type":"BASKET","sizing_type":"QUANTITY","items":[{"item_id":"bank","symbol":"600000.SH","side":"BUY","quantity":100},{"item_id":"etf","symbol":"510300.SH","side":"SELL","quantity":200}],"execution":{"type":"DIRECT"},"price_type":"QUOTE","quote_type":"LATEST"}
```

每项 `item_id` 唯一，证券/方向组合不能重复。Bridge 为每单创建唯一 QMT 篮子名，写后读回比对；三种执行路线都使用 QMT 原生篮子，绝不拆成单票。篮子回报在 `items`、`qmt_tasks`、`qmt_orders`、`fills` 中保留父子标识与每项关联，篮子清理留给人工。

完整 SLICED 单票示例，`ALGO` 可改为 `RANDOM`：

```json
{
  "client_order_id":"sliced-20260926-001", "account_id":"<account>",
  "order_type":"SINGLE", "sizing_type":"QUANTITY", "symbol":"600000.SH", "side":"BUY", "quantity":1000,
  "price_type":"QUOTE", "quote_type":"LATEST",
  "execution":{"type":"SLICED","mode":"ALGO","params":{
    "MaxOrderCount":10,"SinglePriceRange":0,"PriceRangeType":0,"PriceRangeValue":0,
    "PriceRangeRate":0,"SuperPriceType":0,"SuperPriceRate":0,"SuperPriceValue":0,
    "VolumeType":0,"VolumeRate":0.1,"SingleNumMin":100,"SingleNumMax":500,
    "ValidTimeType":0,"ValidTimeElapse":60,"ValidTimeStart":0,"ValidTimeEnd":0,
    "UndealtEntrustRule":0,"PlaceOrderInterval":5,"UseTrigger":0,"TriggerType":0,
    "TriggerPrice":0,"SuperPriceEnable":0
  }}
}
```

SLICED 的 22 个 `params` 必须全部给出，避免 QMT 隐式读取交易面板。`PriceRangeRate`、`SuperPriceRate`、`VolumeRate` 使用 `[0,1]` 小数。SMART 使用 `"execution":{"type":"SMART","algorithm":"VWAP","start_at":"2026-09-26T09:30:00+08:00","end_at":"2026-09-26T14:50:00+08:00","params":{...}}`；`params` 为算法元数据定义的 `m_` 标量字段，`m_strCmdRemark` 由 bridge 保留。缺失字段按 QMT 元数据默认值补齐并校验，不根据名称猜测算法参数。

SMART 的两个时间须带时区，派发当天按上海时区校验仍在该时间窗内。上述日期仅展示 JSON 结构，复制到其他交易日不会成为可提交的 SMART 指令。

## 下单、撤单与查询响应

`POST /submit_order` 返回含 `order_id`、`client_order_id`、`submission_status`、`version` 等字段的订单记录；受理时通常为 `QUEUED`。响应一旦丢失，必须以原 `client_order_id` 重试，或用 `/order` 查询，不得改键直接再下。QMT 提交返回与成交是独立事实。

子记录的 UUID `record_id` 仅用于内部持久化，不作为客户端输入或响应字段。提交、订单详情、订单列表和事件查询均过滤该内部字段；撤单响应也不公开它。已有公开的时间字段保持原语义，数据库新增的内部子记录时间不会悄然扩展响应合同。

例如受理响应为 HTTP 202，下面仅列出关键字段，实际订单还含原请求、各 item、QMT 任务/委托/成交、取消与同步状态：

```json
{"order_id":"<order_id>","client_order_id":"buy-20260926-001","submission_status":"QUEUED","execution_status":"NOT_STARTED","cancel_status":"NONE","version":1,"items":[{"item_id":"single","symbol":"600000.SH","side":"BUY","requested_quantity":100,"filled_quantity":0}],"qmt_tasks":[],"qmt_orders":[],"fills":[],"replayed":false}
```

```http
POST /cancel_order
Content-Type: application/json

{"cancel_request_id":"cancel-20260926-001","account_id":"<account>","client_order_id":"buy-20260926-001"}
```

撤单使用 `cancel_request_id` 持久去重。同键同内容返回原请求，同键不同内容报 409。撤单受理表示持久意图，实际取消以 QMT 子委托/算法任务回报和后续查询为准。若原单尚未进入 QMT，取消可阻止调用；若调用已经开始但尚无 QMT 标识，意图保持等待；若有部分成交，仅取消剩余委托。全部成交后原单仍是 `FILLED`，撤单请求可能结束为无余量可撤。原单或撤单结果不确定时只冻结这一订单，其他订单继续执行。

例如 HTTP 202：

```json
{"cancel_request_id":"cancel-20260926-001","canonical_cancel_request_id":"cancel-20260926-001","order_id":"<order_id>","cancel_status":"WAITING_QMT_ID","replayed":false}
```

查询接口均为 GET；例子使用 query string：

| 路径 | 用途 | 返回内容 |
| --- | --- | --- |
| `/order?client_order_id=buy-20260926-001` | 单单详情 | 含版本、原请求、每项、QMT 任务/委托/成交与取消状态的 JSON object |
| `/orders?active=true&limit=100` | 有界订单列表 | `orders`、`has_more`、`next_cursor`；有下一页时把游标传回 `cursor` |
| `/order_events?after=0&limit=100` | 持久事件 | `events`、`next_after`、`has_more`；下次传入 `after` |
| `/capabilities` | 输入合同与本机可用 QMT API | 三路线、价格类型、参数范围及可用性 |
| `/health` | 服务和交易执行状态 | 区分 HTTP、PostgreSQL、QMT 调度及回报/查询新鲜度 |

查询示例；`/order` 的响应就是上面的订单对象（随后字段随回报变化），`/orders` 与 `/order_events` 是有界列表：

```http
GET /order?client_order_id=buy-20260926-001
GET /orders?active=false&limit=20&cursor=<next_cursor>
GET /order_events?after=0&limit=20
GET /capabilities
GET /health
```

```json
{"orders":[],"has_more":false,"next_cursor":null}
```

```json
{"events":[{"event_seq":1,"order_id":"<order_id>","event_type":"ORDER_ACCEPTED","occurred_at":"2026-09-26T01:30:00+00:00","order":{"order_id":"<order_id>"}}],"next_after":1,"has_more":false}
```

`/capabilities` 返回 `order_types`、`sizing_types`、`executions`、`price_types`、`qmt_functions`、`paths`、`verification_status`、`trading_configured` 等键。`paths` 分别列出 SINGLE/BASKET × DIRECT/SLICED/SMART 六条路径的 `implemented`、`function_available`、`locally_verified`；当前本地交易验收前 `locally_verified` 均为 false。`/health` 中 `lifecycle` 依次反映 `STARTING`、`RECOVERING`、`RUNNING`、`STOPPING`、`STOPPED`；恢复完成且调度、数据库和短期执行授权均有效时才设置 `accepting_orders=true`。后台初始化、账户补齐、执行权取得和恢复未完成前不受理新订单；未配置 PG 时仍可提供旧查询。停止时先关闭受理和派发并取消定时任务，外部后台等待在途工作收尾后再释放数据库连接、执行权和日志资源；QMT 的 `stop` 回调取消调度并关闭 IPC。不存在的 `client_order_id` 返回 404 / `ORDER_NOT_FOUND`，非法过滤/分页参数返回 400 / `INVALID_PARAMS`，数据库或执行器未就绪返回 503；错误响应始终是 `{"error":{"code":"...","message":"..."}}`。

健康采样含 `sampled_at`、`schema_version`、`pending_count`、`unknown_order_count`、`last_reconciled_at`、`observation_gap`、`history_coverage_complete`、`queues`、`overflow_count`、`tick_ms`、`qmt_ms`、`authority_remaining_ms`、`executor_owned` 和 `error_code`。HTTP 层另附 `schedule_settings`、`logging`（包括日志队列大小、丢弃数及写入错误）、`feed` / `order` IPC 状态和 `bridge` 实例状态。这些是缓存或采样值，应结合采样时间判断；单一 HTTP 200 不能证明交易可用或 QMT 实机验收完成。

账户级对账首次启动立即执行；每轮查询任务、委托和成交后，无论结果成功或失败，都从本轮结束时等待 `reconcile_interval_seconds`（默认 30 秒），不会因为上次成功时间未更新而每秒重试。轮次不重叠，等待期间回报归并、下单和撤单调度继续运行。逐笔订单的 `reconcile_due_at` 同样使用此配置；已有到期时间保持原值，下一次选中或完成对账时按当前配置计算。

若早期回报缺委托号，而后续正式委托可通过账户、交易日、市场及两项原生委托引用唯一核对，bridge 会自动解除该项关联缺口，原始回报保存在 `resolved_evidence` 和 `qmt_observations` 中。旧版留下的此类记录也在启动恢复或下次对账时处理。拒单仍须确认相关任务停止、委托终态、成交一致且完整对账成功，才返回 `execution_status=REJECTED`、`sync_status=COMPLETE`、`reconcile_pending=false` 并退出逐单周期对账；有歧义或缺失成交的记录继续核对。

正常启动和运行中缺口扫描只重新安排未结案或明确要求核对的订单；已完整对账结案且没有新问题的
订单保持结案，包括跨交易日恢复。账户级查询继续运行，已知成交不会因历史查询不可用而消失。
新完成对账的完整终态会保存内部结案检查点，覆盖请求、委托、任务、成交、调用尝试及撤单事实。
后续历史覆盖不足时，仅当这些事实仍与检查点严格一致且没有关联冲突，才可复用原结案结果；
恢复时保留原成功对账时间，不用今天的时间冒充历史查询成功。写入前在订单锁下再次核对事实代次。
日志使用 `reconciliation_basis=PERSISTED_TERMINAL_CHECKPOINT` 和 `outcome=RETAINED_TERMINAL`
区分这种恢复与当日查询，同时保留 `coverage_gap=HISTORY_NOT_COVERED`。
没有有效检查点的旧单不会自动生成结案证明；日志提示 `action_required=HISTORICAL_EVIDENCE_REVIEW`。

旧版误重开的完整终态记录可用 `tools/order_terminal_repair.py` 定向核验和修复，支持
`FILLED`、`CANCELLED`、`PARTIALLY_CANCELLED`、`REJECTED`。先从 `order_events` 选定先前成功的
`RECONCILE_STATE_CHANGED` 事件，提供最新订单版本与原因；默认只读校验。
工具依靠当时已持久保存的完整结案事件，不要求当前 QMT 能查询那个历史交易日。
它要求之后仅发生允许的对账检查点变化，QMT 身份、任务、委托、成交、请求和调用事实不变；
后续观察逐条验证，存在新事实、未解决的撤单或关联歧义即拒绝。

```powershell
python tools/order_terminal_repair.py --config config.json --order-id <order_id> --expected-version <version> --checkpoint-event-seq <event_seq> --reason "恢复被历史覆盖缺口误打开的已对账终态"
# 校验通过后使用相同参数，追加下列选项写入；备份路径必须尚不存在。
# --apply --evidence-out C:\evidence\terminal-repair-before.json
```

配置沿用管理工具的共享环境配置或外部配置文件。写入事务锁定账户与订单、再次校验版本和证据，
先落盘完整写前快照，再追加 `MANUAL_TERMINAL_REPAIR` 审计事件并同步订单/成员投影。
修复保持原成功对账时间，修复时间单独记录；提高事实代次使修复前的在途对账失效。
重复执行会因版本或状态不符而拒绝，不重复追加审计。未曾完整结案、结案后有新事实或存在引用冲突的
记录不适用；仍需核对原始回报或券商历史记录，不能用当天空查询替代。

`/health` 另返回 `last_reconcile_attempt_at`（最近轮次开始）、`last_reconcile_finished_at`（最近轮次结束，包括失败）、`next_reconcile_at`（预计下次轮次，执行中为空）及 `last_reconcile_error`（最近查询异常摘要）。`last_reconciled_at` 只表示成功完成对账的时间，失败不得刷新它。调度使用单调时钟，展示时间使用带时区的 UTC 时间戳；这些轮次诊断字段在策略重启后重新采样。

QMT 的 `console_log` 只打印有界元信息，允许字段为 `request_id`、`method`、`elapsed_ms`、`code`、`qmt_method`、`qmt_call_id`、`return_type`、`return_count`、`error_code`、`error_type`、`stage`、`attempt_id`、`order_id`、`client_order_id`。字符串字段最多 256 字符，只接受普通标量；调用参数、原生对象、属性值快照和 traceback 不通过该 logger 打印或转发。外部异常日志可能包含远端调用包装层的堆栈，不能据此认为原生 QMT 堆栈或 `return_snapshot` 已完整保留。

已确认本机 `COrderDetail.m_xtTag` 是无法转换为 Python 值的内部属性，bridge 在读取前排除这个属性，保留委托号、备注、状态、数量等业务字段。首次成功排除时的 `QMT snapshot internal field excluded` console 警告仅保留上述允许的元信息，不输出可读属性值或 `skipped_fields` 诊断正文，也不伪造该属性值。其他属性转换异常仍使本次快照失败，并维持对账缺口。

队列满使用 `OBSERVATION_QUEUE_FULL`，与快照转换失败分别记录；兼容字段 `overflow_count` 仍统计上述回报采集缺口。查询异常日志不等于券商拒单，`qmt_observations` 仍保存成功采集的原始事实。

`/capabilities` 和 `/health` 示例（均只列关键字段）：

```json
{"order_types":["SINGLE","BASKET"],"executions":{"DIRECT":{"required":["type"]}},"qmt_functions":{"passorder":true,"algo_passorder":true,"smart_algo_passorder":false},"verification_status":"UNVERIFIED","trading_configured":true}
```

```json
{"http_running":true,"database_available":true,"scheduler_alive":true,"lifecycle":"RUNNING","recovery_complete":true,"accepting_orders":true,"sampled_at":"2026-09-26T09:30:00+08:00","pending_count":0,"queues":{"cancel":0,"submit":0,"prepare":0,"query":0,"results":0,"observations":0},"overflow_count":0,"authority_remaining_ms":500,"last_reconciled_at":null,"last_reconcile_attempt_at":null,"last_reconcile_finished_at":null,"next_reconcile_at":null,"last_reconcile_error":null,"observation_gap":false,"schedule_settings":{"submit_batch_size":10,"cancel_batch_size":10,"reconcile_batch_size":100,"schedule_budget_ms":50,"reconcile_interval_seconds":30},"error_code":null}
```

错误响应示例：

```json
{"error":{"code":"IDEMPOTENCY_CONFLICT","message":"client_order_id has a different request"}}
```

订单里的 `submission_status`、`execution_status`、`cancel_status` 需要分别看。`UNKNOWN` 表示提交是否进入 QMT 尚无法确认，不可盲目重试原单。`/order_events` 是状态变化证据流，事件保存账户作用域的连续序号、事件类型、发生时间和当时订单快照。健康接口单一成功状态不等于已验证交易能力。

`order_events` 用于客户端按 `after` 增量获取变化和历史追溯；下单派发、幂等、撤单和重启恢复读取当前订单、执行尝试、撤单请求及 QMT 事实，不通过回放事件恢复状态。事件与对应业务状态在同一事务中保存。

常规 `RECONCILE_ATTEMPT`、`RECONCILE_FINISHED` 只记录 INFO 日志，不再追加数据库事件。最近尝试/成功时间、轮次、事实代次和下次执行时间等运行检查点仍会事务持久化；单纯更新时间不会产生事件，也不会重写未变化的子表。对账实际改变订单、撤单或同步完整性等状态时，追加 `RECONCILE_STATE_CHANGED`，保留当时完整快照。既有下单、成交、撤单、UNKNOWN 和缺口等业务事件继续保留。

`event_id` 只在追加事件时递增，旧事件和游标不重排、不清理；订单 `version` 仍随持久化更新递增，用于并发校验，所以相邻事件的 `order_version` 可以跳号。当前版本不提供自动事件清理或归档。

## UNKNOWN 人工处置

管理工具不直接调用 QMT：

```powershell
python tools/order_admin.py --config config.json unknown list
python tools/order_admin.py --config config.json unknown inspect --order-id <order_id>
python tools/order_admin.py --config config.json unknown reconcile --order-id <order_id> --expected-version 7
```

`reconcile` 只写入待对账意图，由策略调度回调查询 QMT。人工 `resolve` 需要先从 `inspect` 取得最新版本与已采集原始观察记录（包括符合账户、证券、方向条件但尚未关联的候选），提供理由和 UTF-8 证据文件。确认已观察到委托时，选择原始观察 ID，并且传入的所有 QMT 标识与记录完全一致：

```powershell
python tools/order_admin.py --config config.json unknown resolve --order-id <order_id> --expected-version 7 --resolution observed --observation-id 19 --qmt-order-id <真实委托号> --reason "人工核对券商委托回报" --evidence-file C:\evidence\observed.json
```

证据文件应包含原始券商/客户端截图转录、采集时刻与核对说明。PostgreSQL 事务内再次校验订单版本、账户、证券、方向、remark 和 QMT 标识，拒绝已关联其他订单的记录；只追加关联及审计，不修改观察原文。若原始记录缺 remark，证据 JSON 须明确包含 `{"manual_attribution":{"order_id":"<order_id>","account_id":"<account>","observation_ids":[19],"basis":"人工核对的具体依据，至少二十个字符"}}`，并与选中记录完全对应；有矛盾 remark 的记录始终拒绝。若确认 QMT 调用从未发生，证据文件必须是含 `{"proof_type":"pre_call_boundary","qmt_call_never_started":true,"basis":"具体的调用前边界证据..."}` 的 JSON：

```powershell
python tools/order_admin.py --config config.json unknown resolve --order-id <order_id> --expected-version 7 --resolution not-submitted --reason "调用前本地执行器故障已取证" --evidence-file C:\evidence\pre-call.json
```

“查不到委托”、超时或缺少回报均不足以证明未提交。事务中再次校验版本；保存证据副本、SHA-256、操作者、理由和前后状态。`not-submitted` 处置后标记 `RESOLVED_NOT_SUBMITTED`，不会把原单重置为 `QUEUED`。证据不够时保留 `UNKNOWN`。

## 本地验证

```powershell
python -m unittest discover -s tests -p "test_ipc_*.py" -v
python tools/build_ipc_strategies.py --check
```

自动测试使用模拟 QMT 函数、本地随机端口 HTTP 服务和临时文件，不连接真实交易账户。持久集成测试应使用新建隔离 PostgreSQL 数据库/schema；勿使用 TradeNest 生产库。QMT 模拟盘须分别验证 DIRECT、SLICED、SMART 的单票与原生篮子，以及提交、子委托、成交、取消和重启对账。当前文档与自动测试不能证明已完成模拟盘或实盘交易验证。

官方参考：[交易查询及算法配置接口](https://dict.thinktrader.net/innerApi/trading_function.html)。
