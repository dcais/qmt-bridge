# QMT HTTP ORDER bridge

策略文件：`strategies/http_order.py`，由 `order_bridge/*.py` 构建。提供原有账户、持仓和智能算法配置查询，以及持久下单、撤单和订单查询。

默认地址为 `http://127.0.0.1:8888/{method}`。独立运行，不依赖 FEED 策略文件。
默认账户为 `66027616`，账户类型为 `STOCK`；账户及端口可通过 QMT「参数设置」传入。

## 启动参数

在策略编辑器右侧「参数设置」中添加以下同名变量，在「最新」列填写本次运行值：

| 参数名 | 未配置时默认值 | 示例 |
| --- | --- | --- |
| `account_id` | `66027616` | `8890763409`（当前配置示例） |
| `http_port` | `8888` | `8886`（当前配置示例） |

优先读取小写 `account_id`、`http_port`，兼容此前的大写 `ACCOUNT_ID`、`HTTP_PORT`。
两种写法同时存在时逐项以小写为准；小写值非法时直接报错，不回退到大写或默认值。
最小／最大／步长用于参数遍历，不是 HTTP 服务配置。
代码保留已有同名变量，在 `init` 时校验并固定实例配置；两个参数均未设置时仍使用原默认值。
参数注入发生在脚本执行前或 `init` 前的两种情况均有本地模拟测试。
参数变更需要停止并重新运行策略，运行中不自动切换账户或监听端口。

账户支持非空字符串、正整数，以及可精确表达的正整数浮点值，例如 `66027616.0`
会转成 `"66027616"`。字符串前导零保留；数值参数无法恢复已丢失的前导零。
端口支持整数、整数浮点值或十进制整数字符串，范围 `1–65535`。
显式填写非法值会阻止启动，不会悄悄使用默认值。

不同实例分别配置各自账户和未占用端口；端口本身不决定模拟／实盘模式，仍以对应客户端
和账户为准。HTTP 请求显式传入 `accountId` 仍可覆盖该次查询的默认账户，端口不是账户访问隔离。

参数面板语义参考本地 `qmt-explore` 官方文档快照（2026-09-22）：
[界面操作](https://dict.thinktrader.net/innerApi/interface_operation.html)。
本机面板的字符串输入支持及实际注入行为仍待 QMT 实机验证。

## 在 QMT 中运行

1. 将 `strategies/http_order.py` 导入 QMT Python 策略，使用 GBK 编码。
2. 确认账户已登录，在参数设置中填写账户与端口，确认端口未被占用；不填使用默认值。
3. 运行策略，检查 console 中的 HTTP listening 日志。
4. 用以下 HTTP 请求查询。策略停止时取消定时任务、结束 HTTP 服务并释放等待请求。

此服务绑定本机回环地址，未提供身份认证。新增写接口会调用 QMT 下单、撤单或启动算法任务；只在隔离模拟账户完成验收后接入上游交易流程。

启动成功后日志包含实际账户和绑定端口。例如上述参数生效时，启动日志为：

```text
... [INFO] QMT HTTP order listening {"host": "127.0.0.1", "port": 8886, "account_id": "8890763409"}
```

这条启动日志会打印完整账户号；查询响应正文仍不写入日志。

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

HTTP 线程只解析请求、入队和等待结果。所有 QMT 查询以及 QMT 对象读取均在
`schedule_run` 回调中执行，避免跨线程调用 QMT。

沿用 FEED 的主要参数：队列容量 64，调度间隔 10ms，每轮最多 10 个请求、50ms 启动预算，
HTTP 等待上限 10 秒。单个 QMT 调用无法被预算中断。

- 队列满：429 / `QUEUE_FULL`。
- 等待超时：504 / `QMT_TIMEOUT`；尚未执行的过期请求跳过，已开始的查询可能继续。
- 服务停止：等待中的请求收到 503。

日志同时写到 QMT console 和 `%USERPROFILE%\qmt-bridge\logs\order-YYYY-MM-DD.log`，
使用上海时区，以 `request_id` 关联请求。日志记录 method、状态及耗时等元信息，不记录账户、
持仓或算法配置响应正文。

## 新增 ORDER 写入前准备

QMT Python 运行时需要 `pg8000==1.22.1`、`scramp==1.4.1`、`asn1crypto==1.5.1`。在目标 Python 3.6 解释器下运行：

```powershell
python tools/install_order_dependencies.py --target "$env:USERPROFILE\qmt-bridge\vendor"
```

若从较新 Python 为 QMT Python 3.6 选择 wheel，增加 `--python36`；完全离线时增加 `--wheel-dir C:\path\to\wheels`。先用 `--dry-run` 检查命令。脚本仅写 `--target`，不安装进系统 site-packages。生成单文件策略：

```powershell
python tools/build_order_strategy.py
python tools/build_order_strategy.py --check
```

策略源模块为 UTF-8，输出文件是实际 GBK 字节。只在构建器输出与源码一致后导入 QMT。运行配置指定 PostgreSQL host、port、database、user、password；模拟盘与实盘使用不同数据库，例如 `paper`、`live`。每个数据库内部固定使用 `qmt_order` schema，不再接受自定义 `pg_schema`；表中不设 `namespace_id`。管理工具接受 `--config config.json`，内容形如：

```json
{"pg_host":"127.0.0.1","pg_port":5432,"pg_database":"paper","pg_user":"order_service","pg_password":"<secret>","account_id":"<account>"}
```

也可使用 `ORDER_PG_HOST`、`ORDER_PG_PORT`、`ORDER_PG_DATABASE`、`ORDER_PG_USER`、`ORDER_PG_PASSWORD`、`ORDER_ACCOUNT_ID` 环境变量。数据库须事先创建。DDL 唯一存放在 `sql/order_v1.sql`，以下安装命令读取该文件，在**目标隔离数据库**显式建表并注册当前账户，再检查：

```powershell
python tools/order_admin.py --config config.json schema init
python tools/order_admin.py --config config.json schema check
```

策略文件不包含 DDL；启动只检查固定 schema 内的关键表、字段、版本和账户记录，缺失时报告 `SCHEMA_NOT_READY`，版本不兼容时报告 `SCHEMA_VERSION_MISMATCH`，不会自动建表或升级。口令不在 CLI 输出中打印；不要把含口令的配置文件提交到仓库。QMT 策略参数面板须分别配置 `pg_host`、`pg_port`、`pg_database`、`pg_user`、`pg_password`，且 `account_id` 与数据库要与管理命令一致；大写 PG 参数名也兼容，小写优先。三项 `pg_database`、`pg_user`、`pg_password` 全部未设置时只启用旧查询，写入接口返回 `TRADING_NOT_CONFIGURED`。旧配置中的 `pg_schema` / `PG_SCHEMA` / `ORDER_PG_SCHEMA` 请移除，配置检查会明确拒绝它们。

## 订单输入

新增写命令只接受 POST UTF-8 JSON；`GET /submit_order` 和 `GET /cancel_order` 返回 405。`POST /submit_order` 至少包含 `client_order_id`、`account_id`、`order_type`、`sizing_type`、`execution`、`price_type`。`client_order_id` 是调用方持久幂等键。账户必须与策略启动时绑定的账户一致。`strategy_id` 是可选非空字符串。`submit_before` 可选，若给出必须是带时区的 ISO 8601 时间。订单一旦被受理即冻结标准化请求；同键同内容重试返回原单，同键不同内容返回 409 / `IDEMPOTENCY_CONFLICT`。HTTP `202` 只表示 PostgreSQL 已受理。

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

`/capabilities` 返回 `order_types`、`sizing_types`、`executions`、`price_types`、`qmt_functions`、`paths`、`verification_status`、`trading_configured` 等键。`paths` 分别列出 SINGLE/BASKET × DIRECT/SLICED/SMART 六条路径的 `implemented`、`function_available`、`locally_verified`；当前本地交易验收前 `locally_verified` 均为 false。`/health` 返回 `http_running`、`database_available`、`scheduler_alive`、`recovery_complete`、`accepting_orders`、`last_reconciled_at`、`observation_gap`、`error_code` 等键，不能把单一 HTTP 200 当成交易可用。不存在的 `client_order_id` 返回 404 / `ORDER_NOT_FOUND`，非法过滤/分页参数返回 400 / `INVALID_PARAMS`，数据库或执行器未就绪返回 503；错误响应始终是 `{"error":{"code":"...","message":"..."}}`。

`/capabilities` 和 `/health` 示例（均只列关键字段）：

```json
{"order_types":["SINGLE","BASKET"],"executions":{"DIRECT":{"required":["type"]}},"qmt_functions":{"passorder":true,"algo_passorder":true,"smart_algo_passorder":false},"verification_status":"UNVERIFIED","trading_configured":true}
```

```json
{"http_running":true,"database_available":true,"scheduler_alive":true,"recovery_complete":true,"accepting_orders":true,"last_reconciled_at":null,"observation_gap":false,"error_code":null}
```

错误响应示例：

```json
{"error":{"code":"IDEMPOTENCY_CONFLICT","message":"client_order_id has a different request"}}
```

订单里的 `submission_status`、`execution_status`、`cancel_status` 需要分别看。`UNKNOWN` 表示提交是否进入 QMT 尚无法确认，不可盲目重试原单。`/order_events` 是状态变化证据流，事件保存账户作用域的连续序号、事件类型、发生时间和当时订单快照。健康接口单一成功状态不等于已验证交易能力。

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
python -m unittest tests.test_http_order tests.test_order_tools -v
```

自动测试使用模拟 QMT 函数、本地随机端口 HTTP 服务和临时文件，不连接真实交易账户。持久集成测试应使用新建隔离 PostgreSQL 数据库/schema；勿使用 TradeNest 生产库。QMT 模拟盘须分别验证 DIRECT、SLICED、SMART 的单票与原生篮子，以及提交、子委托、成交、取消和重启对账。当前文档与自动测试不能证明已完成模拟盘或实盘交易验证。

官方参考：[交易查询及算法配置接口](https://dict.thinktrader.net/innerApi/trading_function.html)。
