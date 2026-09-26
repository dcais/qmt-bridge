# ORDER 本地验收记录

验证日期：2026-09-26，Asia/Shanghai。以下是代码、HTTP 和 PostgreSQL 验收，不是券商模拟盘交易验收。

## 当前改动：缺委托号回报关联后解除对账缺口

- 重放本机模拟订单保存的完整快照，修复前即使 `mark_reconciled(complete=True)` 仍为 `INCOMPLETE / INCOMPLETE / reconcile_pending=true`。原因是早期回报没有 `m_strOrderSysID`，后续拒单已经带正式委托号，但旧 `MISSING_QMT_ID` 证据始终阻断父单收敛。
- 现在仅以明确账户、交易日、市场和 `m_nRef`、`m_strOrderRef` 两项原生引用唯一匹配。身份矛盾、候选不唯一、原始累计成交超出已知量时继续保留缺口；同一正式委托的引用冲突另记阻断证据。消解后保留原文、来源、原时间、关联目标和匹配依据，重复旧回报不重复生成已解决证据。
- 原始快照修复后重放得到 `submission_status=CONFIRMED`、`execution_status=REJECTED`、`sync_status=COMPLETE`、`reconcile_pending=false`，一条旧证据转入 `resolved_evidence`。这仅验证本地状态归并，并非运行中的策略已经更新。账户级查询继续发现迟到事实，已完成订单退出逐单对账选择器。
- 新增 10 项状态用例和 3 项真实 PostgreSQL 用例，覆盖顺序/逆序回报、重复回报、缺失/冲突身份、后到引用、旧持久文档恢复、数据库观测去重、旧轮次和不完整轮次保护、活动任务/委托以及累计成交缺口。三个 PG 用例修复前均失败，修复后通过，日志 `.venv/order-evidence-pg-red.log`、`.venv/order-evidence-pg-green.log`。
- 实际 QMT Python 3.6.8 完整 ORDER 回归 **261 项全部通过，122.382 秒，无跳过**，包含真实 PG 和模拟 QMT 运行时测试；日志 `.venv/order-evidence-full-python36.log`。数据库测试使用 `postgres` 下随机隔离 schema 并清理，没有写 `qmt_paper`/`qmt_live`，没有新下单或撤单。
- 表结构无需迁移。生成策略通过构建一致性、实际 GBK 和 Python 3.6.8 编译检查；`Last modified`：`2026-09-26 22:45:35`；SHA256：`d38ff55efcb703ae1695ade91164c64bc232813afbc9af65bb62463fc8b24c27`。运行中的策略未替换或重启；加载新版后自动恢复旧证据，并由下一轮完整对账确认终态。

## 已完成：常规对账改记日志，业务变化才追加事件

- `RECONCILE_ATTEMPT`、`RECONCILE_FINISHED` 改为后台逐单 INFO 日志，保留账户、订单和轮次关联。完成日志明确区分 `COMPLETE`、`INCOMPLETE`、`STALE_FACT_VERSION`，存储失败不记录虚假完成；`source_version` 是本次完成检查输入文档的版本，不冒充保存后的版本。
- 对账检查点仍事务保存轮次、事实代次、时间和门闩，保留订单 `version` 递增及旧轮次保护；无业务变化不增加 `event_seq`、不写事件、不重写子表。实际订单或同步状态改变时追加 `RECONCILE_STATE_CHANGED` 并仅更新变化的子表。HTTP 事件快照和增量游标接口不变，旧事件不删除、不重排。
- 新增 8 项用例全部通过：5 项真实 PostgreSQL 仓储用例验证无变化轮次、WORKING 订单正常成功重复查询、完整/不完整转换、最终状态与事件快照、撤单子表、旧事实版本和事务回滚；3 项离线后台用例验证多页日志、关联、节流、失败与日志隔离。
- 实际 QMT Python 3.6.8 对完整 ORDER 运行 248 项，首轮 247 项通过；篮子三条路径用例在提交入口遭遇 `EXECUTOR_NOT_READY`，独立重跑该用例通过（4.378 秒）。保留首轮失败，不宣称单次全绿。日志：`.venv/order-event-checkpoint-all.log`、`.venv/order-event-checkpoint-basket-recheck.log`；聚焦 PG 三项及后台 31 项也通过。
- PostgreSQL 测试使用 `postgres` 数据库下各用例随机隔离 schema，结束时清理。未写业务库 `qmt_live`/`qmt_paper`，未调用真实 QMT 交易 API。表结构不变，不需迁移；运行中的策略未替换或重启。
- 重新生成的单文件通过构建一致性、实际 GBK 编码和 Python 3.6.8 编译检查。`Last modified`：`2026-09-26 22:11:05`；SHA256：`ba97fc754d78118bae59f499201e5db333822aecbdf9b0087ed8b7d859963f73`。

## 已完成：全部 ORDER QMT 调用 INFO 追踪

- 下单三条路径、撤销任务/委托、篮子读写、算法配置、当前/历史交易查询、旧账户/持仓查询、账户绑定和定时器启停均记录调用开始及返回/异常。日志携带方法、调用 ID、有界参数、耗时、原始标量返回值或集合条数，并按来源关联持久化尝试、业务订单、撤单、QMT 标识或对账轮次。
- 不为日志遍历原生返回对象，参数和上下文不含数据库凭据或 `ContextInfo`；日志错误不改变原返回、原异常或触发重发。执行指令结束恢复日志上下文，后续查询不串入上一笔订单标识。大参数截断有标记，仍使用原异步双写及丢弃计数。
- 实际 QMT Python 3.6.8 运行 **187 项**接口、适配、状态、日志、调度、启停和工具测试，全部通过，耗时 9.085 秒；日志 `.venv/order-qmt-call-log-tests.log`。覆盖三类下单参数恒等、`0/false/true/null` 返回值、原生异常原样传递、调用前日志、凭据屏蔽、参数限长、失效授权不调用、上下文恢复，以及启动失败和定时器返回 0 的收尾。
- 重新生成的单文件通过构建一致性、实际 GBK 编码及 Python 3.6.8 编译检查。`Last modified`：`2026-09-26 21:50:56`；SHA256：`2454b8d203157027ed6a451726356f6a05abe6c0364694b09de8d2f2f08855ef`。
- 本轮执行测试使用 QMT 替身，没有再次调用真实下单/撤单，没有写业务数据库，也未替换或重启运行中的策略。新增日志须加载该部署文件后生效。

## 已完成：COrderDetail 内部属性转换

- 现场日志已定位为 `get_trade_detail_data(..., "order")` 返回对象 `COrderDetail` 的 `m_xtTag` getter 缺少 `boost::shared_ptr<se::CXtOrderTag>` Python 转换器。bridge 原先枚举所有 `m_` 属性，因此尚未进入 JSON 序列化就失败。订单关联、状态及成交计算不使用这个属性。
- 原生对象只排除 `m_xtTag`，不调用该 getter；保留其他业务字段。未知字段转换失败仍阻断快照并记录有界可读字段及错误来源。排除后没有可读字段的对象返回 `INVALID_QMT_RESULT`，字典输入不改变。首次成功排除时每个 adapter 记录一次警告和可读字段诊断，日志失败不影响成功采集，也不重复刷屏。
- 先用坏 getter 复现回报队列为空、对账成功时间不更新；修复后，实际 QMT Python 3.6.8 执行 **167 项**接口、快照、日志、回调、对账、状态及工具回归全部通过，耗时 8.474 秒，日志 `.venv/order-xttag-final-tests.log`。覆盖实时/历史查询、回报先到、完整轮次恢复、委托号及成员关联、未知字段异常仍失败、可读值日志限长、getter 读取次数及警告仅一次。
- 重建策略为实际 GBK，构建一致性、Python 3.6.8 编译以及生成单文件的坏 getter 查询回归均通过。`Last modified`：`2026-09-26 21:18:40`；SHA256：`8ee356bfbb52071fd38daa28b5c783db022969ce7317177fc6dbaf75006fda67`。
- 本次 QMT 返回对象均为测试替身，没有调用真实交易 API，没有改动业务数据库，也未替换或重启运行中的 QMT 策略。现场恢复结果需加载新版后核对，不能把测试值当作该笔实际委托的返回值。

## 已完成：QMT 异常诊断和对账间隔

- 增加启动参数 `reconcile_interval_seconds=30`，中文文件头和接口说明同步。账户对账首轮立即执行，后续从每轮完成时起等待配置间隔；成功和查询失败均等待，不依赖最近成功时间。逐单 `reconcile_due_at` 使用同一参数。
- 查询及回调失败日志保留异常类型、正文、调用栈、阶段和可取得的字段名/对象类型；堆栈只遍历 traceback 元数据，不读源码、文件或局部变量。队列满有独立错误码，日志仍在后台双写。健康接口分别展示最近尝试、最近完成、下次对账时间及不含堆栈的异常摘要。
- 回归先复现配置缺失、日志丢失明细和堆栈被普通字段上限截断；修复后实际 QMT Python 3.6.8 执行 152 项接口、参数、状态、适配、日志、调度及工具测试全部通过（`.venv/order-reconcile-final-tests.log`）。仓储与运行时另有 53 项测试全部通过（`.venv/order-reconcile-pg-tests.log`），真实 PostgreSQL 部分使用 `postgres` 数据库内随机测试 schema 并在用例结束清理，未变更 `qmt_live` 或 `qmt_paper`。
- 覆盖失败后一秒不重查、默认及自定义间隔、成功时间不被失败覆盖、首次恢复、对账轮次不重叠、等待期继续派发、持久到期时间、错误回调、满队列、原生属性抛异常、异常消息无法格式化，以及日志无源码文件 I/O。未调用真实交易 API。
- 生成策略是实际 GBK，Python 3.6.8 编译和构建一致性检查通过；`Last modified` 为 `2026-09-26 20:35:00`，SHA256 为 `143447857ab128f8d4547b397d9e5136af49694bb7a29bf1dce7db5043436d85`。本轮未替换或重启正在运行的 QMT 策略，现场底层异常须加载新版后由新日志确认。

## 当前结构：单一初始化 DDL

- 开发阶段统一使用 `sql/order_init.sql`，仅保留 `schema init/check`，不再维护历史版本升级脚本或命令。下方升级测试与旧版本产物信息仅为历史记录，现行部署以 `HTTP_ORDER.md` 为准。
- 初始化不创建外键，保留主键、幂等唯一约束和索引。结构版本仍为 2，现有 `qmt_live`、`qmt_paper` 无需因文件合并重建。
- 本次工具测试 17 项在项目 Python 和实际 QMT Python 3.6.8 下均通过；GBK 策略构建一致性与 Python 3.6 编译检查通过。
- QMT Python 3.6.8 + psycopg2 2.9.5 在隔离 PostgreSQL schema 验证：全新初始化得到 11 张表、0 个外键及 20 个索引；重复初始化保留全部表数据指纹、约束和索引；不兼容版本报错且数据与结构不变。临时 schema 已清理，本次未变更两个业务库。

## 历史变更：psycopg2 驱动迁移

- 2026-09-26：ORDER 默认驱动改为 `psycopg2`，QMT Python 3.6 锁定 `psycopg2-binary==2.9.5`；连接使用 `dbname` 和有界整数 `connect_timeout`，保留事务、幂等、执行权锁及提交结果不明保护。
- 实际 QMT Python 3.6.8 + psycopg2 2.9.5 + 专用 PostgreSQL 16 容器执行 `unittest discover -s tests -v`：304 项全部通过，无跳过。日志：`.venv/psycopg2-isolated-tests.log`。包含真实数据库迁移、并发、恢复及模拟 QMT 运行时验证。
- 首轮使用现有容器连接配置时有 47 项数据库连接错误，不计为通过；改用专用容器 `qmt-psycopg2-test-20260926`、端口 15439、数据库 `qmt_order_test` 和随机测试 schema 后完整重跑通过。业务数据库配置和正在运行的 QMT 策略未修改。
- QMT 解释器下生成器 `--check` 通过，GBK 单文件与源码一致。此次未部署、未重启策略、未调用真实交易 API，也未进行驱动性能基准测试。

## 历史变更：非阻塞、分批执行与持久对账退出

- 交付的 QMT 文件为实际 GBK 单文件；`Last modified`：2026-09-26 11:32:46，SHA256：`8688420afee66456aa317d93dc29b146c85054c93829e92bc8555eb343fa8746`。构建一致性、实际 QMT Python 3.6.8 编译及无 DDL 检查通过。
- 启动冻结 `submit_batch_size=10`、`cancel_batch_size=10`、`reconcile_batch_size=100`、`schedule_budget_ms=50`。QMT 线程只运行原生调用和短内存操作；数据库、执行权连接和日志各由后台线程处理。HTTP 持久受理仍等待提交确认后才返回 202。
- 最终 QMT Python 3.6.8 + 独立 PostgreSQL：`unittest discover -s tests -p 'test*order*.py' -v`，**181 项全部通过，120.874 秒，无跳过**。日志：`.venv/order-nb-final-python36.log`。
- 项目 Python 3.14 全套回归 **285 项通过，126.569 秒**，其中包含 FEED 106 项。随后补入两项篮子恢复测试并修正恢复阶段，新增用例在项目解释器定向通过，最终 ORDER 181 项在 QMT 解释器全部重跑通过；FEED 源码没有变化。全套日志：`.venv/order-nb-final-all.log`。
- 非阻塞验证包括：阻塞数据库初始化、结果提交和 HTTP 受理事务；阻塞日志 sink；授权过期和实际 advisory 会话丢失；结果缓冲满；出队时 DB gate 翻转；65 笔撤单候选及每轮实际动作上限；停止时不提前释放本机锁。HTTP 在慢日志收尾期间仍能读取 `STOPPING`。
- 真实 PG 1000 笔 pending 用例验证：冻结与处理均恰好十批，每批最多 100 笔，ID 无遗漏、无重复；同轮共享 QMT 快照。回报队列保持积压时仍提供后台服务机会；历史查询范围在全部冻结页完成后确定。查询归并与异步回报分别维护事实代次，旧轮无法清除后来的回报或缺口。
- 终态退出、迟到事实重新入队、重复回报去重、部分成交撤余量、算法停止后晚到子委托、三种原生篮子路径和提交不明不盲重发均有回归。新增恢复用例模拟篮子创建成功但结果未落库：重启先 `get_basket`，不再次 `set_basket`，只产生一次下单调用；参数快照及篮子名称保留。
- v1→v2 真实迁移保留旧订单、子记录、事件、`event_seq` 和执行代次，重复迁移不改数据。候选查询的 `EXPLAIN` 在事务内禁用顺序扫描、位图扫描及显式排序，确认三个选择器能使用对应索引；这只证明索引适配，不代表生产优化器必然选择该计划。
- 使用专用容器 `qmt-order-nonblocking-test-20260926`，端口 `127.0.0.1:15439`，数据库 `qmt_order_test`，各用例随机 schema。没有迁移业务库 `qmt_paper`，没有调用真实 QMT 下单、撤单或篮子 API。测试容器在验收后清理。
- 当时部署需先停止旧策略并显式升级结构；该历史升级入口现已移除。现行初始化与检查命令见 `HTTP_ORDER.md`。券商模拟盘六条交易路径仍待单独验收，`locally_verified` 保持 false。

## 历史变更：策略启动动态注册账户

- 策略按实际启动参数 `account_id` 在结构检查后、取得执行权前执行 `INSERT ... ON CONFLICT DO NOTHING`。只创建缺失的账户行，已有事件游标、主机绑定和执行代次不被初始化操作覆盖；正常取得执行权仍会更新实例和递增代次。
- 外部安装器只建表并登记版本；`schema init/check` 无需账户参数。实际 PostgreSQL 验证两条 CLI 命令成功后 `account_runtime` 仍为零行，策略启动才注册账户。
- 当前策略 `Last modified`：2026-09-26 09:50:50；SHA256：`591fa6ecc9eb23175c4ef839f713d9e67521efddea51f110e92210bb671095ab`。实际 GBK，构建一致性检查通过，生成策略不含 DDL。
- QMT Python 3.6.8 + 真实 PostgreSQL：完整 ORDER 运行 135 项，其中 134 项通过；新增重启用例的断言从“游标不变”修正为“保留旧事件并继续递增”，随后 runtime 12 项全部通过。重启对账正常产生新事件，不应禁止序号增加。日志：`.venv/order-account-startup-python36-tests.log`、`.venv/order-account-startup-runtime-recheck.log`。
- 覆盖首次注册、八路并发只创建一行、保留已有状态和主机约束、前导零账户、重启幂等重放、事件历史及序号延续、结构检查或注册失败时禁止进入执行。测试使用专用容器 `qmt-order-startup-test-20260926`、端口 15439；不修改 `qmt_paper`，不调用真实 QMT 交易函数。

## 历史变更：DDL 外置及数据库隔离

- 当时 DDL 已从策略中外置，现统一存放在 `sql/order_init.sql`；管理命令通过 `tools/order_schema.py` 显式读取并安装，策略启动只检查既有关键表、字段、版本及账户记录。
- 公开配置移除 `pg_schema`，内部固定使用 `qmt_order`。模拟盘和实盘通过不同 `pg_database` 隔离；旧 schema 配置会明确报错。
- 当前策略 `Last modified`：2026-09-26 08:53:06；SHA256：`6cca508f79c8f574dd8c1b18cd7fbf00e5c32a796d02be55a70e7c025333a215`。
- 实际 QMT Python 3.6.8：`unittest discover -s tests -p 'test*order*.py'`，**129 项通过，45.359 秒**，包括真实 PostgreSQL 测试，未跳过。日志：`.venv/order-ddl-python36-tests.log`。
- 新增验证：缺失 schema 不会自动创建，缺关键表/列只报错，安装前拦截不兼容版本；独立 paper/live 测试数据库使用相同账户与客户端订单 ID，订单、事件及执行锁仍彼此独立，均使用固定 `qmt_order` schema。
- 生成策略中未发现建表、建 schema、建索引或 ALTER TABLE 语句；构建一致性检查通过。专用测试数据库与容器 `qmt-order-ddl-test-20260926` 已在验证后清理。

## 首版交付文件（历史记录）

- 开发源码：`order_bridge/`。
- QMT 部署文件：`strategies/http_order.py`，实际 GBK 字节编码，可独立部署，无需复制源码包。
- 部署文件 `Last modified`：2026-09-26 02:51:52。
- 部署文件 SHA256：`440a49ce456f664e7060398cd95ef1d9c50067ae1f3fe22977dfb3a2904b4a4c`。
- `tools/build_order_strategy.py --check`：current。
- 接口、配置、部署及人工处理说明：`HTTP_ORDER.md`；执行流程：`docs/ORDER_DESIGN.md`。

## 测试结果

| 环境 | 范围 | 结果 |
| --- | --- | --- |
| 项目 Python 3.14 | `unittest discover -s tests` | 224 项通过，57.817 秒 |
| QMT 自带 Python 3.6.8 64 位 | `unittest discover -s tests -p 'test*order*.py'` | 119 项通过，41.201 秒 |
| PostgreSQL 16.14 / pg8000 1.22.1 | 真实事务、并发与持久恢复 | 包含在上述测试中，没有跳过 |
| QMT Python 3.6.8 | 生成策略编译、导入、实际 HTTP 路由 | 通过；QMT API 替换为测试实现 |

119 项 ORDER 测试包含：原查询回归 23 项、参数及 QMT 适配 19 项、状态归并 30 项、PostgreSQL 存储 23 项、端到端执行 11 项、生命周期 5 项、工具和文档示例 8 项。

关键覆盖包括：同 ID 并发受理、提交结果不明、连续事件游标、端口与 schema 隔离、撤单和认领竞争、部分成交撤余量、算法停止后晚到子委托、重复和迟到成交、人工关联证据、三条原生篮子路径、篮子读回校验、UNKNOWN 不重发、SMART 跨日过期拦截、停止不撤单。

测试使用专用容器 `qmt-order-test-20260926`，监听 `127.0.0.1:15439`，每个测试使用随机 schema，测试后删除自身 schema。测试没有使用 TradeNest 数据库，没有调用真实 QMT 交易函数。专用容器在本轮验证后停止并删除。

本地详细输出保存在 `.venv/order-final-tests.log`、`.venv/order-final-python36-tests.log`。这些是当前工作目录的临时日志，不属于部署文件。

一次探索性全套 Python 3.6 运行发现既有 FEED 测试使用 Python 3.8 才支持的 `mock.call.args`，故 FEED 全套采用项目解释器执行；ORDER 测试全部在实际 QMT 解释器验证。未为此次 ORDER 开发修改既有 FEED 文件。

## 尚需模拟盘验证

尚未向真实 QMT 账户提交订单、撤单或创建篮子。没有部署 PostgreSQL 生产配置；未配置 PG 时仍为查询模式。

部署前须安装私有驱动、用 `order_admin.py schema init` 从独立 SQL 文件初始化选定数据库内固定的 qmt_order schema，并填写策略的 `pg_host/pg_port/pg_database/pg_user/pg_password`。`account_id/http_port` 继续使用策略运行参数。

模拟盘应分别记录 SINGLE/BASKET × DIRECT/SLICED/SMART 六条路径的原始请求、HTTP 响应、数据库记录、QMT 回报和时间，核对关联、成交、撤单及重启恢复。`/capabilities` 的六条路径均保留 `locally_verified=false`，代码测试不会将它改为已验证。

具体算法与证券权限由当前 QMT 及券商决定。ETF 金额单仍拒绝；代码前缀检查不证明证券当前上市或可交易；QMT 文档没有明确给出的参数单位不会猜测补充。
