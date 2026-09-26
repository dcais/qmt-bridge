# ORDER 本地验收记录

验证日期：2026-09-26，Asia/Shanghai。以下是代码、HTTP 和 PostgreSQL 验收，不是券商模拟盘交易验收。

## 最新变更：非阻塞、分批执行与持久对账退出

- 交付的 QMT 文件为实际 GBK 单文件；`Last modified`：2026-09-26 11:32:46，SHA256：`8688420afee66456aa317d93dc29b146c85054c93829e92bc8555eb343fa8746`。构建一致性、实际 QMT Python 3.6.8 编译及无 DDL 检查通过。
- 启动冻结 `submit_batch_size=10`、`cancel_batch_size=10`、`reconcile_batch_size=100`、`schedule_budget_ms=50`。QMT 线程只运行原生调用和短内存操作；数据库、执行权连接和日志各由后台线程处理。HTTP 持久受理仍等待提交确认后才返回 202。
- 最终 QMT Python 3.6.8 + 独立 PostgreSQL：`unittest discover -s tests -p 'test*order*.py' -v`，**181 项全部通过，120.874 秒，无跳过**。日志：`.venv/order-nb-final-python36.log`。
- 项目 Python 3.14 全套回归 **285 项通过，126.569 秒**，其中包含 FEED 106 项。随后补入两项篮子恢复测试并修正恢复阶段，新增用例在项目解释器定向通过，最终 ORDER 181 项在 QMT 解释器全部重跑通过；FEED 源码没有变化。全套日志：`.venv/order-nb-final-all.log`。
- 非阻塞验证包括：阻塞数据库初始化、结果提交和 HTTP 受理事务；阻塞日志 sink；授权过期和实际 advisory 会话丢失；结果缓冲满；出队时 DB gate 翻转；65 笔撤单候选及每轮实际动作上限；停止时不提前释放本机锁。HTTP 在慢日志收尾期间仍能读取 `STOPPING`。
- 真实 PG 1000 笔 pending 用例验证：冻结与处理均恰好十批，每批最多 100 笔，ID 无遗漏、无重复；同轮共享 QMT 快照。回报队列保持积压时仍提供后台服务机会；历史查询范围在全部冻结页完成后确定。查询归并与异步回报分别维护事实代次，旧轮无法清除后来的回报或缺口。
- 终态退出、迟到事实重新入队、重复回报去重、部分成交撤余量、算法停止后晚到子委托、三种原生篮子路径和提交不明不盲重发均有回归。新增恢复用例模拟篮子创建成功但结果未落库：重启先 `get_basket`，不再次 `set_basket`，只产生一次下单调用；参数快照及篮子名称保留。
- v1→v2 真实迁移保留旧订单、子记录、事件、`event_seq` 和执行代次，重复迁移不改数据。候选查询的 `EXPLAIN` 在事务内禁用顺序扫描、位图扫描及显式排序，确认三个选择器能使用对应索引；这只证明索引适配，不代表生产优化器必然选择该计划。
- 使用专用容器 `qmt-order-nonblocking-test-20260926`，端口 `127.0.0.1:15439`，数据库 `qmt_order_test`，各用例随机 schema。没有迁移业务库 `qmt_paper`，没有调用真实 QMT 下单、撤单或篮子 API。测试容器在验收后清理。
- 部署需先停止旧策略，显式执行 `schema migrate`、`schema check`，再导入新的 GBK 策略。v2 SQL、迁移 SQL 和命令见 `HTTP_ORDER.md`。券商模拟盘六条交易路径仍待单独验收，`locally_verified` 保持 false。

## 历史变更：策略启动动态注册账户

- 策略按实际启动参数 `account_id` 在结构检查后、取得执行权前执行 `INSERT ... ON CONFLICT DO NOTHING`。只创建缺失的账户行，已有事件游标、主机绑定和执行代次不被初始化操作覆盖；正常取得执行权仍会更新实例和递增代次。
- 外部安装器只建表并登记版本；`schema init/check` 无需账户参数。实际 PostgreSQL 验证两条 CLI 命令成功后 `account_runtime` 仍为零行，策略启动才注册账户。
- 当前策略 `Last modified`：2026-09-26 09:50:50；SHA256：`591fa6ecc9eb23175c4ef839f713d9e67521efddea51f110e92210bb671095ab`。实际 GBK，构建一致性检查通过，生成策略不含 DDL。
- QMT Python 3.6.8 + 真实 PostgreSQL：完整 ORDER 运行 135 项，其中 134 项通过；新增重启用例的断言从“游标不变”修正为“保留旧事件并继续递增”，随后 runtime 12 项全部通过。重启对账正常产生新事件，不应禁止序号增加。日志：`.venv/order-account-startup-python36-tests.log`、`.venv/order-account-startup-runtime-recheck.log`。
- 覆盖首次注册、八路并发只创建一行、保留已有状态和主机约束、前导零账户、重启幂等重放、事件历史及序号延续、结构检查或注册失败时禁止进入执行。测试使用专用容器 `qmt-order-startup-test-20260926`、端口 15439；不修改 `qmt_paper`，不调用真实 QMT 交易函数。

## 历史变更：DDL 外置及数据库隔离

- DDL 唯一存放在 `sql/order_v1.sql`，管理命令通过 `tools/order_schema.py` 显式读取并安装；策略启动只检查既有关键表、字段、版本及账户记录。
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
