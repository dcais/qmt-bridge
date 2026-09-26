# ORDER 本地验收记录

验证日期：2026-09-26，Asia/Shanghai。以下是代码、HTTP 和 PostgreSQL 验收，不是券商模拟盘交易验收。

## 最新变更：DDL 外置及数据库隔离

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
