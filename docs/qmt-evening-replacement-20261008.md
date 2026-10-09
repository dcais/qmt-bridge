# 2026-10-08 晚间模拟环境替换交付

本次仅完成源码、隔离验证和操作说明。当前模拟环境未部署本版；没有重启 1677/7777、发单/撤单、启用策略或执行业务数据库迁移、Git commit/push。

## 交付位置与开发基线

ASUS 仓库：`C:\Users\dying\git\home\qmt-bridge`，分支 `main...origin/main`。

开工时已有 `order_bridge/http.py`、`order_bridge/qmt.py`、`order_bridge/runtime.py`、`strategies/http_order.py` 的修改，以及未跟踪 `docs/research/qmt-longhubang.md`、`tests/test_order_history_capability.py`；全部保留。原有生成策略当时与源码一致，因此以合并后的完整源码重新生成。

- 可导入 QMT 的单文件：`strategies\http_order.py`，实际 GBK 编码。
- 源码：`order_bridge\evidence.py`，及 HTTP、runtime、repository、background 的相应扩展。
- 可选追加列迁移：`sql\order_evidence_v1.sql`，仅写文件，未执行。
- 契约及合成示例：`docs\evidence-v1-contract.md`。
- 新专项测试：`tests\test_order_evidence.py`。
- 最终回归原始输出：`docs\qa\qmt-evidence-regression-20261008.txt`；最终结果以该文件结尾与本次交付回复为准。
- 验证摘要及交付 SHA256：`docs\qa\qmt-evidence-verification-20261008.md`。最终全套 453 项，384 通过、69 数据库集成跳过、无失败；新证据专项 26 项。

## 替换前准备

1. 在实际模拟终端确认账号、ORDER 端口、当前导入策略文件位置、QMT Python 版本、现有 PostgreSQL database/user 参数。不要照搬默认 66027616/8888 或文档合成 fake-account。沿用确认过的模拟部署与预期账号。
2. 先保存现有 ORDER 策略的真实 GBK 文件、参数面板截图/文本、运行日志和只读 `/health`、`/capabilities`、`/account`、`/positions` 输出。备份目录应按本次时间新建，避免覆盖旧备份。
3. 对模拟 ORDER 数据库作完整备份，保留备份文件和 `pg_restore --list` 成功输出。不要从生产库复制配置用于测试；本次开发未提供/使用数据库凭据。确认 UNKNOWN、待决提交与取消请求；有未决副作用时先人工核对，不用替换动作清状态。
4. 在仓库核验生成文件：

   ```powershell
   .venv\Scripts\python.exe tools\build_order_strategy.py --check
   Get-FileHash -Algorithm SHA256 strategies\http_order.py
   ```

   不在 QMT 编辑器中把 GBK 文件直接按 UTF-8 覆盖保存。下面的替换/迁移操作是用户晚间执行方案，本次没有执行。

## 两种安装范围

**仅替换策略、暂不迁移**：旧接口、health 修复、账户/持仓证据、未知费用合同可工作。事件/恢复证据返回 `EVIDENCE_SCHEMA_REQUIRED`；这是正确的安全降级。TradeNest 保持观察/阻塞状态。

**启用本地 stream 证据**：先停止该模拟 ORDER 策略及其写入客户端，完成备份后，人工对核对过的模拟数据库执行 `sql\order_evidence_v1.sql`。它只为 `qmt_order.account_runtime` 追加 `evidence_stream_id uuid NOT NULL` 默认生成列；基础 schema version 仍为 3，不新增库、表、凭据或自动启动 DDL。`IF NOT EXISTS` 不验证既有同名列的类型，若已有同名列须先核对类型与非空约束；异常不要继续。

迁移示例只使用本地配置中经核对的参数，不把口令放入命令行、文档或日志：

```powershell
psql -X -v ON_ERROR_STOP=1 -h <模拟PG主机> -p <端口> -U <用户> -d <模拟数据库> -f sql\order_evidence_v1.sql
```

脚本在事务中执行，锁等待上限 5 秒。失败后保留输出并只读确认事务结果；不要自动重试不明结果、不改序号/epoch、不删事件。重新执行不会轮换已存在的 UUID；库外恢复见证仍不具备。

## 晚间替换顺序

1. 暂停 ORDER 写入来源，停止旧模拟 ORDER 策略并确认停止完成。1677 FEED 不需要随本次更换而重启。
2. 如选择启用 stream 证据，按上节离线应用可选 SQL 并只读检查；否则跳过。
3. 将交付 `strategies\http_order.py` 导入原模拟 ORDER 策略，恢复核对过的原参数。保持账号、端口、PG database/user 与原部署一致，不使用新随机端口作为正式服务配置。
4. 启动替换后的 ORDER 策略。等待自动恢复及当前日对账，先执行下面的 GET 验收。不使用 POST 下单/撤单来验证替换。
5. TradeNest 本次没有修改；它的 provider 未消费这些新路由，也没有获得原子资金、最终费用、资金归属或完整历史证明。接口可读不代表可以启用策略；后续 TradeNest 适配需独立开发和授权验收。

## 只读验收清单

以下以用户确认的 `http://127.0.0.1:7777` 为示意；先确认实际 ORDER 端口。所有请求均 GET，不读取/写入交易命令路径。

```powershell
$orderBase = 'http://127.0.0.1:7777'
Invoke-RestMethod -Method Get -Uri "$orderBase/health"
Invoke-RestMethod -Method Get -Uri "$orderBase/capabilities"
Invoke-RestMethod -Method Get -Uri "$orderBase/account"
Invoke-RestMethod -Method Get -Uri "$orderBase/positions"
Invoke-RestMethod -Method Get -Uri "$orderBase/account_evidence_v1"
Invoke-RestMethod -Method Get -Uri "$orderBase/positions_evidence_v1"
$page = Invoke-RestMethod -Method Get -Uri "$orderBase/order_events_evidence_v1?after=0&limit=100"
if ($page.next_cursor) {
    $cursorText = [Uri]::EscapeDataString($page.next_cursor)
    Invoke-RestMethod -Method Get -Uri "$orderBase/order_events_evidence_v1?cursor=$cursorText&limit=100"
}
Invoke-RestMethod -Method Get -Uri "$orderBase/recovery_evidence_v1?after=0&limit=100"
```

- health 的 account_id 与预期模拟账号一致；数据库、scheduler、executor 当前状态正常。恢复完成只是 bridge 当日运行恢复标记，另看 observation_gap、pending/unknown、last_reconcile_error 和历史范围。
- 若当前持久化已恢复，error_code 应为 null；历史 last_error 可保留。不要为了“测试清除”对真实运行库制造故障；该故障恢复场景已用 fake 存储验证。
- 旧 account 字典、positions 列表、order_events 三字段形状保持兼容。capabilities 的 evidence_v1 明确 unsupported 项。
- 有迁移时 stream UUID 非空，普通替换重启不会改变已保存 stream UUID；新进程旧游标应失效。只有重新 GET 查询，不以这项观察证明没有发生完整数据库回滚。
- 连续页 high_watermark 相同，event_id 递增且不重复；新追加事件在后续新查询中获取。出现 gap/anchor/stream 错误时保留响应并停止自动恢复，不手工修改数据使验收变绿。
- account/positions 采样均 non_atomic、broker_as_of null，资金 cash/currency 未核验时为 null；没有共同原子 snapshot ID。
- 已有订单用原 client_order_id 查询 `/order` 与 `/order_fee_evidence_v1`。费用 pending/null，不能将账户佣金或成交额当最终费用。没有现存订单时这项记为未验，不创建新单补验。
- recovery_status 应为 incomplete，日期范围 null，样本缺失或非原子，blockers 可见；pending_truncated=true 时不能声称完整待决清单。
- 验收期间订单/成交数量与既有状态变化须来自既有业务事实，不由该 GET 清单产生。记录观察时间、文件 SHA256、schema 检查和接口结果，不能用开发单测替代运行环境验收。

## 备份与回滚

策略替换异常时停止新 ORDER 策略，保留错误日志、请求 ID 和数据库状态；恢复先前保存的原 GBK 策略文件及原参数，再做 GET 检查。可选追加列兼容旧策略，通常无需删除列或恢复数据库即可回滚代码。

不要在存在新订单/回报写入后直接把旧数据库备份覆盖回来；这可能丢失未知副作用并制造重复提交。完整数据库恢复须另行停止所有写入、核对交易事实、保留库外 checkpoint 和人工确认，并视为 restore lineage unverified。本说明不包含清库、删除列、重置 epoch/序号或批量重发操作。

## 仍不支持与验证限度

QMT 完整回报序列、原子资金/持仓 cut、逐单最终费用/真实修订、券商原生跨日历史、逻辑资金归属和完整数据库恢复/克隆见证均未提供。本地 UUID 和连续日志只证明受信部署下本地记录范围；不足以通过 TradeNest 的强恢复门禁。

专项测试使用内存事务模型运行实际 repository 查询分支、fake QMT 与随机测试 HTTP 端口，并验证健康恢复；模型不能代替真实 PostgreSQL MVCC/DDL 或原生 QMT 验收。真实 PG 集成测试因为没有获准的独立测试库而明确跳过；未复用业务库、未申请/创建新库。详细数量与独立审查结果见本次最终交付回复及回归输出。
