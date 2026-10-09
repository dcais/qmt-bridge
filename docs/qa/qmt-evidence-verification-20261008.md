# QMT evidence v1 开发验证记录

日期：2026-10-08。仓库 ASUS `C:\Users\dying\git\home\qmt-bridge`，`main...origin/main`。本记录仅证明源码与隔离测试状态，当前 1677/7777 未部署或重启。

## 最终结果

- 完整回归：**453 项，384 项通过，69 项跳过，0 失败，42.061 秒**。
- 新 `tests/test_order_evidence.py`：26 个专项测试，包含在上述回归内。
- PostgreSQL 测试入口 `ORDER_TEST_PGHOST` 在该测试进程中清空；69 项数据库集成场景明确跳过，没有复用业务库。
- `tools/build_order_strategy.py --check`：current；交付策略为实际 GBK 编码。
- 源模块与策略文件的 Python 3.6 AST 语法兼容检查通过；未在原生 QMT Python 中执行。
- `git diff --check` 无错误；Git 提示的工作树 CRLF 转换警告不代表执行了 Git 写操作。
- 可选迁移文件解析为 4 个语句：BEGIN、SET LOCAL、ALTER TABLE、COMMIT。仅解析文件，未执行 PostgreSQL DDL。

完整输出：[qmt-evidence-regression-20261008.txt](qmt-evidence-regression-20261008.txt)。输出末尾的 temporary strategy stale/current 是构建器自身测试对临时文件的预期场景；实际交付路径单独 `--check` 为 current。

最终 `strategies/http_order.py` SHA256：

```text
ABA3DE6DC61A3F6280F02465E738BF233EBAEFAF3BCD0ED35EF0EA016E487CB1
```

复现（仅隔离回归，不启用 PG 测试）：

```powershell
$env:ORDER_TEST_PGHOST = ''
.venv\Scripts\python.exe -m unittest discover -s tests -v
.venv\Scripts\python.exe tools\build_order_strategy.py --check
```

## 场景覆盖

新测试调用真实 repository 证据分支，使用内存事务模型提供查询结果与读快照；HTTP 测试使用 fake QMT、随机端口及临时日志目录。它不等于真实 PostgreSQL MVCC 测试。

- 同一页内并发追加、页间并发追加均不进入旧高水位；重复读取页稳定，旧事件接口三字段形状保持。
- 普通重启保留数据库 stream，进程 MAC 游标失效；账户记录重建、epoch/instance 改变拒绝旧游标。
- 尾锚点改写/消失、库水位回退、内部缺号、保留前缀删除、非法未来 after、伪造/过期/跨路由游标均失败。
- 缺迁移与存储失败不返回成功空证据；完整备份恢复仍显式 unsupported。
- 回报断线遗漏与乱序版本不提升 broker completeness；既有状态测试同时覆盖成交去重、迟到成交修正和乱序撤单回报。
- 资金变化产生独立样本；cash/currency 未核验为 null；非数值资产字段 unknown；旧账户/持仓原始形状保持。
- 前后 marker 只在 HTTP 线程读取；跨账户、marker 失败、缺失/unknown/回退 bracket、epoch 变化或水位在固定区间外，不作为恢复关联样本。
- 汇总佣金、成交额和未经核验 raw 费用变化不生成最终费用或伪造修订；费用始终 pending/null。
- 恢复回执保留原请求 ID、executor epoch、单独待决采样水位、截断 blocker；撤单 RETURNED 仍待事实核对；不发送新提交/取消。
- 真正后台线程中模拟持久化恢复失败后自动成功，不重启即可清除当前 error_code；历史 last_error 保留，authority 错误不因健康探测成功而清除。

## 独立审查

独立只读 reviewer 进行了初审和复审。初审指出的 RETURNED 取消尝试遗漏、unknown bracket 误关联、待决清单截断说明及日志检查区间文案均已修复并纳入测试。复审未发现新的必要修复点；没有把本地日志证据表述为 QMT 完整回报、原子资金、最终费用或数据库恢复强证明。

## 尚未验收

真实 PostgreSQL DDL/MVCC、原生 QMT 调用、真实模拟服务替换和跨进程部署验收尚未执行。还缺完整券商回报证明、原子财务 cut、逐单实际最终费用/修订、券商跨日历史、逻辑资金分配凭证与库外恢复见证。本版接口明确保持 unknown/unsupported/incomplete，不解决或放宽 TradeNest 相应门禁。

未执行交易、策略启用、业务库迁移、清库、当前服务变更或 Git commit/push。源码与说明均保留在工作树中。
