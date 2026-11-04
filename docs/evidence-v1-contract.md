# QMT bridge evidence v1

开发日期：2026-10-08。接口均为 GET，路径名称以 `_evidence_v1` 结尾。本契约描述源码能力；不表示当前 7777 模拟服务已替换，也不解除 TradeNest 的交易门禁。

## 事实来源与能力边界

| 能力 | 本版支持与边界 |
| --- | --- |
| 持久 stream 身份 | 可选迁移为每个账户生成数据库 UUID；普通进程重启保留，重建账户记录生成新 UUID。来源是 bridge 本地库，不是 QMT 原生 stream。 |
| 事件分页 | 单页 PostgreSQL `REPEATABLE READ, READ ONLY`；服务端 MAC 游标固定 high watermark、账户、stream、executor 身份及尾事件锚点。 |
| 日志连续性 | 当前显式 coverage v2 只证明返回的连续页 `(after, next_after]`，不扫描未读尾部；旧 coverage v1 的全区间声明单独保留解释。不证明所有 QMT 回报都已收到。 |
| 完整库恢复/克隆证明 | unsupported。备份同时恢复 UUID、epoch、事件与序号时，同一库无法提供独立恢复世系证明。需要库外可信见证或人工复核。 |
| 资金与持仓 | 独立 QMT 查询，采样区间为 bridge 主机 UTC 时间；始终 non_atomic，`broker_as_of=null`。 |
| 事件与资产关联 | 同一账户可提供 HTTP 线程读取的数据库前后水位 bracket；即使两个序号相等，也不能说明 QMT 资金与日志原子一致。 |
| 费用 | 当前没有经支持版本核验的逐单/逐成交实际费用来源；金额、币种、修订均为 null，finality unknown。 |
| 历史日期覆盖 | 本地保留记录可读，但 QMT 原生历史查询 unsupported；当前对账仅 CURRENT_DAY，日期完整范围保持 null。 |
| 逻辑资金归属 | TradeNest 管理。券商可用余额不是任一策略的资金分配凭证。 |

核验材料：本仓库 `qmt_history_capabilities` 和原有历史能力测试；只读参考 TradeNest `provider_evidence.py`、`qmt_real_provider.py` 及交接文档；本地 qmt-explore 保存的 2026-09-22 官方 [数据结构快照](https://dict.thinktrader.net/innerApi/data_structure.html) 和 [交易查询文档](https://dict.thinktrader.net/innerApi/trading_function.html)。该支持范围未通过本次运行环境上的 QMT 原生调用验证，`verification_status` 仍为 UNVERIFIED。不能将 mock 的函数注入视为真实历史能力。

可选 [schema 6 事件存储](order-journal-storage-v6.md) 在兼容读取后重建相同 v1 原文；[显式紧凑传输](compact-order-events-v1.md) 可独立启用。

## 旧客户端兼容

- `/account` 仍返回全部原始 `m_` 字段字典；`/positions` 仍返回原始记录列表。保留原有 GET/POST 行为。
- `/order_events` 仍只有 `events/next_after/has_more`，其传统 after 分页不新增固定快照承诺。
- `/order`、`/orders` 和命令的原有参数、幂等与 UNKNOWN 处理保持兼容。
- `/capabilities` 追加 `evidence_v1`；旧客户端应忽略未知字段。
- `/health.error_code` 表示当前持久化或 executor 错误；新增 `last_error` 保留历史诊断。健康恢复后可能是 `error_code=null, last_error=PERSISTENCE_UNAVAILABLE`。`last_reconcile_error`、gap、日期不足仍分别查看；error_code 为 null 不代表全部恢复证据充分。
- 当前基础存储为 schema version 5，兼容读取可选 schema 6 事件存储；旧版 v1 初次发布基于 schema 3。策略不会自动执行 DDL。没有独立 evidence stream 可选列时证据事件/恢复接口返回 503 `EVIDENCE_SCHEMA_REQUIRED`，旧接口及账户采样接口仍可工作。

## 事件 `/order_events_evidence_v1`

首请求：`GET /order_events_evidence_v1?after=0&limit=100`。`after` 范围 0..9223372036854775807；limit 1..1000。after 是客户端请求边界，不是客户端已处理事实的来源证明。

后续只使用 `GET /order_events_evidence_v1?cursor=<URL编码的next_cursor>&limit=100`；cursor 与 after 互斥。客户端不能传入 stream_id、epoch、high_watermark 或自选 evidence ID。游标由当前 bridge 进程随机密钥签发，有效 15 分钟；改变 limit 不改变固定高水位。进程重启后密钥更新，旧游标返回 409；客户端先核对自己保存的 stream/epoch/末事件，再开启新查询，不能静默丢弃既有恢复问题。

以下为合成示例，省略订单正文：

```json
{
  "evidence_version": 1,
  "source": "BRIDGE_PERSISTED_RECORDS",
  "stream_id": "de7932f5-481f-4d64-8f88-008f846b239d",
  "stream_identity_origin": "DATABASE_GENERATED",
  "authenticity": "DEPLOYMENT_TRUST_REQUIRED",
  "account_id": "fake-account",
  "account_type": "STOCK",
  "executor_epoch": 7,
  "executor_instance": "fake-executor",
  "retained_from": 1,
  "high_watermark": 4,
  "coverage": {
    "validation_version": 2,
    "validation_scope": "RETURNED_PAGE",
    "sequence_kinds": "ORIGINAL_ONLY",
    "after_exclusive": 0,
    "through_inclusive": 1,
    "journal_contiguous": true,
    "start_origin": "CLIENT_REQUESTED_BOUND_ONLY",
    "broker_complete": "unknown",
    "provable_broker_dates": null
  },
  "restore_lineage_proof": "unsupported",
  "events": [{"event_id": 1, "type": "ORDER_ACCEPTED", "recorded_at": "2026-10-08T01:30:00Z", "data": {"order_id": "example-only"}}],
  "next_after": 1,
  "has_more": true,
  "next_cursor": "<server-issued-token>",
  "runtime_health_consistency": "non_atomic"
}
```

新事件在分页期间追加不会进入这次固定区间。事件页记录是当时持久化的订单快照，单个事件的订单 version 不要求等于 event_id。重复读取不代表新的事实；消费者应按 stream+event_id 去重，并按自己的投影规则处理原始 QMT 重复/乱序回报。服务端复用已有原始观察去重与状态归并，不创建第二套成交账本。

当前 `coverage.validation_version=2`、`validation_scope="RETURNED_PAGE"` 明示逐页语义：
`through_inclusive=next_after`，`journal_contiguous=true` 仅证明本页实际返回的连续区间。
`high_watermark` 仍是固定快照上界，`has_more=(next_after < high_watermark)`，不证明下一事件存在。
没有两个 validation 字段的旧 coverage v1 仍解释为对 `(after, high_watermark]` 的全范围声明，
其 `through_inclusive=high_watermark`；消费者不得把旧/新范围含义混用。外层 `evidence_version` 和 v1 路径不变。

例如冻结 high=4，实际只有 1/3/4：当前先返回连续前缀 `[1]`、`through_inclusive=next_after=1`、
`has_more=true`，下一次从 1 或其 MAC cursor 返回 409 `EVIDENCE_JOURNAL_GAP`，不会跳过缺号 2。
消费者可保存核验过的前缀，但正式恢复必须完整追平；重读保存尾锚 1 不算新的进展。
空日志 high=0 返回空事件、after/through/next_after 全为 0、has_more=false。

`retained_from` 是当前最小保留序号；空日志为 current high+1。原有实现不自动删日志。
人为删除前缀后，after 小于 retained_from-1 会返回 retention gap；不可用当前最小序号伪造被删历史。
内部缺口在没有下一条连续事件可交付时返回 409；未来 after、高水位回退、尾事件消失/变化、
stream 或 executor 变化仍明确失败。已消费前缀被改写且尾锚点未变化不能由本版发现。

显式 `include_void_receipts=true` 可读取已独立按条预授权并持久保存的非财务空序号凭据，
此时 `coverage.sequence_kinds="ORIGINAL_OR_ATTESTED_VOID"`，MAC cursor 固化该模式。
未安装可选表、没有授权或真实未知缺号均不自动生成作废。完整来源、摘要、导入和稳定尾锚规则见
[IPC ORDER 合同](../IPC_ORDER.md#已预授权的空序号作废凭据默认关闭)。

| 错误 | 处理 |
| --- | --- |
| EVIDENCE_CURSOR_INVALID | 游标被改动/过期/进程改变/跨路由；保存诊断后重新核对身份与本地 checkpoint。 |
| EVIDENCE_STREAM_CHANGED | 账户、stream 或 executor 改变；停止自动恢复并核对。 |
| EVIDENCE_RETENTION_GAP / EVIDENCE_JOURNAL_GAP | 不具备所请求区间；保持恢复 incomplete。 |
| EVIDENCE_ANCHOR_CHANGED / EVIDENCE_ROLLBACK_OR_INVALID_RANGE | 尾事实变化、消失或范围回退；按重建/回滚疑点处理。 |
| EVIDENCE_SCHEMA_REQUIRED / PERSISTENCE_UNAVAILABLE | 没有证据 schema 或库不可用；不降级为成功空证据。 |

分页数据库查询保留 statement timeout 保护。当前每页读取原有 `limit + 1` 个候选并验证连续前缀，不再执行完整剩余范围 `count(*)`；未读尾部保留未知，真实缺口不被越过。旧版全区间连续性计数属于旧 coverage v1 实现，不是当前逐页合同的要求。

## 账户与持仓采样

`GET /account_evidence_v1`、`GET /positions_evidence_v1`，均沿用 accountId/accountType 查询参数。证据路由不接受 POST、sample_id 或 snapshot_id。

```json
{
  "evidence_version": 1,
  "sample_id": "<server-generated-random-id>",
  "source": "QMT_GET_TRADE_DETAIL_DATA",
  "account_id": "fake-account",
  "account_type": "STOCK",
  "account_identity": "QUERY_ARGUMENT_ONLY",
  "kind": "account",
  "started_at": "2026-10-08T01:30:00.000000Z",
  "finished_at": "2026-10-08T01:30:00.005000Z",
  "clock_source": "BRIDGE_HOST_UTC",
  "consistency": "non_atomic",
  "broker_as_of": null,
  "event_relation": "non_atomic_bracket",
  "event_watermark": {"before": {"stream_id": "<uuid>", "high_watermark": 4, "executor_epoch": 7}, "after": {"stream_id": "<uuid>", "high_watermark": 5, "executor_epoch": 7}},
  "completeness": "unknown",
  "assets": {
    "available": {"value": 100.0, "source_field": "m_dAvailable", "meaning": "QMT available funds", "status": "observed"},
    "frozen": {"value": 5.0, "source_field": "m_dFrozenCash", "meaning": "QMT frozen funds", "status": "observed"},
    "cash": {"value": null, "source_field": null, "meaning": "total cash is unverified; not inferred from balance or available"},
    "currency": null,
    "logical_allocation": "TRADENEST_OWNED"
  },
  "raw": {"m_dAvailable": 100.0, "m_dFrozenCash": 5.0}
}
```

水位的实际完整对象还带 account_id/account_type/executor_instance/observed_at。before 在 HTTP 线程入队前读取，after 在 QMT 数据转成普通对象并完成任务后读取。期间可能有尚未持久化的 QMT 回报；主机时间也不是券商 cut 时间。每次独立查询产生不同 sample_id，账户和持仓不能合并为共同 snapshot。

未配置存储、缺迁移、数据库读取失败、跨账户/账户类型查询、stream/epoch 切换或水位回退时 `event_relation=unknown`，缺 marker 为 null。合法 QMT 空持仓列表仍不能证明登录状态或券商返回完整。非数值/布尔/非有限资产值返回 value=null/status=unknown，raw 保留原转换结果。`m_dBalance` 只留 raw，不映射成总 cash；不推导 available+frozen 或净资产，币种无已核验来源时为 null。

## 费用 `/order_fee_evidence_v1?client_order_id=<原ID>`

```json
{
  "evidence_version": 1,
  "account_id": "fake-account",
  "account_type": "STOCK",
  "order_id": "example-only",
  "order_version": 4,
  "status": "pending",
  "currency": null,
  "cumulative_amount": null,
  "revision": null,
  "finality": "unknown",
  "source": null,
  "reason": "ORDER_ATTRIBUTABLE_ACTUAL_FEE_SOURCE_UNAVAILABLE",
  "fills": [{"trade_id": "example-fill", "status": "pending", "currency": null, "cumulative_amount": null, "revision": null, "finality": "unknown", "source": null}]
}
```

order_version 是订单快照版本，不是费用 revision。账户汇总佣金、成交额、未核验 raw 字段与费率估算均不分摊为逐单最终费用。raw 佣金值发生变化也不伪造费用修订。未来取得经核验的逐单来源才可扩展有出处的费用合同；本版消费者必须保留未知费用相关占用/门禁。

## 恢复 `/recovery_evidence_v1`

参数与事件接口相同，但 cursor 不可跨路由。回执含同一 read-only 事务读取的 stream、账户、executor 身份、事件区间、未决请求和 `pending_as_of_high_watermark`。未决状态来自当前数据库读快照；续页固定的旧 high_watermark 可能小于 pending_as_of_high_watermark，不能把两者当共同快照。

`pending_requests` 最多 limit 条，包含原 order_id/client_order_id、订单版本、取消请求及 CALLING/UNKNOWN/RETURNED 的待核对尝试。RETURNED 仅证明函数返回，仍需订单/撤单回报确认。`pending_truncated=true` 会追加 blocker，本版没有独立待决分页，不提供完整待决清单承诺；重复事件页中的待决列表也不能累计为完整快照。

`sample_evidence` 只引用当前进程曾实际采集的、前后 bracket 完整且匹配该 stream/账户/executor 的最近账户与持仓样本；bracket 水位还须在此次请求已验证的 `coverage.after_exclusive..coverage.through_inclusive` 范围内，不能绑定未读尾部。缺失、采样切换 executor、unknown bracket、范围外样本或重启后未重新采样时为 null。引用样本仍可能过期，消费者须核对 started_at/finished_at，且始终 non_atomic；样本引用不是持久恢复凭证，不接受客户端自选 ID 注入。

每次回执固定 `recovery_status=incomplete`、`sample_evidence_consistency=non_atomic`、`provable_broker_dates=null`，并列出 broker completeness、atomic sample、final fees、restore lineage 的 blockers。UNKNOWN 继续查询原 `client_order_id` 的 `/order`；保存原 `order_id` 用于核对和人工检查，不自动重发、不清除隔离、不启策略。本版不修改 TradeNest 的 provider 或账本。

## 已发布事件的追加订正引用

已发布的事件正文、序号和既有 core-wire 摘要保持不变。正式 `MANUAL_TERMINAL_REPAIR`、
`MANUAL_UNKNOWN_RESOLUTION` 仍追加新序号，新增快照中的 `event_corrections_v1[]` 保存受控通知。
旧历史没有该字段时保持未知，不回填旧事件。

每条通知具有 `notice_version=1`、`notice_id`、`kind`、`verification_status`、
`financial_revision="NOT_AUTHORIZED"`、`source_event_refs`、`reason`、`operator`、`at`、
`evidence={reference,sha256}` 与 `request_sha256`。
`source_event_refs` 使用原始来源接口完全相同的不可变引用：

```json
{"stream_id":"de7932f5-481f-4d64-8f88-008f846b239d","account_id":"fake-account","account_type":"STOCK","event_id":7,"order_id":"retained-order","order_version":12,"sha256":"原事件完整公共core-wire的64位SHA256"}
```

`sha256` 针对 `{event_id,type,recorded_at,data:public_order(原订单文档)}` 的规范 JSON，
编码规则为 UTF-8、ensure_ascii=false、allow_nan=false、sort_keys=true、compact separators，无 BOM/额外换行。
它不是物理存储压缩文档摘要、当前订单摘要或文件摘要。可以用原引用经 `/order_event_source_v1` 重读核验。
原事件须实际保留，账户/订单/client 身份必须一致；缺少持久 stream 身份、缺原事件或摘要冲突均拒绝新订正。

- `ORDER_STATE_RECONCILIATION_REPAIR` 只恢复经现有严格证明的派生结案状态，引用原成功 checkpoint 及被核查的后续事件。
  引用同时写入 `terminal_repairs` 与独立磁盘审计回执；原成交、委托、金额数量保持原文。
- `UNKNOWN_OUTCOME_RESOLUTION` 引用被处置订单的最后已发布事件，并保留原始 observation IDs、人工材料及理由。
  它仍遵守既有正面证据和不可自动重发规则，不能把一般人工处置当作改写既有成交的授权。

### 独立券商材料的待复核通知

当前没有可核验的在线券商成交订正来源。离线 `order_admin correction-notice` 仅把原始材料引用保留为
`BROKER_CORRECTION_NOTICE` 新事件，不把材料自报内容直接写入成交、数量、金额、资金或作废历史。
默认只读预检，显式 `--apply` 才追加；没有当前订单或已保留原事件时不得制造历史。

```powershell
python tools/order_admin.py --config isolated-config.json correction-notice --order-id retained-order --expected-version 12 --notice-id broker-review-20261103-1 --source-ref-file retained-event-ref.json --evidence-file original-broker-material.bin --reason "独立券商材料需人工核验"
# 经授权并完成预检后，在同样参数上显式追加 --apply。
```

工具实际读取材料原始字节（1..1048576 字节）并计算 SHA256；原始材料须按审计要求持续保留。
引用文件不得超过 16384 字节，完整 ref 必须与持久原事件匹配。`expected_version` 只允许创建于已核对的当前版本。
不提供改价、改量、覆盖 raw、删事件、跳号或签发作废选项。

外部通知的固定 `kind="EXTERNAL_BROKER_CORRECTION_REVIEW_REQUIRED"`、
`verification_status="PENDING_INDEPENDENT_REVIEW"`、`financial_revision="NOT_AUTHORIZED"`，
`source_event_refs` 恰好一条。`notice_id` 为 1..128 ASCII 字符，正则 `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`。
`request_sha256` 是下列精确对象的规范 JSON 摘要：

```json
{"notice_version":1,"notice_id":"broker-review-20261103-1","kind":"EXTERNAL_BROKER_CORRECTION_REVIEW_REQUIRED","source_event_refs":["完整原事件ref对象"],"reason":"独立券商材料需人工核验","evidence_sha256":"实际材料文件字节SHA256"}
```

`operator`、`at`、文件路径不参与 request 摘要；它们作为首次追加的审计事实保留。
相同 `(order_id,notice_id)` 及相同 request 摘要重试返回原通知，不再追加事件，即使 expected_version 已因首次成功而变旧。
相同 ID 的任一材料/来源/理由变更均拒绝。独立新通知须用新 ID 和最新 expected_version。
后续订单版本只可保留原通知数组并追加新元素，不能改写旧元素；消费者不能因重复携带旧通知重新执行经济动作。

TradeNest 对可靠归属的外部待复核通知使用 owner 风险原因 `BROKER_CORRECTION_REVIEW_PENDING` 阻断新风险，
保留原始 BrokerEvent 与原凭据引用。通知自身不是财务修复，也不能解除该原因。
解除须经独立人工复核/既有修复路径，精确匹配
`correction_review={notice_id,notice_sequence,notice_digest,material_sha256,application_unit_id}`；
一般 STATE/UNKNOWN 修复通知不能作为券商成交改量或风险解除授权。
