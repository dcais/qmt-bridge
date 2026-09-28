# ORDER 创建时间、查询字段与子表增量保存计划

状态：已完成代码、测试、两业务库迁移及用户指定的 7777 / qmt_paper 实机启动验收。2026-09-28。qmt_live 仅完成迁移与结构验证，未启动实盘 Bridge。

## 实施与验证记录

- 新版生成策略：`strategies/http_order.py`（GBK），SHA256 `e943064e7b7d0087f42645cbc6aeb3c4fb9ecf40aabada6f888fa168aa7c394b`。构建 `--check` 通过；在本机 QMT Python 3.6.8 中编译并执行模块定义通过，未调用策略 `init` 或交易接口。
- `uv run --python 3.11 --with psycopg2-binary==2.9.5 python .omx/run_storage_checks.py discover -s tests -p 'test_order*.py' -q`：272 项全部通过，无跳过，包含真实 PostgreSQL 隔离 schema、FakeQmt 生命周期、迁移成功/回滚/重复执行、UUID/时间不变、业务唯一及响应兼容测试。`.omx/run_storage_checks.py` 为本机连接环境注入器，不含硬编码口令；复现时也可直接设置 `ORDER_TEST_PG*` 环境变量后运行 unittest。
- 四类编号查询分别在 2,000 行隔离样本上执行 `EXPLAIN ANALYZE`，均采用 Index Scan，命中一行。隔离样本已清理。
- `qmt_paper` 于 `2026-09-28T09:31:01.907589+00:00`、`qmt_live` 于 `2026-09-28T09:31:03.084089+00:00` 单事务迁移完成，回读均为 schema 3；已用实际 QMT Python 3.6.8 + psycopg2 2.9.5 对两库执行只读新结构检查。
- 两库各 11 张表均有 `created_at timestamptz NOT NULL`，无外键、无 `migration_at` 列。Paper 保留 3 笔订单、44 条事件、51 条观测及全部子记录；Live 原无订单，仍无订单。迁移内校验了业务文档、行数、订单列、事件/原始回报及账户状态保全。
- 备份和执行证据目录：`C:/Users/dying/Documents/Codex/2026-09-28/qmt-order-storage/`。含两库 `20260928T092431Z.dump` 备份（pg_restore --list 读取通过）、预检/迁移/回读 JSON 和 `query-plans.json`。
- 实机验收：用户于 17:38:56 启动 7777；启动日志确认数据库 `qmt_paper`、账户 `66027616`。`/health` 返回 schema 3、RUNNING、database_available/recovery_complete/executor_owned/accepting_orders 均为 true，pending_count/unknown_order_count 为 0，last_reconcile_error/error_code 为 null。
- `/orders` 返回该账户全部 3 笔订单，逐笔 `/order` 详情与数据库匹配，内部 UUID 未公开，`/order_events` 正常返回 47 条事件。启动后回读父子文档一致，3 条任务、3 条委托和 1 条成交的创建时间仍为迁移时值；执行尝试仍为 3 条。本次验证只读接口，未发送下单或撤单请求。
- 实机证据：上述证据目录中的 `paper-runtime-verification.json`、`verification-manifest.json`。QMT 当前查询仅覆盖当日，启动验收不证明历史交易查询完整性，也不代表新增交易路径的券商验收。按用户指定先验收 Paper，未启动 Live。

已确认的历史回填规则（2026-09-28）：现有业务库增加字段时，无法证明的历史创建时间统一使用迁移当时的时间戳补齐。

已确认的记录标识规则（2026-09-28）：六张子表统一使用首次创建后不变的 UUID `record_id`；不再根据交易日、市场或业务编号计算，不使用自增或雪花 ID。

已确认的部署前提（2026-09-28，用户说明）：Bridge 已停止，不会有新数据写入；下一次启动使用适配新表结构的 Bridge。按停写状态下的一次性迁移规划，不设计在线迁移、双版本并行或分批恢复框架。此处记录用户确认，不代表本轮已通过进程或数据库检测。

## 目标与范围

为 `qmt_order` 全部 11 张表提供含义明确的 `created_at`，将常用 QMT 标识提取为可直接查询和索引的列，并将六张子表的整单删除重建改为增量保存。

保留 `orders.document` 作为订单聚合状态的权威来源，子表作为关系查询投影，`order_events` 保存事件快照，`qmt_observations.raw` 保留回报证据。父文档、子表和事件仍在同一事务中更新。

沿用单一当前初始化文件 `sql/order_init.sql`，不引入外键或历史版本升级框架。策略启动只检查结构，不执行 DDL。不改变下单、撤单、幂等、UNKNOWN 不自动重发和对账归属规则。

当前工作区存在未提交修改；实施必须在这些修改之上增量进行，不覆盖其他工作的内容。

## 实施前调查依据

| 位置 | 当前行为及问题 |
| --- | --- |
| `sql/order_init.sql` | 仅 `orders` 有 `created_at timestamptz NOT NULL DEFAULT now()`，其余 10 张表缺少该列 |
| `order_bridge/repository.py::repo_save` | 主订单插入时使用文档创建时间，冲突更新不覆盖该列；六张子表按订单删除后重新插入 |
| `repo_save` 子表主键生成 | items、attempts、cancel_requests 使用业务 ID；任务、委托、成交使用交易日、市场和原生 ID 的指纹 |
| `order_bridge/state.py::apply_observation` | 委托和任务可能先缺交易日/市场、后补齐；补齐会改变当前指纹。`observed_at` 不能统一视为首次创建时间 |
| `repository.py::repo_match` | 按文档中的 QMT ID 查找，并保留日期/市场缺失时的候选匹配与歧义处理 |
| `repository.py::request_cancel` | 仍以 `record_id = cancel_request_id` 查询幂等记录；改 UUID 时必须改查独立业务列 |
| `common.py::public_order`、`state.py::cancel_response` | 复制文档生成响应，必须定向过滤新增内部 UUID，避免意外改变接口 |
| `tools/order_schema.py`、`PostgresRepository.check_schema` | 初始化与运行时检查需要同步扩展；`CREATE TABLE IF NOT EXISTS` 不会修改已有表 |

上表记录实施前代码调查；实际完成情况和业务库验证见顶部实施记录。

## 一、时间戳语义

`created_at` 定义为“本系统首次创建该逻辑记录的时间”，使用 `timestamptz`。它不是券商委托时间、成交时间，也不是最近修改时间或数据库提交时间。

历史回填例外：无法证明首次创建时间的既有记录，`created_at` 使用本次迁移时间作为补齐值；此值表示迁移补齐时间，不代表已恢复真实历史创建时间。

统一使用带时区时间；应用生成 UTC 时间，展示时可转换为上海时区。数据库 `now()` 是事务起始时间，只作为插入默认值，不宣称逐行精确到达时间。

| 表 | 新记录的时间来源 |
| --- | --- |
| `schema_version`、`account_runtime` | 数据库首次插入时间；重复初始化、启动和账户更新不覆盖 |
| `orders` | 保留现有文档创建时间及冲突更新行为 |
| `order_items` | 创建订单 item 时赋值，通常与所属订单创建时间相同 |
| `execution_attempts`、`cancel_requests` | 使用现有记录内的 `created_at`，后续状态更新不覆盖 |
| `qmt_tasks`、`qmt_orders`、`fills` | 首次归并为业务记录时赋予独立 `created_at`；回放旧回报时使用本次建档时间，不借用回报业务时间 |
| `order_events`、`qmt_observations` | 本条事件/观测记录首次入库时间；保留原有 `occurred_at`、`observed_at` 的语义 |

六类子记录的文档携带稳定 `record_id` 和首次创建时间，子表列从同一记录取值。委托/任务补齐身份时，UUID 和创建时间均保持不变。正常 UPSERT 不更新 `record_id` 或 `created_at`。

对已存在主键，保存前必须验证传入文档的 `created_at` 与已存列一致；不一致时拒绝并回滚，不能保留旧列却写入带新时间的文档。主订单采用相同一致性检查。受控历史回填单独处理，不能经普通保存路径改写首次时间。

新库及完成回填的现有库均要求 `created_at NOT NULL`；历史未知时间使用统一迁移时间补齐，具体规则见第五节。本次不统一重命名或转换现有业务时间字段，也不额外给所有表增加 `updated_at`。

## 二、关键字段列

从归一化后的子记录 `document` 提取字段，不直接重复解析 `raw`。优先使用显式 `STORED` 生成列，避免应用双写偏差；实施前核实目标 PostgreSQL 版本及表达式支持。`created_at` 使用普通列，因为它需要不可变的首次创建语义。

| 表 | 计划新增查询列 |
| --- | --- |
| `order_items` | `item_id`、`symbol`、`side` |
| `execution_attempts` | `attempt_id`、`kind`、`status`、`target_id`、`cancel_request_id` |
| `cancel_requests` | `cancel_request_id`、`status` |
| `qmt_tasks` | `qmt_task_id`、`trading_day`、`market`、`status` |
| `qmt_orders` | `qmt_order_id`、`qmt_task_id`、`item_id`、`trading_day`、`market`、`symbol`、`side`、`status`、`native_ref`、`native_order_ref` |
| `fills` | `trade_id`、`qmt_order_id`、`item_id`、`trading_day`、`market`、`symbol`、`side`、`quantity`、`amount` |

业务标识字段使用 `text`，不转整数，保留前导零；内部 `record_id` 使用 PostgreSQL 原生 `uuid` 类型，在 JSON 文档中存标准 UUID 字符串。交易日暂用归一化文档中的文本，避免本次引入日期解析与兼容性变更。数量、金额使用 `numeric`；空值保留 NULL，非法数字不得静默转成零。实施前验证现有归一化写入能满足转换要求，并覆盖缺字段、JSON null、空字符串与异常类型。

保留 `account_type`、`account_id`、`order_id` 和包含这些作用域的复合主键结构，将其中的 `record_id` 改为稳定 UUID。它是内部记录标识，不等于 `item_id`、`attempt_id`、`cancel_request_id` 或 QMT 编号；这些业务字段继续保留原语义和引用关系。

现有 `cancel_request_scope_id` 的唯一约束作用依赖 `record_id` 等于撤单请求编号；改 UUID 后必须将其改为 `(account_type, account_id, cancel_request_id)` 唯一索引，继续保障原有跨订单撤单幂等约束，不能仅对 UUID 唯一就认为业务约束仍成立。

### 业务唯一性

UUID 仅代表内部记录身份。应用保存前检查下列业务键非空且不重复，并在相应生成列上保留数据库约束：

| 表 | 业务唯一约束 |
| --- | --- |
| `order_items` | `(account_type, account_id, order_id, item_id)` |
| `execution_attempts` | `(account_type, account_id, order_id, attempt_id)` |
| `cancel_requests` | `(account_type, account_id, cancel_request_id)`，覆盖跨订单幂等 |

这三类业务 ID 的生成列要求 `NOT NULL` 且非空字符串；不能仅靠允许 NULL 的唯一索引保证身份完整。相同业务键换 UUID 的写入必须拒绝。

QMT 任务、委托、成交保持现有归并和业务身份判重；同一订单集合中即使 UUID 不同，也不能接受相同的完整业务身份。旧指纹主键提供的同订单去重保障须以业务身份校验保留；缺少维度时沿用现有候选与歧义处理，不放宽规则，也不引入跨订单自然键唯一约束。

示例目标定义：

```sql
qmt_order_id text GENERATED ALWAYS AS (document->>'qmt_order_id') STORED
```

先不增加整列 JSONB GIN 索引，不把所有原始回报字段拆成列。生成列特性参考 [PostgreSQL 文档](https://www.postgresql.org/docs/current/ddl-generated-columns.html)，JSONB 查询与索引参考 [JSON 类型文档](https://www.postgresql.org/docs/current/datatype-json.html)。

## 三、索引与查询调整

首批建立以下非唯一 B-tree 索引，列顺序优先服务现有按账户和编号查找的路径：

- `qmt_tasks(account_type, account_id, qmt_task_id, trading_day, market)`。
- `qmt_orders(account_type, account_id, qmt_order_id, trading_day, market)`。
- `fills(account_type, account_id, trade_id, trading_day, market)`。
- `fills(account_type, account_id, qmt_order_id, trading_day, market)`，用于按委托找成交。

索引可用编号非 NULL 的部分条件，具体以实际查询与执行计划验证。其余展示字段先不加索引；不能仅因为增加了列就增加索引。

`repo_match` 改用显式列，保留现有账户隔离、remark 优先、缺少维度可作为候选以及多候选不归属的逻辑。动态列名只允许代码中的固定映射，值仍参数化。

`request_cancel` 改为按 `(account_type, account_id, cancel_request_id)` 查找，不再将客户端撤单编号绑定到 UUID 列。相同编号相同内容返回原结果，不同内容继续返回幂等冲突，跨订单重用同样受原规则约束。同步修改测试中以撤单编号查询 `record_id` 的 SQL，并检索其他把业务编号当内部键的读路径。

QMT 编号不是全局唯一标识。此阶段不新增跨订单自然键唯一约束；先审计账户、交易日、市场缺失与跨订单重复，不改变现有去重及冲突证据处理。

### HTTP 响应兼容

`record_id` 只在持久化模型内部使用，不作为新的客户端参数或响应字段。`public_order` 对六类已知子记录集合定向过滤 `record_id`，`cancel_response` 对撤单记录执行同样过滤；提交、详情、列表、事件和撤单接口统一使用这些转换函数。

过滤在响应副本上完成，不修改权威文档、历史事件或原始回报，不对任意 `raw` 内容递归删除同名字段。内部新增的子记录 `created_at` 也不在本次悄然扩展 HTTP 合同：原响应已有时间字段保留，仅为数据库新增的字段在相应响应路径中隐藏。测试分别明确各类原有字段，避免误删原来已公开的撤单创建时间等字段。

## 四、六张子表改为增量保存

继续保留 `child_fields` 控制的局部刷新，以及既有账户锁、事务与事件序号处理。

### 稳定 UUID 生命周期

- 六类子记录首次创建时由应用调用 `uuid.uuid4()`，同时写入权威文档的 `record_id`，数据库投影复用该值；不依赖数据库默认生成后回填。
- 先按现有业务身份规则匹配与去重，再决定是否创建记录。重复回报、重复请求、状态更新、重启恢复和身份补齐均复用已有 UUID；不能在每次保存或每次回报时重新生成。
- 保存层只读取和校验 UUID，不重新计算或补发。对已存在业务记录试图更换 UUID 的写入应拒绝，不能将其解释为集合中的一删一增。
- 使用标准库 UUID，不增加 ID 服务、节点编号配置或时钟依赖。UUID 只解决内部身份稳定性，不能替代账户、交易日、市场及业务编号的归属判断和去重。

### 保存步骤

每个需要刷新的子表执行：

1. 在现有账户锁保护下，读取需要保存的子集合对应的既有行，一次性取得 UUID、业务身份与创建时间，避免逐行查询。
2. 验证 UUID、业务唯一性和首次时间一致性。已存 UUID 必须仍在目标集合中；若记录消失（包括原非空集合被清空），报告持久化不一致并回滚，不自动删除。业务身份补齐只允许更新原 UUID。
3. 按主键执行 `INSERT ... ON CONFLICT ... DO UPDATE`，仅更新 `document` 等可变字段，不更新 `record_id`、`created_at` 或账户/订单归属。
4. 使用 `IS DISTINCT FROM` 等条件跳过内容完全相同的行。未列入 `child_fields` 的表不写入；调用方不得同时修改这些表对应的文档集合，需在同一事务内对照持久父文档验证，防止局部保存造成漂移。
5. 与父文档、事件一起提交，任意失败整体回滚；原集合为空且目标仍为空时无操作。

移除保存层对业务 ID、指纹和数组下标的 `record_id` 回退，所有合法创建路径都必须先赋予 UUID。外部观察不需要携带内部 UUID；若缺少现有归属规则要求的业务标识，则拒绝加入正式业务集合，走现有 `qmt_observations` / 未归属证据路径并正常提交证据。保存阶段若发现内部集合缺少合法 UUID，则整体回滚并报告错误，此次事务不能宣称已持久留证。不能通过重新编号掩盖问题。

身份补齐采用现有业务归并结果：同一委托/任务补齐交易日或市场时保留 `record_id` 与 `created_at`，对同一主键原地 UPDATE，不允许插入新键再删除旧键。不得按相同原生编号跨交易日/市场猜测同一记录；有歧义仍交由现有证据路径处理。

正常保存路径不执行子记录 DELETE。订单项、尝试、撤单请求、任务、委托、成交按当前业务只新增或更新；确需删除数据时使用独立、明确的数据维护操作，不把删除隐含在普通保存中。本次不新增删除功能，也不修改原始观测和历史事件快照。

## 五、初始化和已有数据边界

### 代码交付

修改唯一当前初始化 SQL，扩展启动检查及管理工具检查，覆盖全部新列的类型、默认值/空值约束、生成表达式及必要索引。保持运行时只读检查、独立工具建结构。

本次将结构标识从 2 更新为 3，代码、测试和文档已同步；仍只保留一份当前初始化 SQL，不增加 v2/v3 初始化文件或自动升级命令。版本数字更新用于拒绝旧结构，不能替代列与索引检查。

对兼容新库重复初始化必须保留数据、创建时间、账户事件序号和执行权信息。旧库初始化应在 DDL 前拒绝，不自动删表、重建或清数据。

### 现有业务库：停写后一次性迁移

用户已确认 Bridge 停止且无新写入，迁移后只运行新版 Bridge。两业务库已按下述流程完成适配，实际启动由用户导入最终策略后进行。

为本次变更编写一个专用离线工具（计划路径 `tools/order_storage_backfill.py`），提供只读预检和显式执行入口。它不构成长期历史升级框架，不由策略启动或 `schema init` 自动调用。

执行流程：

1. **预检与备份。** 确认目标数据库，读取版本、列类型、记录数量、业务键重复、文档与子表一一对应关系及时间来源；备份现有库。预检发现歧义、数据不一致或非法生成列输入时先报告，不猜测配对或丢弃记录。
2. **开始单一数据库事务。** 一次取得带时区的 `migration_at`，供本次所有未知历史时间复用。它是工具变量，不新增表字段。使用独立维护连接及适合离线 DDL 的超时，不复用 Bridge 默认短查询超时。
3. **转换结构与回填。** 为缺少内部标识的每条子记录生成 UUID；本事务内建立旧键到新 UUID 的映射，同步更新当前订单文档、子表文档及其 `record_id`。按临时 UUID 列回填后替换旧列或等价的原地转换方案完成类型切换，不把哈希强转 UUID，不删除重建业务记录。不改变原有业务 ID、QMT 关联、订单版本、事件序号或账户执行状态。
4. **补齐时间和约束。** 已有合法且语义明确的创建时间保留；其余统一填入 `migration_at`。当前权威文档与投影同步更新，原始回报和历史事件 JSON 快照保持原样。建立生成列、业务唯一约束和查询索引，验证后施加 `NOT NULL`。
5. **校验后提交。** 比较各表行数、业务 ID/业务值、文档投影对应关系、UUID/时间一致性及账户状态；仅允许预定的结构和内部元数据变化。全部通过后在同一事务内更新结构标识并提交。输出一次性执行报告，记录数据库、时间、各表处理数量、校验结果及是否提交，不新增迁移状态表或恢复检查点。
6. **启动新版。** 构建并验证新版策略文件，使用新代码检查新结构，再启动 Bridge 并确认恢复与查询就绪。不把恢复就绪视为真实下单验收，也不为验证结构额外发送交易指令。

失败在提交前整体回滚，无半完成状态；重新执行可生成新的尚未持久化 UUID 和迁移时间。提交成功后再次执行只检查已完成状态，不重复回填。如果提交结果不确定，先重新连接检查结构标识及数据一致性，再决定是否重试。无需跨完全回滚的尝试保存逐记录 UUID 映射。

若有多个业务数据库，逐库独立备份、事务和验收，不声称跨数据库原子提交；每个库有自己的 `migration_at`。历史事件快照中的旧格式不改写，HTTP 展示仍通过统一响应转换保持兼容。

## 六、实施顺序与文件范围

1. 定义六类子记录稳定 UUID 与创建时间，覆盖全部创建入口、身份补齐和重复回报：`order_bridge/common.py`、`order_bridge/state.py`、`order_bridge/repository.py`、相关状态测试。
2. 修改当前结构、版本和结构检查：`sql/order_init.sql`、`tools/order_schema.py`、`order_bridge/repository.py`。
3. 修改子表 UPSERT、业务唯一校验、缺失记录拒绝、局部保存一致性和归属查询；特别修改 `request_cancel` 及其测试，不再用业务编号查询 UUID 列：`order_bridge/repository.py`、`tests/test_order_repository.py`。
4. 修改响应转换及接口测试，过滤新增内部字段并保留既有响应：`order_bridge/common.py`、`order_bridge/state.py`，检查 `order_bridge/runtime.py` 的提交、查询、列表和事件调用路径。
5. 实现一次性离线工具，在隔离 PostgreSQL 构造旧结构样本，验证迁移成功、失败回滚、重复执行、UUID 映射及历史回填；同时验证新库初始化及业务回归。不得用业务表执行测试。
6. 更新 `HTTP_ORDER.md`、`docs/ORDER_DESIGN.md`，通过项目已有构建流程生成 `strategies/http_order.py`，核对生成结果；不手工单独修补生成文件。
7. 在已停写的目标业务库执行预检、备份、迁移和新结构验收，再使用新 Bridge 启动验证；记录每个数据库的实际结果。
8. 将本计划状态更新为实际完成情况，列明通过项、跳过项及尚未完成的部署步骤。

## 七、验收要求

| 验证场景 | 必须满足的结果 |
| --- | --- |
| 新库初始化、重复初始化 | 11 张表均有时间列，生成列及四类索引正确；重复初始化不改变已有时间及账户状态；无外键 |
| 旧结构和损坏结构 | 旧版本被拒绝；缺列、错误生成表达式或缺必要索引可检出；检查不执行修复 DDL |
| 新订单、尝试、撤单、任务、委托、成交、事件、观测 | 各表时间语义正确，时区正确；父文档与投影一致 |
| 现有库历史回填 | 可信时间保留；每库未知时间统一为一次事务的 `migration_at`；权威文档与投影一致、无 NULL；业务数据及行数保持；不增迁移字段或状态表 |
| 迁移失败与重复执行 | 提交前失败整体回滚；成功后重复执行不修改 UUID/时间；提交结果不确定时先核查，不盲目重新回填 |
| 更新、重复/乱序回报、对账保存 | 首次时间保持；传入文档时间与已存列冲突时整体拒绝；无变化子行不重写；现有去重与终态规则不变 |
| 稳定 UUID | 六类记录首次创建生成合法 UUID；重复回报、重启恢复、更新和保存重试复用原值；缺失、重复或替换已有 UUID 的非法写入被拒绝 |
| 身份补齐 | 同一 UUID 原地 UPDATE，创建时间保留，不产生替换性的 DELETE/INSERT；相同编号跨日或跨市场不合并 |
| 业务唯一性 | 同订单重复 `item_id`、`attempt_id` 及同账户重复 `cancel_request_id` 不能用不同 UUID 绕过；NULL/空字符串拒绝；QMT 业务身份重复仍按原规则处理 |
| 撤单查询与重试 | 非 UUID 撤单编号可正常查询；同编号同内容重放原结果，不同内容冲突，跨订单冲突规则不变 |
| 集合追加、缺失和空集合 | 新增/更新正常；已有 UUID 消失或非空集合被清空时整体拒绝，不发出 DELETE；始终为空时无操作 |
| 局部保存 | 未选子表不写入，其对应文档集合也不能变化；其他账户和订单不变 |
| 事务失败和并发保存 | 无半成品父子状态，无事件序号或归属回归 |
| 字段投影 | 缺字段、NULL、前导零、金额精度及异常类型按约定处理，生成列不能与 JSONB 漂移 |
| 查询 | 完整/缺失身份、多候选、跨账户及跨日编号复用的结果与原规则一致；代表性数据上检查执行计划 |
| HTTP 合同 | 提交、详情、列表、事件、撤单均不泄露新增内部字段，原公开 ID/时间字段保持；转换不修改持久文档或 raw |
| 构建和回归 | 相关状态、生命周期、仓库、初始化及策略构建检查通过；没有 PostgreSQL 时明确报告集成测试跳过，不宣称数据库验收通过 |

需要在真实隔离 PostgreSQL 中验证 `created_at` 不变、生成列行为、约束/索引和事务回滚，不能仅靠 SQL 字符串断言或内存替身证明。

## 交付完成标准

代码、当前初始化 SQL、生成策略、离线迁移工具、文档与必要测试一致；六类子记录 UUID 与创建时间不会因正常更新或身份补齐改变，QMT 关键标识可直接查询，普通保存仅 INSERT/UPDATE、不执行 DELETE，原业务唯一性和 HTTP 合同保持。现有业务库在停写状态完成一次性迁移，再启动新版 Bridge；最终交付分别报告代码测试、各库迁移和实际启动验证结果。
