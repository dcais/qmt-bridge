# qmt-bridge

IPC 服务的统一入口文档：[IPC_BRIDGE.md](IPC_BRIDGE.md)（部署、接口索引、curl、排队与日志）。

本仓库同时保留 HTTP 直连与 IPC 桥接两种部署方式。

原 HTTP 方案使用 `strategies/http_feed.py`、`strategies/http_order.py`，
部署说明见 [HTTP_FEED.md](HTTP_FEED.md) 和 [HTTP_ORDER.md](HTTP_ORDER.md)。
ORDER 源码仍在 `order_bridge`，构建器为 `tools/build_order_strategy.py`，
依赖安装工具为 `tools/install_order_dependencies.py`，依赖清单为 `requirements-order.txt`。

以下介绍新增 IPC 方案：通过 Windows named pipe 将 QMT 原生调用与外部 HTTP、PostgreSQL 和文件日志分开。
统一服务地址为 `http://127.0.0.1:8888/{method}`，保留现有根路径 API。
行情方法走 Feed IPC；账户、持仓、算法配置、健康和持久订单方法走 ORDER。

## IPC 传输目录

```text
service/ipc_bridge/transport/
├── pipe/    # 当前命名管道：消息帧、Windows 管道、bridge 连接管理
└── smem/    # 预留共享内存实现，尚未实现或启用
```

当前服务和 QMT 策略均使用 `pipe`。`smem` 仅预留目录，没有切换配置。
调整 QMT 侧传输源码后，使用 `tools/build_ipc_strategies.py` 重新生成策略。

## 外部服务与 QMT 策略

外部服务需要 Python 3.10 或更新版本，本次使用 Python 3.14.6 验证。
从仓库根目录安装外部依赖、创建配置并生成策略：

```powershell
python -m pip install -r requirements-service.txt
Copy-Item .env.example .env
python tools/build_ipc_strategies.py
python tools/build_ipc_strategies.py --check
python -m service.ipc_bridge
```

已有 `.env` 时直接编辑。服务从当前目录读取 `.env`，进程环境变量覆盖文件值。
使用单 worker 启动，避免同一桥名下重复创建 IPC 管道和数据库执行器。

分环境启动时，环境文件位于 `service/ipc_bridge/`。分别复制该目录的
`.env.staging.example` 为 `.env.staging`、`.env.prd.example` 为 `.env.prd`，填写各自账户和数据库配置，然后选择文件：

```powershell
python -m service.ipc_bridge --env-file service/ipc_bridge/.env.staging
python -m service.ipc_bridge --env-file service/ipc_bridge/.env.prd
```

Windows 可直接使用根目录启动脚本（PowerShell）：

```powershell
.\start_ipc_bridge.cmd staging
.\start_ipc_bridge.cmd prd
```

CMD 中可使用 `start_ipc_bridge staging` / `start_ipc_bridge prd`。
脚本固定工作目录为项目根目录，使用 `.venv\Scripts\python.exe`，
自动选择 `service/ipc_bridge/.env.<环境>`，前台运行，按 Ctrl+C 停止。
只接受 `staging` 或 `prd`；缺少虚拟环境或环境文件时直接报错。


显式指定的文件不存在时启动报错；只读取选中的文件，不叠加默认 `.env`。
进程环境变量仍优先于文件。未指定 `--env-file` 时保持读取 `.env`。
模板分别使用桥名 `staging` / `prd`、端口 `8888` / `8889` 和独立日志目录，
账户留空，必须填写。对应 QMT 策略的桥名和账户须与该环境一致。
真实 `.env.*` 文件已忽略，不提交凭据；仅提交 `.example` 模板。


在 QMT 中导入并运行生成的 GBK 单文件：

- [strategies/ipc_feed.py](strategies/ipc_feed.py)：行情查询与下载。
- [strategies/ipc_order.py](strategies/ipc_order.py)：账户查询、交易调用及回报。

QMT 策略兼容 Python 3.6，源模块在 `service/ipc_bridge`。
构建器将安全模块内嵌到策略中；运行时不读取应用源文件、不监听 HTTP、不连接数据库、
不写日志文件。QMT 只做非阻塞 IPC、定时调度、原生调用和回报快照，诊断打印 console。
外部 Python 环境负责持久受理、幂等、执行权、恢复、对账、HTTP 和 UTF-8 文件日志。

Feed 和 ORDER 的 QMT 参数 `bridge_id` 与外部 `QMT_BRIDGE_ID` 一致，`account_id`
与 `QMT_BRIDGE_ACCOUNT_ID` 一致。默认桥名 `default`、账户 `66027616`。
`schedule_interval_ms` 默认 10，`schedule_budget_ms` 默认 50；参数变化需停止并重启策略。
两个角色默认管道名分别为 `\\.\pipe\qmt-bridge.default.feed` 和
`\\.\pipe\qmt-bridge.default.order`。

外部配置使用以下环境变量：

| 变量 | 默认值 | 用途 |
| --- | --- | --- |
| `QMT_BRIDGE_HOST` | `127.0.0.1` | HTTP 绑定地址 |
| `QMT_BRIDGE_PORT` | `8888` | Feed 与 ORDER 共用 HTTP 端口 |
| `QMT_BRIDGE_ID` | `default` | 管道桥名，双方一致 |
| `QMT_BRIDGE_ACCOUNT_ID` | `66027616` | ORDER 绑定账户，保留字符串前导零 |
| `QMT_BRIDGE_REQUEST_TIMEOUT` | `10` | 普通 IPC 请求等待秒数 |
| `QMT_BRIDGE_DOWNLOAD_TIMEOUT` | `120` | 下载后台 IPC 等待秒数，不自动重试 |
| `QMT_BRIDGE_SUBMIT_BATCH_SIZE` | `10` | 每轮下单业务数量上限 |
| `QMT_BRIDGE_CANCEL_BATCH_SIZE` | `10` | 每轮撤单动作上限 |
| `QMT_BRIDGE_RECONCILE_BATCH_SIZE` | `100` | 对账批量上限 |
| `QMT_BRIDGE_SCHEDULE_BUDGET_MS` | `50` | 外部订单派发预算 |
| `QMT_BRIDGE_RECONCILE_INTERVAL_SECONDS` | `30` | 每轮及逐单对账间隔 |
| `QMT_BRIDGE_LOG_LEVEL` | `INFO` | 外部日志级别 |
| `QMT_BRIDGE_LOG_DIRECTORY` | `var/log/ipc_bridge` | 外部日志目录 |
| `QMT_BRIDGE_PG_HOST` | `127.0.0.1` | 外部 PostgreSQL 主机 |
| `QMT_BRIDGE_PG_PORT` | `5432` | 外部 PostgreSQL 端口 |
| `QMT_BRIDGE_PG_DATABASE` | 空 | PostgreSQL 数据库 |
| `QMT_BRIDGE_PG_USER` | 空 | PostgreSQL 用户 |
| `QMT_BRIDGE_PG_PASSWORD` | 空 | PostgreSQL 口令 |

PG 数据库、用户、口令须同时设置；全部为空时只提供查询，写入返回
`TRADING_NOT_CONFIGURED`。固定 schema 为 `qmt_order`，外部启动只检查结构，
不自动建表或迁移。初始化、管理与订单语义见 [IPC_ORDER.md](IPC_ORDER.md)。
口令仅保存在外部环境中，`.env` 不提交到仓库。

```powershell
Invoke-RestMethod 'http://127.0.0.1:8888/health'
Invoke-RestMethod 'http://127.0.0.1:8888/account'
```

`/health` 返回 ORDER 状态、两个 IPC 角色、外部实例和日志计数。
HTTP 200 只表示读到了快照；交易可用需检查 `accepting_orders`、连接、执行权和对账状态。
派发后结果不明的订单保留 `UNKNOWN`，不能自动重发；下载 `completed` 只表示原生函数返回，
不证明数据覆盖完整。真实 QMT 和账户验收需单独完成。

IPC 接口合同见 [IPC_FEED.md](IPC_FEED.md) 和 [IPC_ORDER.md](IPC_ORDER.md)。
`tests/fixtures/legacy_http_*.py` 保留为 IPC 兼容性测试基线，不替代可部署的 HTTP 策略。
同一账户的交易执行器应选择一种方案运行，切换前先停止原执行器。
检查当前实现与策略产物：

```powershell
python -m unittest discover -s tests -p "test_ipc_*.py" -v
python tools/build_ipc_strategies.py --check
```

## 展示账户信息策略

策略文件：[strategies/show_account.py](strategies/show_account.py)

策略固定查询股票账户 `66027616`：

```python
get_trade_detail_data("66027616", "STOCK", "account")
```

策略通过新版 QMT `schedule_run` 注册每秒定时任务，将账号对象的全部 `m_` 字段转换成 JSON 打印。
策略结束进入 `stop` 回调时，会通过任务 ID 调用 `cancel_schedule_run` 关闭定时任务。
