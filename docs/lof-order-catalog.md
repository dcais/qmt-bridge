# LOF 下单目录

LOF 使用独立证券类型。IPC 与保留的 HTTP ORDER 通过相同的官方目录快照识别沪深 LOF，
不把全部 `16xxxx.SZ`、`501xxx.SH` 或基金名称中含 LOF 的证券自动视为可下单标的。
目录内代码支持数量单；数量单位为份，原值传递，不按手乘 100。LOF 与 ETF 均拒绝金额单，
继续遵循现有“金额单仅股票”边界。

`QUANTITY + DIRECT + LIMIT` 映射到原生 `passorder`：买/卖 `opType=23/24`、
`orderType=1101`、`prType=11`，价格与数量保持请求值。此修复不改变算法执行模式或账户权限。

## 来源与维护

`docs/data/lof_catalog.json` 保存逐代码的交易所分类、上市日期、来源 URL，及原始响应 SHA-256、
获取时间、深市目录日期；独立数据模块由更新工具生成。

- 沪市：交易所 `COMMON_JJZWZ_JJLB_L` 基金目录，按
  `COMMON_JJZWZ_JJLB_JJLX_C` 分类树的 `F200`（上证 LOF）及其子类筛选。
- 深市：交易所 `1000_lf` 全基金目录，分页数量/日期一致性核验后，仅接受 `jjlb=LOF`。
- 目录 presence 和历史上市日期均不证明当前上市、停牌、交易权限或可成交；交易所目录可能保留
  历史已终止证券。现有股票/ETF 前缀校验规则保持不变，订单仍受券商/QMT 门禁约束。
- 首个快照于 2026-10-27 获取：沪市 119、深市 285，共 404 项；未知代码拒绝。

新增 LOF 需先刷新并审阅目录差异；每笔订单校验不联网、不读取数据库：

```powershell
python tools/update_lof_catalog.py --refresh
python tools/update_lof_catalog.py --check
python -m pytest -p no:cacheprovider tests/test_order_lof.py tests/test_order_contracts.py tests/test_order_qmt.py
python tools/build_order_strategy.py
python tools/build_order_strategy.py --check
```

刷新入口仅访问公开 SSE/SZSE 目录并更新快照及两份生成数据模块，不修改证券基础数据库。
无参数时从已审阅的保存快照离线重建；`--check` 离线比较快照与模块。
提交时同时保存快照、两个模块；HTTP 直连方式还需重新生成 `strategies/http_order.py`。
`tests/fixtures/legacy_http_order.py` 是冻结兼容性样本，不重新生成。

IPC 的 `contracts.py` 和 LOF 目录仅在外部 Python bridge 运行，不嵌入
`strategies/ipc_order.py`。部署本修复只需在主任务协调下有序停止并重启外部 bridge，
核对相同环境、PID、监听端口、health 与 ORDER 连接；不需重启或替换 QMT IPC 策略。
变更运行中源码后必须完成重启，不能用磁盘新代码冒充进程已加载代码。
