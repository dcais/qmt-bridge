# qmt-bridge

## 展示账户信息策略

策略文件：[strategies/show_account.py](strategies/show_account.py)

策略固定查询股票账户 `66027616`：

```python
get_trade_detail_data("66027616", "STOCK", "account")
```

策略通过新版 QMT `schedule_run` 注册每秒定时任务，将账号对象的全部 `m_` 字段转换成 JSON 打印。
策略结束进入 `stop` 回调时，会通过任务 ID 调用 `cancel_schedule_run` 关闭定时任务。
