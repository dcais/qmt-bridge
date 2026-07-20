# -*- coding: gbk -*-

import json
import datetime as dt


ACCOUNT_ID = "66027616"
ACCOUNT_TYPE = "STOCK"


def to_dict(obj):
    data = {}
    for name in dir(obj):
        if name.startswith("m_"):
            try:
                data[name] = getattr(obj, name)
            except Exception:
                data[name] = None
    return data


def print_account(ContextInfo):
    accounts = get_trade_detail_data(ACCOUNT_ID, ACCOUNT_TYPE, "account")
    if not accounts:
        print(
            json.dumps(
                {"accountId": ACCOUNT_ID, "error": "未查询到账户信息，请检查账户是否已登录"},
                ensure_ascii=False,
            )
        )
        return

    print(json.dumps(to_dict(accounts[0]), ensure_ascii=False, default=str))


def init(ContextInfo):
    ContextInfo.set_account(ACCOUNT_ID)
    ContextInfo.account_timer_id = ContextInfo.schedule_run(
        print_account,
        "20200101000000",
        -1,
        dt.timedelta(seconds=1),
        "account_timer",
    )


def handlebar(ContextInfo):
    pass


def stop(ContextInfo):
    timer_id = getattr(ContextInfo, "account_timer_id", None)
    if timer_id is not None:
        ContextInfo.cancel_schedule_run(timer_id)
