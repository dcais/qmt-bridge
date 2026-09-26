# -*- coding: utf-8 -*-
"""纯订单事实归并；所有调用副作用由持久库和运行时负责。"""
from decimal import Decimal, InvalidOperation
from .common import OrderError, copy_json, fingerprint, iso_datetime, parse_timestamp, utc_now


state_ORDER_STATUS = {48: 'WORKING', 49: 'WORKING', 50: 'WORKING',
                      51: 'WORKING', 52: 'PARTIALLY_FILLED', 53: 'PARTIALLY_CANCELLED',
                      54: 'CANCELLED', 55: 'PARTIALLY_FILLED', 56: 'FILLED', 57: 'REJECTED'}
state_TASK_STATUS = {0: 'UNKNOWN', 1: 'WAITING', 2: 'SUBMITTING', 3: 'RUNNING',
                     4: 'PAUSED', 5: 'CANCELLING', 6: 'CANCELLING', 7: 'COMPLETED',
                     8: 'CANCELLED', 9: 'REJECTED', 10: 'STOPPED', 11: 'DROPPED', 12: 'STOPPED'}
state_TERMINAL = {'FILLED', 'CANCELLED', 'PARTIALLY_CANCELLED', 'REJECTED'}
state_TASK_TERMINAL = {'COMPLETED', 'CANCELLED', 'REJECTED', 'STOPPED', 'DROPPED'}
state_CANCEL_ACTIVE = {'REQUESTED', 'WAITING_QMT_ID', 'PENDING', 'UNKNOWN'}


def state_value(raw, *keys):
    for key in keys:
        if raw.get(key) not in (None, ''):
            return raw[key]
    return None


def state_id(value):
    return str(value) if value not in (None, '', 0, '0', -1, '-1') else None


def state_number(raw, *keys):
    value = state_value(raw, *keys)
    try:
        return max(0, int(value or 0))
    except (ValueError, TypeError):
        return 0


def state_status(value, mapping):
    try:
        return mapping.get(int(value), 'UNKNOWN')
    except (TypeError, ValueError):
        return str(value).upper() if value else 'UNKNOWN'


def observation_identifiers(kind, raw):
    market = state_value(raw, 'market', 'm_strExchangeID')
    market = {'SSE': 'SH', 'SZSE': 'SZ', 'SHSE': 'SH'}.get(str(market).upper(), market)
    symbol = state_value(raw, 'symbol', 'm_strInstrumentID', 'm_stockCode')
    if symbol and '.' in str(symbol):
        market = market or str(symbol).rsplit('.', 1)[1]
    elif symbol and market:
        symbol = str(symbol) + '.' + str(market)
    side = state_value(raw, 'side', 'm_nOffsetFlag')
    side = {48: 'BUY', 49: 'SELL', 23: 'BUY', 24: 'SELL', '48': 'BUY', '49': 'SELL',
            '23': 'BUY', '24': 'SELL'}.get(side, str(side).upper() if side is not None else None)
    if side is None:
        # CTaskDetail 使用 EOperationType，其18/19与passorder的23/24不同。
        side = {18: 'BUY', 19: 'SELL', '18': 'BUY', '19': 'SELL'}.get(raw.get('m_eOperationType'))
    day = state_value(raw, 'trading_day', 'm_strTradingDay', 'm_strTradeDate', 'm_strInsertDate')
    return {'remark': state_value(raw, 'remark', 'm_strRemark', 'userOrderId'),
            'qmt_order_id': state_id(state_value(raw, 'qmt_order_id', 'm_strOrderSysID')),
            'qmt_task_id': state_id(state_value(raw, 'qmt_task_id', 'm_nTaskId', 'm_nTaskID')),
            'trade_id': state_id(state_value(raw, 'trade_id', 'm_strTradeID')),
            'trading_day': str(day) if day is not None else None,
            'market': market, 'symbol': symbol, 'side': side,
            'account_id': state_value(raw, 'account_id', 'm_strAccountID')}


def state_item(doc, identifiers):
    candidates = [item for item in doc['items'] if item['symbol'] == identifiers.get('symbol')
                  and item['side'] == identifiers.get('side')]
    return candidates[0] if len(candidates) == 1 else None


def state_evidence(doc, kind, raw, source, reason, observed_at):
    evidence = doc.setdefault('unassociated_evidence', [])
    key = fingerprint({'kind': kind, 'raw': raw, 'reason': reason})
    if not any(row['evidence_id'] == key for row in evidence):
        evidence.append({'evidence_id': key, 'kind': kind, 'raw': copy_json(raw),
                         'source': source, 'reason': reason, 'observed_at': observed_at})
    doc['reconciliation_complete'] = False
    doc['sync_status'] = 'INCOMPLETE'


def apply_observation(doc, kind, raw, source, observed_at=None):
    before = copy_json(doc)
    stamp = observed_at or iso_datetime()
    ids = observation_identifiers(kind, raw)
    if kind == 'error':
        # 错误回调不能证明先前不确定调用未进入交易系统。
        doc['error'] = {'raw': copy_json(raw), 'source': source, 'observed_at': stamp}
        if doc['submission_status'] in ('QUEUED', 'SUBMITTING') and not doc['qmt_orders'] and not doc['qmt_tasks']:
            doc['submission_status'] = 'REJECTED'
    elif kind == 'deal':
        key = [ids['trading_day'], ids['market'], ids['trade_id']]
        item = state_item(doc, ids)
        if not all(key) or item is None:
            state_evidence(doc, kind, raw, source, 'MISSING_FILL_IDENTITY_OR_ITEM', stamp)
        else:
            previous = next((row for row in doc['fills'] if row['fill_key'] == key), None)
            quantity = state_number(raw, 'quantity', 'filled_quantity', 'm_nVolume')
            amount = state_value(raw, 'amount', 'filled_amount', 'm_dTradeAmount')
            try:
                amount = Decimal(str(amount))
                valid_amount = amount.is_finite() and amount >= 0
            except (InvalidOperation, ValueError):
                valid_amount = False
            if not valid_amount or quantity <= 0:
                state_evidence(doc, kind, raw, source, 'MISSING_FILL_ECONOMICS', stamp)
            elif previous is None:
                record = dict(ids, fill_key=key, item_id=item['item_id'], quantity=quantity,
                              amount=str(Decimal(str(amount))), raw=copy_json(raw), source=source, observed_at=stamp)
                doc['fills'].append(record)
                doc['submission_status'] = 'CONFIRMED'
                doc['reconciliation_complete'] = False
            elif previous['quantity'] != quantity or Decimal(previous['amount']) != Decimal(str(amount)) or previous['item_id'] != item['item_id']:
                state_evidence(doc, kind, raw, source, 'CONFLICTING_FILL_IDENTITY', stamp)
    elif kind in ('order', 'task'):
        id_name = 'qmt_order_id' if kind == 'order' else 'qmt_task_id'
        collection = doc['qmt_orders'] if kind == 'order' else doc['qmt_tasks']
        if not ids[id_name]:
            state_evidence(doc, kind, raw, source, 'MISSING_QMT_ID', stamp)
        else:
            # 委托号可能跨交易日复用；缺日期的已关联记录仅在唯一时补齐。
            matching = [row for row in collection if row[id_name] == ids[id_name]
                        and (not ids['trading_day'] or not row.get('trading_day') or row['trading_day'] == ids['trading_day'])
                        and (not ids['market'] or not row.get('market') or row['market'] == ids['market'])]
            if len(matching) > 1:
                state_evidence(doc, kind, raw, source, 'AMBIGUOUS_QMT_ID', stamp)
            else:
                record = matching[0] if matching else dict(ids)
                if not matching:
                    collection.append(record)
                old_status = record.get('status', 'UNKNOWN')
                status = state_status(state_value(raw, 'status', 'execution_status', 'm_nOrderStatus') if kind == 'order'
                                      else state_value(raw, 'status', 'task_status', 'm_eStatus', 'm_nTaskStatus'),
                                      state_ORDER_STATUS if kind == 'order' else state_TASK_STATUS)
                terminal = state_TERMINAL if kind == 'order' else state_TASK_TERMINAL
                # 终态可由真实成交修正，旧的活动快照不能令已撤委托复活。
                if old_status in terminal and status not in terminal:
                    status = old_status
                if old_status == 'FILLED':
                    status = 'FILLED'
                previous_record = copy_json(record)
                record.update({key: value for key, value in ids.items() if value is not None})
                record.update(status=status, terminal=status in terminal)
                if kind == 'order':
                    record['quantity'] = max(record.get('quantity', 0), state_number(raw, 'quantity', 'm_nVolumeTotalOriginal'))
                    record['filled_quantity'] = max(record.get('filled_quantity', 0), state_number(raw, 'filled_quantity', 'm_nVolumeTraded'))
                    item = state_item(doc, record)
                    record['item_id'] = item['item_id'] if item else None
                    if not item:
                        state_evidence(doc, kind, raw, source, 'MISSING_ITEM_MAPPING', stamp)
                    if str(raw.get('m_nOrderSubmitStatus')) == '53' and not record['terminal']:
                        for attempt in doc['attempts']:
                            if (attempt.get('kind') == 'CANCEL_ORDER'
                                    and str(attempt.get('target_id')) == ids['qmt_order_id']
                                    and attempt.get('cancel_request_id') == doc.get('active_cancel_request_id')):
                                attempt['status'] = 'REJECTED'
                                attempt['rejection_evidence'] = copy_json(raw)
                semantic_changed = previous_record != record
                if semantic_changed:
                    record.update(raw=copy_json(raw), source=source, observed_at=stamp)
                    doc['reconciliation_complete'] = False
                doc['submission_status'] = 'CONFIRMED'
    else:
        raise ValueError('unsupported observation kind: ' + str(kind))
    recompute_order(doc)
    return doc != before


def state_algorithm(doc):
    request = doc.get('request', {})
    return bool(doc.get('qmt_tasks') or request.get('execution', {}).get('type') in ('SLICED', 'SMART'))


def state_execution_finished(doc):
    # 未达目标可以是完整的结束事实；同步缺口则绝不能据此清理。
    if (doc.get('submission_status') != 'CONFIRMED' or not doc.get('reconciliation_complete')
            or doc.get('sync_status') != 'COMPLETE' or doc.get('unassociated_evidence')):
        return False
    tasks, orders = doc['qmt_tasks'], doc['qmt_orders']
    if not (tasks or orders) or (state_algorithm(doc) and not tasks):
        return False
    if any(not row.get('terminal') for row in tasks + orders):
        return False
    return doc['execution_status'] in state_TERMINAL or doc['execution_status'] == 'INCOMPLETE'


def recompute_order(doc, now=None, reconciled=False):
    before = copy_json(doc)
    if reconciled:
        doc['reconciliation_complete'] = True
    if doc['submission_status'] == 'QUEUED' and doc.get('submit_before'):
        current = parse_timestamp(now) if isinstance(now, str) else (now or utc_now())
        if current >= parse_timestamp(doc['submit_before']):
            doc['submission_status'] = 'EXPIRED'
    identity_complete = all(row.get('trading_day') and row.get('market') and row.get('item_id')
                            for row in doc['qmt_orders'])
    complete = bool(doc.get('reconciliation_complete')) and not doc.get('unassociated_evidence') and identity_complete
    tasks = doc['qmt_tasks']
    tasks_stopped = bool(tasks) and all(row.get('terminal') for row in tasks)
    algorithm = state_algorithm(doc)
    producer_stopped = tasks_stopped if algorithm else True
    local = doc['submission_status'] in ('CANCELLED_LOCAL', 'EXPIRED', 'REJECTED') and not doc['qmt_orders'] and not doc['fills']
    fill_gap = False
    for item in doc['items']:
        orders = [row for row in doc['qmt_orders'] if row.get('item_id') == item['item_id']]
        fills = [row for row in doc['fills'] if row.get('item_id') == item['item_id']]
        item['filled_quantity'] = sum(row['quantity'] for row in fills)
        item['filled_amount'] = str(sum((Decimal(row['amount']) for row in fills), Decimal(0)))
        open_qty, cancelled_qty = 0, 0
        all_orders_filled = bool(orders)
        for row in orders:
            actual = sum(fill['quantity'] for fill in fills if fill.get('qmt_order_id') == row['qmt_order_id']
                         and (not row.get('trading_day') or fill['trading_day'] == row['trading_day'])
                         and (not row.get('market') or fill['market'] == row['market']))
            traded = max(row.get('filled_quantity', 0), actual)
            fill_gap = (fill_gap or traded > actual
                        or (row.get('status') == 'FILLED' and (not row.get('quantity') or actual < row['quantity'])))
            if row.get('quantity') and actual >= row['quantity']:
                row.update(status='FILLED', terminal=True)
            all_orders_filled = (all_orders_filled and row.get('status') == 'FILLED'
                                 and row.get('quantity', 0) > 0 and actual >= row['quantity'])
            remaining = max(0, row.get('quantity', 0) - traded)
            if row['status'] in ('CANCELLED', 'PARTIALLY_CANCELLED'):
                cancelled_qty += remaining
            elif not row.get('terminal'):
                open_qty += remaining
        item['open_quantity'] = open_qty
        item['cancelled_quantity'] = cancelled_qty
        qty = item.get('requested_quantity')
        filled = item['filled_quantity']
        terminal_orders = bool(orders) and all(row.get('terminal') for row in orders)
        # 达到目标不意味着算法停止，也不意味着超量/迟到子委托已无风险。
        if qty is not None and filled >= qty and producer_stopped and not any(not row.get('terminal') for row in orders):
            status = 'FILLED'
        elif (qty is None and item.get('requested_amount') is not None and not algorithm
              and doc.get('request', {}).get('execution', {}).get('type') == 'DIRECT'
              and complete and all_orders_filled):
            # 金额单的整手余款不要求成交额等于预算；须由每笔委托及稳定成交共同证明已成。
            status = 'FILLED'
        elif local:
            status = 'REJECTED' if doc['submission_status'] == 'REJECTED' else 'NOT_STARTED'
        elif terminal_orders and producer_stopped and complete:
            if cancelled_qty:
                status = 'PARTIALLY_CANCELLED' if filled else 'CANCELLED'
            elif all(row['status'] == 'REJECTED' for row in orders):
                status = 'REJECTED'
            else:
                status = 'INCOMPLETE'
        elif open_qty:
            status = 'PARTIALLY_FILLED' if filled else 'WORKING'
        elif terminal_orders or tasks_stopped:
            status = 'INCOMPLETE'
        elif filled:
            status = 'PARTIALLY_FILLED'
        elif orders or tasks:
            status = 'WORKING' if any(row.get('status') != 'UNKNOWN' for row in orders + tasks) else 'UNKNOWN'
        else:
            status = 'UNKNOWN' if doc['submission_status'] in ('UNKNOWN', 'CONFIRMED') else 'NOT_STARTED'
        item['execution_status'] = status
    if fill_gap:
        complete = False
    statuses = [item['execution_status'] for item in doc['items']]
    if all(status == 'FILLED' for status in statuses):
        execution = 'FILLED'
    elif len(statuses) == 1:
        execution = statuses[0]
    elif all(status in state_TERMINAL for status in statuses):
        execution = 'PARTIALLY_CANCELLED' if any(item['filled_quantity'] for item in doc['items']) else ('REJECTED' if all(s == 'REJECTED' for s in statuses) else 'CANCELLED')
    elif 'WORKING' in statuses or 'PARTIALLY_FILLED' in statuses:
        execution = 'PARTIALLY_FILLED' if any(item['filled_quantity'] for item in doc['items']) else 'WORKING'
    elif 'INCOMPLETE' in statuses:
        execution = 'INCOMPLETE'
    else:
        execution = 'UNKNOWN' if 'UNKNOWN' in statuses else 'NOT_STARTED'
    if fill_gap or doc.get('unassociated_evidence'):
        execution = 'INCOMPLETE'
    doc['execution_status'] = execution
    if doc['order_type'] == 'SINGLE':
        for field in ('filled_quantity', 'filled_amount', 'open_quantity', 'cancelled_quantity'):
            doc[field] = doc['items'][0][field]
    if doc.get('cancel_requested'):
        attempts = [row for row in doc['attempts'] if row.get('kind') in ('CANCEL_ORDER', 'CANCEL_TASK')
                    and row.get('cancel_request_id') == doc.get('active_cancel_request_id')]
        unresolved_attempts = [attempt for attempt in attempts if any(
            not row.get('terminal') and str(row.get('qmt_order_id' if attempt['kind'] == 'CANCEL_ORDER' else 'qmt_task_id')) == str(attempt.get('target_id'))
            for row in (doc['qmt_orders'] if attempt['kind'] == 'CANCEL_ORDER' else tasks))]
        live = any(not row.get('terminal') for row in doc['qmt_orders'])
        if doc['submission_status'] == 'CANCELLED_LOCAL':
            cancel = 'CONFIRMED'
        elif local:
            cancel = 'NOT_NEEDED'
        elif execution == 'FILLED' and producer_stopped and not live and complete:
            cancel = 'NOT_NEEDED'
        elif producer_stopped and not live and complete and (doc['qmt_orders'] or tasks):
            cancel = 'CONFIRMED'
        elif any(row.get('status') == 'UNKNOWN' for row in unresolved_attempts):
            cancel = 'UNKNOWN'
        elif unresolved_attempts and all(row.get('status') == 'REJECTED' for row in unresolved_attempts):
            cancel = 'REJECTED'
        elif not doc['qmt_orders'] and not tasks:
            cancel = 'WAITING_QMT_ID'
        else:
            cancel = 'PENDING' if attempts else 'REQUESTED'
        doc['cancel_status'] = cancel
        for request in doc['cancel_requests']:
            if request['canonical_cancel_request_id'] == doc.get('active_cancel_request_id'):
                request['status'] = cancel
    doc['sync_status'] = 'COMPLETE' if complete else ('PENDING' if doc['submission_status'] == 'QUEUED' and not doc.get('unassociated_evidence') else 'INCOMPLETE')
    doc['basket_cleanup_eligible'] = doc['order_type'] != 'SINGLE' and state_execution_finished(doc)
    return before != doc


def apply_cancel_request(doc, request):
    cancel_id = request['cancel_request_id']
    digest = fingerprint(request)
    previous = next((row for row in doc['cancel_requests'] if row['cancel_request_id'] == cancel_id), None)
    if previous:
        if previous['request_hash'] != digest:
            raise OrderError(409, 'CANCEL_ID_CONFLICT', 'cancel_request_id already has a different request')
        return copy_json(previous), 200
    recompute_order(doc)
    canonical = doc.get('active_cancel_request_id') if doc['cancel_status'] in state_CANCEL_ACTIVE else cancel_id
    record = {'cancel_request_id': cancel_id, 'canonical_cancel_request_id': canonical,
              'request_hash': digest, 'request': copy_json(request), 'created_at': iso_datetime(), 'status': 'REQUESTED'}
    doc['cancel_requests'].append(record)
    doc.update(cancel_requested=True, active_cancel_request_id=canonical)
    if doc['submission_status'] == 'QUEUED':
        doc['submission_status'] = 'CANCELLED_LOCAL'
    recompute_order(doc)
    return copy_json(record), 200 if record['status'] in ('CONFIRMED', 'NOT_NEEDED') else 202


def cancel_response(doc, cancel_request_id, replayed=False):
    record = next((row for row in doc['cancel_requests'] if row['cancel_request_id'] == cancel_request_id), None)
    if record is None:
        raise OrderError(404, 'CANCEL_NOT_FOUND', 'cancel request does not exist')
    result = copy_json(record)
    result.pop('request_hash', None)
    result.update(order_id=doc['order_id'], client_order_id=doc['client_order_id'],
                  account_id=doc['account_id'], submission_status=doc['submission_status'],
                  execution_status=doc['execution_status'], cancel_status=record['status'], replayed=replayed)
    return result


def pending_cancellations(doc):
    if not doc.get('cancel_requested') or doc['submission_status'] == 'CANCELLED_LOCAL':
        return []
    request_id = doc.get('active_cancel_request_id')
    tasks = doc['qmt_tasks']
    if state_algorithm(doc) and not tasks:
        return []
    live_tasks = [row for row in tasks if not row.get('terminal')]
    targets = [('CANCEL_TASK', row['qmt_task_id'], row) for row in live_tasks]
    if not live_tasks:
        targets = [('CANCEL_ORDER', row['qmt_order_id'], row) for row in doc['qmt_orders'] if not row.get('terminal')]
    actions = []
    for kind, target, row in targets:
        attempts = [attempt for attempt in doc['attempts'] if attempt.get('kind') == kind and str(attempt.get('target_id')) == str(target)]
        # 返回成功只是调用确认；等待真实回报，绝不对UNKNOWN盲重试。
        if any(attempt.get('status') in ('CALLING', 'RETURNED', 'UNKNOWN', 'CONFIRMED') for attempt in attempts):
            continue
        # 停止门闩证明未进入QMT的调用可以续派；明确失败仍要求新撤单ID。
        if any(attempt.get('cancel_request_id') == request_id and attempt.get('status') != 'ABORTED_NO_CALL'
               for attempt in attempts):
            continue
        actions.append({'kind': kind, 'target_id': target, 'cancel_request_id': request_id,
                        'market': row.get('market'), 'trading_day': row.get('trading_day')})
    return actions


def is_order_active(doc):
    if doc['submission_status'] == 'QUEUED':
        return True
    if doc['submission_status'] in ('CANCELLED_LOCAL', 'EXPIRED', 'REJECTED') and not doc['qmt_orders'] and not doc['qmt_tasks']:
        return False
    return not state_execution_finished(doc)


def mark_reconciled(doc, complete=True, now=None):
    before = copy_json(doc)
    doc['reconciliation_complete'] = bool(complete)
    doc['last_reconciled_at'] = now if isinstance(now, str) else iso_datetime(now)
    doc['reconcile_requested'] = False
    recompute_order(doc, now=now)
    return before != doc
