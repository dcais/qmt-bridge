# -*- coding: utf-8 -*-
"""PostgreSQL 订单事实库；事务头锁同时保护投影和连续事件游标。"""
import hashlib
import json
import re
import threading
import uuid

from .common import (OrderError, copy_json, fingerprint, iso_datetime,
                     json_text, new_order_document, parse_timestamp, utc_now)
from .state import (apply_cancel_request, apply_observation, cancel_response,
                    is_order_active, observation_identifiers, recompute_order)


repo_SCHEMA_VERSION = 1
repo_CHILD_TABLES = {"order_items": "items", "execution_attempts": "attempts",
                     "cancel_requests": "cancel_requests", "qmt_tasks": "qmt_tasks",
                     "qmt_orders": "qmt_orders", "fills": "fills"}


def repo_schema(value):
    if not isinstance(value, str) or not re.match(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$", value):
        raise OrderError(503, "INVALID_PERSISTENCE_CONFIG", "invalid PostgreSQL schema identifier")
    return '"' + value + '"'


def repo_json(value):
    return json.loads(value) if isinstance(value, str) else copy_json(value)


def repo_error(code="PERSISTENCE_UNAVAILABLE"):
    # 驱动异常可能包含连接串和口令，不向调用方透出原始错误。
    return OrderError(503, code, "PostgreSQL persistence unavailable" if code != "PERSISTENCE_OUTCOME_UNKNOWN"
                      else "transaction commit outcome unknown; reconcile using original request identifier")


class PostgresRepository(object):
    def __init__(self, config, account_id, account_type="STOCK", connect_factory=None):
        self.config = dict(config)
        self.account_id = str(account_id)
        self.account_type = str(account_type)
        self.repo_scope = (self.account_type, self.account_id)
        self.repo_s = repo_schema(config.get("pg_schema", "qmt_order"))
        self.repo_connect_factory = connect_factory
        self.repo_executor = None
        self.repo_executor_epoch = None
        self.repo_executor_instance = None
        self.repo_executor_lost = False
        self.repo_executor_mutex = threading.RLock()
        # PostgreSQL advisory lock 本身已隔离数据库，不把可别名的连接参数混入锁键。
        lock_scope = [config.get("pg_schema", "qmt_order"),
                      self.account_type, self.account_id]
        self.repo_lock_key = int.from_bytes(hashlib.sha256(json_text(lock_scope).encode("utf-8")).digest()[:8],
                                           byteorder="big", signed=True)

    def repo_connect(self):
        try:
            factory = self.repo_connect_factory
            if factory is None:
                from pg8000 import dbapi
                factory = dbapi.connect
            conn = factory(host=self.config.get("pg_host", "127.0.0.1"),
                           port=int(self.config.get("pg_port", 5432)),
                           database=self.config.get("pg_database", "postgres"),
                           user=self.config.get("pg_user", "postgres"),
                           password=self.config.get("pg_password", ""),
                           timeout=float(self.config.get("pg_connect_timeout", 3)))
            cur = conn.cursor()
            cur.execute("SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)",
                        (str(int(self.config.get("pg_statement_timeout_ms", 2000))),
                         str(int(self.config.get("pg_lock_timeout_ms", 1000)))))
            conn.commit()
            cur.close()
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            raise repo_error() from None

    def repo_run(self, callback, mutation=False):
        conn = self.repo_connect()
        try:
            cur = conn.cursor()
            if mutation:
                cur.execute("SELECT event_seq FROM " + self.repo_s +
                            ".account_runtime WHERE account_type=%s AND account_id=%s FOR UPDATE", self.repo_scope)
                if cur.fetchone() is None:
                    raise OrderError(503, "SCHEMA_NOT_READY", "account runtime record is missing; restart the bridge")
            result = callback(cur)
            try:
                conn.commit()
            except Exception:
                raise repo_error("PERSISTENCE_OUTCOME_UNKNOWN") from None
            return result
        except OrderError:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise repo_error() from None
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def check_schema(self):
        """只检查既有表结构和版本；账户记录由策略启动时自动补齐。"""
        def check(cur):
            required = {
                "schema_version": ("version",),
                "account_runtime": ("account_type", "account_id", "event_seq", "executor_host", "executor_instance", "executor_epoch"),
                "orders": ("account_type", "account_id", "order_id", "client_order_id", "request_hash", "remark", "active", "document"),
                "order_events": ("account_type", "account_id", "event_seq", "order_id", "event_type", "occurred_at", "document"),
                "qmt_observations": ("observation_id", "account_type", "account_id", "kind", "source", "observed_at", "raw", "order_id", "applied", "observation_hash"),
            }
            required.update({table: ("account_type", "account_id", "order_id", "record_id", "document")
                             for table in repo_CHILD_TABLES})
            cur.execute("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema=%s",
                        (self.config.get("pg_schema", "qmt_order"),))
            existing = {}
            for table, column in cur.fetchall():
                existing.setdefault(table, set()).add(column)
            for table, columns in required.items():
                if table not in existing:
                    raise OrderError(503, "SCHEMA_NOT_READY", "missing required table: " + table)
                missing = sorted(set(columns) - existing[table])
                if missing:
                    raise OrderError(503, "SCHEMA_NOT_READY", "missing required columns in " + table + ": " + ", ".join(missing))
            cur.execute("SELECT version FROM " + self.repo_s + ".schema_version")
            if [row[0] for row in cur.fetchall()] != [repo_SCHEMA_VERSION]:
                raise OrderError(503, "SCHEMA_VERSION_MISMATCH", "unsupported order schema version")
            return {"schema_version": repo_SCHEMA_VERSION, "ready": True}
        return self.repo_run(check)

    def ensure_account_runtime(self):
        """按启动账户幂等插入运行记录；冲突时保留事件序号、主机和执行代次。"""
        def ensure(cur):
            cur.execute("INSERT INTO " + self.repo_s +
                        ".account_runtime(account_type,account_id) VALUES(%s,%s) "
                        "ON CONFLICT(account_type,account_id) DO NOTHING RETURNING account_id", self.repo_scope)
            return cur.fetchone() is not None
        # 首次运行尚无可锁的账户行，依靠主键和 ON CONFLICT 处理并发创建。
        return self.repo_run(ensure)

    def health(self):
        result = self.check_schema()
        result["executor"] = self.check_executor() if self.repo_executor is not None else False
        def unknown_count(cur):
            cur.execute("SELECT count(*) FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "AND document->>'submission_status'='UNKNOWN'", self.repo_scope)
            return cur.fetchone()[0]
        result["unknown_order_count"] = self.repo_run(unknown_count)
        return result

    def acquire_executor(self, instance_id, host_id):
        with self.repo_executor_mutex:
            if self.repo_executor_lost:
                raise OrderError(503, "EXECUTOR_LOCK_LOST", "executor lock was lost; restart required")
            if self.repo_executor is not None:
                self.check_executor()
                return {"epoch": self.repo_executor_epoch, "instance_id": self.repo_executor_instance}
            conn = self.repo_connect()
            try:
                cur = conn.cursor()
                cur.execute("SELECT pg_try_advisory_lock(%s)", (self.repo_lock_key,))
                if not cur.fetchone()[0]:
                    raise OrderError(503, "EXECUTOR_ALREADY_RUNNING", "another executor holds this account")
                cur.execute("SELECT executor_host,executor_epoch FROM " + self.repo_s +
                            ".account_runtime WHERE account_type=%s AND account_id=%s FOR UPDATE", self.repo_scope)
                row = cur.fetchone()
                if row is None:
                    raise OrderError(503, "SCHEMA_NOT_READY", "account runtime has not been initialized")
                if row[0] and row[0] != host_id:
                    raise OrderError(503, "EXECUTOR_HOST_MISMATCH", "account is bound to another QMT host")
                epoch = row[1] + 1
                cur.execute("UPDATE " + self.repo_s + ".account_runtime SET executor_host=%s,executor_instance=%s,"
                            "executor_epoch=%s WHERE account_type=%s AND account_id=%s",
                            (host_id, instance_id, epoch) + self.repo_scope)
                try:
                    conn.commit()
                except Exception:
                    raise repo_error("PERSISTENCE_OUTCOME_UNKNOWN") from None
                self.repo_executor = conn
                self.repo_executor_epoch = epoch
                self.repo_executor_instance = instance_id
                return {"epoch": epoch, "instance_id": instance_id, "host_id": host_id}
            except Exception as exc:
                try:
                    conn.close()
                except Exception:
                    pass
                if isinstance(exc, OrderError):
                    raise
                raise repo_error() from None

    def check_executor(self):
        with self.repo_executor_mutex:
            if self.repo_executor is None or self.repo_executor_lost:
                raise OrderError(503, "EXECUTOR_LOCK_LOST", "executor session is not available")
            try:
                cur = self.repo_executor.cursor()
                # 不再次获取 advisory lock，避免重入计数掩盖意外 unlock。
                key = self.repo_lock_key & ((1 << 64) - 1)
                cur.execute("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' "
                            "AND pid=pg_backend_pid() AND classid=%s::oid AND objid=%s::oid "
                            "AND objsubid=1 AND granted)", (key >> 32, key & 0xffffffff))
                held = cur.fetchone()[0]
                self.repo_executor.commit()
                cur.close()
                if not held:
                    raise RuntimeError("lock absent")
                return True
            except Exception:
                self.repo_executor_lost = True
                raise OrderError(503, "EXECUTOR_LOCK_LOST", "executor lock lost; automatic takeover disabled") from None

    def release_executor(self):
        with self.repo_executor_mutex:
            conn, self.repo_executor = self.repo_executor, None
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def close(self):
        self.release_executor()

    def repo_load(self, cur, value, by_client=False, lock=False):
        column = "client_order_id" if by_client else "order_id"
        cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s AND " +
                    column + "=%s" + (" FOR UPDATE" if lock else ""), self.repo_scope + (value,))
        row = cur.fetchone()
        return repo_json(row[0]) if row else None

    def repo_require(self, doc):
        if doc is None:
            raise OrderError(404, "ORDER_NOT_FOUND", "order not found")
        return doc

    def repo_save(self, cur, doc, event_type, fresh=False):
        if not fresh:
            doc["version"] = int(doc.get("version", 0)) + 1
        doc["updated_at"] = iso_datetime()
        cur.execute("INSERT INTO " + self.repo_s + ".orders(account_type,account_id,order_id,client_order_id,request_hash,"
                    "remark,active,document) VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb) "
                    "ON CONFLICT(account_type,account_id,order_id) DO UPDATE SET active=EXCLUDED.active,document=EXCLUDED.document",
                    self.repo_scope + (doc["order_id"], doc["client_order_id"], doc["request_hash"], doc["remark"],
                                       is_order_active(doc), json_text(doc)))
        for table, field in repo_CHILD_TABLES.items():
            cur.execute("DELETE FROM " + self.repo_s + "." + table +
                        " WHERE account_type=%s AND account_id=%s AND order_id=%s", self.repo_scope + (doc["order_id"],))
            for index, record in enumerate(doc.get(field, [])):
                keys = {"items": "item_id", "attempts": "attempt_id", "cancel_requests": "cancel_request_id",
                        "qmt_tasks": "qmt_task_id", "qmt_orders": "qmt_order_id", "fills": "trade_id"}
                record_id = str(record.get(keys[field]) or record.get("id") or index)
                if field in ("qmt_tasks", "qmt_orders", "fills"):
                    record_id = fingerprint([record.get("trading_day"), record.get("market"), record_id])
                cur.execute("INSERT INTO " + self.repo_s + "." + table +
                            "(account_type,account_id,order_id,record_id,document) VALUES(%s,%s,%s,%s,%s::jsonb)",
                            self.repo_scope + (doc["order_id"], record_id, json_text(record)))
        cur.execute("UPDATE " + self.repo_s + ".account_runtime SET event_seq=event_seq+1 "
                    "WHERE account_type=%s AND account_id=%s RETURNING event_seq", self.repo_scope)
        seq = cur.fetchone()[0]
        cur.execute("INSERT INTO " + self.repo_s + ".order_events(account_type,account_id,event_seq,order_id,event_type,"
                    "occurred_at,document) VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb)",
                    self.repo_scope + (seq, doc["order_id"], event_type, doc["updated_at"], json_text(doc)))
        return doc

    def accept_order(self, normalized):
        if str(normalized.get("account_id")) != self.account_id:
            raise OrderError(400, "ACCOUNT_MISMATCH", "request does not belong to this account")
        def accept(cur):
            doc = self.repo_load(cur, normalized["client_order_id"], by_client=True, lock=True)
            if doc:
                if doc["request_hash"] != fingerprint(normalized):
                    raise OrderError(409, "IDEMPOTENCY_CONFLICT", "client_order_id already has different request content")
                return False, doc
            doc = new_order_document(normalized)
            doc["account_type"] = self.account_type
            return True, self.repo_save(cur, doc, "ORDER_ACCEPTED", fresh=True)
        return self.repo_run(accept, mutation=True)

    def get_order(self, client_order_id):
        return self.repo_run(lambda cur: self.repo_require(self.repo_load(cur, client_order_id, by_client=True)))

    def get_by_id(self, order_id):
        return self.repo_run(lambda cur: self.repo_require(self.repo_load(cur, order_id)))

    def update_order(self, order_id, event_type, mutator):
        def update(cur):
            doc = self.repo_require(self.repo_load(cur, order_id, lock=True))
            before = json_text(doc)
            mutator(doc)
            return self.repo_save(cur, doc, event_type) if json_text(doc) != before else doc
        return self.repo_run(update, mutation=True)

    def claim_submission(self, order_id):
        self.check_executor()
        def claim(cur):
            self.check_executor()
            cur.execute("SELECT executor_instance,executor_epoch FROM " + self.repo_s +
                        ".account_runtime WHERE account_type=%s AND account_id=%s", self.repo_scope)
            if tuple(cur.fetchone()) != (self.repo_executor_instance, self.repo_executor_epoch):
                raise OrderError(503, "EXECUTOR_LOCK_LOST", "executor epoch changed")
            doc = self.repo_require(self.repo_load(cur, order_id, lock=True))
            if doc["submission_status"] != "QUEUED" or doc.get("cancel_requested"):
                return False, doc
            if doc.get("submit_before") and parse_timestamp(doc["submit_before"]) <= utc_now():
                doc["submission_status"] = "EXPIRED"
                return False, self.repo_save(cur, doc, "SUBMISSION_EXPIRED")
            doc["submission_status"] = "SUBMITTING"
            doc["attempts"].append({"attempt_id": str(uuid.uuid4()), "kind": "SUBMIT", "remark": doc["remark"],
                                    "status": "CALLING", "created_at": iso_datetime(),
                                    "executor_epoch": self.repo_executor_epoch})
            return True, self.repo_save(cur, doc, "SUBMISSION_CLAIMED")
        return self.repo_run(claim, mutation=True)

    def request_cancel(self, normalized):
        if normalized.get("account_id") is not None and str(normalized["account_id"]) != self.account_id:
            raise OrderError(400, "ACCOUNT_MISMATCH", "request does not belong to this account")
        def cancel(cur):
            cancel_id = normalized["cancel_request_id"]
            cur.execute("SELECT order_id,document FROM " + self.repo_s + ".cancel_requests "
                        "WHERE account_type=%s AND account_id=%s AND record_id=%s", self.repo_scope + (cancel_id,))
            row = cur.fetchone()
            if row:
                record = repo_json(row[1])
                if record.get("request_hash") != fingerprint(normalized):
                    raise OrderError(409, "IDEMPOTENCY_CONFLICT", "cancel_request_id already has different request content")
                doc = self.repo_require(self.repo_load(cur, row[0], lock=True))
                return record.get("http_status", 200), cancel_response(doc, cancel_id, replayed=True)
            doc = self.repo_require(self.repo_load(cur, normalized["client_order_id"], by_client=True, lock=True))
            record, status = apply_cancel_request(doc, normalized)
            record = next(row for row in doc["cancel_requests"] if row["cancel_request_id"] == cancel_id)
            record["request_hash"] = fingerprint(normalized)
            record["http_status"] = status
            self.repo_save(cur, doc, "CANCEL_REQUESTED")
            return status, cancel_response(doc, cancel_id)
        return self.repo_run(cancel, mutation=True)

    def list_orders(self, active=True, limit=100, cursor=None):
        limit = max(1, min(int(limit), 1000))
        def listing(cur):
            params = self.repo_scope
            sql = "SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s"
            if active:
                sql += " AND active=true"
            if cursor:
                sql += " AND order_id>%s"
                params += (str(cursor),)
            cur.execute(sql + " ORDER BY order_id LIMIT %s", params + (limit + 1,))
            docs = [repo_json(row[0]) for row in cur.fetchall()]
            return {"orders": docs[:limit], "has_more": len(docs) > limit,
                    "next_cursor": docs[limit - 1]["order_id"] if len(docs) > limit else None}
        return self.repo_run(listing)

    def events(self, after=0, limit=100):
        limit = max(1, min(int(limit), 1000))
        def listing(cur):
            cur.execute("SELECT event_seq,order_id,event_type,occurred_at,document FROM " + self.repo_s +
                        ".order_events WHERE account_type=%s AND account_id=%s AND event_seq>%s "
                        "ORDER BY event_seq LIMIT %s", self.repo_scope + (int(after), limit + 1))
            rows = cur.fetchall()
            events = []
            for row in rows[:limit]:
                doc = repo_json(row[4])
                events.append({"event_id": row[0], "order_id": row[1], "order_version": doc["version"],
                               "type": row[2], "recorded_at": row[3], "data": doc,
                               "event_seq": row[0], "event_type": row[2], "occurred_at": row[3], "order": doc})
            return {"events": events, "next_after": events[-1]["event_seq"] if events else int(after),
                    "has_more": len(rows) > limit}
        return self.repo_run(listing)

    def work_orders(self, limit=100):
        return self.list_orders(active=True, limit=limit)["orders"]

    def queued_orders(self, limit=10):
        def listing(cur):
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "AND document->>'submission_status'='QUEUED' ORDER BY document->>'created_at',order_id LIMIT %s",
                        self.repo_scope + (max(1, int(limit)),))
            return [repo_json(row[0]) for row in cur.fetchall()]
        return self.repo_run(listing)

    def cancellation_orders(self, limit=100):
        def listing(cur):
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "AND document->>'cancel_requested'='true' AND document->>'cancel_status' "
                        "NOT IN ('CONFIRMED','NOT_NEEDED','REJECTED') "
                        "ORDER BY CASE document->>'cancel_status' WHEN 'REQUESTED' THEN 0 "
                        "WHEN 'UNKNOWN' THEN 2 ELSE 1 END,"
                        "COALESCE(document->>'last_reconciled_at',''),order_id LIMIT %s",
                        self.repo_scope + (max(1, int(limit)),))
            return [repo_json(row[0]) for row in cur.fetchall()]
        return self.repo_run(listing)

    def reconcile_orders(self, limit=1000):
        # 历史终态也可能迟到成交；按最后核对时间轮转避免首页永久饥饿。
        def listing(cur):
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "ORDER BY COALESCE(document->>'last_reconciled_at',''),order_id LIMIT %s",
                        self.repo_scope + (max(1, int(limit)),))
            return [repo_json(row[0]) for row in cur.fetchall()]
        return self.repo_run(listing)

    def repo_match(self, cur, kind, raw):
        identifiers = observation_identifiers(kind, raw)
        if identifiers.get("account_id") and str(identifiers["account_id"]) != self.account_id:
            return None
        remark = identifiers.get("remark")
        if remark:
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s AND remark=%s FOR UPDATE",
                        self.repo_scope + (str(remark),))
            row = cur.fetchone()
            if row:
                return repo_json(row[0])
        matches = {}
        for field, table in (("qmt_task_id", "qmt_tasks"), ("qmt_order_id", "qmt_orders")):
            value = identifiers.get(field)
            if value is None or str(value) in ("", "0", "-1"):
                continue
            sql = "SELECT DISTINCT order_id FROM " + self.repo_s + "." + table + \
                  " WHERE account_type=%s AND account_id=%s AND document->>%s=%s"
            params = self.repo_scope + (field, str(value))
            for dimension in ("trading_day", "market"):
                if identifiers.get(dimension):
                    sql += " AND (document->>%s IS NULL OR document->>%s=%s)"
                    params += (dimension, dimension, str(identifiers[dimension]))
            cur.execute(sql, params)
            for row in cur.fetchall():
                matches[row[0]] = True
        if len(matches) == 1:
            doc = self.repo_load(cur, next(iter(matches)), lock=True)
            if remark and str(remark) != doc["remark"]:
                return None
            return doc
        return None

    def repo_apply_pending(self, cur):
        cur.execute("SELECT observation_id,kind,raw,source,observed_at FROM " + self.repo_s +
                    ".qmt_observations WHERE account_type=%s AND account_id=%s AND applied=false ORDER BY observation_id",
                    self.repo_scope)
        rows = cur.fetchall()
        # 新父记录可能建立关联，重复扫到固定点后停止。
        while rows:
            remaining = []
            for row in rows:
                doc = self.repo_match(cur, row[1], repo_json(row[2]))
                if doc is None:
                    remaining.append(row)
                    continue
                before = json_text(doc)
                apply_observation(doc, row[1], repo_json(row[2]), row[3], observed_at=row[4])
                if json_text(doc) != before:
                    self.repo_save(cur, doc, "QMT_OBSERVATION")
                cur.execute("UPDATE " + self.repo_s + ".qmt_observations SET order_id=%s,applied=true WHERE observation_id=%s",
                            (doc["order_id"], row[0]))
            if len(remaining) == len(rows):
                break
            rows = remaining

    def ingest_observation(self, kind, raw, source="callback"):
        def ingest(cur):
            digest = fingerprint({"kind": kind, "source": source, "raw": raw})
            cur.execute("SELECT order_id FROM " + self.repo_s + ".qmt_observations "
                        "WHERE account_type=%s AND account_id=%s AND observation_hash=%s", self.repo_scope + (digest,))
            previous = cur.fetchone()
            if previous is not None:
                return self.repo_load(cur, previous[0]) if previous[0] else None
            stamp = iso_datetime()
            cur.execute("INSERT INTO " + self.repo_s + ".qmt_observations(account_type,account_id,kind,source,observed_at,raw,observation_hash) "
                        "VALUES(%s,%s,%s,%s,%s,%s::jsonb,%s) RETURNING observation_id",
                        self.repo_scope + (kind, source, stamp, json_text(raw), digest))
            obs_id = cur.fetchone()[0]
            doc = self.repo_match(cur, kind, raw)
            if doc is None:
                return None
            before = json_text(doc)
            identities_before = self.repo_qmt_identities(doc)
            apply_observation(doc, kind, raw, source, observed_at=stamp)
            if json_text(doc) != before:
                self.repo_save(cur, doc, "QMT_OBSERVATION")
            cur.execute("UPDATE " + self.repo_s + ".qmt_observations SET order_id=%s,applied=true WHERE observation_id=%s",
                        (doc["order_id"], obs_id))
            # 外部回报不全扫历史积压；仅新增可关联身份时重放先到的子回报。
            if self.repo_qmt_identities(doc) != identities_before:
                self.repo_apply_pending(cur)
            return self.repo_load(cur, doc["order_id"])
        return self.repo_run(ingest, mutation=True)

    def repo_qmt_identities(self, doc):
        return {(field, str(row.get(field)), str(row.get("trading_day")), str(row.get("market")))
                for collection, field in (("qmt_orders", "qmt_order_id"), ("qmt_tasks", "qmt_task_id"))
                for row in doc.get(collection, [])}

    def recover(self):
        self.check_executor()
        def recovery(cur):
            cur.execute("SELECT document FROM " + self.repo_s + ".orders WHERE account_type=%s AND account_id=%s "
                        "ORDER BY order_id FOR UPDATE", self.repo_scope)
            docs = [repo_json(row[0]) for row in cur.fetchall()]
            count = 0
            for doc in docs:
                before = json_text(doc)
                submitting = doc["submission_status"] == "SUBMITTING"
                if submitting:
                    doc["submission_status"] = "UNKNOWN"
                    doc["sync_status"] = "PENDING"
                for attempt in doc.get("attempts", []):
                    is_cancel = attempt.get("kind") in ("CANCEL", "CANCEL_ORDER", "CANCEL_TASK")
                    if (is_cancel or submitting) and attempt.get("status") in ("CALLING", "RETURNED"):
                        attempt["status"] = "UNKNOWN"
                        if is_cancel:
                            doc["cancel_status"] = "UNKNOWN"
                if json_text(doc) != before:
                    recompute_order(doc)
                    self.repo_save(cur, doc, "EXECUTOR_RECOVERY")
                    count += 1
            self.repo_apply_pending(cur)
            return {"recovered": count}
        return self.repo_run(recovery, mutation=True)

    def lookup_observations(self, order_id, include_unassociated=False):
        def lookup(cur):
            doc = self.repo_require(self.repo_load(cur, order_id))
            predicate = "(order_id=%s OR order_id IS NULL)" if include_unassociated else "order_id=%s"
            cur.execute("SELECT observation_id,kind,raw,source,observed_at,applied,order_id FROM " + self.repo_s +
                        ".qmt_observations WHERE account_type=%s AND account_id=%s AND " + predicate + " ORDER BY observation_id",
                        self.repo_scope + (order_id,))
            result = []
            members = {(item["symbol"], item["side"]) for item in doc["items"]}
            for row in cur.fetchall():
                raw = repo_json(row[2])
                ids = observation_identifiers(row[1], raw)
                if row[6] is None:
                    if (ids.get("symbol"), ids.get("side")) not in members:
                        continue
                    if ids.get("remark") and str(ids["remark"]) != doc["remark"]:
                        continue
                    if ids.get("account_id") and str(ids["account_id"]) != self.account_id:
                        continue
                result.append({"observation_id": row[0], "kind": row[1], "raw": raw, "source": row[3],
                               "observed_at": row[4], "applied": row[5], "order_id": row[6]})
            return result
        return self.repo_run(lookup)

    def manual_associate(self, order_id, observation_ids, expected_version, audit):
        if not isinstance(audit, dict) or any(not audit.get(key) for key in ("reason", "operator", "evidence")):
            raise OrderError(400, "MANUAL_AUDIT_REQUIRED", "reason, operator and positive evidence are required")
        if not isinstance(observation_ids, (list, tuple)) or not observation_ids:
            raise OrderError(400, "INVALID_OBSERVATIONS", "select persisted QMT observations")
        if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in observation_ids):
            raise OrderError(400, "INVALID_OBSERVATIONS", "observation IDs must be positive integers")
        selected = sorted(set(observation_ids))
        if len(selected) != len(observation_ids):
            raise OrderError(400, "INVALID_OBSERVATIONS", "observation IDs must be unique")
        def associate(cur):
            doc = self.repo_require(self.repo_load(cur, order_id, lock=True))
            if doc["version"] != expected_version:
                raise OrderError(409, "ORDER_VERSION_CONFLICT", "order changed; inspect current facts")
            if doc["submission_status"] != "UNKNOWN":
                raise OrderError(409, "ORDER_NOT_UNKNOWN", "manual association requires UNKNOWN submission")
            members = {(row["symbol"], row["side"]) for row in doc["items"]}
            observations, qmt_orders, qmt_tasks = [], set(), set()
            for observation_id in selected:
                cur.execute("SELECT kind,raw,source,observed_at,order_id FROM " + self.repo_s +
                            ".qmt_observations WHERE account_type=%s AND account_id=%s AND observation_id=%s FOR UPDATE",
                            self.repo_scope + (observation_id,))
                row = cur.fetchone()
                if row is None:
                    raise OrderError(404, "OBSERVATION_NOT_FOUND", "observation is not in this account")
                kind, raw, source, observed_at, associated_order = row
                raw = repo_json(raw)
                ids = observation_identifiers(kind, raw)
                if associated_order is not None and associated_order != order_id:
                    raise OrderError(409, "OBSERVATION_CONFLICT", "observation already belongs to another order")
                if ids.get("account_id") and str(ids["account_id"]) != self.account_id:
                    raise OrderError(409, "OBSERVATION_CONFLICT", "observation account differs")
                if ids.get("remark") and str(ids["remark"]) != doc["remark"]:
                    raise OrderError(409, "OBSERVATION_CONFLICT", "observation remark contradicts target order")
                if not ids.get("remark"):
                    evidence = audit.get("evidence")
                    attribution = evidence.get("manual_attribution") if isinstance(evidence, dict) else None
                    attributed_ids = attribution.get("observation_ids") if isinstance(attribution, dict) else None
                    if (not isinstance(attribution, dict) or attribution.get("order_id") != order_id
                            or attribution.get("account_id") != self.account_id
                            or not isinstance(attributed_ids, list)
                            or any(type(value) is not int for value in attributed_ids)
                            or sorted(attributed_ids) != selected
                            or not isinstance(attribution.get("basis"), str) or len(attribution["basis"].strip()) < 20):
                        raise OrderError(409, "MANUAL_ATTRIBUTION_REQUIRED", "missing remark requires explicit order/account/observations and attribution basis")
                if (ids.get("symbol"), ids.get("side")) not in members:
                    raise OrderError(409, "OBSERVATION_CONFLICT", "observation symbol and side do not match an order item")
                if kind not in ("order", "task", "deal") or not (ids.get("qmt_order_id") or ids.get("qmt_task_id")):
                    raise OrderError(409, "OBSERVATION_CONFLICT", "observation has no positive QMT identity")
                for field, table, found in (("qmt_order_id", "qmt_orders", qmt_orders),
                                            ("qmt_task_id", "qmt_tasks", qmt_tasks)):
                    if not ids.get(field):
                        continue
                    found.add(str(ids[field]))
                    sql = "SELECT 1 FROM " + self.repo_s + "." + table + \
                          " WHERE account_type=%s AND account_id=%s AND order_id<>%s AND document->>%s=%s"
                    params = self.repo_scope + (order_id, field, str(ids[field]))
                    for dimension in ("trading_day", "market"):
                        if ids.get(dimension):
                            sql += " AND (document->>%s IS NULL OR document->>%s=%s)"
                            params += (dimension, dimension, str(ids[dimension]))
                    cur.execute(sql + " LIMIT 1", params)
                    if cur.fetchone() is not None:
                        raise OrderError(409, "QMT_ID_CONFLICT", "QMT identity already belongs to another order")
                observations.append((observation_id, kind, raw, source, observed_at))
            for key, actual in (("qmt_order_ids", qmt_orders), ("qmt_task_ids", qmt_tasks)):
                if key in audit and {str(value) for value in audit[key]} != actual:
                    raise OrderError(409, "QMT_ID_CONFLICT", "supplied QMT identities differ from selected observations")
            # 核验全部证据后才归并；原始记录保持不变，人工凭据单独追加审计。
            for observation_id, kind, raw, source, observed_at in observations:
                apply_observation(doc, kind, raw, source, observed_at=observed_at)
                cur.execute("UPDATE " + self.repo_s + ".qmt_observations SET order_id=%s,applied=true WHERE observation_id=%s",
                            (order_id, observation_id))
            if doc["submission_status"] != "CONFIRMED":
                raise OrderError(409, "INSUFFICIENT_QMT_EVIDENCE", "selected facts do not confirm submission")
            entry = copy_json(audit)
            entry.update(expected_version=expected_version, resolution="observed", observation_ids=selected,
                         qmt_order_ids=sorted(qmt_orders), qmt_task_ids=sorted(qmt_tasks),
                         before_submission_status="UNKNOWN", after_submission_status=doc["submission_status"], at=iso_datetime())
            doc.setdefault("manual_resolutions", []).append(entry)
            doc["resolution"] = "OBSERVED"
            doc["reconcile_requested"] = True
            self.repo_save(cur, doc, "MANUAL_UNKNOWN_RESOLUTION")
            self.repo_apply_pending(cur)
            return self.repo_load(cur, order_id)
        return self.repo_run(associate, mutation=True)
