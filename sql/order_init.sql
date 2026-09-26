-- PostgreSQL ORDER 当前结构初始化；由独立工具执行，策略运行时不执行 DDL。
-- 表间关联由应用层维护，不使用外键约束。
-- Last modified (Asia/Shanghai): 2026-09-26 20:05:51
CREATE SCHEMA IF NOT EXISTS "qmt_order";

CREATE TABLE IF NOT EXISTS "qmt_order".schema_version(version integer PRIMARY KEY);
CREATE TABLE IF NOT EXISTS "qmt_order".account_runtime (
 account_type text NOT NULL, account_id text NOT NULL, event_seq bigint NOT NULL DEFAULT 0,
 executor_host text, executor_instance text, executor_epoch bigint NOT NULL DEFAULT 0,
 PRIMARY KEY(account_type,account_id));
CREATE TABLE IF NOT EXISTS "qmt_order".orders (
 account_type text NOT NULL, account_id text NOT NULL, order_id text NOT NULL,
 client_order_id text NOT NULL, request_hash text NOT NULL, remark text NOT NULL,
 active boolean NOT NULL, document jsonb NOT NULL,
 submission_status text NOT NULL, cancel_ready boolean NOT NULL DEFAULT false,
 reconcile_pending boolean NOT NULL DEFAULT false,
 reconcile_priority boolean NOT NULL DEFAULT false,
 reconcile_due_at timestamptz NOT NULL DEFAULT 'epoch',
 last_reconcile_attempt_at timestamptz,
 last_reconciled_at timestamptz,
 fact_version bigint NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(account_type,account_id,order_id),
 UNIQUE(account_type,account_id,client_order_id), UNIQUE(remark));
CREATE INDEX IF NOT EXISTS order_queued_v2 ON "qmt_order".orders(account_type,account_id,order_id)
 WHERE submission_status='QUEUED';
CREATE INDEX IF NOT EXISTS order_cancel_ready_v2 ON "qmt_order".orders(account_type,account_id,order_id)
 WHERE cancel_ready;
CREATE INDEX IF NOT EXISTS order_reconcile_rotation_v2 ON "qmt_order".orders
 (account_type,account_id,reconcile_priority DESC,COALESCE(last_reconcile_attempt_at,'epoch'::timestamptz),order_id)
 WHERE reconcile_pending;
CREATE INDEX IF NOT EXISTS order_reconcile_due_v2 ON "qmt_order".orders(account_type,account_id,reconcile_due_at)
 WHERE reconcile_pending;
CREATE TABLE IF NOT EXISTS "qmt_order".order_events (
 account_type text NOT NULL, account_id text NOT NULL, event_seq bigint NOT NULL,
 order_id text NOT NULL, event_type text NOT NULL, occurred_at text NOT NULL, document jsonb NOT NULL,
 PRIMARY KEY(account_type,account_id,event_seq));
CREATE TABLE IF NOT EXISTS "qmt_order".qmt_observations (
 observation_id bigserial PRIMARY KEY, account_type text NOT NULL, account_id text NOT NULL,
 kind text NOT NULL, source text NOT NULL, observed_at text NOT NULL, raw jsonb NOT NULL,
 order_id text, applied boolean NOT NULL DEFAULT false, observation_hash text);
CREATE UNIQUE INDEX IF NOT EXISTS qmt_observation_content ON "qmt_order".qmt_observations(account_type,account_id,observation_hash)
 WHERE observation_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS qmt_observations_pending ON "qmt_order".qmt_observations(account_type,account_id,applied);

CREATE TABLE IF NOT EXISTS "qmt_order".order_items (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id));

CREATE TABLE IF NOT EXISTS "qmt_order".execution_attempts (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id));

CREATE TABLE IF NOT EXISTS "qmt_order".cancel_requests (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id));

CREATE TABLE IF NOT EXISTS "qmt_order".qmt_tasks (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id));

CREATE TABLE IF NOT EXISTS "qmt_order".qmt_orders (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id));

CREATE TABLE IF NOT EXISTS "qmt_order".fills (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id));
CREATE UNIQUE INDEX IF NOT EXISTS cancel_request_scope_id ON "qmt_order".cancel_requests(account_type,account_id,record_id);
-- The schema version is seeded by tools/order_schema.py; strategy startup creates its account_runtime row.
