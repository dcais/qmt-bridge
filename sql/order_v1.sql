-- PostgreSQL ORDER v1. Explicit installer; runtime never executes DDL.
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
 PRIMARY KEY(account_type,account_id,order_id),
 UNIQUE(account_type,account_id,client_order_id), UNIQUE(remark));
CREATE TABLE IF NOT EXISTS "qmt_order".order_events (
 account_type text NOT NULL, account_id text NOT NULL, event_seq bigint NOT NULL,
 order_id text NOT NULL, event_type text NOT NULL, occurred_at text NOT NULL, document jsonb NOT NULL,
 PRIMARY KEY(account_type,account_id,event_seq));
CREATE TABLE IF NOT EXISTS "qmt_order".qmt_observations (
 observation_id bigserial PRIMARY KEY, account_type text NOT NULL, account_id text NOT NULL,
 kind text NOT NULL, source text NOT NULL, observed_at text NOT NULL, raw jsonb NOT NULL,
 order_id text, applied boolean NOT NULL DEFAULT false, observation_hash text);
ALTER TABLE "qmt_order".qmt_observations ADD COLUMN IF NOT EXISTS observation_hash text;
CREATE UNIQUE INDEX IF NOT EXISTS qmt_observation_content ON "qmt_order".qmt_observations(account_type,account_id,observation_hash)
 WHERE observation_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS qmt_observations_pending ON "qmt_order".qmt_observations(account_type,account_id,applied);

CREATE TABLE IF NOT EXISTS "qmt_order".order_items (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id),FOREIGN KEY(account_type,account_id,order_id) REFERENCES "qmt_order".orders(account_type,account_id,order_id));

CREATE TABLE IF NOT EXISTS "qmt_order".execution_attempts (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id),FOREIGN KEY(account_type,account_id,order_id) REFERENCES "qmt_order".orders(account_type,account_id,order_id));

CREATE TABLE IF NOT EXISTS "qmt_order".cancel_requests (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id),FOREIGN KEY(account_type,account_id,order_id) REFERENCES "qmt_order".orders(account_type,account_id,order_id));

CREATE TABLE IF NOT EXISTS "qmt_order".qmt_tasks (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id),FOREIGN KEY(account_type,account_id,order_id) REFERENCES "qmt_order".orders(account_type,account_id,order_id));

CREATE TABLE IF NOT EXISTS "qmt_order".qmt_orders (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id),FOREIGN KEY(account_type,account_id,order_id) REFERENCES "qmt_order".orders(account_type,account_id,order_id));

CREATE TABLE IF NOT EXISTS "qmt_order".fills (account_type text NOT NULL,account_id text NOT NULL,order_id text NOT NULL,record_id text NOT NULL,document jsonb NOT NULL,PRIMARY KEY(account_type,account_id,order_id,record_id),FOREIGN KEY(account_type,account_id,order_id) REFERENCES "qmt_order".orders(account_type,account_id,order_id));
CREATE UNIQUE INDEX IF NOT EXISTS cancel_request_scope_id ON "qmt_order".cancel_requests(account_type,account_id,record_id);
-- Version and current-account rows are seeded by tools/order_schema.py in the same transaction.
