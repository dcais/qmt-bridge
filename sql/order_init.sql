-- PostgreSQL ORDER current schema; installed by the independent tool, never by the strategy.
-- Application transactions maintain relationships. No foreign keys are used.
CREATE SCHEMA IF NOT EXISTS "qmt_order";

CREATE TABLE IF NOT EXISTS "qmt_order".schema_version (
 version integer PRIMARY KEY,
 created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS "qmt_order".account_runtime (
 account_type text NOT NULL, account_id text NOT NULL, event_seq bigint NOT NULL DEFAULT 0,
 executor_host text, executor_instance text, executor_epoch bigint NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT now(),
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
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(account_type,account_id,event_seq));
CREATE TABLE IF NOT EXISTS "qmt_order".qmt_observations (
 observation_id bigserial PRIMARY KEY, account_type text NOT NULL, account_id text NOT NULL,
 kind text NOT NULL, source text NOT NULL, observed_at text NOT NULL, raw jsonb NOT NULL,
 order_id text, applied boolean NOT NULL DEFAULT false, observation_hash text,
 created_at timestamptz NOT NULL DEFAULT now());
CREATE UNIQUE INDEX IF NOT EXISTS qmt_observation_content ON "qmt_order".qmt_observations(account_type,account_id,observation_hash)
 WHERE observation_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS qmt_observations_pending ON "qmt_order".qmt_observations(account_type,account_id,applied);

CREATE TABLE IF NOT EXISTS "qmt_order".order_items (
 account_type text NOT NULL, account_id text NOT NULL, order_id text NOT NULL,
 record_id uuid NOT NULL, document jsonb NOT NULL,
 item_id text GENERATED ALWAYS AS (document->>'item_id') STORED NOT NULL,
 symbol text GENERATED ALWAYS AS (document->>'symbol') STORED,
 side text GENERATED ALWAYS AS (document->>'side') STORED,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(account_type,account_id,order_id,record_id),
 CONSTRAINT order_items_item_id_nonempty CHECK (btrim(item_id) <> ''));
CREATE UNIQUE INDEX IF NOT EXISTS order_items_business_id ON "qmt_order".order_items(account_type,account_id,order_id,item_id);

CREATE TABLE IF NOT EXISTS "qmt_order".execution_attempts (
 account_type text NOT NULL, account_id text NOT NULL, order_id text NOT NULL,
 record_id uuid NOT NULL, document jsonb NOT NULL,
 attempt_id text GENERATED ALWAYS AS (document->>'attempt_id') STORED NOT NULL,
 kind text GENERATED ALWAYS AS (document->>'kind') STORED,
 status text GENERATED ALWAYS AS (document->>'status') STORED,
 target_id text GENERATED ALWAYS AS (document->>'target_id') STORED,
 cancel_request_id text GENERATED ALWAYS AS (document->>'cancel_request_id') STORED,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(account_type,account_id,order_id,record_id),
 CONSTRAINT execution_attempts_attempt_id_nonempty CHECK (btrim(attempt_id) <> ''));
CREATE UNIQUE INDEX IF NOT EXISTS execution_attempts_business_id ON "qmt_order".execution_attempts(account_type,account_id,order_id,attempt_id);

CREATE TABLE IF NOT EXISTS "qmt_order".cancel_requests (
 account_type text NOT NULL, account_id text NOT NULL, order_id text NOT NULL,
 record_id uuid NOT NULL, document jsonb NOT NULL,
 cancel_request_id text GENERATED ALWAYS AS (document->>'cancel_request_id') STORED NOT NULL,
 status text GENERATED ALWAYS AS (document->>'status') STORED,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(account_type,account_id,order_id,record_id),
 CONSTRAINT cancel_requests_request_id_nonempty CHECK (btrim(cancel_request_id) <> ''));
CREATE UNIQUE INDEX IF NOT EXISTS cancel_request_scope_id ON "qmt_order".cancel_requests(account_type,account_id,cancel_request_id);

CREATE TABLE IF NOT EXISTS "qmt_order".qmt_tasks (
 account_type text NOT NULL, account_id text NOT NULL, order_id text NOT NULL,
 record_id uuid NOT NULL, document jsonb NOT NULL,
 qmt_task_id text GENERATED ALWAYS AS (document->>'qmt_task_id') STORED,
 trading_day text GENERATED ALWAYS AS (document->>'trading_day') STORED,
 market text GENERATED ALWAYS AS (document->>'market') STORED,
 status text GENERATED ALWAYS AS (document->>'status') STORED,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(account_type,account_id,order_id,record_id));
CREATE INDEX IF NOT EXISTS qmt_tasks_identity_lookup ON "qmt_order".qmt_tasks(account_type,account_id,qmt_task_id,trading_day,market);

CREATE TABLE IF NOT EXISTS "qmt_order".qmt_orders (
 account_type text NOT NULL, account_id text NOT NULL, order_id text NOT NULL,
 record_id uuid NOT NULL, document jsonb NOT NULL,
 qmt_order_id text GENERATED ALWAYS AS (document->>'qmt_order_id') STORED,
 qmt_task_id text GENERATED ALWAYS AS (document->>'qmt_task_id') STORED,
 item_id text GENERATED ALWAYS AS (document->>'item_id') STORED,
 trading_day text GENERATED ALWAYS AS (document->>'trading_day') STORED,
 market text GENERATED ALWAYS AS (document->>'market') STORED,
 symbol text GENERATED ALWAYS AS (document->>'symbol') STORED,
 side text GENERATED ALWAYS AS (document->>'side') STORED,
 status text GENERATED ALWAYS AS (document->>'status') STORED,
 native_ref text GENERATED ALWAYS AS (document->>'native_ref') STORED,
 native_order_ref text GENERATED ALWAYS AS (document->>'native_order_ref') STORED,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(account_type,account_id,order_id,record_id));
CREATE INDEX IF NOT EXISTS qmt_orders_identity_lookup ON "qmt_order".qmt_orders(account_type,account_id,qmt_order_id,trading_day,market);

CREATE TABLE IF NOT EXISTS "qmt_order".fills (
 account_type text NOT NULL, account_id text NOT NULL, order_id text NOT NULL,
 record_id uuid NOT NULL, document jsonb NOT NULL,
 trade_id text GENERATED ALWAYS AS (document->>'trade_id') STORED,
 qmt_order_id text GENERATED ALWAYS AS (document->>'qmt_order_id') STORED,
 item_id text GENERATED ALWAYS AS (document->>'item_id') STORED,
 trading_day text GENERATED ALWAYS AS (document->>'trading_day') STORED,
 market text GENERATED ALWAYS AS (document->>'market') STORED,
 symbol text GENERATED ALWAYS AS (document->>'symbol') STORED,
 side text GENERATED ALWAYS AS (document->>'side') STORED,
 quantity numeric GENERATED ALWAYS AS ((document->>'quantity')::numeric) STORED,
 amount numeric GENERATED ALWAYS AS ((document->>'amount')::numeric) STORED,
 created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(account_type,account_id,order_id,record_id));
CREATE INDEX IF NOT EXISTS fills_trade_identity_lookup ON "qmt_order".fills(account_type,account_id,trade_id,trading_day,market);
CREATE INDEX IF NOT EXISTS fills_order_identity_lookup ON "qmt_order".fills(account_type,account_id,qmt_order_id,trading_day,market);
-- tools/order_schema.py seeds the version. Strategy startup creates its account_runtime row.
