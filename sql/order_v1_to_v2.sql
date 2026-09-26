-- ORDER v1 -> v2. tools/order_admin.py checks version 1 and runs this in one transaction.
-- Never apply this file to the business database except through the explicit schema migrate command.
ALTER TABLE "qmt_order".orders ADD COLUMN submission_status text;
ALTER TABLE "qmt_order".orders ADD COLUMN cancel_ready boolean NOT NULL DEFAULT false;
ALTER TABLE "qmt_order".orders ADD COLUMN reconcile_pending boolean NOT NULL DEFAULT false;
ALTER TABLE "qmt_order".orders ADD COLUMN reconcile_priority boolean NOT NULL DEFAULT false;
ALTER TABLE "qmt_order".orders ADD COLUMN reconcile_due_at timestamptz NOT NULL DEFAULT 'epoch';
ALTER TABLE "qmt_order".orders ADD COLUMN last_reconcile_attempt_at timestamptz;
ALTER TABLE "qmt_order".orders ADD COLUMN last_reconciled_at timestamptz;
ALTER TABLE "qmt_order".orders ADD COLUMN fact_version bigint NOT NULL DEFAULT 0;
ALTER TABLE "qmt_order".orders ADD COLUMN created_at timestamptz NOT NULL DEFAULT now();

-- A prior terminal state alone cannot prove that all QMT facts were covered.
-- Only orders with no QMT evidence, no attempts and a locally terminal/pre-call
-- submission state are known never to have entered QMT.
WITH classified AS (
 SELECT o.account_type, o.account_id, o.order_id,
        COALESCE(NULLIF(o.document->>'submission_status', ''), 'UNKNOWN') AS status,
        COALESCE((o.document->>'cancel_requested')::boolean, false) AS cancel_requested,
        COALESCE((o.document->>'reconcile_requested')::boolean, false) AS reconcile_requested,
        COALESCE(NULLIF(o.document->>'created_at', '')::timestamptz, o.created_at) AS original_created_at,
        (COALESCE(o.document->'qmt_tasks', '[]'::jsonb) <> '[]'::jsonb
         OR COALESCE(o.document->'qmt_orders', '[]'::jsonb) <> '[]'::jsonb
         OR COALESCE(o.document->'fills', '[]'::jsonb) <> '[]'::jsonb
         OR COALESCE(o.document->'attempts', '[]'::jsonb) <> '[]'::jsonb
         OR COALESCE(o.document->'unassociated_evidence', '[]'::jsonb) <> '[]'::jsonb
         OR EXISTS (SELECT 1 FROM "qmt_order".qmt_tasks t WHERE t.account_type=o.account_type AND t.account_id=o.account_id AND t.order_id=o.order_id)
         OR EXISTS (SELECT 1 FROM "qmt_order".qmt_orders q WHERE q.account_type=o.account_type AND q.account_id=o.account_id AND q.order_id=o.order_id)
         OR EXISTS (SELECT 1 FROM "qmt_order".fills f WHERE f.account_type=o.account_type AND f.account_id=o.account_id AND f.order_id=o.order_id)
         OR EXISTS (SELECT 1 FROM "qmt_order".qmt_observations x WHERE x.account_type=o.account_type AND x.account_id=o.account_id AND x.order_id=o.order_id)) AS evidence
 FROM "qmt_order".orders o
), projected AS (
 SELECT *,
        (reconcile_requested OR evidence OR status NOT IN ('QUEUED', 'CANCELLED_LOCAL', 'EXPIRED', 'REJECTED')) AS pending
 FROM classified
)
UPDATE "qmt_order".orders o
SET submission_status=p.status,
    -- Recovery recalculates exact actionable targets from the full document.
    cancel_ready=false,
    reconcile_pending=p.pending,
    reconcile_priority=(p.pending AND (p.cancel_requested OR p.status='UNKNOWN')),
    created_at=p.original_created_at,
    document=o.document || jsonb_build_object(
      'submission_status', p.status,
      'cancel_ready', false,
      'reconcile_pending', p.pending,
      'reconcile_requested', p.pending,
      'reconcile_priority', (p.pending AND (p.cancel_requested OR p.status='UNKNOWN')),
      'reconcile_due_at', to_jsonb(o.reconcile_due_at),
      'last_reconcile_attempt_at', to_jsonb(o.last_reconcile_attempt_at),
      'last_reconciled_at', to_jsonb(o.last_reconciled_at),
      'fact_version', o.fact_version,
      'created_at', COALESCE(o.document->'created_at', to_jsonb(p.original_created_at)))
FROM projected p
WHERE o.account_type=p.account_type AND o.account_id=p.account_id AND o.order_id=p.order_id;

ALTER TABLE "qmt_order".orders ALTER COLUMN submission_status SET NOT NULL;
CREATE INDEX order_queued_v2 ON "qmt_order".orders(account_type,account_id,order_id)
 WHERE submission_status='QUEUED';
CREATE INDEX order_cancel_ready_v2 ON "qmt_order".orders(account_type,account_id,order_id)
 WHERE cancel_ready;
CREATE INDEX order_reconcile_rotation_v2 ON "qmt_order".orders
 (account_type,account_id,reconcile_priority DESC,COALESCE(last_reconcile_attempt_at,'epoch'::timestamptz),order_id)
 WHERE reconcile_pending;
CREATE INDEX order_reconcile_due_v2 ON "qmt_order".orders(account_type,account_id,reconcile_due_at)
 WHERE reconcile_pending;
