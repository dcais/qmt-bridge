-- OPTIONAL additive migration for schema v3. Execute only offline after backup.
-- This file is NOT executed by strategy startup or normal schema initialization.
-- No extension or external identity service required. This is local stream identity,
-- NOT proof against restoring/cloning a complete database backup.
BEGIN;
SET LOCAL lock_timeout = '5s';
ALTER TABLE qmt_order.account_runtime
  ADD COLUMN IF NOT EXISTS evidence_stream_id uuid NOT NULL
  DEFAULT md5(random()::text || clock_timestamp()::text)::uuid;
COMMIT;
