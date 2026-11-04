-- Optional offline contract. Creates NO trust, reservations, receipts or event history.
-- Source and approval evidence must be independently authorized before import.
BEGIN;
SET LOCAL lock_timeout = '5s';
CREATE TABLE qmt_order.event_sequence_void_authorizations (
 stream_id uuid NOT NULL, account_type text NOT NULL, account_id text NOT NULL,
 event_seq bigint NOT NULL CHECK (event_seq > 0), authorization_id text NOT NULL UNIQUE,
 source_statement_sha256 text NOT NULL CHECK (source_statement_sha256 ~ '^[0-9a-f]{64}$'),
 approval_statement_sha256 text NOT NULL CHECK (approval_statement_sha256 ~ '^[0-9a-f]{64}$'),
 authorized_by text NOT NULL, authorized_at timestamptz NOT NULL,
 reason text NOT NULL, evidence_reference text NOT NULL,
 evidence_sha256 text NOT NULL CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
 PRIMARY KEY(stream_id,account_type,account_id,event_seq));
CREATE TABLE qmt_order.event_sequence_void_receipts (
 stream_id uuid NOT NULL, account_type text NOT NULL, account_id text NOT NULL,
 event_seq bigint NOT NULL CHECK (event_seq > 0), document jsonb NOT NULL,
 recorded_at timestamptz NOT NULL,
 PRIMARY KEY(stream_id,account_type,account_id,event_seq));
-- Grant INSERT on the authorization table only to an independently controlled administrator.
-- The import role needs SELECT authorization + INSERT receipt; runtime needs SELECT only.
REVOKE ALL ON qmt_order.event_sequence_void_authorizations FROM PUBLIC;
REVOKE ALL ON qmt_order.event_sequence_void_receipts FROM PUBLIC;
COMMIT;
