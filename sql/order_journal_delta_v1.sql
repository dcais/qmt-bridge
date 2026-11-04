-- Optional offline migration, only after backup and stopping every existing executor.
-- Existing documents are untouched. Every account remains FULL until separately opted in.
BEGIN;
SET LOCAL lock_timeout='5s';
LOCK TABLE qmt_order.schema_version IN ACCESS EXCLUSIVE MODE;
DO $guard$
BEGIN
 IF (SELECT array_agg(version) FROM qmt_order.schema_version) IS DISTINCT FROM ARRAY[5] THEN
  RAISE EXCEPTION 'expected exactly order schema 5';
 END IF;
END
$guard$;
-- Requires the existing evidence-v1 stream migration.
DO $stream$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid='qmt_order.account_runtime'::regclass
       AND attname='evidence_stream_id' AND atttypid='uuid'::regtype AND attnotnull AND NOT attisdropped) THEN
  RAISE EXCEPTION 'persistent evidence stream migration required';
 END IF;
END
$stream$;
ALTER TABLE qmt_order.account_runtime ADD COLUMN event_journal_write_mode text NOT NULL DEFAULT 'FULL'
 CHECK (event_journal_write_mode IN ('FULL','BASE_DELTA_V1'));
ALTER TABLE qmt_order.order_events ADD COLUMN base_event_seq bigint, ADD COLUMN storage_stream_id uuid;
CREATE INDEX order_event_order_sequence ON qmt_order.order_events(account_type,account_id,order_id,event_seq);
CREATE UNIQUE INDEX order_event_stream_sequence ON qmt_order.order_events(account_type,account_id,order_id,storage_stream_id,event_seq);
ALTER TABLE qmt_order.order_events ADD CONSTRAINT order_event_base_fk
 FOREIGN KEY(account_type,account_id,order_id,storage_stream_id,base_event_seq)
 REFERENCES qmt_order.order_events(account_type,account_id,order_id,storage_stream_id,event_seq) ON DELETE RESTRICT;
ALTER TABLE qmt_order.order_events ADD CONSTRAINT order_event_storage_shape CHECK (
 (jsonb_typeof(document)='object' AND base_event_seq IS NULL) OR
 (jsonb_typeof(document)='array' AND storage_stream_id IS NOT NULL AND base_event_seq>0 AND base_event_seq<event_seq
 AND storage_stream_id::text=document->1->>'stream_id'
 AND jsonb_array_length(document)=2 AND document->>0='ORDER_JOURNAL_BASE_DELTA_V1'
 AND document->1->>'base_seq' IS NOT NULL AND base_event_seq=(document->1->>'base_seq')::bigint
 AND document->1->>'seq' IS NOT NULL AND event_seq=(document->1->>'seq')::bigint
 AND document->1->>'order_id' IS NOT NULL AND order_id=document->1->>'order_id') IS TRUE);
CREATE FUNCTION qmt_order.reject_order_event_update() RETURNS trigger LANGUAGE plpgsql AS $immutable$
BEGIN
 RAISE EXCEPTION 'order journal rows are immutable';
END
$immutable$;
CREATE TRIGGER order_events_no_update BEFORE UPDATE ON qmt_order.order_events
 FOR EACH ROW EXECUTE FUNCTION qmt_order.reject_order_event_update();
CREATE FUNCTION qmt_order.guard_order_journal_stream() RETURNS trigger LANGUAGE plpgsql AS $identity$
BEGIN
 IF TG_OP='UPDATE' AND (OLD.account_type,OLD.account_id,OLD.evidence_stream_id)
    IS NOT DISTINCT FROM (NEW.account_type,NEW.account_id,NEW.evidence_stream_id) THEN
    RETURN NEW;
 END IF;
 IF EXISTS (SELECT 1 FROM qmt_order.order_events WHERE account_type=OLD.account_type AND account_id=OLD.account_id) THEN
   RAISE EXCEPTION 'retained order journal stream is immutable';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END
$identity$;
CREATE TRIGGER order_event_stream_guard BEFORE DELETE OR UPDATE OF account_id,account_type,evidence_stream_id
 ON qmt_order.account_runtime FOR EACH ROW EXECUTE FUNCTION qmt_order.guard_order_journal_stream();
UPDATE qmt_order.schema_version SET version=6 WHERE version=5;
COMMIT;
