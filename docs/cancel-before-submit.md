# Durable cancellation before submit

The v5 ORDER schema adds `submission_bans`, keyed by account type, account ID,
and client order ID. POST `/cancel_order` retains its three required fields:
`account_id`, `client_order_id`, and immutable `cancel_request_id`.

If no order exists, cancellation stores a `SUBMISSION_BAN` receipt. The account
runtime row lock serializes this operation with acceptance and submission claims.
A later submit of the same account/client identity fails `409 SUBMISSION_BANNED`.
A real existing order follows normal cancellation, including pending/UNKNOWN;
this path never fabricates a ban or claims a QMT cancellation has completed.

The capability is `cancel_before_submit_v1: {supported: true,
receipt_type: "SUBMISSION_BAN", schema_version: 1}`. This advertises protocol
support, not storage readiness. A receipt contains `receipt_version: 1`,
`source: BRIDGE_PERSISTED_SUBMISSION_BAN`, persisted `stream_id`, `account_type`,
`account_id`, `client_order_id`, `cancel_request_id`, UUID `ban_id`, aware UTC
`created_at`, `submission_status: CANCELLED_LOCAL`, `cancel_status: CONFIRMED`,
`no_submission: true`, and `execution_attempt_count: 0`. It has no original order,
items, fills, or order ID. GET `/order` returns this exact saved receipt for the
banned identity. Same-ID same-content retries return the exact receipt; changing
request content, or changing cancel ID for an already banned identity, conflicts.
Cancel IDs cannot be reused across real cancellations and bans within an account.

After an unknown commit response, retry the original cancel ID or read `/order`;
absence or a transport error is never proof of non-submission. Clients must
validate capability and every proof identity; old bridge rejection/404, missing
stream identity, or failed persistence retains reservations and a pending state.
The existing optional `sql/order_evidence_v1.sql` migration is required for a ban;
without it absent-order cancellation fails `503 EVIDENCE_SCHEMA_REQUIRED`.

## Offline migration and rollout

**Stop ALL old and new ORDER writers first and retain a database backup. Old
binaries must not resume against v5.** Runtime never performs migrations.
`python -m tools.order_submission_ban_migration --config <explicit-config>` is a
read-only v4/v5 preflight. Applying requires `--apply --backup <nonempty-pg-dump>`.
The migration locks schema/account runtime tables, creates only the new table and
index, updates the version atomically, and checks the result. It does not alter
historical orders or JSON. Unknown COMMIT requires a fresh read-only preflight.
New databases use the normal v5 initializer; older schemas require their existing
explicit migration to v4 first. No deployment or live migration is implied by
these source changes.

## Verification

`python -m unittest discover -s tests -p test_order_submission_ban.py` includes
protocol/restart-readback/unknown-commit unit tests and migration rollback checks.
Real PostgreSQL serialization tests run only with explicit `ORDER_TEST_PGHOST`
and companion ORDER_TEST_* settings; each test creates and drops its own random
`qmt_test_*` schema. Never point this runner at a business database. These tests
cover cancel-first replay, account scoping, concurrent accept/cancel, and claim
before cancel without false local terminal proof.

Wiki impact: none — bridge protocol and migration instructions are recorded here.
