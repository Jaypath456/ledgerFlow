# ADR 0002: Strict velocity with a per-payer guard row

Status: accepted (Roadmap Phase 1b), 2026-11-05.

## Context

The velocity rule (`RiskRule.Velocity`) declines a payment when the payer already has `ledgerflow.risk.velocity-max` (default 5)
non-declined payments in the last 60 seconds. Up to `v1.2.0-postgres` it was a plain `count(*)` inside the payment transaction, and
`docs/DECISIONS.md` (21) called it a soft limit: concurrent requests from one payer can all read the same count before any of them
commits, so more than the limit can be admitted. The MongoDB version made it strict with a guard document and a whole-transaction retry.
Phase 1b needs the same guarantee on PostgreSQL (`docs/ROADMAP.md`).

## Decision

Table `payments.velocity_guards(payer_account_id BIGINT PRIMARY KEY)` (migration `V3__velocity_guards.sql`). It is only a lock: the count
still comes from `payments`, so history from before the guard row existed still counts.

Inside the payment transaction (`PaymentService.create`, `@Transactional(isolation = READ_COMMITTED)`), in this order:

1. Ownership is checked in the controller before the service is called; the idempotency key is inserted first, so a replay returns the
   stored response and never touches the guard.
2. Amount and blocked-account rules run first (first decline wins).
3. `INSERT INTO velocity_guards (payer_account_id) VALUES (?) ON CONFLICT DO NOTHING`. On a first-use race the second inserter waits for
   the first one instead of failing, so no retry is needed.
4. `SELECT payer_account_id FROM velocity_guards WHERE payer_account_id = ? FOR UPDATE`. The lock is held until commit or rollback.
5. `SELECT count(*) FROM payments WHERE payer_account_id = ? AND status <> 'DECLINED' AND created_at > now() - interval '60 seconds'`.
   Under read committed each statement takes a fresh snapshot, so after waiting for the lock the count sees every admission committed by
   earlier holders.
6. Insert the payment (and, if admitted, its outbox row), store the response, commit.

Declines, validation failures, replays and rolled-back transactions consume no slot. `Velocity` refuses to run outside a transaction, and a
non-positive limit fails at startup.

Evidence in the repository: `PaymentApiTest.concurrentHotPayerNeverExceedsFiveAdmissionsAndDeclinesReplayExactly` sends 64 concurrent
requests for one payer with the default limit of 5 and asserts exactly 5 admitted (HTTP 202) and 59 declined with `VELOCITY_LIMIT`
(HTTP 201), one outbox row per admitted payment and none for declines, and byte-identical replays of the declines. Other tests in the same
class cover history from before the guard row, the rolling window, a replay while another transaction holds the guard, and a rolled-back
payment. On the Compose stack, the `scripts/smoke.sh` scenario "concurrent velocity limit" sent 32 concurrent requests for one payer:
5 completed, 27 declined, 0 invariant violations (`docs/evidence/smoke-postgres-phase1b-2026-11-05-2b58c27.txt`).

## Alternatives

- **Advisory lock per payer** (`pg_advisory_xact_lock(payer)`). Same serialization without a table, but the lock key space is shared with
  anything else that uses advisory locks, and the lock is invisible in the schema. Rejected for the explicit row.
- **Serializable isolation for the payment transaction.** Would also stop the over-admission, but conflicts surface as serialization
  failures that need a whole-transaction retry (as in the MongoDB version), and it would apply to every statement of the transaction, not
  only the velocity check.
- **Soft count, as in `v1.2.0-postgres` and before the MongoDB guard.** Simplest, but concurrent requests from one payer can exceed the
  limit. Rejected: the limit is a risk control and must hold under concurrency.

## Consequences

- Admission is serialized per payer: concurrent requests for the same payer wait on one row lock for the rest of the payment transaction.
  Different payers do not contend.
- A hot payer limits only itself: its requests queue on its own guard row while other payers proceed.
- Load and chaos runs must spread load over many payers (they use 200, `chaos/load.js`) and raise the limit
  (`RISK_VELOCITY_MAX=1000000` in `chaos/run_chaos.sh` and `chaos/bench.sh`) so the load reaches the ledger.
- One guard row per payer is kept forever; it holds no data besides the key.
