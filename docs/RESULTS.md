# Results

## Phase 0
- `./mvnw verify`: pass (2 Testcontainers tests per service).
- `docker compose up --wait`: Postgres and Kafka healthy.
- Both services start; `/actuator/health` UP; Flyway history has 1 applied migration in each own schema.
- Isolation: `payment_user` gets `permission denied for schema ledger` (and vice versa), checked via psql and tests.
- Phase 0 CI: PASS (commit 25924ed, GitHub Actions duration 49s).

## Phase 1 (ledger core)
- `./mvnw verify`: pass — ledger-service 12 tests (posting 6, concurrency 3, seeded randomized invariant test × 100 sequences, smoke 2); payment-service 2.
- Concurrency: 200 attempts against a payer funded for 50 → exactly 50 succeeded, 150 `InsufficientFunds`, final balance 0, no deadlocks; opposite-direction storm and 16-way same-paymentId also clean. Concurrency tests re-run 5× with no failures.
- Invariants I1, I2, I4, I5 asserted after every posting/concurrency test and after each randomized sequence.
- CI: not yet run for this branch.

## Phase 2 (payment API, idempotency, outbox)
- `./mvnw verify`: pass — payment-service 15 tests (API 13, smoke 2), ledger-service 12 (unchanged).
- Accepted → 202, declined → 201; replays return the byte-identical stored response, also after the payment's status changed.
- Same-key storm: 32 concurrent POSTs with one key → all 202 with byte-identical bodies and the same payment id, exactly 1 non-replayed response, 1 payment row, 1 outbox row. Storm, replay and rollback tests re-run 5× with no failures.
- Accepted → 1 outbox row (topic, key = payer, payload fields checked); declined (amount, blocked, velocity) → 0 outbox rows.
- Injected failure at the outbox insert → 500, no payment/key/outbox rows; retry with the same key then succeeds as an original.
