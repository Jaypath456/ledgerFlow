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
