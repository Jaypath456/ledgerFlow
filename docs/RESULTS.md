# Results

## Phase 0
- `./mvnw verify`: pass (2 Testcontainers tests per service).
- `docker compose up --wait`: Postgres and Kafka healthy.
- Both services start; `/actuator/health` UP; Flyway history has 1 applied migration in each own schema.
- Isolation: `payment_user` gets `permission denied for schema ledger` (and vice versa), checked via psql and tests.
- Phase 0 CI: PASS (commit 25924ed, GitHub Actions duration 49s).
