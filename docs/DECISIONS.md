# Decisions

1. Spring Boot 4.1.1 on Java 25 (Temurin).
2. `apache/kafka:4.0.0`, single combined broker/controller node, KRaft, PLAINTEXT. External port 29092.
3. One Postgres 17 DB `ledgerflow`; schemas/roles created by `infra/postgres/init/01-roles-schemas.sql`, reused by Testcontainers tests (single source of truth).
4. Dev-only plaintext credentials (user name == password); env-overridable (`DB_URL`, `DB_USER`, `DB_PASSWORD`).
5. Ports: payment 8081, ledger 8082.
6. `common` is an empty placeholder; services don't depend on it yet.
7. Awaitility, jqwik, spring-kafka deferred to the phase that first uses them.
8. Smoke tests are `*Test` run by surefire, so `./mvnw verify` covers them (no failsafe).
9. Maven wrapper is the `only-script` type (downloads Maven on first run).

## Phase 1
10. `payment_id` is UUID with a UNIQUE constraint (I2 backstop).
11. Duplicate paymentId throws `DuplicatePayment`; no replay of the original result (idempotency is a later phase). The duplicate check runs before the funds check.
12. Payer == payee is rejected (`SamePayerPayee`). SYSTEM accounts may go negative; non-SYSTEM balances are protected by service check plus a DB CHECK.
13. Locking: `SELECT … FOR UPDATE` on one account per statement, lowest id first, inside one `@Transactional` at READ COMMITTED. No version column.
14. I1 (entries sum to zero) is enforced by service logic and tests, not a DB trigger.
15. Plain Spring `JdbcClient`, no JPA. jqwik 1.10.1 (test scope, pinned; not BOM-managed).
16. Ledger tests share one Postgres container and one application context per JVM (`PostgresTestSupport`); tests create their own accounts.
