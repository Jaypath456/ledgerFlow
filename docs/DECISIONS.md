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
