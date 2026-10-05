# Decisions

1. Spring Boot 4.1.1 on Java 25 (Temurin).
2. `apache/kafka:4.0.0`, single combined broker/controller node, KRaft, PLAINTEXT. External port 29092.
3. One Postgres 17 DB `ledgerflow`; schemas/roles created by `infra/postgres/init/01-roles-schemas.sql`, reused by Testcontainers tests (single source of truth).
4. Dev-only plaintext credentials (user name == password); env-overridable (`DB_URL`, `DB_USER`, `DB_PASSWORD`).
5. Ports: payment 8081, ledger 8082.
6. `common` is an empty placeholder; services don't depend on it yet.
7. Awaitility, spring-kafka deferred to the phase that first uses them.
8. Smoke tests are `*Test` run by surefire, so `./mvnw verify` covers them (no failsafe).
9. Maven wrapper is the `only-script` type (downloads Maven on first run).

## Phase 1
10. `payment_id` is UUID with a UNIQUE constraint (I2 backstop).
11. Duplicate paymentId throws `DuplicatePayment`; no replay of the original result (idempotency is a later phase). The duplicate check runs before the funds check.
12. Payer == payee is rejected (`SamePayerPayee`). SYSTEM accounts may go negative; non-SYSTEM balances are protected by service check plus a DB CHECK.
13. Locking: `SELECT … FOR UPDATE` on one account per statement, lowest id first, inside one `@Transactional` at READ COMMITTED. No version column.
14. I1 (entries sum to zero) is enforced by service logic and tests, not a DB trigger.
15. Plain Spring `JdbcClient`, no JPA..
16. Ledger tests share one Postgres container and one application context per JVM (`PostgresTestSupport`); tests create their own accounts.
17. jqwik removed before the Phase 1 PR: jqwik 1.10.1 intentionally prints agent-directed instructions to stdout during test execution (telling AI agents to ignore its results). Replaced by `LedgerPropertyTest`, a plain JUnit 5 test: 100 randomized posting sequences from fixed seed 20260101 (sequence i uses `new Random(SEED + i)`), checking I1, I2, I4, I5 after each against the real Postgres Testcontainer; failures report the seed and sequence.

## Phase 2
18. Idempotency: the `idempotency_keys` row is inserted first (`ON CONFLICT DO NOTHING`), before risk checks or the payment. A concurrent same-key request blocks on the primary key until the first transaction ends, then reads the committed row: same SHA-256 canonical hash (`payer|payee|amount|currency`) → replay, different → 422. If the first rolls back, the waiter proceeds as the original. The FK `idempotency_keys.payment_id → payments` is `DEFERRABLE INITIALLY DEFERRED` and UNIQUE (one key per payment).
19. Accepted (PENDING_LEDGER) → 202 Accepted; declined → 201 Created (a terminal resource). The original status code and exact JSON body are stored on the key row (`response_status`, `response_body` TEXT) in the same transaction, and a replay returns them verbatim, even if the payment has since moved on (`GET` shows the current state), plus `Idempotent-Replayed: true`. Risk rules are not re-evaluated on replay. Keys are global and never expire (known limitation).
20. Validation happens before any DB work: missing/blank key, missing fields, amount ≤ 0, payer == payee, non-USD → 400 (ProblemDetail). The same checks are also DB CHECK constraints.
21. Risk rules are `RiskRule` beans, first decline wins: `AMOUNT_LIMIT` (> 1,000,000 minor = $10,000), `BLOCKED_ACCOUNT` (`ledgerflow.risk.blocked-accounts`, payer or payee), `VELOCITY_LIMIT` (payer already has ≥ `ledgerflow.risk.velocity-max` (default 5) non-declined payments in 60 s). Velocity is configurable so load/chaos runs can reach the ledger; it is a soft limit under concurrent requests from one payer.
22. Declined payments are stored (status DECLINED + reason) with their idempotency key, but no outbox row.
23. Outbox `payload` is the `PaymentRequested` JSON (record in `common`), `event_key` = payer account id, `topic` = `payments.requested`. Nothing is sent to Kafka in Phase 2.
