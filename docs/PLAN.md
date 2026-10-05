# Plan

## Phase 0 — Foundation (this phase)
Maven multi-module, minimal Spring Boot services, Postgres + Kafka (KRaft) via Compose, Flyway, isolated DB users/schemas, Testcontainers smoke tests, CI.
No payments, ledger, idempotency, risk, Kafka clients, outbox, reconciliation, chaos or load tests.

## Later phases
Defined by the owner; not started.

## Phase 1 — Ledger core (ledger-service only)
Double-entry `LedgerService.post` on PostgreSQL: `accounts`, `ledger_transactions`, `ledger_entries`, seeded via Flyway; row locks in ascending account-id order; overdraft prevention; invariants I1, I2, I4, I5 tested incl. 200-way concurrency and a seeded randomized (JUnit) invariant test.
Out of scope: Kafka, payment-service logic, idempotent replay, outbox, risk, reconciliation, chaos/load tests.

## Phase 2 — Payment API, idempotency, outbox (payment-service only)
`POST /api/payments` (Idempotency-Key required) and `GET /api/payments/{id}`; tables `payments`, `idempotency_keys`, `outbox`, `processed_events`; risk rules (amount limit, velocity, blocked accounts); accepted payment + key + PaymentRequested outbox row in one transaction, declined payments get no outbox row.
Out of scope: Kafka clients, outbox relay, consumers, reconciliation, chaos/load.

## Phase 3 — Kafka, outbox relay, idempotent consumers
Shared versioned events (`PaymentRequested`, `LedgerPosted`, `LedgerRejected`) in `common`; topics `payments.requested` (key payer) and `ledger.results` (key paymentId), 3 partitions, DLT after 3 attempts; outbox relays in both services; ledger consumer (dedupe + posting + outcome + result outbox in one transaction); payment result consumer; `e2e-tests` module running both services.
Out of scope: reconciliation, fault injection, invariant SQL, chaos/load, service containers.

## Phase 4 — Recovery, reconciliation, fault injection
Reconciler for PENDING_LEDGER payments older than 30 s; fault injection points under the `chaos` profile; `chaos/verify_invariants.sql` (I1–I6) proven against healthy data and controlled corruption.
Out of scope: chaos runner, load tests, service containers.
