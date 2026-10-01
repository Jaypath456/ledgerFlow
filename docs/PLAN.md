# Plan

## Phase 0 — Foundation (this phase)
Maven multi-module, minimal Spring Boot services, Postgres + Kafka (KRaft) via Compose, Flyway, isolated DB users/schemas, Testcontainers smoke tests, CI.
No payments, ledger, idempotency, risk, Kafka clients, outbox, reconciliation, chaos or load tests.

## Later phases
Defined by the owner; not started.

## Phase 1 — Ledger core (ledger-service only)
Double-entry `LedgerService.post` on PostgreSQL: `accounts`, `ledger_transactions`, `ledger_entries`, seeded via Flyway; row locks in ascending account-id order; overdraft prevention; invariants I1, I2, I4, I5 tested incl. 200-way concurrency and a jqwik property.
Out of scope: Kafka, payment-service logic, idempotent replay, outbox, risk, reconciliation, chaos/load tests.
