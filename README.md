# LedgerFlow

A local, event-driven payments backend built to stay **financially correct** under retries, duplicate Kafka messages, crashes, database stalls and concurrent spending.

Java 25 · Spring Boot 4.1 (MVC, JdbcClient — no JPA) · PostgreSQL 17 · Kafka 4 (KRaft) · Flyway · Testcontainers · k6 · Docker Compose.

## Problem

A payment must move money **exactly once** and always end in a definite state:
- a client retry must not create a second payment;
- a redelivered Kafka message must not post a second ledger transaction;
- a crash at any point must neither lose a payment nor leave it stuck;
- concurrent spending must never overdraw an account.

## Architecture

```
client ──HTTP──▶ payment-service ──(outbox)──▶ Kafka: payments.requested ──▶ ledger-service
                    ▲   (schema payments)                                    (schema ledger)
                    └──────────── Kafka: ledger.results ◀──(outbox)──────────────┘
```

The system has exactly two services. They share one Postgres instance, but each service owns its own schema and DB role, so neither can read the other's data. Both topics have 3 partitions. Failed messages go to `<topic>-dlt` after 3 attempts.

## End-to-end payment flow

1. `POST /api/payments` (with an `Idempotency-Key` header) is validated and risk-checked: amount ≤ $10,000, at most 5 payments per payer per 60 s, no blocked accounts. One DB transaction then stores the payment as `PENDING_LEDGER`, the idempotency record and a `PaymentRequested` outbox row. The response is **202**. A declined payment is stored as `DECLINED` (201) and gets no outbox row.
2. The outbox relay publishes the row to `payments.requested` (keyed by payer).
3. The ledger consumer posts the payment, or rejects it for insufficient funds or an unknown account. It records the outcome and writes a `LedgerPosted` or `LedgerRejected` outbox row, all in one transaction.
4. The ledger relay publishes the result to `ledger.results` (keyed by paymentId).
5. The payment consumer moves the payment to `COMPLETED` or `FAILED`. `GET /api/payments/{id}` shows the current state.

## How correctness is enforced

- **Idempotency:** the key row is inserted first. A concurrent request with the same key blocks on the primary key until the first one commits. The same request (compared by SHA-256 of `payer|payee|amount|currency`) then gets the stored original response back, byte for byte. A different request with that key gets **422**.
- **Transactional outbox:** events are written in the same DB transaction as the state change, and published only by a separate relay. A row is marked published only after the broker acknowledges it. Kafka is never called inside a business transaction.
- **Kafka delivery:** at least once. Producers use `acks=all` with idempotence. Offsets are committed only after the consumer's DB transaction commits. Malformed events and unknown versions go to the DLT after 3 attempts, without blocking the events behind them.
- **Idempotent consumers:** both services insert the eventId into `processed_events` in the same transaction as the side effect, so duplicates are no-ops. The payment only changes state with `WHERE status = 'PENDING_LEDGER'`, so it can transition at most once.
- **Double-entry ledger:** every posting is one transaction with balanced debit and credit entries plus cached balances. `ledger_transactions.payment_id` is UNIQUE.
- **Row locking and race protection:** account rows are locked `FOR UPDATE` in ascending id order, which prevents deadlocks. The funds check runs under that lock, with a DB CHECK as a backstop, so concurrent spending cannot overdraw. A per-payment advisory lock serializes duplicate requests for the same payment.
- **Reconciliation:** payments still `PENDING_LEDGER` after 30 s are re-requested with a fresh eventId. If the ledger already decided, it re-emits its stored outcome (`payment_outcomes`) and does not post again. If it never saw the request, it decides now. Either way the payment converges to the ledger's result.
- **Failure recovery:** a crash before commit leaves nothing, and the client retries with the same key. A crash after commit leaves an outbox row that gets (re)published. A crash before an offset commit causes redelivery, which dedupe absorbs. Design details are in [docs/DECISIONS.md](docs/DECISIONS.md) (34–40).

Invariants are checked by `chaos/verify_invariants.sql` across both schemas:
- **I1:** ledger transactions and the global ledger sum to zero.
- **I2:** each payment is posted at most once, with the correct amount and accounts.
- **I3:** accepted payments end terminal and match the ledger's outcome.
- **I4:** no negative customer balance.
- **I5:** cached balance = sum of entries.
- **I6:** idempotency records are consistent.

## How to run

Requires Docker; building and testing also needs JDK 25.

```
docker compose -f infra/docker-compose.yml up --build
curl localhost:8081/actuator/health    # payment-service
curl localhost:8082/actuator/health    # ledger-service

curl -X POST localhost:8081/api/payments -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
     -d '{"payerAccountId":2,"payeeAccountId":5,"amountMinor":1500,"currency":"USD"}'
curl localhost:8081/api/payments/<id>
```

Seeded accounts:
- 1: SYSTEM.
- 2, 3, 4: customers funded with $1,000, $500 and $250.
- 5, 6: merchants.

## How to test

```
./mvnw verify     # 57 tests, real Postgres + Kafka via Testcontainers (Docker required)
```

- Unit and integration tests run per service.
- `e2e-tests` boots both services against a shared Postgres and Kafka. It covers the end-to-end flows, recovery of stale payments, and the invariant checker on healthy data and on 14 controlled corruptions.
- CI (GitHub Actions) runs the same command.

## Chaos and load

```
FRESH=1 chaos/bench.sh 200 60s    # fresh stack + seed, k6 at 200 payments/s for 60 s, then drain + invariants
chaos/bench.sh 600 60s
chaos/run_chaos.sh 27             # 9 fault scenarios x 3 under 100 payments/s load
```

The chaos scenarios are:
- kill -9 and restart of each service;
- a 10 s Postgres pause;
- a Kafka restart;
- a replay of 3000 already-processed events into each consumer;
- 200 concurrent payments against an account funded for 50;
- real JVM halts at the three fault points (`chaos` Spring profile).

A run fails on any invariant violation.

## Measured results

From [docs/RESULTS.md](docs/RESULTS.md). Everything ran on one laptop (i5-10500H, 7.6 GiB RAM, Ubuntu 24.04) that was under heavy memory pressure, so throughput varied between sessions.

- **Chaos:** 27/27 runs passed, with **0 invariant violations**, **0 DLT records** and 14 real JVM halts. Replays: 18,000 duplicate events absorbed.
- **Hot account:** in each run, exactly 50 of 200 concurrent payments completed; the final balance was 0.
- **Recovery** (median, until a new payment completes):
  - Postgres pause: 0.7 s.
  - Kafka restart: 17.9 s.
  - Event replay: 20.4 s.
  - Killed services: 41–45 s, mostly JVM restart time on this host.
- **Sustained throughput, best session:** 600 payments/s, POST p50/p95/p99 2.6 / 44.3 / 181.3 ms, request-to-terminal p95 496 ms.
- **Sustained throughput, overloaded host:** ≈ 200 payments/s, POST p50/p95/p99 3.0 / 186.2 / 762.7 ms, request-to-terminal p95 999 ms.
- **Optimization:** reducing the outbox poll interval from 50 to 10 ms cut request-to-terminal p95 from 121 to 51 ms at 200/s.

## Limitations

- Single node for everything: one Kafka broker (RF 1), one Postgres instance, local only. There is no auth, TLS or multi-currency support.
- Throughput is bounded by the ledger consumer (3 partitions, one DB transaction per event). The numbers come from one shared, memory-constrained laptop.
- Delivery is at-least-once, with dedupe tables (`processed_events`, outbox) that grow without bound. There is no retention or archiving.
- Idempotency keys are global and never expire. The velocity rule is a soft limit under concurrent requests from one payer.
- The ledger's decision is final: a payment rejected for insufficient funds stays FAILED even if funds arrive before a retry.
- Accounts are created by seed SQL only; there is no account API.
