I built LedgerFlow to study a local payments backend with Java 25, Spring Boot, PostgreSQL 17 and Kafka. It has a payment service and a double-entry ledger service, each with its own database schema and user. I use it to study retries, duplicate messages, concurrent payments and recovery after crashes. I built this project with help from AI tools, including Claude Code.

I run the stack with Docker and Docker Compose:

```sh
docker compose -f infra/docker-compose.yml up --build
```

Once both services are ready, I can send $15 from seeded customer account 2 to merchant account 5. Amounts are in cents. I reuse the idempotency key when retrying the same request:

```sh
curl -X POST http://localhost:8081/api/payments \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: study-1' \
  -d '{"payerAccountId":2,"payeeAccountId":5,"amountMinor":1500,"currency":"USD"}'
```

When I submit a payment, payment-service checks risk rules and commits the payment, idempotency record and outbox event together. A relay sends the event to Kafka. Ledger-service deduplicates it, posts balanced entries or records a rejection, and commits the outcome with a result outbox event. That result returns through Kafka so payment-service can mark the payment COMPLETED or FAILED. I read its status at `GET /api/payments/{id}`. The reconciler retries stale pending payments, and the ledger reuses its stored outcome to avoid posting twice.

I run all 58 tests with Docker running and JDK 25 selected through `JAVA_HOME`. The tests use real PostgreSQL and Kafka through Testcontainers:

```sh
./mvnw clean verify -fae
```
