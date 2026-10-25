I built LedgerFlow to study a local payments backend with Java 25, Spring Boot, PostgreSQL 17 and Kafka. It has a payment service and a double-entry ledger service, each with its own database schema and user. I use it to study retries, duplicate messages, concurrent payments and recovery after crashes. I built this project with help from AI tools, including Claude Code and Codex.

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

Kafka delivery is at-least-once. Idempotency keys and deduplication make the effects idempotent, so a repeated message does not post twice.

I run all 58 tests (23 payment-service, 21 ledger-service and 14 e2e) with Docker running and JDK 25 selected through `JAVA_HOME`. The tests use real PostgreSQL and Kafka through Testcontainers:

```sh
./mvnw clean verify -fae
```

## Chaos testing

I also run a chaos campaign against the Compose stack. Each run applies load, injects one fault, waits for recovery and then checks the ledger invariants. The campaign covers 9 scenarios, 3 runs each:

- kill-ledger
- kill-payment
- pause-postgres
- restart-kafka
- replay
- hot-account
- crash-before-payment-outbox-commit
- crash-after-publish-before-mark
- crash-after-ledger-commit-before-ack

The latest campaign ran on 2026-10-25 at commit `75c618e`. The host was a MacBook Air with 10 CPUs and 16 GiB RAM running Docker Desktop, with a load of 100 payments/s for 60 s per run. This is a local laptop, not a production environment. The raw results are in `docs/evidence/chaos-campaign-2026-10-25-75c618e.csv`.

| Result | Value |
|---|---|
| Runs passed | 27 of 27 |
| Ledger invariant violations | 0 |
| New dead-letter messages | 0 |
| Payments accepted by the API | 157,083 |
| Client retries (same idempotency key) | 16,734 |
| Payments abandoned after 5 attempts | 896 |

The 896 abandoned payments appeared only in 8 runs: kill-payment (3 of 3), replay (3 of 3) and crash-before-payment-outbox-commit (2 of 3). The other 19 runs had none. The load script counts a payment as abandoned when the client gives up after 5 attempts, one second apart. The invariant checker passed in every run regardless. Accepted plus abandoned does not always add up to 6,000 per run, so I report only what each counter measures and not a success rate.

In the CSV, the `dup_ledger` and `dup_payment` columns count duplicate events absorbed by idempotency. They do not count duplicate payments.

To rerun it, I need bash 4 or newer, k6, jq and Docker:

```sh
RESET=1 bash chaos/run_chaos.sh 27
```

Results are written to `chaos/results/`, which git ignores.

## Scope

This project uses one Kafka broker and one PostgreSQL instance, so it is not highly available and I make no production or high-availability claims. An earlier demo dashboard and a local Kubernetes setup were removed in commit `3605f33`. The docs in `docs/` may still describe them as history.