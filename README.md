I built LedgerFlow to study a local payments backend with Java 25, Spring Boot, MongoDB and Kafka. It has a payment service and a double-entry ledger service, each with its own database and user. I use it to study retries, duplicate messages, concurrent payments and recovery after crashes. I built this project with help from AI tools, including Claude Code and Codex.

I run the stack with Docker and Docker Compose:

```sh
docker compose -f infra/docker-compose.yml up --build
```

Once both services are ready, I can send $15 from seeded customer account 2 to merchant account 5. Amounts are in cents. I reuse the idempotency key when retrying the same request:

```sh
curl -u alice:alice-local -X POST http://localhost:8081/api/payments \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: study-1' \
  -d '{"payerAccountId":2,"payeeAccountId":5,"amountMinor":1500,"currency":"USD"}'
```

When I submit a payment, payment-service checks risk rules and commits the payment, idempotency record and outbox event together. A relay sends the event to Kafka. Ledger-service deduplicates it, posts balanced entries or records a rejection, and commits the outcome with a result outbox event. That result returns through Kafka so payment-service can mark the payment COMPLETED or FAILED. I read its status at `GET /api/payments/{id}`. The reconciler retries stale pending payments, and the ledger reuses its stored outcome to avoid posting twice.

Kafka delivery is at-least-once. Idempotency keys and deduplication make the effects idempotent, so a repeated message does not post twice.

The API uses local HTTP Basic identities: `alice` can debit account 2 and `bob` can debit account 4. Payment reads also require credentials and ownership of either participating account. Configure explicit ownership with `ALICE_ACCOUNTS` / `BOB_ACCOUNTS` (comma-separated ids) and passwords with `ALICE_PASSWORD` / `BOB_PASSWORD`. The defaults are `alice-local` / `bob-local`. Application and MongoDB host ports bind to loopback. The default Kafka binding stays unchanged to preserve existing brokers; the isolated smoke runner uses a loopback-only overlay. This is a local CLI demonstration with a trusted host; it has no browser login or external identity provider.

Velocity admission is a strict rolling limit of 5 non-declined payments per payer per 60 seconds, enforced inside the MongoDB payment transaction. Replays consume no additional slot. Ledger failures such as insufficient funds still count as admissions.

I run all 66 tests (28 payment-service, 21 ledger-service, 17 end-to-end) with Docker running and JDK 25 selected through `JAVA_HOME`. The tests use real MongoDB (a single-node replica set) and Kafka through Testcontainers:

```sh
./mvnw clean verify -fae
```

When saving test output, collect a sanitized distributable log:

```sh
python3 -B scripts/evidence.py capture /tmp/ledgerflow-verify.log -- ./mvnw -B verify
```

For a self-checking demonstration of successful/declined payments, idempotency, authentication, concurrent velocity admission and service recovery:

```sh
bash scripts/smoke.sh
# Add real payment-process kill, MongoDB/Kafka interruptions and both dead-letter topics:
bash scripts/smoke.sh --extended
```

Each run creates a new isolated `ledgerflow-delivery-*` project on ports 18081, 18082, 37017 and 39092 (override with `PAYMENT_PORT`, `LEDGER_PORT`, `MONGO_PORT`, `KAFKA_PORT`). Python 3 and Docker Compose with `!override` support are required (verified with Compose 5.6.0). The runner applies `infra/docker-compose.delivery.yml` only to its new project, preserves its containers and volume, sanitizes saved command output, refuses to reuse an existing environment even under Python optimization, and prints the exact non-destructive stop command. Stop an earlier delivery project before reusing its ports. See [delivery evidence and limitations](docs/LEDGERFLOW_DELIVERY.md) and [review remediation](docs/REVIEW_REMEDIATION.md).

## Kubernetes (k3d)

The same system runs on a local single-node k3d cluster (plain YAML in `infra/k8s/`, scripts in `k8s/`): a MongoDB StatefulSet (single-node replica set `rs0`, initialised by `k8s/start.sh`), a Kafka StatefulSet and one Deployment for each service. The APIs are on host ports 8081 and 8082.

```sh
bash k8s/start.sh
bash k8s/smoke.sh
bash k8s/stop.sh
```

`start.sh` builds the images, creates the cluster and waits for MongoDB, Kafka and both services. `smoke.sh` makes a funded payment, an insufficient-funds payment and an idempotent replay, then deletes a ledger-service pod and checks that a new payment still completes. `stop.sh` deletes the cluster and its data.

Requirements: Docker Desktop (I ran it with its memory limit set to 10 GB), k3d and kubectl, and ports 8081 and 8082 free (stop the Compose stack first). The Docker Desktop setting "Use containerd for pulling and storing images" must be off, because with it on `k3d image import` fails with a missing content digest.

The latest from-scratch run (`docs/evidence/k8s-smoke-2026-10-26-a54fed0.txt`) passed every check. It is a local single-node cluster: it shows that the services start, talk to each other and recover from a deleted pod, not high availability.

## Chaos testing

I also run a chaos campaign against the Compose stack. Each run applies load, injects one fault, waits for recovery and then checks the ledger invariants (`chaos/verify_invariants.js`). The campaign covers 9 scenarios, 3 runs each:

- kill-ledger
- kill-payment
- pause-mongodb
- restart-kafka
- replay
- hot-account
- crash-before-payment-outbox-commit
- crash-after-publish-before-mark
- crash-after-ledger-commit-before-ack

The latest campaign ran on 2026-10-26 at commit `6a63c33` on a MacBook Air (10 CPUs, 16 GiB RAM, macOS kernel Darwin 27.0.0) with Docker Desktop, with 100 payments per second for 60 seconds per run. It is a local laptop, not a production environment. The raw results are in `docs/evidence/chaos-campaign-mongodb-2026-10-26-6a63c33.csv`.

| Result | Value |
|---|---|
| Runs passed | 27 of 27 |
| Ledger invariant violations | 0 |
| New dead-letter messages | 0 |
| Payments accepted by the API | 152,815 |
| Client retries (same idempotency key) | 26,297 |
| Payments abandoned after 5 attempts | 3,563 (in 10 of 27 runs) |
| JVM halts recorded by the runner | 12 |

Abandoned payments are requests where the client gave up after 5 attempts, one second apart. They appeared only in these scenarios: `kill-payment`, `replay`, `crash-before-payment-outbox-commit`, `crash-after-publish-before-mark`. The invariant checker passed in every run regardless. Accepted plus abandoned does not always add up to 6,000 per run, so these are counters and not a success rate.

| Scenario (3 runs each) | Abandoned per run | Recovery seconds |
|---|---|---|
| kill-ledger | 0 | 54.2 to 54.6 |
| kill-payment | 386 to 461 | 42.8 to 43.5 |
| pause-mongodb | 0 | 1.8 to 2.3 |
| restart-kafka | 0 | 20.5 to 25.6 |
| replay | 460 to 534 | 29.6 to 32 |
| hot-account | 0 | 1.5 to 2.8 |
| crash-before-payment-outbox-commit | 0 to 361 | 7.6 to 49.8 |
| crash-after-publish-before-mark | 0 to 350 | 27.6 to 65.9 |
| crash-after-ledger-commit-before-ack | 0 | 11.9 to 69.5 |

The earlier PostgreSQL campaign (tag `v1.2.0-postgres`, `docs/evidence/chaos-campaign-2026-10-25-75c618e.csv`) also passed 27 of 27 with 0 violations: 157,083 payments accepted and 896 abandoned. This MongoDB campaign abandoned more client requests (3,563). In `replay`, which restarts both services while load is running, abandoned payments per run were 7 to 27 on PostgreSQL and 460 to 534 on MongoDB, and recovery took 6.4 to 6.6 seconds against 29.6 to 32 seconds. Restarts took longer on the MongoDB build in these runs. I did not measure service startup time separately, so I do not know why.

In the CSV, the `dup_ledger` and `dup_payment` columns count duplicate events absorbed by idempotency. They do not count duplicate payments.

To rerun it, I need bash 4 or newer (macOS ships 3.2, so I install a newer one with Homebrew), k6, jq and Docker:

```sh
PROJECT=ledgerflow-chaos-new bash chaos/run_chaos.sh 27
```

The historical campaign used `RESET=1`; destructive reset flags are now disabled. Choose a new project with free ports for a fresh campaign. Load scripts configure an explicit fixture account inventory for `alice` and send credentials; `PAYMENT_AUTH` overrides the CLI credential pair. No new 27-run campaign is claimed by the delivery verification.

Results are written to `chaos/results/`, which git ignores.

## Scope

This project runs one Kafka broker and one MongoDB node (a single-node replica set, which MongoDB requires for multi-document transactions), so it is not highly available and I make no production, high-availability or failover claims. MongoDB runs with authentication and a replica-set key file; the local root user and the service users have fixed passwords that are for local use only. The earlier PostgreSQL version is tagged `v1.2.0-postgres`. An earlier demo dashboard was removed in commit `3605f33` and remains on the `phase-7-demo-experience` branch.
