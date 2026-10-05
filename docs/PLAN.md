# Plan

## Phase 0 — Foundation
Maven multi-module, minimal Spring Boot services, Postgres + Kafka (KRaft) via Compose, Flyway, isolated DB users/schemas, Testcontainers smoke tests, CI.
No payments, ledger, idempotency, risk, Kafka clients, outbox, reconciliation, chaos or load tests.

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

## Phase 5 — Chaos, load, performance
Service images (`Dockerfile`) and Compose services; `chaos/bench.sh` (k6 load + drain + invariants), `chaos/run_chaos.sh` (9 fault scenarios under load, invariant gate per run), `chaos/create_accounts.sql` seeding. Measured first; one evidence-backed optimization (outbox poll interval).

## Phase 6 — Local final package
`docker compose -f infra/docker-compose.yml up --build` runs Postgres, Kafka and both services (Actuator health UP); README; docs updated; CI keeps running `./mvnw -B verify` (no image builds or publishing).

Status: Phases 0–7 complete. No cloud deployment, Redis, auth, gateway or extra services by design. Kubernetes was added later as Phase 8 (optional, local only).

## Phase 7 — Demo experience
`./start.sh` / `./stop.sh` / `./reset.sh` (Compose project `ledgerflow-demo` + `infra/docker-compose.demo.yml`); static dashboard served by payment-service under the `demo` profile; narrow demo-only endpoints in both services; `demo/` self-checking scenario scripts (application races and shell-driven infrastructure faults).
Out of scope: changes to the payment/Kafka/ledger design, new services, HTTP control of infrastructure.

## Phase 8 — Optional local Kubernetes (k3d)
Plain Kubernetes YAML in `infra/k8s/` on a single-server k3d cluster (`k8s/start.sh`, `status.sh`, `logs.sh`, `demo-restart.sh`, `stop.sh`): namespace, PostgreSQL and Kafka StatefulSets with PVCs, one Deployment and Service per application service, ConfigMap, Secret, startup/readiness/liveness probes, `demo,k8s` Spring profiles. Dashboard on localhost:8081, ledger health on localhost:8082.
Verified on this cluster: application pod replacement, PostgreSQL and Kafka pod restarts on preserved PVCs, the existing pod-recovery demo, and the Compose demo still working.
Out of scope: cloud or managed Kubernetes, Helm, Terraform, GitOps, service mesh, operators, autoscaling, multi-node clusters, and any high-availability claim. Compose remains the primary local stack.
