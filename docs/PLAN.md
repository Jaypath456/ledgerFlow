# Plan

> **PostgreSQL version.** Unless a section says otherwise, this document describes the PostgreSQL version of LedgerFlow, tagged `v1.2.0-postgres`. LedgerFlow now runs on MongoDB (single-node replica set) and on a local k3d cluster. See the section at the end of this file and the README.


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

Status: Phases 0–6 are the current scope on `main`. Phases 7 (demo dashboard) and 8 (local Kubernetes) were built and later removed in `3605f33`; they remain on the `phase-7-demo-experience` and `feature/kubernetes-v1.1.0` branches. No cloud deployment, Redis, auth, gateway or extra services by design.

## Phase 7 — Demo experience

> **Removed in `3605f33`.** The demo dashboard and the local Kubernetes setup are no longer on `main`. The code is still available on the `phase-7-demo-experience` and `feature/kubernetes-v1.1.0` branches and in tags `v1.0.0` (Phase 7) and `v1.1.0` (Phases 7 and 8). Test counts in this section include tests that were removed with them.
`./start.sh` / `./stop.sh` / `./reset.sh` (Compose project `ledgerflow-demo` + `infra/docker-compose.demo.yml`); static dashboard served by payment-service under the `demo` profile; narrow demo-only endpoints in both services; `demo/` self-checking scenario scripts (application races and shell-driven infrastructure faults).
Out of scope: changes to the payment/Kafka/ledger design, new services, HTTP control of infrastructure.

## Phase 8 — Optional local Kubernetes (k3d)

> **Removed in `3605f33`.** The demo dashboard and the local Kubernetes setup are no longer on `main`. The code is still available on the `phase-7-demo-experience` and `feature/kubernetes-v1.1.0` branches and in tags `v1.0.0` (Phase 7) and `v1.1.0` (Phases 7 and 8). Test counts in this section include tests that were removed with them.
Plain Kubernetes YAML in `infra/k8s/` on a single-server k3d cluster (`k8s/start.sh`, `status.sh`, `logs.sh`, `demo-restart.sh`, `stop.sh`): namespace, PostgreSQL and Kafka StatefulSets with PVCs, one Deployment and Service per application service, ConfigMap, Secret, startup/readiness/liveness probes, `demo,k8s` Spring profiles. Dashboard on localhost:8081, ledger health on localhost:8082.
Verified on this cluster: application pod replacement, PostgreSQL and Kafka pod restarts on preserved PVCs, the existing pod-recovery demo, and the Compose demo still working.
Out of scope: cloud or managed Kubernetes, Helm, Terraform, GitOps, service mesh, operators, autoscaling, multi-node clusters, and any high-availability claim. Compose remains the primary local stack.

## Status update, October 2026

The PostgreSQL version of Phases 0 to 6 is tagged `v1.2.0-postgres`. LedgerFlow now runs on MongoDB (single-node replica set) in Docker Compose and on a local k3d cluster. Phase 7 (demo dashboard) remains removed. See the README and the end of `docs/RESULTS.md`.

## Authorized local delivery milestones, 2026-10-28

After the completed core phases: establish the unmodified baseline; add a local identity-to-account API boundary; make rolling velocity admission strict using MongoDB transactions; investigate three-partition ordering without speculative key changes; verify recovery and financial invariants; deliver a reproducible CLI demonstration and evidence. This explicitly adds authentication to the local scope. No dashboard, OAuth, external identity service, gateway, additional database, or cloud infrastructure is included. Commands, results and limits are recorded in `docs/LEDGERFLOW_DELIVERY.md`.
