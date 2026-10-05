# AGENTS.md

LedgerFlow: local event-driven payments backend; financial correctness under retries, duplicate Kafka messages, crashes, DB failures, concurrency.

- Stack: Java 25, Spring Boot 4.1, Maven multi-module (`common`, `payment-service`, `ledger-service`), PostgreSQL 17, Kafka (KRaft), Flyway, Testcontainers.
- Local only. No cloud, Kubernetes, frontend framework, OAuth, gateway, Debezium, Avro, Schema Registry, Grafana, OpenTelemetry, or extra services. Exception (Phase 7): the static, demo-profile-only dashboard served by payment-service (plain HTML/CSS/JS, no build step).
- Each service accesses only its own DB schema/user (`payments`/`payment_user`, `ledger`/`ledger_user`).
- Work phase by phase per `docs/PLAN.md`; do not build later-phase features early.
- Build/test: `./mvnw verify` (Docker required). Infra: `docker compose -f infra/docker-compose.yml up -d`.
- Run with JDK 25 (`JAVA_HOME`). Record decisions in `docs/DECISIONS.md`, outcomes in `docs/RESULTS.md`.
