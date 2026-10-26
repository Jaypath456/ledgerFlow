# AGENTS.md

LedgerFlow: local event-driven payments backend; financial correctness under retries, duplicate Kafka messages, crashes, DB failures, concurrency.

- Stack: Java 25, Spring Boot 4.1.1, Maven multi-module (`common`, `payment-service`, `ledger-service`, `e2e-tests`), MongoDB 8.0, Kafka (KRaft), Testcontainers.
- Local only: Docker Compose or a single-node k3d Kubernetes cluster, one replica per service. No cloud, frontend framework, OAuth, gateway, Debezium, Avro, Schema Registry, Grafana, OpenTelemetry, or extra services.
- MongoDB is the only database: single-node replica set `rs0`, databases/users `payments`/`payment_user` and `ledger`/`ledger_user`. Each service accesses only its own database. Use MongoTemplate, driver `session.withTransaction(...)`, startup unique indexes, and long minor-unit amounts.
- Work phase by phase per `docs/PLAN.md`; do not build later-phase features early.
- Build/test: `./mvnw verify` (Docker required). Infra: `docker compose -f infra/docker-compose.yml up -d`. Kubernetes: `bash k8s/start.sh`, `bash k8s/smoke.sh`, `bash k8s/stop.sh` (deletes the cluster and its data).
- Run with JDK 25 (`JAVA_HOME`). Record decisions in `docs/DECISIONS.md`, outcomes in `docs/RESULTS.md`.
