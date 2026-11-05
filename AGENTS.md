# AGENTS.md

LedgerFlow: local event-driven payments backend; financial correctness under retries, duplicate Kafka messages, crashes, DB failures, concurrency.

- Stack: Java 25, Spring Boot 4.1.1, Maven multi-module (`common`, `payment-service`, `ledger-service`, `e2e-tests`), PostgreSQL 17, Kafka (KRaft), Flyway, Testcontainers.
- PostgreSQL 17 is the only database: one database `ledgerflow`, schemas/roles `payments`/`payment_user` and `ledger`/`ledger_user` (`infra/postgres/init`). Each service accesses only its own schema. Use `JdbcClient`, Flyway migrations, DB constraints and long minor-unit amounts. Until Phase 1a is merged, `main` still runs MongoDB 8.0 (tag `v1.3.0-mongodb`).
- Local and free: Docker Compose or a local k3d cluster. No paid services or accounts beyond GitHub (no HCP Terraform, Grafana Cloud or k6 Cloud). No frontend framework, OAuth, gateway, Debezium, Avro or Schema Registry.
- Allowed only in their roadmap phase: OpenTelemetry (Micrometer Tracing bridge, OTLP), Prometheus, Grafana OSS and Jaeger (Phase 2, opt-in Compose profile); multiple service replicas (Phase 3); GitHub Actions CI that runs Maven verify and builds both Docker images (Phase 5); Terraform against the local k3d context only, with local git-ignored state, only in the optional Phase 4 (not part of v2.0). No Trivy or GitHub Pages in v2.0.
- Work phase by phase per `docs/ROADMAP.md` (its global rules apply to every phase); do not build later-phase features early. `docs/PLAN.md` records the earlier phases.
- Do not change financial semantics (idempotency, outbox, dedupe, stored outcomes, invariants) except the documented Phase 3 fix.
- Git: never commit, push, tag, merge, rebase or reset. After each logical step, stop and show the files changed, test status and a one-line commit message with no trailers.
- Do not read `docs/evidence/delivery/**` or `docs/evidence/remediation/**`. Read other evidence only when the task names it.
- Pin all versions. Every container gets a memory limit; set JVM heap with `-XX:MaxRAMPercentage`. Never run Compose and k3d at the same time (Docker Desktop has 10 GB).
- The user runs long jobs (chaos campaigns, k6 runs over 5 min): print the exact command and stop.
- Build/test: `./mvnw clean verify` (Docker required). Infra: `docker compose -f infra/docker-compose.yml up -d`. Kubernetes: `bash k8s/start.sh`, `bash k8s/smoke.sh`, `bash k8s/stop.sh` (deletes the cluster and its data). PR gate: verify, `bash scripts/smoke.sh`, invariant checker.
- Run with JDK 25 (`JAVA_HOME`). New decisions go in `docs/adr/` as short ADRs (`docs/DECISIONS.md` is the earlier log). Outcomes go in `docs/RESULTS.md`. Evidence goes in `docs/evidence/` as `<what>-<date>-<shortsha>`. Docs quote only numbers from committed evidence.
