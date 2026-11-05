# LedgerFlow Roadmap (v2.0)

Phases here are "Roadmap Phase N". Phase numbers in `docs/PLAN.md`, `docs/DECISIONS.md` and `docs/RESULTS.md` refer to the earlier plan.
Lines starting with "Note:" record where the roadmap was aligned with the repo; they do not change its intent.

## Goal
A payments backend that shows production engineering practice: relational correctness, observability, measured performance with
a fixed bottleneck, horizontal scaling, Kubernetes deployment and clear docs. Everything runs locally and for free.

## Baseline
- `v1.3.0-mongodb` = `33cd86d` (main when this roadmap was written; MongoDB 8.0 build).
- `v1.2.0-postgres` tests: 58 (payment-service 23, ledger-service 21, e2e-tests 14), from `docs/evidence/verify-2026-10-25.txt`.
- Current main tests: 66 (payment-service 28, ledger-service 21, e2e-tests 17), as stated in the README.

## Global rules (every phase)
- Never run git commit, push, tag, merge, rebase or reset. I create branches, tags and commits myself. After each logical step, stop and show:
  files changed, test status, and a suggested one-line commit message with no trailers. Wait for me to commit before the next step.
- Do not read docs/evidence/delivery/** or docs/evidence/remediation/**. Read other evidence files only when the task names them.
- Free only: no paid services or accounts beyond GitHub. No HCP Terraform, Grafana Cloud or k6 Cloud. Terraform state is local and git-ignored.
- Do not change financial semantics (idempotency, outbox, dedupe, stored outcomes, invariants) except the documented Phase 3 fix.
- Gate before I open a PR: ./mvnw clean verify, bash scripts/smoke.sh, and the invariant checker, all green.
  Note: in Phase 1a, scripts/smoke.sh is not ported yet. The 1a gate is ./mvnw clean verify, one end-to-end payment on the Compose stack,
  and chaos/verify_invariants.sql (run with psql as in v1.2.0-postgres chaos/lib.sh). The full gate applies from Phase 1b on.
- Long runs (chaos campaigns, k6 benchmarks over 5 minutes) are run by me. Print the exact command and stop.
- Evidence: commit summaries plus raw CSV/JSON, named <what>-<date>-<shortsha>. No multi-thousand-line logs.
- README and docs quote only numbers from committed evidence, with the environment recorded next to them.
- Pin all versions (images, libraries, providers). Container memory limits on every container; JVM heap via -XX:MaxRAMPercentage.
  Note: today's Compose file has no memory limits and uses -Xmx384m (apps) and KAFKA_HEAP_OPTS -Xmx512m. v1.2.0-postgres used the floating
  tag postgres:17. Phase 1a fixes this for Compose. Phase 5 pins the GitHub Actions versions (currently @v4).
- Docker Desktop has 10 GB. Never run Compose and k3d at the same time.
- Record decisions as short ADRs in docs/adr/ (context, decision, alternatives, consequences).
  Note: files are docs/adr/NNNN-<slug>.md. docs/DECISIONS.md is kept as the earlier decision log and is not extended.
- If something blocks you, stop and ask. No workarounds that weaken tests or checks.

## Phase 1a: PostgreSQL core (branch feat/postgres-core, created by me after I tag current main as v1.3.0-mongodb)
Note: done, `v1.3.0-mongodb` → `33cd86d`.
- Restore the PostgreSQL persistence layer from v1.2.0-postgres: JdbcClient repositories, Flyway migrations, infra/postgres/init,
  chaos/verify_invariants.sql and create_accounts.sql, Postgres Testcontainers support, Compose Postgres 17.
  Note: pin Postgres to an exact 17.x image tag, add memory limits to every Compose container, and switch the app JVMs to
  -XX:MaxRAMPercentage (Kafka through KAFKA_HEAP_OPTS).
- Remove MongoDB code, config, scripts and infra.
  Note: this includes infra/mongo/, MongoStore, the Mongo test fixtures, chaos/verify_invariants.js, chaos/create_accounts.js and
  chaos/checker_perf.sh. The v1.2.0-postgres chaos scripts (lib.sh, load.js, run_chaos.sh, bench.sh) are restored. infra/k8s/ and k8s/
  stay untouched in 1a and are MongoDB-only until Phase 1b converts them.
- Bind all published Compose ports (postgres, kafka, payment-service, ledger-service) to 127.0.0.1; kafka's advertised listener stays localhost.
- Every test that existed at v1.2.0-postgres passes. Record test counts per module.
- Done when ./mvnw clean verify is green and the Compose stack processes a payment end to end.

## Phase 1b: port newer features (branch feat/postgres-port, created by me after 1a is merged)
- Port PaymentSecurity (HTTP Basic + account ownership) and its tests, OrderingTest, scripts/smoke.sh, scripts/evidence.py,
  infra/k8s and k8s/ scripts (Postgres StatefulSet instead of MongoDB).
  Note: scripts/smoke.sh runs scripts/demo.sh and scripts/demo.py (tests in scripts/tests/) and uses infra/docker-compose.delivery.yml,
  so those are ported too, together with main's chaos-script changes (credentials in load.js/lib.sh, reset refusal).
  Note: tag v1.1.0 / branch feature/kubernetes-v1.1.0 has a Postgres StatefulSet and init ConfigMap to use as reference.
- Strict velocity on Postgres: table velocity_guards(payer_account_id PK). Inside the payment transaction: INSERT ... ON CONFLICT DO NOTHING,
  then SELECT ... FOR UPDATE on the payer's guard row, then count non-declined payments in the 60 s window. Replays, validation failures and
  declines consume no slot. Keep auth before idempotency lookup.
- Every test from current main passes, adapted to Postgres. Record test counts per module.
- Gate: verify, smoke, k8s smoke, then print the 9-run chaos command (one run per scenario) for me.
- ADR: why PostgreSQL over MongoDB, using only numbers from the committed chaos CSVs of both versions.

## Phase 2: observability (branch feat/observability)
- Compose profile "observability" (opt-in): Prometheus (15 s scrape, 2 d retention), Grafana OSS (provisioned datasources and dashboards
  as JSON in the repo, anonymous read-only locally), Jaeger all-in-one (in-memory, trace count capped).
- Metrics via Micrometer Prometheus registry: HTTP rate/errors/latency; request-to-terminal payment latency histogram; outbox backlog and
  oldest-unpublished age; Kafka consumer lag; reconciler re-requests; duplicates absorbed; DLT messages; ledger posting rate.
- Tracing: Micrometer Tracing with the OpenTelemetry bridge and OTLP exporter straight to Jaeger. No Collector, no Java agent.
  Sampling configurable (default 1.0).
- Trace across the outbox: store traceparent/tracestate on outbox rows in both services; the relay restores the context and sends it in Kafka
  headers; listeners continue the trace. One trace covers HTTP POST -> ledger posting -> result -> payment update.
  Add a test that asserts one trace id across all hops.
- Evidence: docker stats snapshot during one short load run with the profile on.

## Phase 3: performance and horizontal scaling (branch feat/performance)
- perf/: k6 ramping-arrival-rate test to find saturation, then a steady-state test at about 70% of saturation. Report throughput, HTTP
  p50/p95/p99 and request-to-terminal p50/p95/p99. k6 runs on the host.
- Stop point 1: after the k6 scripts are written, print the baseline commands and stop. Continue only after I commit the results
  (perf/results/baseline-<date>-<sha>/ with environment, container limits and settings).
- Find the bottleneck with metrics and traces (expected: ledger consumer, 3 partitions x 1 thread, one transaction per event). Pick the smallest
  correct fix (for example more partitions plus replicas, or batched postings in one transaction) in its own commit, with an ADR.
  Lock ordering and all invariants unchanged.
  Note: "3 partitions x 1 thread" means 3 partitions with listener concurrency 3 (one thread per partition; ledger-service.yml concurrency: 3).
- Horizontal scaling: 2 payment-service and 3 ledger-service replicas with enough partitions. Add chaos scenario kill-ledger-replica.
  Note: Compose binds ledger-service and payment-service to fixed host ports 8082/8081, so replicas need that changed.
- Stop point 2: after the fix and scaling changes, print the after-run and final campaign commands and stop.
- Commit the after results and a before/after table.
- Final full chaos campaign on the final code (all scenarios, 3 runs each, including the new one), run by me.

## Phase 4 (optional, not part of v2.0): Terraform for the local k3d cluster, only if needed later.

## Phase 5: README, docs and release (branch docs/v2)
- README readable in 60 seconds: one-line summary, Mermaid architecture diagram, headline numbers table (committed evidence only), 5 key design
  decisions linking ADRs, 3-command quick start, short scope and limits, AI-tools disclosure kept.
- README results as tables from committed evidence plus one Grafana screenshot (I take it).
- CI: build both Docker images in GitHub Actions.
- Fix stale numbers and test counts everywhere. Link older docs instead of repeating them.
- Release notes for tag v2.0.0 (I create the GitHub Release).
  Note: the project version (pom.xml 1.1.0) and k8s image tags (1.1.0-local) are bumped to 2.0.0 first.
