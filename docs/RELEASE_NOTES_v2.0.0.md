# LedgerFlow v2.0.0

LedgerFlow is a local, event-driven payments backend: a payment service, a double-entry ledger service, Kafka between them, and one PostgreSQL 17 database with a schema per service. v2.0.0 is the PostgreSQL line with authentication, strict velocity limits, observability, a measured performance baseline, an opt-in scaled topology and a clean chaos campaign. It is a solo learning project run on one laptop; it is not production-ready. Built with help from AI tools, including Claude Code and Codex.

## Highlights

- **PostgreSQL 17 is the only database** (MongoDB build: tag `v1.3.0-mongodb`). Flyway migrations, `JdbcClient`, constraints for part of the invariants. Why: [ADR 0001](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/adr/0001-postgres-over-mongodb.md).
- **HTTP Basic authentication with account ownership** and an exact **per-payer velocity limit** (guard row, [ADR 0002](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/adr/0002-strict-velocity-guard-row.md)).
- **Observability, opt-in:** Prometheus, Grafana OSS and Jaeger; trace context carried through both outboxes so one trace spans both services; scraping on an unpublished management port ([ADR 0004](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/adr/0004-observability.md)).
- **Performance work:** a k6-based toolkit in `perf/`, a measured baseline, a bottleneck diagnosis, and a negative result for more partitions ([ADR 0005](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/adr/0005-ledger-throughput.md)).
- **Scaled topology, opt-in:** 2 payment and 3 ledger replicas behind HAProxy with 6 partitions, and a `kill-ledger-replica` chaos scenario ([ADR 0006](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/adr/0006-horizontal-scaling.md)).
- **Chaos runner:** a crash scenario whose fault never fired is reported as not passed and re-run; duplicate counts are read before services are recreated.
- **Kafka pinned to 4.0.2** after a broker coordinator error on 4.0.0.
- **CI:** GitHub Actions runs `./mvnw -B verify` and builds both service images (no push).

## Measured numbers

All numbers are from one MacBook Air (M4) with Docker Desktop (VM with 10 CPUs and 9.7 GiB), single runs unless stated. Load and chaos runs authenticate with credentials hashed at bcrypt cost 4 ([ADR 0003](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/adr/0003-load-test-credentials.md)) and a raised velocity limit so the load reaches the ledger. The full record is [RESULTS.md](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/RESULTS.md).

| Result | Number | Conditions | Source |
|---|---|---|---|
| Tests | 84 (payment-service 39, ledger-service 25, e2e-tests 20), 0 failures | Real PostgreSQL 17.11 and Kafka via Testcontainers | [evidence](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/evidence/verify-postgres-phase3b-2026-11-09-b96d66e.txt) |
| Chaos, Kafka 4.0.2 | 27 of 27 runs passed, 0 invariant violations, every crash scenario fired; `kill-ledger-replica` 3 of 3 | 9 scenarios x 3 runs, 100 payments/s for 60 s; one campaign | [evidence](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/evidence/chaos-postgres-phase3b-2026-11-09-b96d66e.txt) |
| Chaos, Kafka 4.0.0 | 27 of 28 recorded runs passed; 1 failure with a broker coordinator error | Two parts; the failed scenario was re-run; the error did not recur on 4.0.2 in the three broker logs checked | [evidence](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/evidence/chaos-postgres-phase3-2026-11-07-4a4e645.txt) |
| Steady load | 840 payments/s achieved in each of 3 runs, 0 failed; POST p99 19.3 to 30.3 ms; request-to-terminal p99 142 to 202 ms | 180 s measured after a 60 s warm-up; Kafka 4.0.0 | [steady-1](https://github.com/Jaypath456/ledgerFlow/blob/main/perf/results/baseline-2026-11-07-5aa8d89/steady-1/summary.md), [steady-2](https://github.com/Jaypath456/ledgerFlow/blob/main/perf/results/baseline-2026-11-07-5aa8d89/steady-2/summary.md), [steady-3](https://github.com/Jaypath456/ledgerFlow/blob/main/perf/results/baseline-2026-11-07-5aa8d89/steady-3/summary.md) |
| Steady load, re-run | 840 payments/s in each of 3 runs, 0 failed; POST p99 11.0 to 13.7 ms; request-to-terminal p99 71 to 84 ms | Kafka 4.0.2, after a host-state gate; not a like-for-like comparison with the row above | [summary](https://github.com/Jaypath456/ledgerFlow/blob/main/perf/results/p3b-2026-11-09-623565f/summary.md) |
| Saturation | Sustained 1,200 payments/s, first saturated step 1,300/s, best achieved 1,641.8/s | One ramp; the best rate was reached while a backlog grew (a peak, not capacity); no request failed | [ramp](https://github.com/Jaypath456/ledgerFlow/blob/main/perf/results/baseline-2026-11-07-5aa8d89/ramp-2/summary.md) |
| Shipped password hashing | One run offered 100 payments/s and achieved 54.0/s | bcrypt cost 10, the shipped default; the reason load runs use cost 4 | [ADR 0003](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/adr/0003-load-test-credentials.md) |
| Smoke | 7 of 7 scenarios, invariants I1 to I6 clean after each | Fresh Compose project | [evidence](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/evidence/smoke-postgres-phase3b-2026-11-09-b96d66e.txt) |

## Known limits

- One laptop, one Kafka broker, one PostgreSQL node, no failover. Authentication is HTTP Basic with in-memory demo credentials.
- Not measured: whether the database commit path (`synchronous_commit`) is the shared limit, and the throughput of the scaled topology. The recovery times of the `kill-ledger-replica` runs and of the Kafka 4.0.2 campaign were taken with the host under memory pressure and are not performance numbers.
- Six partitions with six consumer threads gave no clear gain; the default stays at three partitions and three threads.
- The strict velocity limit serializes requests of one payer.
- The Maven project version is still 1.1.0 and the Kubernetes image tags are `1.1.0-local`: the roadmap's version bump to 2.0.0 was not done.
- The Kubernetes smoke (4 of 4 checks on local k3d) was last run in Roadmap Phase 1b ([evidence](https://github.com/Jaypath456/ledgerFlow/blob/main/docs/evidence/k8s-smoke-postgres-phase1b-2026-11-05-2b58c27.txt)).
- Not production-ready: it would still need TLS and a real identity provider, managed secrets, replicated Kafka and PostgreSQL with backups and tested failover, alerting tied to service-level objectives, and capacity measured on real hardware.
