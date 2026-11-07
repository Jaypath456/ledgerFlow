# ADR 0004: Observability: management port, opt-in profile, traces across the outbox

Status: accepted (Roadmap Phase 2), 2026-11-07.

## Context

Phase 2 adds Prometheus metrics, Grafana dashboards and OpenTelemetry traces to Jaeger (`docs/ROADMAP.md`). Four constraints shaped
the design:

- Payment-service requires authentication on every request (`anyRequest().authenticated()`), and Prometheus should scrape without
  credentials.
- Today's actuator paths on the API ports (8081/8082) are used by:
  - the Compose healthchecks and the k8s probes
  - `chaos/lib.sh` (`healthy`, `metric`), `scripts/demo.py` and the k8s scripts
- Chaos and load runs must behave exactly as before when the observability stack is not running.
- A payment crosses two outboxes. The transaction that writes an outbox row ends before the relay publishes it, so no thread carries
  the trace context from the write to the publish.

## Decision

- **Opt-in management port.** `management.server.port` is set only by `infra/docker-compose.observability.yml` (9081 and 9082) and is
  never published to the host.
  - In Boot 4.1.1 the security filter chain also applies on a separate management port
    (`ServletManagementChildContextConfiguration`). Payment-service therefore gets a second `SecurityFilterChain`, `@Order(0)`, active
    only when the property is set.
  - That chain matches requests by local port: it permits `/actuator/health/**` and `/actuator/prometheus` and denies everything
    else. The API chain is unchanged.
  - Without the overlay, actuator stays on the API ports with today's rules.
- **Profile plus overlay.** Prometheus, Grafana OSS and Jaeger are services in `infra/docker-compose.yml` with
  `profiles: [observability]`, so without the profile Compose starts the same four services as before.
  - A profile cannot change other services' environment, so the overlay switches the apps on: management port, OTLP endpoint, and
    healthchecks on the management port.
  - Both go together: `docker compose -f infra/docker-compose.yml -f infra/docker-compose.observability.yml --profile observability up`.
  - `chaos/bench.sh` supports `OBSERVABILITY=1`. `chaos/run_chaos.sh` refuses it, because its probes read actuator on the API port.
  - Images are pinned (`prom/prometheus:v3.15.1`, `grafana/grafana-oss:12.4.3`, `jaegertracing/jaeger:2.23.0`). Each has a memory
    limit and `GOMEMLIMIT`. Host ports are bound to 127.0.0.1, and Jaeger's OTLP port is not published at all.
  - Grafana provisions its datasources and dashboard from the repo, with anonymous Viewer access and the login form off.
- **Explicit modules instead of the starter.** The services depend on `micrometer-registry-prometheus`,
  `spring-boot-micrometer-tracing-opentelemetry`, `micrometer-tracing-bridge-otel` and `opentelemetry-exporter-otlp`, all with versions
  from the Boot 4.1.1 BOM.
  - `spring-boot-starter-opentelemetry` is not used: it also brings `micrometer-registry-otlp`, and
    `management.otlp.metrics.export.enabled` defaults to true, which would push metrics as well.
  - Spans are exported only when `MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT` is set. The exporter is created on that
    property alone, so it is set only in the overlay and never as an empty placeholder.
  - Sampling is `TRACING_SAMPLING_PROBABILITY` (default 1.0; Boot's own default is 0.1).
  - Scheduled-task observations are dropped: the relay runs every 10 ms and would otherwise create one trace per poll.
- **Trace context on the outbox rows.** Both outbox tables get nullable `traceparent` and `tracestate` text columns, with no default
  (payments V4, ledger V5).
  - `OutboxRelay.enqueue` stores the current W3C context inside the caller's transaction. Any tracing error stores NULL.
  - The relay reads the two columns with the same `WHERE`, `ORDER BY id`, `LIMIT` and `FOR UPDATE SKIP LOCKED`. It starts one
    `outbox publish <topic>` span per row with the stored context as parent (a new trace when NULL), and calls `kafka.send` inside it.
  - It then waits for all acks, hits the fault point and marks the rows, in the same order as before. All sends are still issued
    before any ack is awaited.
  - Spring Kafka observation adds producer and consumer spans and carries `traceparent` in the record headers.
  - One trace then covers HTTP POST → payment outbox → ledger consumption and posting → ledger result outbox → payment update. The
    test is `e2e-tests` `TracePropagationTest`.
- **Metrics:**
  - `ledgerflow.payment.e2e` histogram: `updated_at - created_at`, returned by the existing UPDATE and recorded after commit.
  - `ledgerflow.outbox.backlog` and `ledgerflow.outbox.oldest.unpublished.age`: one read-only query every 15 s on a dedicated thread,
    with a 2 s statement timeout. They take no locks and run neither per scrape nor on the shared scheduler.
  - `ledgerflow.ledger.outcomes`: after commit, new outcomes only.
  - `ledgerflow.dlt.published`: a retry listener on the existing error handler.
  - Built-in: the HTTP server histogram and Kafka client lag.

## Alternatives

- **Scrape the API port with a metrics identity.** Puts a credential into the Prometheus config, and the metrics then share the API's
  auth path. Rejected in favour of an unpublished port.
- **Permit `/actuator/prometheus` on the API port.** Weakens the API's rules. Rejected.
- **Management port always on.** Breaks every host-side consumer of `/actuator` on 8081/8082 (chaos, smoke, k8s scripts) for runs
  that don't need it. Rejected for now.
- **An OpenTelemetry Collector or the Java agent.** Not needed for one local Jaeger, and the roadmap rules them out.
- **Kafka headers only, no columns.** The publish happens in a later transaction on another thread, and only the database row connects
  the two.

## Consequences

- Without the overlay nothing is exported, but spans are still created at sampling 1.0 in Compose, k8s and the chaos runs. A
  single-run comparison of sampling 1.0, the profile on, and sampling 0 is in
  `docs/evidence/observability-load-2026-11-07-0c5dc1d.txt`. It showed no consistent difference; results from Phase 3 on must state
  the sampling setting.
- With the overlay, actuator is no longer on 8081/8082. `chaos/run_chaos.sh` and `scripts/smoke.sh` are run without it.
- Kafka consumer lag is reported by the consumers themselves and disappears while a consumer is down. The chaos runner's broker-side
  lag check stays the authority.
- Reconciler re-requests and replayed messages do not belong to the original request's trace: re-requests start new traces, and
  replays add consumer spans to old traces.
- DLT and duplicate counters restart with the service. DLT topic offsets and `processed_events` stay the authoritative records.
- Outbox rows written before V4/V5 have NULL trace columns and publish as before, tested in `PaymentTracingTest`.
