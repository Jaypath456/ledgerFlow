package com.ledgerflow.common;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Publish in a separate transaction; consumers dedupe resends after a crash. */
public class OutboxRelay {

    private static final Log log = LogFactory.getLog(OutboxRelay.class);
    static final int BATCH = 100;

    private record Row(long id, String topic, String key, String payload, String traceparent, String tracestate) {}

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectProvider<FaultInjector> faults;
    private final ObjectProvider<Tracer> tracer;
    private final ObjectProvider<Propagator> propagator;

    OutboxRelay(JdbcClient jdbc, PlatformTransactionManager tm, KafkaTemplate<String, String> kafka,
                ObjectProvider<FaultInjector> faults, ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(tm);
        this.kafka = kafka;
        this.faults = faults;
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /**
     * Called inside the caller's transaction; the relay picks the row up after commit. The row also keeps the caller's
     * W3C trace context (or none), so the publish continues the caller's trace.
     */
    public static void enqueue(JdbcClient jdbc, UUID aggregateId, String topic, String key, String payloadJson) {
        Map<String, String> trace = currentTraceContext();
        jdbc.sql("""
                        INSERT INTO outbox (aggregate_id, topic, event_key, payload, traceparent, tracestate)
                        VALUES (?, ?, ?, CAST(? AS jsonb), CAST(? AS text), CAST(? AS text))""")
                .params(aggregateId, topic, key, payloadJson, trace.get("traceparent"), trace.get("tracestate"))
                .update();
    }

    /** W3C headers of the current span, or none. Never throws: tracing must not affect the caller's transaction. */
    static Map<String, String> currentTraceContext() {
        try {
            Map<String, String> carrier = new HashMap<>();
            W3CTraceContextPropagator.getInstance().inject(Context.current(), carrier, Map::put);
            return carrier;
        } catch (RuntimeException e) {
            return Map.of();
        }
    }

    @Scheduled(fixedDelayString = "${ledgerflow.outbox.poll-ms:10}")
    public void drain() {
        try {
            while (publishBatch() == BATCH) {
            }
        } catch (RuntimeException e) {
            log.warn("outbox publish failed, will retry: " + e);
        }
    }

    /** Keep rows unpublished unless every send is acknowledged. */
    public int publishBatch() {
        Integer published = tx.execute(status -> {
            List<Row> rows = jdbc.sql("""
                            SELECT id, topic, event_key, payload::text AS payload, traceparent, tracestate FROM outbox
                            WHERE published_at IS NULL ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED""")
                    .param(BATCH)
                    .query((rs, n) -> new Row(rs.getLong("id"), rs.getString("topic"), rs.getString("event_key"),
                            rs.getString("payload"), rs.getString("traceparent"), rs.getString("tracestate")))
                    .list();
            if (rows.isEmpty()) {
                return 0;
            }
            List<Span> spans = new ArrayList<>(rows.size());
            try {
                // Every send is issued before any ack is awaited, as before; each inside its row's restored trace.
                List<CompletableFuture<SendResult<String, String>>> acks = new ArrayList<>(rows.size());
                for (Row r : rows) {
                    Span span = publishSpan(r);
                    spans.add(span);
                    acks.add(inScope(span, () -> kafka.send(r.topic(), r.key(), r.payload())));
                }
                for (var ack : acks) {
                    try {
                        ack.get(30, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new IllegalStateException("Kafka send not acknowledged", e);
                    }
                }
            } finally {
                spans.forEach(OutboxRelay::end);
            }
            faults.ifAvailable(f -> f.hit(FaultInjector.Point.AFTER_OUTBOX_PUBLISH_BEFORE_MARK));
            jdbc.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)")
                    .param("ids", rows.stream().map(Row::id).toList())
                    .update();
            return rows.size();
        });
        return published == null ? 0 : published;
    }

    /** A span whose parent is the row's stored trace context (a new trace when there is none). Null if tracing is unavailable. */
    private Span publishSpan(Row row) {
        try {
            Tracer t = tracer.getIfAvailable();
            Propagator p = propagator.getIfAvailable();
            if (t == null || p == null) {
                return null;
            }
            Span.Builder builder;
            if (row.traceparent() != null) {
                Map<String, String> carrier = new HashMap<>();
                carrier.put("traceparent", row.traceparent());
                if (row.tracestate() != null) {
                    carrier.put("tracestate", row.tracestate());
                }
                builder = p.extract(carrier, Map::get);
            } else {
                builder = t.spanBuilder().setNoParent();
            }
            return builder.name("outbox publish " + row.topic()).tag("messaging.destination.name", row.topic()).start();
        } catch (RuntimeException e) {
            return null;
        }
    }

    private <T> T inScope(Span span, Supplier<T> send) {
        Tracer t = span == null ? null : tracer.getIfAvailable();
        if (t == null) {
            return send.get();
        }
        try (Tracer.SpanInScope ignored = t.withSpan(span)) {
            return send.get();
        }
    }

    private static void end(Span span) {
        if (span != null) {
            span.end();
        }
    }
}
