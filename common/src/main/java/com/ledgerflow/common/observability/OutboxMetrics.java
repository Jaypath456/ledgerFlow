package com.ledgerflow.common.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Outbox backlog gauges. One read-only query per refresh on its own thread, never on a scrape and never on the shared
 * scheduler (the relay and reconciler run there). It takes no row locks, so it cannot delay the relay's FOR UPDATE SKIP
 * LOCKED; a 2 s statement timeout bounds it when the database is slow or paused, and the gauges then read NaN.
 */
@Component
public class OutboxMetrics implements DisposableBean {

    private static final Log log = LogFactory.getLog(OutboxMetrics.class);

    private record Snapshot(double backlog, double oldestAgeSeconds) {}

    private final JdbcClient jdbc;
    private final TransactionTemplate readOnly;
    private final ScheduledExecutorService refresher;
    private volatile Snapshot snapshot = new Snapshot(Double.NaN, Double.NaN);

    OutboxMetrics(JdbcClient jdbc, PlatformTransactionManager tm, MeterRegistry meters,
                  @Value("${ledgerflow.metrics.outbox-refresh-ms:15000}") long refreshMs) {
        this.jdbc = jdbc;
        this.readOnly = new TransactionTemplate(tm);
        this.readOnly.setReadOnly(true);
        Gauge.builder("ledgerflow.outbox.backlog", this, m -> m.snapshot.backlog())
                .description("Outbox rows not yet published to Kafka").register(meters);
        Gauge.builder("ledgerflow.outbox.oldest.unpublished.age", this, m -> m.snapshot.oldestAgeSeconds())
                .description("Age of the oldest unpublished outbox row (0 when none)").baseUnit("seconds").register(meters);
        this.refresher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "outbox-metrics");
            t.setDaemon(true);
            return t;
        });
        refresher.scheduleWithFixedDelay(this::refresh, 0, refreshMs, TimeUnit.MILLISECONDS);
    }

    /** Public for tests; normally called by the refresher thread only. */
    public void refresh() {
        try {
            snapshot = readOnly.execute(status -> {
                jdbc.sql("SET LOCAL statement_timeout = '2s'").update();
                return jdbc.sql("""
                                SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
                                FROM outbox WHERE published_at IS NULL""")
                        .query((rs, n) -> new Snapshot(rs.getLong(1), rs.getDouble(2))).single();
            });
        } catch (RuntimeException e) {
            snapshot = new Snapshot(Double.NaN, Double.NaN);
            log.debug("outbox metrics refresh failed: " + e);
        }
    }

    @Override
    public void destroy() {
        refresher.shutdownNow();
    }
}
