package com.ledgerflow.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ledgerflow.common.LedgerPosted;
import com.ledgerflow.common.Topics;
import com.ledgerflow.common.observability.OutboxMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

/** Payment-side metrics: request-to-terminal latency and the outbox backlog gauges. */
class PaymentMetricsTest extends PaymentTestSupport {

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final AtomicLong ACCOUNTS = new AtomicLong(70_000);

    @Autowired
    PaymentService payments;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    DataSource dataSource;

    @Autowired
    JsonMapper json;

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    MeterRegistry meters;

    @Autowired
    OutboxMetrics outboxMetrics;

    @Test
    void requestToTerminalLatencyIsRecordedOncePerPayment() {
        long before = e2eCount();
        double duplicatesBefore = meters.counter("ledgerflow.events.duplicate").count();
        UUID id = accept(ACCOUNTS.incrementAndGet());
        String posted = json.writeValueAsString(LedgerPosted.of(id, 11));

        kafka.send(Topics.LEDGER_RESULTS, id.toString(), posted);
        await().atMost(WAIT).until(() -> payments.find(id).orElseThrow().status() == PaymentStatus.COMPLETED);
        await().atMost(WAIT).until(() -> e2eCount() == before + 1);

        kafka.send(Topics.LEDGER_RESULTS, id.toString(), posted); // redelivery: deduped, no second sample
        await().atMost(WAIT).until(() -> meters.counter("ledgerflow.events.duplicate").count() > duplicatesBefore);
        assertThat(e2eCount()).isEqualTo(before + 1);
    }

    @Test
    void outboxBacklogGaugesCountUnpublishedRowsWithoutBlockingOnLocks() throws Exception {
        // Hold a row lock on one unpublished, 120 s old row: the relay skips it (SKIP LOCKED) and the gauges still see it.
        try (Connection holder = lockFreshUnpublishedRow()) {
            outboxMetrics.refresh();
            assertThat(gauge("ledgerflow.outbox.backlog")).isGreaterThanOrEqualTo(1);
            assertThat(gauge("ledgerflow.outbox.oldest.unpublished.age")).isGreaterThanOrEqualTo(120);
            try (var mark = holder.prepareStatement("UPDATE outbox SET published_at = now() WHERE id = ?")) {
                mark.setLong(1, lockedId);
                mark.executeUpdate();
            }
            holder.commit();
        }
        await().atMost(WAIT).untilAsserted(() -> {
            outboxMetrics.refresh();
            assertThat(gauge("ledgerflow.outbox.oldest.unpublished.age")).isLessThan(120);
        });
    }

    private long lockedId;

    private Connection lockFreshUnpublishedRow() throws SQLException {
        for (int attempt = 0; attempt < 50; attempt++) {
            long id = jdbc.sql("""
                            INSERT INTO outbox (aggregate_id, topic, event_key, payload, created_at)
                            VALUES (?, ?, 'outbox-metrics-test', '{"test":"outbox-metrics"}'::jsonb, now() - interval '120 seconds')
                            RETURNING id""")
                    .params(UUID.randomUUID(), Topics.PAYMENTS_REQUESTED).query(Long.class).single();
            Connection c = dataSource.getConnection();
            c.setAutoCommit(false);
            try (var lock = c.prepareStatement("SELECT id FROM outbox WHERE id = ? AND published_at IS NULL FOR UPDATE NOWAIT")) {
                lock.setLong(1, id);
                if (lock.executeQuery().next()) {
                    lockedId = id;
                    return c;
                }
            } catch (SQLException relayHoldsIt) {
                // The relay locked it first; it will publish it. Try a new row.
            }
            c.rollback();
            c.close();
        }
        throw new IllegalStateException("could not lock an unpublished outbox row before the relay published it");
    }

    private double gauge(String name) {
        return meters.get(name).gauge().value();
    }

    private long e2eCount() {
        return meters.get("ledgerflow.payment.e2e").tag("status", "COMPLETED").timer().count();
    }

    private UUID accept(long payer) {
        var result = payments.create(UUID.randomUUID().toString(), new PaymentRequest(payer, ACCOUNTS.incrementAndGet(), 100L, "USD"));
        assertThat(result.status()).isEqualTo(202);
        return UUID.fromString(json.readTree(result.body()).get("id").asString());
    }
}
