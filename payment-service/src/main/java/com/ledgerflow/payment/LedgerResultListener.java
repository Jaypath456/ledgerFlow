package com.ledgerflow.payment;

import com.ledgerflow.common.LedgerPosted;
import com.ledgerflow.common.LedgerRejected;
import com.ledgerflow.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Commit dedupe and the payment update together before acknowledging Kafka. */
@Component
class LedgerResultListener {

    private static final Logger log = LoggerFactory.getLogger(LedgerResultListener.class);

    private final JdbcClient jdbc;
    private final JsonMapper json;
    private final TransactionTemplate tx;
    private final Counter duplicates;
    private final Map<PaymentStatus, Timer> requestToTerminal = new EnumMap<>(PaymentStatus.class);

    LedgerResultListener(JdbcClient jdbc, JsonMapper json, PlatformTransactionManager tm, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.json = json;
        this.tx = new TransactionTemplate(tm);
        this.duplicates = meters.counter("ledgerflow.events.duplicate");
        for (PaymentStatus terminal : new PaymentStatus[] {PaymentStatus.COMPLETED, PaymentStatus.FAILED}) {
            requestToTerminal.put(terminal, Timer.builder("ledgerflow.payment.e2e")
                    .description("Payment request to terminal status (updated_at - created_at, as chaos/lib.sh e2e_latency_ms)")
                    .tag("status", terminal.name())
                    .publishPercentileHistogram()
                    .minimumExpectedValue(Duration.ofMillis(1))
                    .maximumExpectedValue(Duration.ofMinutes(5))
                    .register(meters));
        }
    }

    /** Let invalid events reach the retry and dead-letter handler. */
    @KafkaListener(topics = Topics.LEDGER_RESULTS)
    void onMessage(String value) {
        JsonNode node = json.readTree(value);
        String type = node.path("eventType").asString("");
        switch (type) {
            case LedgerPosted.TYPE -> {
                var e = json.treeToValue(node, LedgerPosted.class);
                apply(e.eventId(), e.paymentId(), PaymentStatus.COMPLETED, null);
            }
            case LedgerRejected.TYPE -> {
                var e = json.treeToValue(node, LedgerRejected.class);
                apply(e.eventId(), e.paymentId(), PaymentStatus.FAILED, e.reason());
            }
            default -> throw new IllegalArgumentException("unknown eventType: " + type);
        }
    }

    private void apply(UUID eventId, UUID paymentId, PaymentStatus status, String reason) {
        tx.executeWithoutResult(s -> {
            boolean firstDelivery = jdbc.sql("INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING")
                    .param(eventId).update() == 1;
            if (!firstDelivery) {
                duplicates.increment();
                return;
            }
            // Same update as before; RETURNING only reads the request-to-terminal time of the row it moved.
            Optional<Double> seconds = jdbc.sql("""
                            UPDATE payments SET status = ?, decline_reason = ?, updated_at = now()
                            WHERE id = ? AND status = 'PENDING_LEDGER'
                            RETURNING EXTRACT(EPOCH FROM updated_at - created_at)""")
                    .params(status.name(), reason, paymentId)
                    .query((rs, n) -> rs.getDouble(1))
                    .optional();
            if (seconds.isEmpty()) {
                log.info("result {} for payment {} ignored: not PENDING_LEDGER", status, paymentId);
                return;
            }
            Timer timer = requestToTerminal.get(status);
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    timer.record(Duration.ofNanos(Math.round(seconds.get() * 1e9)));
                }
            });
        });
    }
}
