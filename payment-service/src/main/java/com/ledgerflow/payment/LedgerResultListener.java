package com.ledgerflow.payment;

import com.ledgerflow.common.LedgerPosted;
import com.ledgerflow.common.LedgerRejected;
import com.ledgerflow.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Applies ledger results. Dedupe and the state transition commit in one DB transaction before the
 * listener returns (and the offset is committed). Only PENDING_LEDGER moves, so a repeated or
 * re-emitted result can never cause a second transition.
 */
@Component
class LedgerResultListener {

    private static final Logger log = LoggerFactory.getLogger(LedgerResultListener.class);

    private final JdbcClient jdbc;
    private final JsonMapper json;
    private final TransactionTemplate tx;
    private final Counter duplicates;
    private final ObjectProvider<DemoPaymentControls> demo;

    LedgerResultListener(JdbcClient jdbc, JsonMapper json, PlatformTransactionManager tm, MeterRegistry meters,
                         ObjectProvider<DemoPaymentControls> demo) {
        this.demo = demo;
        this.jdbc = jdbc;
        this.json = json;
        this.tx = new TransactionTemplate(tm);
        this.duplicates = meters.counter("ledgerflow.events.duplicate");
    }

    /** Malformed or unknown events throw, so the error handler retries and then dead-letters them. */
    @KafkaListener(topics = Topics.LEDGER_RESULTS)
    void onMessage(String value) {
        DemoPaymentControls controls = demo.getIfAvailable(); // demo profile only
        if (controls != null && controls.discardResult()) {
            log.info("demo: ledger result discarded (simulated lost message)");
            return;
        }
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
            int moved = jdbc.sql("""
                            UPDATE payments SET status = ?, decline_reason = ?, updated_at = now()
                            WHERE id = ? AND status = 'PENDING_LEDGER'""")
                    .params(status.name(), reason, paymentId)
                    .update();
            if (moved == 0) {
                log.info("result {} for payment {} ignored: not PENDING_LEDGER", status, paymentId);
            }
        });
    }
}
