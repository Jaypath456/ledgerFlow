package com.ledgerflow.payment;

import com.ledgerflow.common.LedgerPosted;
import com.ledgerflow.common.LedgerRejected;
import com.ledgerflow.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ledgerflow.common.MongoStore;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import java.util.Date;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Commit dedupe and the payment update together before acknowledging Kafka. */
@Component
class LedgerResultListener {

    private static final Logger log = LoggerFactory.getLogger(LedgerResultListener.class);

    private final MongoStore store;
    private final JsonMapper json;
    private final Counter duplicates;

    LedgerResultListener(MongoStore store, JsonMapper json, MeterRegistry meters) {
        this.store = store;
        this.json = json;
        this.duplicates = meters.counter("ledgerflow.events.duplicate");
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
        try {
            boolean first = store.transaction(() -> {
                if (store.processed(eventId)) return false;
                store.recordEvent(eventId);
                long moved = store.mongo().updateFirst(Query.query(Criteria.where("_id").is(paymentId.toString())
                                .and("status").is("PENDING_LEDGER")),
                        Update.update("status", status.name()).set("decline_reason", reason).set("updated_at", new Date()), "payments")
                        .getModifiedCount();
                if (moved == 0) log.info("result {} for payment {} ignored: not PENDING_LEDGER", status, paymentId);
                return true;
            });
            if (!first) duplicates.increment();
        } catch (DuplicateKeyException duplicate) {
            if (!store.processed(eventId)) throw duplicate;
            duplicates.increment();
        }
    }
}
