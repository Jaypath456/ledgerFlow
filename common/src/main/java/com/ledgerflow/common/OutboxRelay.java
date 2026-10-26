package com.ledgerflow.common;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

/** Publish after commit; consumers dedupe resends after a crash. */
public class OutboxRelay {
    private static final Log log = LogFactory.getLog(OutboxRelay.class);
    static final int BATCH = 100;
    private final MongoStore store;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectProvider<FaultInjector> faults;

    OutboxRelay(MongoStore store, KafkaTemplate<String, String> kafka, ObjectProvider<FaultInjector> faults) {
        this.store = store;
        this.kafka = kafka;
        this.faults = faults;
    }

    /** The caller's session commits this document with its business changes. */
    public static void enqueue(MongoStore store, UUID aggregateId, String topic, String key, String payloadJson) {
        store.mongo().insert(new Document("aggregate_id", aggregateId.toString()).append("topic", topic)
                .append("event_key", key).append("payload", payloadJson).append("created_at", new Date())
                .append("published_at", null), "outbox");
    }

    @Scheduled(fixedDelayString = "${ledgerflow.outbox.poll-ms:10}")
    public void drain() {
        try {
            while (publishBatch() == BATCH) { }
        } catch (RuntimeException e) {
            log.warn("outbox publish failed, will retry: " + e);
        }
    }

    /** Single replica: serialize scheduled/manual drains and mark only acknowledged sends. */
    public synchronized int publishBatch() {
        var rows = store.mongo().find(Query.query(Criteria.where("published_at").is(null))
                .with(Sort.by("_id")).limit(BATCH), Document.class, "outbox");
        var acks = rows.stream().map(r -> kafka.send(r.getString("topic"), r.getString("event_key"), r.getString("payload"))).toList();
        for (var ack : acks) {
            try { ack.get(30, TimeUnit.SECONDS); }
            catch (Exception e) { throw new IllegalStateException("Kafka send not acknowledged", e); }
        }
        if (!rows.isEmpty()) {
            faults.ifAvailable(f -> f.hit(FaultInjector.Point.AFTER_OUTBOX_PUBLISH_BEFORE_MARK));
            store.mongo().updateMulti(Query.query(Criteria.where("_id").in(rows.stream().map(r -> r.get("_id")).toList())),
                    Update.update("published_at", new Date()), "outbox");
        }
        return rows.size();
    }
}
