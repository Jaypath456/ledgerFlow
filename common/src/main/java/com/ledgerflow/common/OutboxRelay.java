package com.ledgerflow.common;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Publishes unpublished outbox rows to Kafka, then marks them published. Runs in its own short
 * transaction, never inside the business transaction that wrote the row. At-least-once: a crash
 * between the broker ack and the mark republishes the batch, which consumers dedupe by eventId.
 */
public class OutboxRelay {

    private static final Log log = LogFactory.getLog(OutboxRelay.class);
    static final int BATCH = 100;

    private record Row(long id, String topic, String key, String payload) {}

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;

    OutboxRelay(JdbcClient jdbc, PlatformTransactionManager tm, KafkaTemplate<String, String> kafka) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(tm);
        this.kafka = kafka;
    }

    /** Called inside the caller's transaction; the relay picks the row up after commit. */
    public static void enqueue(JdbcClient jdbc, UUID aggregateId, String topic, String key, String payloadJson) {
        jdbc.sql("INSERT INTO outbox (aggregate_id, topic, event_key, payload) VALUES (?, ?, ?, CAST(? AS jsonb))")
                .params(aggregateId, topic, key, payloadJson)
                .update();
    }

    @Scheduled(fixedDelayString = "${ledgerflow.outbox.poll-ms:50}")
    public void drain() {
        try {
            while (publishBatch() == BATCH) {
                // keep draining a backlog
            }
        } catch (RuntimeException e) {
            log.warn("outbox publish failed, will retry: " + e);
        }
    }

    /** Returns the number of rows published. Rows stay unpublished if any send is not acked. */
    public int publishBatch() {
        Integer published = tx.execute(status -> {
            List<Row> rows = jdbc.sql("""
                            SELECT id, topic, event_key, payload::text AS payload FROM outbox
                            WHERE published_at IS NULL ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED""")
                    .param(BATCH)
                    .query((rs, n) -> new Row(rs.getLong("id"), rs.getString("topic"),
                            rs.getString("event_key"), rs.getString("payload")))
                    .list();
            if (rows.isEmpty()) {
                return 0;
            }
            var acks = rows.stream().map(r -> kafka.send(r.topic(), r.key(), r.payload())).toList();
            for (var ack : acks) {
                try {
                    ack.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException("Kafka send not acknowledged", e);
                }
            }
            jdbc.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)")
                    .param("ids", rows.stream().map(Row::id).toList())
                    .update();
            return rows.size();
        });
        return published == null ? 0 : published;
    }
}
