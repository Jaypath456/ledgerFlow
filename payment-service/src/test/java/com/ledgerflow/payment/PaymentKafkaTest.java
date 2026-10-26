package com.ledgerflow.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ledgerflow.common.FaultInjector;
import com.ledgerflow.common.LedgerPosted;
import com.ledgerflow.common.LedgerRejected;
import com.ledgerflow.common.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.domain.Sort;
import java.util.Date;
import static com.ledgerflow.common.MongoStore.eq;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

/** Outbox relay and ledger-result consumer against real Kafka + MongoDB. */
class PaymentKafkaTest extends PaymentTestSupport {

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final AtomicLong ACCOUNTS = new AtomicLong(50_000);

    @Autowired
    PaymentService payments;

    @Autowired
    MongoTemplate mongo;

    @Autowired
    JsonMapper json;

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    MeterRegistry meters;

    @Autowired
    FaultInjector faults;

    @Autowired
    Reconciler reconciler;

    @Test
    void acceptedPaymentIsRelayedToKafkaExactlyOnceAndMarkedPublished() {
        long payer = ACCOUNTS.incrementAndGet();
        UUID id = accept(payer);

        await().atMost(WAIT).until(() -> mongo.exists(Query.query(Criteria.where("aggregate_id").is(id.toString()).and("published_at").ne(null)), "outbox"));
        var records = await().atMost(WAIT).until(
                () -> readAll(Topics.PAYMENTS_REQUESTED).stream()
                        .filter(r -> r.value().contains(id.toString())).toList(),
                list -> !list.isEmpty());
        assertThat(records).hasSize(1);
        ConsumerRecord<String, String> record = records.getFirst();
        assertThat(record.key()).isEqualTo(String.valueOf(payer));
        assertThat(json.readTree(record.value()).get("paymentId").asString()).isEqualTo(id.toString());
        assertThat(mongo.count(eq("aggregate_id", id), "outbox")).isEqualTo(1);
    }

    
    @Test
    void crashAfterPublishBeforeMarkRepublishesAndStillMarksOnce() {
        await().atMost(WAIT).until(() -> mongo.count(eq("published_at", null), "outbox") == 0);
        faults.armOnce(FaultInjector.Point.AFTER_OUTBOX_PUBLISH_BEFORE_MARK);
        UUID id = accept(ACCOUNTS.incrementAndGet());
        
        await().atMost(WAIT).until(() -> mongo.exists(Query.query(Criteria.where("aggregate_id").is(id.toString()).and("published_at").ne(null)), "outbox"));
        var copies = await().atMost(WAIT).until(
                () -> readAll(Topics.PAYMENTS_REQUESTED).stream().filter(r -> r.value().contains(id.toString()))
                        .map(r -> json.readTree(r.value()).get("eventId").asString()).toList(),
                list -> list.size() >= 2);
        assertThat(copies).containsOnly(copies.getFirst()); // same event published twice: consumers dedupe
        assertThat(mongo.count(eq("aggregate_id", id), "outbox")).isEqualTo(1);
    }

    @Test
    void reconcilerReRequestsOnlyStalePendingPayments() {
        UUID stale = accept(ACCOUNTS.incrementAndGet());
        UUID fresh = accept(ACCOUNTS.incrementAndGet());
        await().atMost(WAIT).until(() -> mongo.count(eq("published_at", null), "outbox") == 0);
        mongo.updateFirst(eq("_id", stale), Update.update("updated_at", Date.from(Instant.now().minusSeconds(31))), "payments");

        // Other tests' payments may be stale too (and the scheduled run may get there first): assert ours only.
        reconciler.reconcile();
        assertThat(outboxRows(stale)).isEqualTo(2);
        assertThat(outboxRows(fresh)).isEqualTo(1);
        var requests = mongo.find(eq("aggregate_id", stale), Document.class, "outbox").stream()
                .map(d -> json.readTree(d.getString("payload")).get("eventId").asString()).toList();
        assertThat(requests).doesNotHaveDuplicates(); // fresh eventId, so the ledger does not drop it as a duplicate

        // Touched: not re-requested again until another stale period passes.
        reconciler.reconcile();
        assertThat(outboxRows(stale)).isEqualTo(2);
    }

    @Test
    void ledgerPostedCompletesPayment() {
        UUID id = accept(ACCOUNTS.incrementAndGet());
        send(id, json.writeValueAsString(LedgerPosted.of(id, 42)));
        await().atMost(WAIT).until(() -> status(id) == PaymentStatus.COMPLETED);
    }

    @Test
    void ledgerRejectedFailsPaymentWithReason() {
        UUID id = accept(ACCOUNTS.incrementAndGet());
        send(id, json.writeValueAsString(LedgerRejected.of(id, "INSUFFICIENT_FUNDS")));
        await().atMost(WAIT).until(() -> status(id) == PaymentStatus.FAILED);
        assertThat(payments.find(id).orElseThrow().declineReason()).isEqualTo("INSUFFICIENT_FUNDS");
    }

    @Test
    void duplicateAndConflictingResultsCauseOneTransition() {
        UUID id = accept(ACCOUNTS.incrementAndGet());
        String posted = json.writeValueAsString(LedgerPosted.of(id, 7));
        send(id, posted);
        await().atMost(WAIT).until(() -> status(id) == PaymentStatus.COMPLETED);
        Instant completedAt = payments.find(id).orElseThrow().updatedAt();
        double duplicatesBefore = duplicates();

        send(id, posted); // exact redelivery
        var late = LedgerRejected.of(id, "INSUFFICIENT_FUNDS"); // different event for a terminal payment
        send(id, json.writeValueAsString(late));

        await().atMost(WAIT).until(() -> duplicates() - duplicatesBefore >= 1 && processed(late.eventId()));
        Payment after = payments.find(id).orElseThrow();
        assertThat(after.status()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(after.updatedAt()).isEqualTo(completedAt);
        assertThat(after.declineReason()).isNull();
    }

    @Test
    void poisonResultGoesToDltAndValidResultBehindItIsApplied() {
        UUID id = accept(ACCOUNTS.incrementAndGet());
        String garbage = "{\"eventType\":\"LedgerPosted\",\"paymentId\":\"" + id + "\"}"; // no eventId/version
        String unknownType = "{\"eventType\":\"Bogus\",\"eventId\":\"" + UUID.randomUUID() + "\"}";

        send(id, garbage); // same key => same partition as the valid event
        send(id, unknownType);
        send(id, json.writeValueAsString(LedgerPosted.of(id, 9)));

        await().atMost(WAIT).until(() -> status(id) == PaymentStatus.COMPLETED);
        await().atMost(WAIT).untilAsserted(() -> assertThat(
                readAll(Topics.LEDGER_RESULTS + Topics.DLT_SUFFIX).stream().map(ConsumerRecord::value).toList())
                .contains(garbage, unknownType));
    }

    private UUID accept(long payer) {
        var request = new PaymentRequest(payer, ACCOUNTS.incrementAndGet(), 100L, "USD");
        var result = payments.create(UUID.randomUUID().toString(), request);
        assertThat(result.status()).isEqualTo(202);
        return UUID.fromString(json.readTree(result.body()).get("id").asString());
    }

    private void send(UUID paymentId, String value) {
        kafka.send(Topics.LEDGER_RESULTS, paymentId.toString(), value);
    }

    private PaymentStatus status(UUID id) {
        return payments.find(id).orElseThrow().status();
    }

    private long outboxRows(UUID paymentId) {
        return mongo.count(eq("aggregate_id", paymentId), "outbox");
    }

    private boolean processed(UUID eventId) {
        return mongo.count(eq("event_id", eventId), "processed_events") == 1;
    }

    private double duplicates() {
        return meters.counter("ledgerflow.events.duplicate").count();
    }
}
