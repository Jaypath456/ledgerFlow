package com.ledgerflow.payment;

import com.ledgerflow.common.MongoStore;
import com.ledgerflow.common.OutboxRelay;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import static com.ledgerflow.common.MongoStore.eq;

@Repository
class PaymentRepository {
    record IdempotencyKey(String requestHash, int responseStatus, String responseBody) {}
    private final MongoStore store;

    PaymentRepository(MongoStore store) {
        this.store = store;
        store.collection("payments");
        store.collection("idempotency_keys");
        store.unique("idempotency_keys", "key");
        store.unique("idempotency_keys", "payment_id");
        store.mongo().indexOps("payments").createIndex(new Index().on("payer_account_id", Sort.Direction.ASC).on("created_at", Sort.Direction.ASC));
        store.mongo().indexOps("payments").createIndex(new Index().on("status", Sort.Direction.ASC).on("updated_at", Sort.Direction.ASC));
    }

    void insertKey(String key, String requestHash, UUID paymentId) {
        store.mongo().insert(new Document("_id", key).append("key", key).append("request_hash", requestHash)
                .append("payment_id", paymentId.toString()).append("created_at", new Date()), "idempotency_keys");
    }

    Optional<IdempotencyKey> findKey(String key) {
        return Optional.ofNullable(store.mongo().findOne(eq("_id", key), Document.class, "idempotency_keys"))
                .map(d -> new IdempotencyKey(d.getString("request_hash"), d.getInteger("response_status"), d.getString("response_body")));
    }

    void storeResponse(String key, int status, String body) {
        store.mongo().updateFirst(eq("_id", key), Update.update("response_status", status).set("response_body", body), "idempotency_keys");
    }

    void insertPayment(UUID id, PaymentRequest r, PaymentStatus status, String declineReason) {
        Date now = new Date();
        store.mongo().insert(new Document("_id", id.toString()).append("payer_account_id", r.payerAccountId())
                .append("payee_account_id", r.payeeAccountId()).append("amount_minor", r.amountMinor())
                .append("currency", r.currency()).append("status", status.name()).append("decline_reason", declineReason)
                .append("created_at", now).append("updated_at", now), "payments");
    }

    Optional<Payment> find(UUID id) {
        return Optional.ofNullable(store.mongo().findOne(eq("_id", id), Document.class, "payments")).map(PaymentRepository::payment);
    }

    /** The transaction's touch conflicts with a concurrent result or reconciliation. */
    List<Payment> lockStalePending(int staleAfterSeconds, int limit) {
        return store.mongo().find(Query.query(Criteria.where("status").is("PENDING_LEDGER")
                        .and("updated_at").lt(Date.from(Instant.now().minusSeconds(staleAfterSeconds))))
                .with(Sort.by("updated_at")).limit(limit), Document.class, "payments").stream()
                .filter(d -> !store.mongo().exists(Query.query(Criteria.where("aggregate_id").is(d.getString("_id"))
                        .and("published_at").is(null)), "outbox")).map(PaymentRepository::payment).toList();
    }

    void touch(UUID id) {
        store.mongo().updateFirst(eq("_id", id), Update.update("updated_at", new Date()), "payments");
    }

    void insertOutbox(UUID aggregateId, String topic, String eventKey, String payloadJson) {
        OutboxRelay.enqueue(store, aggregateId, topic, eventKey, payloadJson);
    }

    private static Payment payment(Document d) {
        return new Payment(UUID.fromString(d.getString("_id")), d.getLong("payer_account_id"), d.getLong("payee_account_id"),
                d.getLong("amount_minor"), d.getString("currency"), PaymentStatus.valueOf(d.getString("status")),
                d.getString("decline_reason"), d.getDate("created_at").toInstant(), d.getDate("updated_at").toInstant());
    }
}
