package com.ledgerflow.payment;

import com.ledgerflow.common.OutboxRelay;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class PaymentRepository {

    record IdempotencyKey(String requestHash, int responseStatus, String responseBody) {}

    private final JdbcClient jdbc;

    PaymentRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * False when the key exists. A concurrent insert of the same key blocks here until the
     * other transaction ends, so at most one request per key gets past this point.
     */
    boolean insertKey(String key, String requestHash, UUID paymentId) {
        return jdbc.sql("""
                        INSERT INTO idempotency_keys (key, request_hash, payment_id) VALUES (?, ?, ?)
                        ON CONFLICT (key) DO NOTHING""")
                .params(key, requestHash, paymentId)
                .update() == 1;
    }

    Optional<IdempotencyKey> findKey(String key) {
        return jdbc.sql("SELECT request_hash, response_status, response_body FROM idempotency_keys WHERE key = ?")
                .param(key)
                .query((rs, n) -> new IdempotencyKey(
                        rs.getString("request_hash"), rs.getInt("response_status"), rs.getString("response_body")))
                .optional();
    }

    void storeResponse(String key, int status, String body) {
        jdbc.sql("UPDATE idempotency_keys SET response_status = ?, response_body = ? WHERE key = ?")
                .params(status, body, key)
                .update();
    }

    void insertPayment(UUID id, PaymentRequest r, PaymentStatus status, String declineReason) {
        jdbc.sql("""
                        INSERT INTO payments (id, payer_account_id, payee_account_id, amount_minor, currency, status, decline_reason)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""")
                .params(id, r.payerAccountId(), r.payeeAccountId(), r.amountMinor(), r.currency(), status.name(), declineReason)
                .update();
    }

    Optional<Payment> find(UUID id) {
        return jdbc.sql("SELECT * FROM payments WHERE id = ?").param(id).query(PaymentRepository::payment).optional();
    }

    /**
     * Locks stale PENDING_LEDGER payments that have no unpublished outbox row (an unsent request
     * is still on its way; re-requesting would only pile up duplicates).
     */
    List<Payment> lockStalePending(int staleAfterSeconds, int limit) {
        return jdbc.sql("""
                        SELECT * FROM payments p
                        WHERE p.status = 'PENDING_LEDGER'
                          AND p.updated_at < now() - (? * interval '1 second')
                          AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.aggregate_id = p.id AND o.published_at IS NULL)
                        ORDER BY p.updated_at LIMIT ? FOR UPDATE OF p SKIP LOCKED""")
                .params(staleAfterSeconds, limit)
                .query(PaymentRepository::payment)
                .list();
    }

    void touch(UUID id) {
        jdbc.sql("UPDATE payments SET updated_at = now() WHERE id = ?").param(id).update();
    }

    void insertOutbox(UUID aggregateId, String topic, String eventKey, String payloadJson) {
        OutboxRelay.enqueue(jdbc, aggregateId, topic, eventKey, payloadJson);
    }

    private static Payment payment(ResultSet rs, int n) throws SQLException {
        return new Payment(
                rs.getObject("id", UUID.class),
                rs.getLong("payer_account_id"),
                rs.getLong("payee_account_id"),
                rs.getLong("amount_minor"),
                rs.getString("currency"),
                PaymentStatus.valueOf(rs.getString("status")),
                rs.getString("decline_reason"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }
}
