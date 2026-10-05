package com.ledgerflow.ledger;

import com.ledgerflow.common.FaultInjector;
import com.ledgerflow.common.LedgerPosted;
import com.ledgerflow.common.LedgerRejected;
import com.ledgerflow.common.OutboxRelay;
import com.ledgerflow.common.PaymentRequested;
import com.ledgerflow.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/**
 * Consumes PaymentRequested. One DB transaction covers dedupe, the ledger effect, the stored
 * outcome and the result outbox row; the Kafka offset is committed only after it commits.
 */
@Component
class PaymentRequestListener {

    private record Outcome(String status, Long ledgerTransactionId, String reason) {}

    private final JdbcClient jdbc;
    private final LedgerService ledger;
    private final JsonMapper json;
    private final TransactionTemplate tx;
    private final TransactionTemplate savepoint;
    private final Counter duplicates;
    private final Counter reemitted;
    private final ObjectProvider<FaultInjector> faults;

    PaymentRequestListener(JdbcClient jdbc, LedgerService ledger, JsonMapper json,
                           PlatformTransactionManager tm, MeterRegistry meters, ObjectProvider<FaultInjector> faults) {
        this.faults = faults;
        this.jdbc = jdbc;
        this.ledger = ledger;
        this.json = json;
        this.tx = new TransactionTemplate(tm);
        this.savepoint = new TransactionTemplate(tm);
        this.savepoint.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
        this.duplicates = meters.counter("ledgerflow.events.duplicate");
        this.reemitted = meters.counter("ledgerflow.outcomes.reemitted");
    }

    /** Malformed JSON or an invalid event throws, so the error handler retries and then dead-letters it. */
    @KafkaListener(topics = Topics.PAYMENTS_REQUESTED)
    void onMessage(String value) {
        handle(json.readValue(value, PaymentRequested.class));
        // Committed but offset not yet committed: a crash here means redelivery, absorbed by dedupe.
        faults.ifAvailable(f -> f.hit(FaultInjector.Point.AFTER_LEDGER_COMMIT_BEFORE_ACK));
    }

    void handle(PaymentRequested event) {
        tx.executeWithoutResult(status -> process(event));
    }

    private void process(PaymentRequested e) {
        boolean firstDelivery = jdbc.sql("INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING")
                .param(e.eventId()).update() == 1;
        if (!firstDelivery) {
            duplicates.increment();
            return;
        }

        // Serializes all events for one payment (redeliveries, reconciliation retries) until commit.
        jdbc.sql("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))").param(e.paymentId().toString())
                .query().listOfRows();
        Optional<Outcome> stored = findOutcome(e.paymentId());
        if (stored.isPresent()) {
            reemitted.increment();
            emit(e.paymentId(), stored.get());
            return;
        }

        Outcome outcome;
        try {
            long txId = savepoint.execute(s ->
                    ledger.post(e.paymentId(), e.payerAccountId(), e.payeeAccountId(), e.amountMinor()).transactionId());
            outcome = new Outcome("POSTED", txId, null);
        } catch (LedgerException.DuplicatePayment dup) {
            // Posted outside this consumer (e.g. seed data): report the existing posting, never post again.
            outcome = new Outcome("POSTED", existingTransactionId(e.paymentId()), null);
        } catch (LedgerException rejected) {
            outcome = new Outcome("REJECTED", null, reason(rejected));
        }
        jdbc.sql("INSERT INTO payment_outcomes (payment_id, status, ledger_transaction_id, reason) VALUES (?, ?, ?, ?)")
                .params(e.paymentId(), outcome.status(), outcome.ledgerTransactionId(), outcome.reason())
                .update();
        emit(e.paymentId(), outcome);
    }

    private static String reason(LedgerException e) {
        return switch (e) {
            case LedgerException.InsufficientFunds x -> "INSUFFICIENT_FUNDS";
            case LedgerException.AccountNotFound x -> "UNKNOWN_ACCOUNT";
            case LedgerException.InvalidAmount x -> "INVALID_AMOUNT";
            case LedgerException.SamePayerPayee x -> "SAME_PAYER_PAYEE";
            case LedgerException.DuplicatePayment x -> throw new IllegalStateException(x);
        };
    }

    private void emit(UUID paymentId, Outcome outcome) {
        Object event = outcome.status().equals("POSTED")
                ? LedgerPosted.of(paymentId, outcome.ledgerTransactionId())
                : LedgerRejected.of(paymentId, outcome.reason());
        OutboxRelay.enqueue(jdbc, paymentId, Topics.LEDGER_RESULTS, paymentId.toString(), json.writeValueAsString(event));
    }

    private Optional<Outcome> findOutcome(UUID paymentId) {
        return jdbc.sql("SELECT status, ledger_transaction_id, reason FROM payment_outcomes WHERE payment_id = ?")
                .param(paymentId)
                .query((rs, n) -> new Outcome(rs.getString("status"),
                        rs.getObject("ledger_transaction_id", Long.class), rs.getString("reason")))
                .optional();
    }

    private long existingTransactionId(UUID paymentId) {
        return jdbc.sql("SELECT id FROM ledger_transactions WHERE payment_id = ?").param(paymentId)
                .query(Long.class).single();
    }
}
