package com.ledgerflow.ledger;

import com.ledgerflow.common.FaultInjector;
import com.ledgerflow.common.LedgerPosted;
import com.ledgerflow.common.LedgerRejected;
import com.ledgerflow.common.MongoStore;
import com.ledgerflow.common.OutboxRelay;
import com.ledgerflow.common.PaymentRequested;
import com.ledgerflow.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Date;
import java.util.UUID;
import org.bson.Document;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import static com.ledgerflow.common.MongoStore.eq;

/** Commit dedupe, posting, outcome and result outbox before acknowledging Kafka. */
@Component
class PaymentRequestListener {
    private final MongoStore store;
    private final LedgerService ledger;
    private final LedgerRepository repo;
    private final JsonMapper json;
    private final Counter duplicates;
    private final Counter reemitted;
    private final ObjectProvider<FaultInjector> faults;

    PaymentRequestListener(MongoStore store, LedgerService ledger, LedgerRepository repo, JsonMapper json,
                           MeterRegistry meters, ObjectProvider<FaultInjector> faults) {
        this.store = store;
        this.ledger = ledger;
        this.repo = repo;
        this.json = json;
        this.faults = faults;
        this.duplicates = meters.counter("ledgerflow.events.duplicate");
        this.reemitted = meters.counter("ledgerflow.outcomes.reemitted");
    }

    @KafkaListener(topics = Topics.PAYMENTS_REQUESTED)
    void onMessage(String value) {
        handle(json.readValue(value, PaymentRequested.class));
        faults.ifAvailable(f -> f.hit(FaultInjector.Point.AFTER_LEDGER_COMMIT_BEFORE_ACK));
    }

    void handle(PaymentRequested event) {
        try {
            apply(event);
        } catch (DuplicateKeyException duplicate) {
            // A unique-key race aborted the entire transaction. Read/re-emit the winner in a new one.
            if (!store.processed(event.eventId()) && !store.mongo().exists(eq("payment_id", event.paymentId()), "payment_outcomes")) throw duplicate;
            apply(event);
        }
    }

    private void apply(PaymentRequested event) {
        int result = store.transaction(() -> process(event));
        if (result == 0) duplicates.increment();
        if (result == 2) reemitted.increment();
    }

    private int process(PaymentRequested e) {
        if (store.processed(e.eventId())) return 0;
        store.recordEvent(e.eventId());
        Document stored = store.mongo().findOne(eq("payment_id", e.paymentId()), Document.class, "payment_outcomes");
        if (stored != null) {
            emit(e.paymentId(), stored);
            return 2;
        }
        Document outcome = new Document("_id", e.paymentId().toString()).append("payment_id", e.paymentId().toString())
                .append("created_at", new Date());
        try {
            long txId = ledger.post(e.paymentId(), e.payerAccountId(), e.payeeAccountId(), e.amountMinor()).transactionId();
            outcome.append("status", "POSTED").append("ledger_transaction_id", txId);
        } catch (LedgerException.DuplicatePayment duplicate) {
            outcome.append("status", "POSTED").append("ledger_transaction_id", repo.transactionId(e.paymentId()).orElseThrow());
        } catch (LedgerException rejected) {
            // All business validation precedes writes; an unmatched conditional debit writes nothing.
            outcome.append("status", "REJECTED").append("reason", reason(rejected));
        }
        store.mongo().insert(outcome, "payment_outcomes");
        emit(e.paymentId(), outcome);
        return 1;
    }

    private static String reason(LedgerException e) {
        return switch (e) {
            case LedgerException.InsufficientFunds _ -> "INSUFFICIENT_FUNDS";
            case LedgerException.AccountNotFound _ -> "UNKNOWN_ACCOUNT";
            case LedgerException.InvalidAmount _ -> "INVALID_AMOUNT";
            case LedgerException.SamePayerPayee _ -> "SAME_PAYER_PAYEE";
            case LedgerException.DuplicatePayment x -> throw new IllegalStateException(x);
        };
    }

    private void emit(UUID paymentId, Document outcome) {
        Object event = outcome.getString("status").equals("POSTED")
                ? LedgerPosted.of(paymentId, outcome.getLong("ledger_transaction_id"))
                : LedgerRejected.of(paymentId, outcome.getString("reason"));
        OutboxRelay.enqueue(store, paymentId, Topics.LEDGER_RESULTS, paymentId.toString(), json.writeValueAsString(event));
    }
}
