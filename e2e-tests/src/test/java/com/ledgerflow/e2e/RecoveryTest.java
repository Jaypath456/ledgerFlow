package com.ledgerflow.e2e;

import static com.ledgerflow.e2e.Stack.SYSTEM_ACCOUNT;
import static com.ledgerflow.e2e.Stack.account;
import static com.ledgerflow.e2e.Stack.awaitTerminal;
import static com.ledgerflow.e2e.Stack.balance;
import static com.ledgerflow.e2e.Stack.ledgerDb;
import static com.ledgerflow.e2e.Stack.ledgerTransactions;
import static com.ledgerflow.e2e.Stack.pay;
import static com.ledgerflow.e2e.Stack.paymentDb;
import static com.ledgerflow.e2e.Stack.post;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.Date;
import java.time.Instant;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Update;
import static com.ledgerflow.common.MongoStore.eq;
import org.junit.jupiter.api.Test;

/** Reconciliation of stale PENDING_LEDGER payments with both real services. */
class RecoveryTest {

    @Test
    void ledgerAlreadyPostedButPaymentStillPendingRecoversToCompletedWithoutSecondPosting() throws Exception {
        long payer = account("CUSTOMER", 1_000);
        long payee = account("MERCHANT", 0);
        UUID id = UUID.fromString(pay(UUID.randomUUID().toString(), payer, payee, 400).body().get("id").asString());
        assertThat(awaitTerminal(id).get("status").asString()).isEqualTo("COMPLETED");

        // The failure case: the ledger posted, but payment-service never applied the result.
        markStalePending(id);

        assertThat(awaitTerminal(id).get("status").asString()).isEqualTo("COMPLETED");
        assertThat(ledgerTransactions(id)).isEqualTo(1);
        assertThat(balance(payer)).isEqualTo(600);
        assertThat(balance(payee)).isEqualTo(400);
        assertThat(ledgerResults(id)).isEqualTo(2); // original + re-emitted stored outcome
    }

    @Test
    void ledgerAlreadyRejectedButPaymentStillPendingRecoversToFailedEvenIfFundsArriveLater() throws Exception {
        long payer = account("CUSTOMER", 100);
        long payee = account("MERCHANT", 0);
        UUID id = UUID.fromString(pay(UUID.randomUUID().toString(), payer, payee, 500).body().get("id").asString());
        assertThat(awaitTerminal(id).get("status").asString()).isEqualTo("FAILED");

        markStalePending(id);
        post(SYSTEM_ACCOUNT, payer, 1_000); // now it could be paid, but the ledger already decided

        var recovered = awaitTerminal(id);
        assertThat(recovered.get("status").asString()).isEqualTo("FAILED");
        assertThat(recovered.get("declineReason").asString()).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(ledgerTransactions(id)).isZero();
        assertThat(balance(payer)).isEqualTo(1_100);
    }

    @Test
    void requestThatNeverReachedTheLedgerIsPostedByReconciliation() {
        long payer = account("CUSTOMER", 1_000);
        long payee = account("MERCHANT", 0);
        UUID id = UUID.randomUUID();
        // An accepted payment whose PaymentRequested never got processed (no outbox row left).
        Date stale = Date.from(Instant.now().minusSeconds(31));
        paymentDb().insert(new Document("_id", id.toString()).append("payer_account_id", payer).append("payee_account_id", payee)
                .append("amount_minor", 250L).append("currency", "USD").append("status", "PENDING_LEDGER")
                .append("created_at", stale).append("updated_at", stale), "payments");
        String key = UUID.randomUUID().toString();
        paymentDb().insert(new Document("_id", key).append("key", key)
                .append("request_hash", Stack.requestHash(payer, payee, 250L))
                .append("payment_id", id.toString()).append("response_status", 202)
                .append("response_body", "{\"id\":\"" + id + "\",\"status\":\"PENDING_LEDGER\"}"), "idempotency_keys");

        assertThat(awaitTerminal(id).get("status").asString()).isEqualTo("COMPLETED");
        assertThat(ledgerTransactions(id)).isEqualTo(1);
        assertThat(balance(payer)).isEqualTo(750);
        assertThat(balance(payee)).isEqualTo(250);
    }

    private static void markStalePending(UUID id) {
        paymentDb().updateFirst(eq("_id", id), Update.update("status", "PENDING_LEDGER").set("decline_reason", null)
                .set("updated_at", Date.from(Instant.now().minusSeconds(31))), "payments");
    }

    private static long ledgerResults(UUID paymentId) {
        return ledgerDb().count(eq("aggregate_id", paymentId), "outbox");
    }
}
