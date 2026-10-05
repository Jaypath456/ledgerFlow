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
        paymentDb().sql("""
                        INSERT INTO payments (id, payer_account_id, payee_account_id, amount_minor, currency, status,
                                              created_at, updated_at)
                        VALUES (?, ?, ?, 250, 'USD', 'PENDING_LEDGER', now() - interval '31 seconds', now() - interval '31 seconds')""")
                .params(id, payer, payee).update();
        paymentDb().sql("""
                        INSERT INTO idempotency_keys (key, request_hash, payment_id, response_status, response_body)
                        VALUES (?, encode(sha256(convert_to(?, 'UTF8')), 'hex'), ?, 202,
                                json_build_object('id', ?::text, 'status', 'PENDING_LEDGER')::text)""")
                .params(UUID.randomUUID().toString(), payer + "|" + payee + "|250|USD", id, id.toString()).update();

        assertThat(awaitTerminal(id).get("status").asString()).isEqualTo("COMPLETED");
        assertThat(ledgerTransactions(id)).isEqualTo(1);
        assertThat(balance(payer)).isEqualTo(750);
        assertThat(balance(payee)).isEqualTo(250);
    }

    private static void markStalePending(UUID id) {
        paymentDb().sql("""
                        UPDATE payments SET status = 'PENDING_LEDGER', decline_reason = NULL,
                                            updated_at = now() - interval '31 seconds'
                        WHERE id = ?""")
                .param(id).update();
    }

    private static long ledgerResults(UUID paymentId) {
        return ledgerDb().sql("SELECT count(*) FROM outbox WHERE aggregate_id = ?").param(paymentId)
                .query(Long.class).single();
    }
}
