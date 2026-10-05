package com.ledgerflow.e2e;

import static com.ledgerflow.e2e.Stack.account;
import static com.ledgerflow.e2e.Stack.balance;
import static com.ledgerflow.e2e.Stack.ledgerTransactions;
import static com.ledgerflow.e2e.Stack.pay;
import static com.ledgerflow.e2e.Stack.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** POST → outbox → Kafka → ledger → outbox → Kafka → payment status, with both real services. */
class PaymentFlowTest {

    private static final Duration WAIT = Duration.ofSeconds(60);

    @Test
    void fundedPaymentReachesCompletedAndMovesMoneyOnce() throws Exception {
        long payer = account("CUSTOMER", 10_000);
        long payee = account("MERCHANT", 0);

        var created = pay(UUID.randomUUID().toString(), payer, payee, 2_500);
        assertThat(created.status()).isEqualTo(202);
        UUID id = UUID.fromString(created.body().get("id").asString());

        JsonNode done = awaitTerminal(id);
        assertThat(done.get("status").asString()).isEqualTo("COMPLETED");
        assertThat(ledgerTransactions(id)).isEqualTo(1);
        assertThat(balance(payer)).isEqualTo(7_500);
        assertThat(balance(payee)).isEqualTo(2_500);
    }

    @Test
    void insufficientFundsReachesFailedWithoutMovingMoney() throws Exception {
        long payer = account("CUSTOMER", 100);
        long payee = account("MERCHANT", 0);

        UUID id = UUID.fromString(pay(UUID.randomUUID().toString(), payer, payee, 500).body().get("id").asString());

        JsonNode done = awaitTerminal(id);
        assertThat(done.get("status").asString()).isEqualTo("FAILED");
        assertThat(done.get("declineReason").asString()).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(ledgerTransactions(id)).isZero();
        assertThat(balance(payer)).isEqualTo(100);
        assertThat(balance(payee)).isZero();
    }

    @Test
    void unknownPayeeReachesFailed() throws Exception {
        long payer = account("CUSTOMER", 1_000);

        UUID id = UUID.fromString(pay(UUID.randomUUID().toString(), payer, 987_654_321L, 10).body().get("id").asString());

        JsonNode done = awaitTerminal(id);
        assertThat(done.get("status").asString()).isEqualTo("FAILED");
        assertThat(done.get("declineReason").asString()).isEqualTo("UNKNOWN_ACCOUNT");
        assertThat(balance(payer)).isEqualTo(1_000);
    }

    @Test
    void clientRetryAfterCompletionDoesNotPayTwice() throws Exception {
        long payer = account("CUSTOMER", 1_000);
        long payee = account("MERCHANT", 0);
        String key = UUID.randomUUID().toString();

        UUID id = UUID.fromString(pay(key, payer, payee, 300).body().get("id").asString());
        awaitTerminal(id);
        var retry = pay(key, payer, payee, 300);

        assertThat(retry.status()).isEqualTo(202);
        assertThat(retry.body().get("id").asString()).isEqualTo(id.toString());
        assertThat(ledgerTransactions(id)).isEqualTo(1);
        assertThat(balance(payer)).isEqualTo(700);
    }

    private static JsonNode awaitTerminal(UUID id) {
        return await().atMost(WAIT).until(() -> payment(id),
                p -> !p.get("status").asString().equals("PENDING_LEDGER"));
    }
}
