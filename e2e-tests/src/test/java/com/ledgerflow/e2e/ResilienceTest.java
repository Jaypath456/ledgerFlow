package com.ledgerflow.e2e;

import static com.ledgerflow.e2e.DemoStack.account;
import static com.ledgerflow.e2e.DemoStack.balance;
import static com.ledgerflow.e2e.DemoStack.get;
import static com.ledgerflow.e2e.DemoStack.ledger;
import static com.ledgerflow.e2e.DemoStack.payment;
import static com.ledgerflow.e2e.DemoStack.postJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** Resilience Lab controls (demo profile, application-level): payments wait safely and recover once. */
class ResilienceTest {

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @AfterEach
    void resetFaults() throws Exception {
        postJson(ledger("/demo/controls"), Map.of("ledgerPaused", false, "delayMs", 0));
        postJson(payment("/demo/controls"), Map.of("resultsPaused", false, "loseResults", false));
    }

    @Test
    void pausedLedgerProcessingHoldsThePaymentThenItCompletesOnce() throws Exception {
        long payer = account("Jay", 10_000).get("id").asLong(), payee = account("Ajay", 0).get("id").asLong();
        assertThat(postJson(ledger("/demo/controls"), Map.of("ledgerPaused", true, "delayMs", 0)).get("ledgerPaused").asBoolean()).isTrue();

        String id = pay(payer, payee, 2_500);
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).until(() -> status(id).equals("PENDING_LEDGER"));
        assertThat(balance(payer)).as("no money moved while paused").isEqualTo(10_000);
        assertThat(ledgerOf(id).get("postings").asLong()).isZero();

        postJson(ledger("/demo/controls"), Map.of("ledgerPaused", false, "delayMs", 0));
        await().atMost(Duration.ofSeconds(30)).until(() -> status(id).equals("COMPLETED"));
        assertThat(ledgerOf(id).get("postings").asLong()).isEqualTo(1);
        assertThat(balance(payer)).isEqualTo(7_500);
        assertThat(balance(payee)).isEqualTo(2_500);
    }

    @Test
    void pausedResultProcessingLeavesThePaymentPendingAfterTheLedgerPostedThenCompletes() throws Exception {
        long payer = account("Jay", 10_000).get("id").asLong(), payee = account("Ajay", 0).get("id").asLong();
        postJson(payment("/demo/controls"), Map.of("resultsPaused", true, "loseResults", false));

        String id = pay(payer, payee, 2_500);
        await().atMost(Duration.ofSeconds(30)).until(() -> "POSTED".equals(ledgerOf(id).path("outcome").asString(null)));
        assertThat(status(id)).as("ledger moved the money; payment service has not applied it yet").isEqualTo("PENDING_LEDGER");
        assertThat(balance(payer)).isEqualTo(7_500);

        postJson(payment("/demo/controls"), Map.of("resultsPaused", false, "loseResults", false));
        await().atMost(Duration.ofSeconds(30)).until(() -> status(id).equals("COMPLETED"));
        assertThat(ledgerOf(id).get("postings").asLong()).isEqualTo(1);
    }

    @Test
    void lostResultIsRecoveredByReconciliationWithoutASecondPosting() throws Exception {
        long payer = account("Jay", 10_000).get("id").asLong(), payee = account("Ajay", 0).get("id").asLong();
        long lostBefore = get(payment("/demo/controls")).get("resultsLost").asLong();
        postJson(payment("/demo/controls"), Map.of("resultsPaused", false, "loseResults", true));

        String id = pay(payer, payee, 2_500);
        await().atMost(Duration.ofSeconds(30)).until(() -> get(payment("/demo/controls")).get("resultsLost").asLong() > lostBefore);
        assertThat(ledgerOf(id).get("outcome").asString()).isEqualTo("POSTED");
        assertThat(status(id)).isEqualTo("PENDING_LEDGER");
        postJson(payment("/demo/controls"), Map.of("resultsPaused", false, "loseResults", false));

        // nothing re-sends the lost result by itself: reconciliation (stale after 30 s) asks the ledger again
        await().atMost(Duration.ofSeconds(90)).until(() -> status(id).equals("COMPLETED"));
        JsonNode l = ledgerOf(id);
        assertThat(l.get("postings").asLong()).isEqualTo(1);
        assertThat(l.get("resultsEmitted").asLong()).isGreaterThanOrEqualTo(2);
        assertThat(balance(payer)).isEqualTo(7_500);
    }

    @Test
    void processingDelayIsReal() throws Exception {
        long payer = account("Jay", 10_000).get("id").asLong(), payee = account("Ajay", 0).get("id").asLong();
        postJson(ledger("/demo/controls"), Map.of("ledgerPaused", false, "delayMs", 2_000));

        long t0 = System.nanoTime();
        String id = pay(payer, payee, 100);
        await().atMost(Duration.ofSeconds(30)).until(() -> status(id).equals("COMPLETED"));
        assertThat((System.nanoTime() - t0) / 1_000_000).isGreaterThanOrEqualTo(2_000);
        assertThat(ledgerOf(id).get("postings").asLong()).isEqualTo(1);
    }

    private static String pay(long payer, long payee, long amountMinor) throws Exception {
        var res = HTTP.send(HttpRequest.newBuilder(URI.create(payment("/api/payments")))
                .header("Content-Type", "application/json").header("Idempotency-Key", "e2e-lab-" + UUID.randomUUID())
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"payerAccountId":%d,"payeeAccountId":%d,"amountMinor":%d,"currency":"USD"}""".formatted(payer, payee, amountMinor)))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).isEqualTo(202);
        return DemoStack.JSON.readTree(res.body()).get("id").asString();
    }

    private static String status(String id) throws Exception {
        return get(payment("/api/payments/" + id)).get("status").asString();
    }

    private static JsonNode ledgerOf(String id) throws Exception {
        return postJson(ledger("/demo/payments/ledger"), List.of(id)).get(0);
    }
}
