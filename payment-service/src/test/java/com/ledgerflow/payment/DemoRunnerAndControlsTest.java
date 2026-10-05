package com.ledgerflow.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ledgerflow.common.LedgerPosted;
import com.ledgerflow.common.Topics;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Demo profile: Transaction Playground runner and result-processing controls (no ledger in this module). */
class DemoRunnerAndControlsTest extends PaymentTestSupport {

    private static final AtomicLong ACCOUNTS = new AtomicLong(90_000);

    @LocalServerPort
    int port;

    @Autowired
    JsonMapper json;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    PaymentService payments;

    @Autowired
    DemoPaymentControls controls;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Autowired
    KafkaTemplate<String, String> kafka;

    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void clearFaults() {
        controls.set(new DemoPaymentControls.Settings(false, false));
    }

    @Test
    void runnerValidatesTheTransactionList() throws Exception {
        String one = "[" + tx("k1", 1_000_001, 0) + "]";
        String six = "[" + String.join(",", java.util.Collections.nCopies(6, tx("k", 1_000_001, 0))) + "]";
        assertThat(post("/demo/transactions", one).statusCode()).isEqualTo(400);
        assertThat(post("/demo/transactions", six).statusCode()).isEqualTo(400);
        assertThat(post("/demo/transactions", "[" + tx("a", 5, -1) + "," + tx("b", 5, 0) + "]").statusCode()).isEqualTo(400);
        assertThat(post("/demo/transactions", "[" + tx("a", 5, 10_001) + "," + tx("b", 5, 0) + "]").statusCode()).isEqualTo(400);
        assertThat(post("/demo/transactions", "[" + tx("", 5, 0) + "," + tx("b", 5, 0) + "]").statusCode()).isEqualTo(400);
    }

    @Test
    void runnerHonoursStartDelaysAndReportsMeasuredTimes() throws Exception {
        // Over the $10,000 limit: declined up front, so each reaches a final state without a ledger.
        String a = "delay-a-" + UUID.randomUUID();
        String b = "delay-b-" + UUID.randomUUID();
        JsonNode run = json.readTree(post("/demo/transactions", "[" + tx(a, 1_000_001, 0) + "," + tx(b, 1_000_001, 600) + "]").body());

        var results = run.get("results").valueStream().toList();
        assertThat(results).allSatisfy(r -> {
            assertThat(r.get("httpStatus").asInt()).isEqualTo(201);
            assertThat(r.get("status").asString()).isEqualTo("DECLINED");
            assertThat(r.get("respondedMs").asLong()).isGreaterThanOrEqualTo(r.get("startedMs").asLong());
            assertThat(r.get("finishedMs").asLong()).isGreaterThanOrEqualTo(r.get("respondedMs").asLong());
        });
        assertThat(results.get(1).get("startedMs").asLong()).isGreaterThanOrEqualTo(600);
        assertThat(results.get(1).get("startedMs").asLong() - results.get(0).get("startedMs").asLong()).isGreaterThanOrEqualTo(590);
        assertThat(results.get(0).get("key").asString()).isEqualTo(a); // the caller's own idempotency keys are used
        assertThat(jdbc.sql("SELECT count(*) FROM idempotency_keys WHERE key IN (?, ?)").params(a, b).query(Long.class).single())
                .isEqualTo(2);
    }

    @Test
    void pausedResultProcessingHoldsResultsUntilResumed() {
        UUID id = pending();
        var state = controls.set(new DemoPaymentControls.Settings(true, false));
        assertThat(state.resultsPaused()).isTrue();
        assertThat(registry.getListenerContainers()).allSatisfy(c -> assertThat(c.isPauseRequested()).isTrue());

        kafka.send(Topics.LEDGER_RESULTS, id.toString(), json.writeValueAsString(LedgerPosted.of(id, 77)));
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).until(() -> status(id).equals("PENDING_LEDGER"));

        controls.set(new DemoPaymentControls.Settings(false, false));
        await().atMost(Duration.ofSeconds(30)).until(() -> status(id).equals("COMPLETED"));
    }

    @Test
    void lostResultsAreDiscardedWithoutBeingRecorded() {
        UUID id = pending();
        long lostBefore = controls.get().resultsLost();
        controls.set(new DemoPaymentControls.Settings(false, true));
        var lost = LedgerPosted.of(id, 78);
        kafka.send(Topics.LEDGER_RESULTS, id.toString(), json.writeValueAsString(lost));

        await().atMost(Duration.ofSeconds(30)).until(() -> controls.get().resultsLost() > lostBefore);
        assertThat(status(id)).isEqualTo("PENDING_LEDGER");
        assertThat(jdbc.sql("SELECT count(*) FROM processed_events WHERE event_id = ?").param(lost.eventId())
                .query(Long.class).single()).isZero(); // not deduped away: a re-sent result can still apply

        controls.set(new DemoPaymentControls.Settings(false, false));
        kafka.send(Topics.LEDGER_RESULTS, id.toString(), json.writeValueAsString(LedgerPosted.of(id, 78)));
        await().atMost(Duration.ofSeconds(30)).until(() -> status(id).equals("COMPLETED"));
    }

    @Test
    void controlsAcceptSettingsWithoutTheReadOnlyCounter() throws Exception {
        // regression: the browser sends only the two settings; a missing counter field used to be a 400
        var res = post("/demo/controls", "{\"resultsPaused\":false,\"loseResults\":false}");
        assertThat(res.statusCode()).isEqualTo(200);
        assertThat(json.readTree(res.body()).has("resultsLost")).isTrue();
    }

    private UUID pending() {
        var r = payments.create("ctl-" + UUID.randomUUID(),
                new PaymentRequest(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet(), 100L, "USD"));
        return UUID.fromString(json.readTree(r.body()).get("id").asString());
    }

    private String status(UUID id) {
        return payments.find(id).orElseThrow().status().name();
    }

    private String tx(String key, long amountMinor, long delayMs) {
        return """
                {"key":"%s","payerAccountId":%d,"payeeAccountId":%d,"amountMinor":%d,"startDelayMs":%d}"""
                .formatted(key, ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet(), amountMinor, delayMs);
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
