package com.ledgerflow.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Demo-profile payment endpoints and the dashboard (the shared test context runs with "demo"). */
class DemoPaymentTest extends PaymentTestSupport {

    private static final AtomicLong ACCOUNTS = new AtomicLong(70_000);

    @LocalServerPort
    int port;

    @Autowired
    JsonMapper json;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    DemoPaymentController demo;

    @Autowired
    PlatformTransactionManager tm;

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void dashboardIsServed() throws Exception {
        var index = get("/");
        assertThat(index.statusCode()).isEqualTo(200);
        assertThat(index.body()).contains("LEDGERFLOW").contains("Reliable Event-Driven Payments").contains("/js/main.js");
        for (String asset : new String[] {"/css/app.css", "/js/main.js", "/js/api.js", "/js/scenarios.js", "/favicon.svg"}) {
            assertThat(get(asset).statusCode()).as(asset).isEqualTo(200);
        }
        assertThat(index.body()).doesNotContain("http://", "https://cdn"); // no external runtime dependency
    }

    @Test
    void burstWithOneKeyCreatesOnePaymentAndReplaysTheRest() throws Exception {
        int n = 16;
        String key = "burst-" + UUID.randomUUID();
        String body = """
                {"payerAccountId":%d,"payeeAccountId":%d,"amountMinor":300,"currency":"USD"}"""
                .formatted(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet());
        String items = String.join(",", java.util.Collections.nCopies(n, "{\"key\":\"" + key + "\",\"body\":" + body + "}"));

        JsonNode burst = json.readTree(post("/demo/burst", "[" + items + "]").body());

        assertThat(burst.get("requests").asInt()).isEqualTo(n);
        assertThat(burst.get("responses").asInt()).isEqualTo(n);
        var results = burst.get("results").valueStream().toList();
        assertThat(results).allSatisfy(r -> assertThat(r.get("httpStatus").asInt()).isEqualTo(202));
        assertThat(results.stream().map(r -> r.get("id").asString()).distinct()).hasSize(1);
        assertThat(results.stream().filter(r -> r.get("replayed").asBoolean()).count()).isEqualTo(n - 1);
        assertThat(jdbc.sql("SELECT count(*) FROM idempotency_keys WHERE key = ?").param(key).query(Long.class).single()).isEqualTo(1);
    }

    @Test
    void burstIsCappedAndBatchStatusReportsPayments() throws Exception {
        String tooMany = "[" + String.join(",", java.util.Collections.nCopies(DemoPaymentController.MAX_BURST + 1, "{\"key\":\"k\",\"body\":{}}")) + "]";
        assertThat(post("/demo/burst", tooMany).statusCode()).isEqualTo(400);

        var payment = demo.burst(java.util.List.of(new DemoPaymentController.BurstRequest("s-" + UUID.randomUUID(),
                json.readTree("{\"payerAccountId\":%d,\"payeeAccountId\":%d,\"amountMinor\":2000000,\"currency\":\"USD\"}"
                        .formatted(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet()))))).results().getFirst();
        assertThat(payment.httpStatus()).isEqualTo(201); // over $10,000: declined up front
        JsonNode statuses = json.readTree(post("/demo/payments/status", "[\"" + payment.id() + "\",\"" + UUID.randomUUID() + "\"]").body());
        assertThat(statuses.size()).isEqualTo(1);
        assertThat(statuses.get(0).get("status").asString()).isEqualTo("DECLINED");
        assertThat(statuses.get(0).get("declineReason").asString()).isEqualTo("AMOUNT_LIMIT");
    }

    @Test
    void configReportsTheRiskSettingsInEffect() throws Exception {
        JsonNode cfg = json.readTree(get("/demo/config").body());
        assertThat(cfg.get("velocityMaxPerMinute").asInt()).isEqualTo(5); // demo profile alone does not change risk policy
        assertThat(cfg.get("blockedAccounts").get(0).asLong()).isEqualTo(BLOCKED_ACCOUNT);
        assertThat(cfg.get("maxAmountMinor").asLong()).isEqualTo(1_000_000);
    }

    @Test
    void invariantSummaryMatchesTheDatabaseAndCatchesI6Corruption() throws Exception {
        var summary = demo.invariants();
        assertThat(violations(summary, "I6")).isZero();
        // I3 instant view must equal an independent count (this module has no ledger, so old PENDING payments exist)
        long stale = jdbc.sql("SELECT count(*) FROM payments WHERE status = 'PENDING_LEDGER' AND created_at < now() - interval '30 seconds'")
                .query(Long.class).single();
        assertThat(violations(summary, "I3")).isEqualTo(stale);
        assertThat(summary.pending()).isGreaterThanOrEqualTo(stale);

        String key = "tamper-" + UUID.randomUUID();
        demo.burst(java.util.List.of(new DemoPaymentController.BurstRequest(key,
                json.readTree("{\"payerAccountId\":%d,\"payeeAccountId\":%d,\"amountMinor\":1,\"currency\":\"USD\"}"
                        .formatted(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet())))));
        new TransactionTemplate(tm).executeWithoutResult(status -> {
            jdbc.sql("UPDATE idempotency_keys SET request_hash = 'tampered' WHERE key = ?").param(key).update();
            assertThat(violations(demo.invariants(), "I6")).isEqualTo(1);
            status.setRollbackOnly();
        });
        assertThat(violations(demo.invariants(), "I6")).isZero();
    }

    private static long violations(DemoPaymentController.InvariantSummary s, String invariant) {
        return s.checks().stream().filter(c -> c.invariant().equals(invariant)).findFirst().orElseThrow().violations();
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
