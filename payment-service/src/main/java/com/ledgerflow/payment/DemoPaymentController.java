package com.ledgerflow.payment;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Demo-only (profile "demo") support for the dashboard. Browsers keep ~6 connections per origin,
 * so {@code /demo/burst} fires a batch of ordinary {@code POST /api/payments} requests concurrently
 * over loopback HTTP: it can do nothing a client could not. The other endpoints are read-only.
 */
@RestController
@RequestMapping("/demo")
@Profile("demo")
class DemoPaymentController {

    static final int MAX_BURST = 500;

    record BurstRequest(String key, JsonNode body) {}

    record BurstResult(int index, int httpStatus, String id, String status, boolean replayed, String error) {}

    record Burst(int requests, int responses, long elapsedMs, List<BurstResult> results) {}

    record PaymentStatus(UUID id, String status, String declineReason) {}

    record Check(String invariant, String description, long violations, boolean pass) {}

    record InvariantSummary(String scope, List<Check> checks, long pending, long violations, boolean pass) {}

    private final JdbcClient jdbc;
    private final JsonMapper json;
    private final Environment env;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    DemoPaymentController(JdbcClient jdbc, JsonMapper json, Environment env) {
        this.jdbc = jdbc;
        this.json = json;
        this.env = env;
    }

    /** Sends all requests at once (one virtual thread each, released together) and reports every response. */
    @PostMapping("/burst")
    Burst burst(@RequestBody List<BurstRequest> requests) throws Exception {
        if (requests == null || requests.isEmpty() || requests.size() > MAX_BURST) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "between 1 and " + MAX_BURST + " requests");
        }
        URI target = URI.create("http://localhost:" + env.getProperty("local.server.port") + "/api/payments");
        var start = new CountDownLatch(1);
        List<Future<BurstResult>> futures = new ArrayList<>();
        long began;
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < requests.size(); i++) {
                int index = i;
                BurstRequest r = requests.get(i);
                futures.add(pool.submit(() -> {
                    start.await();
                    return send(index, target, r);
                }));
            }
            began = System.nanoTime();
            start.countDown();
        }
        List<BurstResult> results = new ArrayList<>();
        for (var f : futures) {
            results.add(f.get());
        }
        long responses = results.stream().filter(r -> r.httpStatus() > 0).count();
        return new Burst(requests.size(), (int) responses, (System.nanoTime() - began) / 1_000_000, results);
    }

    private BurstResult send(int index, URI target, BurstRequest r) {
        try {
            var request = HttpRequest.newBuilder(target)
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", r.key())
                    .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(r.body())))
                    .build();
            var response = http.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode body = json.readTree(response.body());
            return new BurstResult(index, response.statusCode(),
                    body.path("id").isMissingNode() ? null : body.path("id").asString(),
                    body.path("status").isMissingNode() ? null : body.path("status").asString(),
                    response.headers().firstValue("Idempotent-Replayed").map("true"::equals).orElse(false),
                    response.statusCode() >= 400 ? body.path("detail").asString("") : null);
        } catch (Exception e) {
            return new BurstResult(index, 0, null, null, false, e.toString());
        }
    }

    record RiskConfig(int velocityMaxPerMinute, List<Long> blockedAccounts, long maxAmountMinor) {}

    /** The risk settings actually in effect, so the dashboard never assumes them. */
    @GetMapping("/config")
    RiskConfig config() {
        return new RiskConfig(
                env.getProperty("ledgerflow.risk.velocity-max", Integer.class, 5),
                List.of(env.getProperty("ledgerflow.risk.blocked-accounts", Long[].class, new Long[0])),
                RiskRule.MaxAmount.MAX_MINOR);
    }

    @PostMapping("/payments/status")
    List<PaymentStatus> statuses(@RequestBody List<UUID> ids) {
        if (ids == null || ids.isEmpty() || ids.size() > 1000) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "between 1 and 1000 payment ids");
        }
        return jdbc.sql("SELECT id, status, decline_reason FROM payments WHERE id IN (:ids)")
                .param("ids", ids)
                .query((rs, n) -> new PaymentStatus(rs.getObject("id", UUID.class), rs.getString("status"),
                        rs.getString("decline_reason")))
                .list();
    }

    /**
     * The payments-schema parts of chaos/verify_invariants.sql: I6 in full, and an instant view of
     * I3 (I3 proper is "terminal after recovery", so a payment still in flight is not a violation;
     * one stuck beyond the 30 s reconciliation threshold is reported).
     */
    @GetMapping("/invariants")
    InvariantSummary invariants() {
        long pending = jdbc.sql("SELECT count(*) FROM payments WHERE status = 'PENDING_LEDGER'").query(Long.class).single();
        List<Check> checks = List.of(
                check("I3", "no accepted payment stuck in PENDING_LEDGER beyond the 30 s reconciliation threshold", """
                        SELECT count(*) FROM payments
                        WHERE status = 'PENDING_LEDGER' AND created_at < now() - interval '30 seconds'"""),
                check("I6", "exactly one idempotency key per payment; each key's request hash, stored response id and status code match its payment", """
                        SELECT (SELECT count(*) FROM (
                                    SELECT p.id FROM payments p LEFT JOIN idempotency_keys k ON k.payment_id = p.id
                                    GROUP BY p.id HAVING count(k.key) <> 1) x)
                             + (SELECT count(*) FROM idempotency_keys k LEFT JOIN payments p ON p.id = k.payment_id
                                WHERE p.id IS NULL)
                             + (SELECT count(*) FROM idempotency_keys k JOIN payments p ON p.id = k.payment_id
                                WHERE k.request_hash IS DISTINCT FROM encode(sha256(convert_to(
                                          p.payer_account_id || '|' || p.payee_account_id || '|' || p.amount_minor
                                          || '|' || p.currency, 'UTF8')), 'hex')
                                   OR k.response_body IS NULL
                                   OR (k.response_body::jsonb ->> 'id') IS DISTINCT FROM p.id::text
                                   OR k.response_status IS DISTINCT FROM
                                      (CASE WHEN p.status = 'DECLINED' THEN 201 ELSE 202 END))"""));
        long violations = checks.stream().mapToLong(Check::violations).sum();
        return new InvariantSummary("payments schema", checks, pending, violations, violations == 0);
    }

    private Check check(String invariant, String description, String countSql) {
        long violations = jdbc.sql(countSql).query(Long.class).single();
        return new Check(invariant, description, violations, violations == 0);
    }
}
