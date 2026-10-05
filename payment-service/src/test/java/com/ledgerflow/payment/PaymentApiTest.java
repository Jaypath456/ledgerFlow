package com.ledgerflow.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;

import com.ledgerflow.common.FaultInjector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class PaymentApiTest extends PaymentTestSupport {

    // Fresh payer per test keeps the velocity rule from coupling tests.
    private static final AtomicLong ACCOUNTS = new AtomicLong(10_000);

    @LocalServerPort
    int port;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    JsonMapper json;

    @MockitoSpyBean
    PaymentRepository repo;

    @Autowired
    FaultInjector faults;

    private final HttpClient http = HttpClient.newHttpClient();

    record Response(int status, JsonNode body, HttpResponse<String> raw) {}

    @Test
    void acceptedPaymentIsPendingWithExactlyOneOutboxRow() throws Exception {
        long payer = ACCOUNTS.incrementAndGet();
        long payee = ACCOUNTS.incrementAndGet();
        var r = post(UUID.randomUUID().toString(), body(payer, payee, 2_500, "USD"));

        assertThat(r.status()).isEqualTo(202);
        assertThat(r.body().get("status").asString()).isEqualTo("PENDING_LEDGER");
        assertThat(r.raw().headers().firstValue("Idempotent-Replayed")).hasValue("false");
        UUID id = UUID.fromString(r.body().get("id").asString());

        var outbox = jdbc.sql("SELECT topic, event_key, payload::text AS payload FROM outbox WHERE aggregate_id = ?")
                .param(id).query().listOfRows();
        assertThat(outbox).hasSize(1);
        assertThat(outbox.getFirst().get("topic")).isEqualTo("payments.requested");
        assertThat(outbox.getFirst().get("event_key")).isEqualTo(String.valueOf(payer));
        JsonNode event = json.readTree((String) outbox.getFirst().get("payload"));
        assertThat(event.get("eventType").asString()).isEqualTo("PaymentRequested");
        assertThat(event.get("version").asInt()).isEqualTo(1);
        assertThat(event.get("paymentId").asString()).isEqualTo(id.toString());
        assertThat(event.get("payerAccountId").asLong()).isEqualTo(payer);
        assertThat(event.get("payeeAccountId").asLong()).isEqualTo(payee);
        assertThat(event.get("amountMinor").asLong()).isEqualTo(2_500);
        assertThat(event.get("eventId").asString()).isNotBlank();
        assertThat(event.get("occurredAt").asString()).isNotBlank();
    }

    @Test
    void amountOverLimitIsDeclinedWithoutOutbox() throws Exception {
        assertDeclined(body(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet(), 1_000_001, "USD"), "AMOUNT_LIMIT");
    }

    @Test
    void amountAtLimitIsAccepted() throws Exception {
        var r = post(UUID.randomUUID().toString(), body(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet(), 1_000_000, "USD"));
        assertThat(r.status()).isEqualTo(202);
        assertThat(r.body().get("status").asString()).isEqualTo("PENDING_LEDGER");
    }

    @Test
    void blockedAccountIsDeclinedWithoutOutbox() throws Exception {
        assertDeclined(body(BLOCKED_ACCOUNT, ACCOUNTS.incrementAndGet(), 100, "USD"), "BLOCKED_ACCOUNT");
        assertDeclined(body(ACCOUNTS.incrementAndGet(), BLOCKED_ACCOUNT, 100, "USD"), "BLOCKED_ACCOUNT");
    }

    @Test
    void sixthPaymentFromPayerWithinMinuteIsDeclined() throws Exception {
        long payer = ACCOUNTS.incrementAndGet();
        for (int i = 0; i < 5; i++) {
            var r = post(UUID.randomUUID().toString(), body(payer, ACCOUNTS.incrementAndGet(), 100, "USD"));
            assertThat(r.body().get("status").asString()).isEqualTo("PENDING_LEDGER");
        }
        assertDeclined(body(payer, ACCOUNTS.incrementAndGet(), 100, "USD"), "VELOCITY_LIMIT");
    }

    @Test
    void invalidRequestsAreRejectedAndStoreNothing() throws Exception {
        long payer = ACCOUNTS.incrementAndGet();
        long payee = ACCOUNTS.incrementAndGet();
        long before = count("payments") + count("idempotency_keys") + count("outbox");

        assertThat(post(UUID.randomUUID().toString(), body(payer, payee, 0, "USD")).status()).isEqualTo(400);
        assertThat(post(UUID.randomUUID().toString(), body(payer, payee, -5, "USD")).status()).isEqualTo(400);
        assertThat(post(UUID.randomUUID().toString(), body(payer, payer, 100, "USD")).status()).isEqualTo(400);
        assertThat(post(UUID.randomUUID().toString(), body(payer, payee, 100, "EUR")).status()).isEqualTo(400);
        assertThat(post(UUID.randomUUID().toString(), "{\"payerAccountId\":1,\"amountMinor\":5,\"currency\":\"USD\"}").status())
                .isEqualTo(400);
        assertThat(post(UUID.randomUUID().toString(), "not json").status()).isEqualTo(400);
        assertThat(post(" ", body(payer, payee, 100, "USD")).status()).isEqualTo(400);
        assertThat(post(null, body(payer, payee, 100, "USD")).status()).isEqualTo(400);

        assertThat(count("payments") + count("idempotency_keys") + count("outbox")).isEqualTo(before);
    }

    @Test
    void getReturnsPaymentOr404() throws Exception {
        var created = post(UUID.randomUUID().toString(), body(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet(), 700, "USD"));
        String id = created.body().get("id").asString();

        var found = get("/api/payments/" + id);
        assertThat(found.status()).isEqualTo(200);
        assertThat(found.body().get("id").asString()).isEqualTo(id);
        assertThat(found.body().get("amountMinor").asLong()).isEqualTo(700);
        assertThat(found.body().get("currency").asString()).isEqualTo("USD");
        assertThat(found.body().get("status").asString()).isEqualTo("PENDING_LEDGER");

        assertThat(get("/api/payments/" + UUID.randomUUID()).status()).isEqualTo(404);
    }

    @Test
    void sameKeySameBodyReplaysOriginalPayment() throws Exception {
        String key = UUID.randomUUID().toString();
        long payer = ACCOUNTS.incrementAndGet();
        long payee = ACCOUNTS.incrementAndGet();
        var first = post(key, body(payer, payee, 900, "USD"));
        // Same canonical request, different JSON field order and whitespace.
        var replay = post(key, """
                { "currency": "USD", "amountMinor": 900,
                  "payeeAccountId": %d, "payerAccountId": %d }""".formatted(payee, payer));

        assertThat(first.status()).isEqualTo(202);
        assertThat(replay.status()).isEqualTo(202);
        assertThat(replay.raw().body()).isEqualTo(first.raw().body());
        assertThat(replay.raw().headers().firstValue("Idempotent-Replayed")).hasValue("true");
        assertThat(paymentsForKey(key)).isEqualTo(1);
        assertThat(outboxRows(UUID.fromString(first.body().get("id").asString()))).isEqualTo(1);
    }

    @Test
    void declinedPaymentReplaysAsDeclined() throws Exception {
        String key = UUID.randomUUID().toString();
        String request = body(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet(), 2_000_000, "USD");
        var first = post(key, request);
        var replay = post(key, request);
        assertThat(first.status()).isEqualTo(201);
        assertThat(replay.status()).isEqualTo(201);
        assertThat(replay.raw().body()).isEqualTo(first.raw().body());
        assertThat(replay.body().get("status").asString()).isEqualTo("DECLINED");
        assertThat(paymentsForKey(key)).isEqualTo(1);
    }

    @Test
    void replayReturnsStoredResponseEvenAfterPaymentProgresses() throws Exception {
        String key = UUID.randomUUID().toString();
        String request = body(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet(), 450, "USD");
        var first = post(key, request);
        jdbc.sql("UPDATE payments SET status = 'COMPLETED', updated_at = now() WHERE id = ?")
                .param(UUID.fromString(first.body().get("id").asString())).update();

        var replay = post(key, request);
        assertThat(replay.status()).isEqualTo(202);
        assertThat(replay.raw().body()).isEqualTo(first.raw().body());
        assertThat(replay.body().get("status").asString()).isEqualTo("PENDING_LEDGER");
        assertThat(get("/api/payments/" + first.body().get("id").asString()).body().get("status").asString())
                .isEqualTo("COMPLETED");
    }

    @Test
    void sameKeyDifferentBodyIs422() throws Exception {
        String key = UUID.randomUUID().toString();
        long payer = ACCOUNTS.incrementAndGet();
        long payee = ACCOUNTS.incrementAndGet();
        var first = post(key, body(payer, payee, 100, "USD"));
        var conflict = post(key, body(payer, payee, 101, "USD"));

        assertThat(conflict.status()).isEqualTo(422);
        assertThat(paymentsForKey(key)).isEqualTo(1);
        assertThat(get("/api/payments/" + first.body().get("id").asString()).body().get("amountMinor").asLong())
                .isEqualTo(100);
    }

    @Test
    void concurrentSameKeyStormCreatesExactlyOnePayment() throws Exception {
        int threads = 32;
        String key = UUID.randomUUID().toString();
        String request = body(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet(), 4_200, "USD");
        var start = new CountDownLatch(1);
        List<Future<Response>> futures = new ArrayList<>();
        try (var pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return post(key, request);
                }));
            }
            start.countDown();
        }

        var ids = new ArrayList<String>();
        int originals = 0;
        for (var f : futures) {
            Response r = f.get();
            assertThat(r.status()).isEqualTo(202);
            assertThat(r.raw().body()).isEqualTo(futures.getFirst().get().raw().body());
            ids.add(r.body().get("id").asString());
            if (r.raw().headers().firstValue("Idempotent-Replayed").orElseThrow().equals("false")) {
                originals++;
            }
        }
        assertThat(ids).hasSize(threads).containsOnly(ids.getFirst());
        assertThat(originals).isEqualTo(1);
        assertThat(paymentsForKey(key)).isEqualTo(1);
        assertThat(outboxRows(UUID.fromString(ids.getFirst()))).isEqualTo(1);
    }

    @Test
    void failureBeforeCommitLeavesNoPartialStateAndKeyIsReusable() throws Exception {
        String key = UUID.randomUUID().toString();
        String request = body(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet(), 300, "USD");
        long before = count("payments") + count("idempotency_keys") + count("outbox");

        doThrow(new IllegalStateException("injected outbox failure"))
                .when(repo).insertOutbox(any(), anyString(), anyString(), anyString());
        assertThat(post(key, request).status()).isEqualTo(500);
        assertThat(count("payments") + count("idempotency_keys") + count("outbox")).isEqualTo(before);

        org.mockito.Mockito.reset(repo);
        var retry = post(key, request);
        assertThat(retry.status()).isEqualTo(202);
        assertThat(retry.raw().headers().firstValue("Idempotent-Replayed")).hasValue("false");
        assertThat(paymentsForKey(key)).isEqualTo(1);
        assertThat(outboxRows(UUID.fromString(retry.body().get("id").asString()))).isEqualTo(1);
    }

    @Test
    void crashBeforeOutboxCommitLeavesNothingAndRetrySucceeds() throws Exception {
        String key = UUID.randomUUID().toString();
        String request = body(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet(), 310, "USD");
        long before = count("payments") + count("idempotency_keys") + count("outbox");

        faults.armOnce(FaultInjector.Point.BEFORE_PAYMENT_OUTBOX_COMMIT);
        assertThat(post(key, request).status()).isEqualTo(500);
        assertThat(count("payments") + count("idempotency_keys") + count("outbox")).isEqualTo(before);

        var retry = post(key, request);
        assertThat(retry.status()).isEqualTo(202);
        assertThat(paymentsForKey(key)).isEqualTo(1);
        assertThat(outboxRows(UUID.fromString(retry.body().get("id").asString()))).isEqualTo(1);
    }

    private void assertDeclined(String request, String reason) throws Exception {
        var r = post(UUID.randomUUID().toString(), request);
        assertThat(r.status()).isEqualTo(201);
        assertThat(r.body().get("status").asString()).isEqualTo("DECLINED");
        assertThat(r.body().get("declineReason").asString()).isEqualTo(reason);
        UUID id = UUID.fromString(r.body().get("id").asString());
        assertThat(outboxRows(id)).isZero();
        assertThat(jdbc.sql("SELECT count(*) FROM idempotency_keys WHERE payment_id = ?").param(id)
                .query(Long.class).single()).isEqualTo(1);
    }

    private static String body(long payer, long payee, long amount, String currency) {
        return """
                {"payerAccountId":%d,"payeeAccountId":%d,"amountMinor":%d,"currency":"%s"}"""
                .formatted(payer, payee, amount, currency);
    }

    private Response post(String key, String body) throws Exception {
        var b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/payments"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (key != null) {
            b.header("Idempotency-Key", key);
        }
        return send(b.build());
    }

    private Response get(String path) throws Exception {
        return send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build());
    }

    private Response send(HttpRequest request) throws Exception {
        var raw = http.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode body = raw.body().isEmpty() ? null : json.readTree(raw.body());
        return new Response(raw.statusCode(), body, raw);
    }

    private long count(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
    }

    private long paymentsForKey(String key) {
        return jdbc.sql("SELECT count(*) FROM payments p JOIN idempotency_keys k ON k.payment_id = p.id WHERE k.key = ?")
                .param(key).query(Long.class).single();
    }

    private long outboxRows(UUID paymentId) {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE aggregate_id = ?").param(paymentId).query(Long.class).single();
    }
}
