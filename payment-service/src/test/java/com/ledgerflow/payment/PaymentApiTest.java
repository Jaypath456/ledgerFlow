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
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import java.util.Date;
import static com.ledgerflow.common.MongoStore.eq;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class PaymentApiTest extends PaymentTestSupport {

    // Fresh payer per test keeps the velocity rule from coupling tests.
    private static final AtomicLong ACCOUNTS = new AtomicLong(10_000);

    @LocalServerPort
    int port;

    @Autowired
    MongoTemplate mongo;

    @Autowired
    JsonMapper json;

    @MockitoSpyBean
    PaymentRepository repo;

    @Autowired
    FaultInjector faults;

    @Autowired
    InMemoryUserDetailsManager users;

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

        var outbox = mongo.find(eq("aggregate_id", id), Document.class, "outbox");
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
    void concurrentHotPayerNeverExceedsFiveAdmissionsAndDeclinesReplayExactly() throws Exception {
        long payer = ACCOUNTS.incrementAndGet();
        String payload = body(payer, ACCOUNTS.incrementAndGet(), 100, "USD");
        var ready = new CountDownLatch(64);
        var start = new CountDownLatch(1);
        List<Future<Response>> futures = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 64; i++) {
                String key = UUID.randomUUID().toString();
                keys.add(key);
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    return post(key, payload);
                }));
            }
            assertThat(ready.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            start.countDown();
        }
        List<Response> responses = new ArrayList<>();
        for (var f : futures) responses.add(f.get(30, java.util.concurrent.TimeUnit.SECONDS));
        assertThat(responses.stream().filter(r -> r.status() == 202)).hasSize(5);
        assertThat(responses.stream().filter(r -> r.status() == 201)).hasSize(59);
        for (int i = 0; i < responses.size(); i++) {
            var r = responses.get(i);
            UUID id = UUID.fromString(r.body().get("id").asString());
            assertThat(outboxRows(id)).isEqualTo(r.status() == 202 ? 1 : 0);
            if (r.status() == 201) {
                assertThat(r.body().get("declineReason").asString()).isEqualTo("VELOCITY_LIMIT");
                assertThat(post(keys.get(i), payload).raw().body()).isEqualTo(r.raw().body());
            }
        }
        assertThat(mongo.count(Query.query(org.springframework.data.mongodb.core.query.Criteria.where("payer_account_id")
                .is(payer).and("status").ne("DECLINED")), "payments")).isEqualTo(5);
    }

    @Test
    void rollingWindowHonorsExistingHistoryAndReplayDoesNotConsumeAdmission() throws Exception {
        long payer = ACCOUNTS.incrementAndGet();
        String payload = body(payer, ACCOUNTS.incrementAndGet(), 100, "USD");
        List<String> ids = new ArrayList<>();
        String key = UUID.randomUUID().toString();
        var first = post(key, payload);
        ids.add(first.body().get("id").asString());
        for (int i = 0; i < 4; i++) ids.add(post(UUID.randomUUID().toString(), payload).body().get("id").asString());
        long version = mongo.findOne(eq("_id", payer), Document.class, "velocity_guards").getLong("version");
        assertThat(post(key, payload).raw().body()).isEqualTo(first.raw().body());
        assertThat(mongo.findOne(eq("_id", payer), Document.class, "velocity_guards").getLong("version")).isEqualTo(version);
        assertDeclined(payload, "VELOCITY_LIMIT");
        // The guard is only a lock, not the count: history from before guard creation still counts.
        mongo.remove(eq("_id", payer), "velocity_guards");
        assertDeclined(payload, "VELOCITY_LIMIT");
        for (var id : ids.subList(0, 2)) mongo.updateFirst(eq("_id", id),
                Update.update("created_at", Date.from(java.time.Instant.now().minusSeconds(61))), "payments");
        assertThat(post(UUID.randomUUID().toString(), payload).status()).isEqualTo(202);
        assertThat(post(UUID.randomUUID().toString(), payload).status()).isEqualTo(202);
        assertDeclined(payload, "VELOCITY_LIMIT");
    }

    @Test
    void rolledBackPaymentDoesNotConsumeVelocitySlot() throws Exception {
        long payer = ACCOUNTS.incrementAndGet();
        String payload = body(payer, ACCOUNTS.incrementAndGet(), 100, "USD");
        faults.armOnce(FaultInjector.Point.BEFORE_PAYMENT_OUTBOX_COMMIT);
        String key = UUID.randomUUID().toString();
        assertThat(post(key, payload).status()).isEqualTo(500);
        assertThat(mongo.exists(eq("_id", payer), "velocity_guards")).isFalse();
        assertThat(mongo.count(eq("payer_account_id", payer), "payments")).isZero();
        assertThat(post(key, payload).status()).isEqualTo(202);
        for (int i = 0; i < 4; i++) assertThat(post(UUID.randomUUID().toString(), payload).status()).isEqualTo(202);
        assertDeclined(payload, "VELOCITY_LIMIT");
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
        mongo.updateFirst(eq("_id", first.body().get("id").asString()),
                Update.update("status", "COMPLETED").set("updated_at", new Date()), "payments");

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
        assertThat(mongo.count(eq("payment_id", id), "idempotency_keys")).isEqualTo(1);
    }

    private static String body(long payer, long payee, long amount, String currency) {
        return """
                {"payerAccountId":%d,"payeeAccountId":%d,"amountMinor":%d,"currency":"%s"}"""
                .formatted(payer, payee, amount, currency);
    }

    private Response post(String key, String body) throws Exception {
        long payer = 2;
        try { payer = json.readTree(body).path("payerAccountId").asLong(2); }
        catch (RuntimeException ignored) { }
        var b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/payments"))
                .header("Authorization", authorization(payer))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (key != null) {
            b.header("Idempotency-Key", key);
        }
        return send(b.build());
    }

    private Response get(String path) throws Exception {
        var payment = mongo.findOne(eq("_id", path.substring(path.lastIndexOf('/') + 1)), Document.class, "payments");
        long owner = payment == null ? 2 : payment.getLong("payer_account_id");
        return send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Authorization", authorization(owner)).GET().build());
    }

    private synchronized String authorization(long account) {
        String name = "test-" + account;
        if (!users.userExists(name)) users.createUser(User.withUsername(name).password("{noop}test-only")
                .authorities(PaymentSecurity.accountAuthority(account)).build());
        return "Basic " + Base64.getEncoder().encodeToString((name + ":test-only").getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void authenticationAndAccountOwnershipAreEnforcedBeforeIdempotency() throws Exception {
        long payer = ACCOUNTS.incrementAndGet();
        long other = ACCOUNTS.incrementAndGet();
        String key = UUID.randomUUID().toString();
        String payload = body(payer, other, 100, "USD");
        long before = count("payments") + count("idempotency_keys") + count("outbox");
        var uri = URI.create("http://localhost:" + port + "/api/payments");
        for (String credentials : List.of("", "Basic YWxpY2U6d3Jvbmc=", authorization(other))) {
            var request = HttpRequest.newBuilder(uri).header("Content-Type", "application/json")
                    .header("Idempotency-Key", key).POST(HttpRequest.BodyPublishers.ofString(payload));
            if (!credentials.isEmpty()) request.header("Authorization", credentials);
            assertThat(http.send(request.build(), HttpResponse.BodyHandlers.ofString()).statusCode())
                    .isEqualTo(credentials.equals(authorization(other)) ? 403 : 401);
        }
        assertThat(count("payments") + count("idempotency_keys") + count("outbox")).isEqualTo(before);
        var created = post(key, payload);
        assertThat(created.status()).isEqualTo(202);
        var idUri = URI.create(uri + "/" + created.body().get("id").asString());
        assertThat(http.send(HttpRequest.newBuilder(idUri).GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(401);
        assertThat(http.send(HttpRequest.newBuilder(idUri).header("Authorization", authorization(ACCOUNTS.incrementAndGet()))
                .GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(404);
        // The payee can read the payment, but cannot replay a debit from someone else's account.
        assertThat(http.send(HttpRequest.newBuilder(idUri).header("Authorization", authorization(other))
                .GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
        assertThat(http.send(HttpRequest.newBuilder(uri).header("Content-Type", "application/json")
                .header("Authorization", authorization(other)).header("Idempotency-Key", key)
                .POST(HttpRequest.BodyPublishers.ofString(payload)).build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(403);
    }

    @Test
    void browserOriginAndNonJsonWritesAreRejectedAndHealthStaysPublic() throws Exception {
        String payload = body(ACCOUNTS.incrementAndGet(), ACCOUNTS.incrementAndGet(), 100, "USD");
        long payer = json.readTree(payload).get("payerAccountId").asLong();
        var uri = URI.create("http://localhost:" + port + "/api/payments");
        assertThat(http.send(HttpRequest.newBuilder(uri).header("Authorization", authorization(payer))
                .header("Origin", "https://example.invalid").header("Content-Type", "application/json")
                .header("Idempotency-Key", UUID.randomUUID().toString()).POST(HttpRequest.BodyPublishers.ofString(payload))
                .build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(403);
        assertThat(http.send(HttpRequest.newBuilder(uri).header("Authorization", authorization(payer))
                .header("Content-Type", "text/plain").header("Idempotency-Key", UUID.randomUUID().toString())
                .POST(HttpRequest.BodyPublishers.ofString(payload)).build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(415);
        assertThat(http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/health"))
                .GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
    }

    private Response send(HttpRequest request) throws Exception {
        var raw = http.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode body = raw.body().isEmpty() ? null : json.readTree(raw.body());
        return new Response(raw.statusCode(), body, raw);
    }

    private long count(String table) {
        return mongo.count(new Query(), table);
    }

    private long paymentsForKey(String key) {
        return mongo.find(eq("key", key), Document.class, "idempotency_keys").stream()
                .filter(k -> mongo.exists(eq("_id", k.getString("payment_id")), "payments")).count();
    }

    private long outboxRows(UUID paymentId) {
        return mongo.count(eq("aggregate_id", paymentId), "outbox");
    }
}
