package com.ledgerflow.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ledgerflow.common.Topics;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import tools.jackson.databind.json.JsonMapper;

/** Trace context across the payment outbox: captured with the payment, restored by the relay, sent as a Kafka header. */
class PaymentTracingTest extends PaymentTestSupport {

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final AtomicLong ACCOUNTS = new AtomicLong(80_000);

    @LocalServerPort
    int port;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    JsonMapper json;

    @Autowired
    InMemoryUserDetailsManager users;

    @Test
    void outboxRowWithoutTraceContextStillPublishes() {
        // As written before the trace columns existed (or with no current span): both columns NULL.
        String marker = "legacy-" + UUID.randomUUID();
        long id = jdbc.sql("""
                        INSERT INTO outbox (aggregate_id, topic, event_key, payload)
                        VALUES (?, ?, 'legacy', CAST(? AS jsonb)) RETURNING id""")
                .params(UUID.randomUUID(), Topics.PAYMENTS_REQUESTED, "{\"marker\":\"" + marker + "\"}")
                .query(Long.class).single();

        await().atMost(WAIT).until(() -> jdbc.sql("SELECT published_at IS NOT NULL FROM outbox WHERE id = ?")
                .param(id).query(Boolean.class).single());
        var records = await().atMost(WAIT).until(() -> readAll(Topics.PAYMENTS_REQUESTED).stream()
                .filter(r -> r.value().contains(marker)).toList(), list -> !list.isEmpty());
        assertThat(records).hasSize(1);
        assertThat(jdbc.sql("SELECT traceparent IS NULL AND tracestate IS NULL FROM outbox WHERE id = ?").param(id)
                .query(Boolean.class).single()).isTrue();
        assertThat(header(records.getFirst(), "traceparent")).matches("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}"); // a new trace
    }

    @Test
    void paymentOutboxRowCarriesTheRequestTraceContext() throws Exception {
        String traceId = HexFormat.of().formatHex(randomBytes(16));
        String incoming = "00-" + traceId + "-" + HexFormat.of().formatHex(randomBytes(8)) + "-01";
        long payer = ACCOUNTS.incrementAndGet();
        String body = """
                {"payerAccountId":%d,"payeeAccountId":%d,"amountMinor":100,"currency":"USD"}"""
                .formatted(payer, ACCOUNTS.incrementAndGet());
        var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/payments"))
                .header("Authorization", authorization(payer)).header("Content-Type", "application/json")
                .header("Idempotency-Key", UUID.randomUUID().toString()).header("traceparent", incoming)
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        UUID id = UUID.fromString(json.readTree(response.body()).get("id").asString());

        String stored = jdbc.sql("SELECT traceparent FROM outbox WHERE aggregate_id = ?").param(id).query(String.class).single();
        assertThat(stored).startsWith("00-" + traceId + "-");
        var record = await().atMost(WAIT).until(() -> readAll(Topics.PAYMENTS_REQUESTED).stream()
                .filter(r -> r.value().contains(id.toString())).toList(), list -> !list.isEmpty()).getFirst();
        assertThat(header(record, "traceparent")).startsWith("00-" + traceId + "-");
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        var h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        ThreadLocalRandom.current().nextBytes(b);
        b[0] |= 1; // never all zero (invalid in W3C)
        return b;
    }

    private String authorization(long account) {
        String name = "trace-test-" + account;
        if (!users.userExists(name)) users.createUser(User.withUsername(name).password("{noop}test-only")
                .authorities(PaymentSecurity.accountAuthority(account)).build());
        return "Basic " + Base64.getEncoder().encodeToString((name + ":test-only").getBytes(StandardCharsets.UTF_8));
    }
}
