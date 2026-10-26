package com.ledgerflow.e2e;

import com.ledgerflow.ledger.LedgerService;
import com.ledgerflow.ledger.LedgerServiceApplication;
import com.ledgerflow.payment.PaymentServiceApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import com.ledgerflow.common.MongoStore;
import com.ledgerflow.common.MongoFixture;
import org.bson.Document;
import static com.ledgerflow.common.MongoStore.eq;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Both real services (each with its own database and user) against one MongoDB 8.0 and one Kafka,
 * started once per JVM.
 */
final class Stack {

    static final long SYSTEM_ACCOUNT = 1;

    static final MongoDBContainer MONGO = MongoFixture.start();
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.0");

    static final ConfigurableApplicationContext LEDGER;
    static final ConfigurableApplicationContext PAYMENT;
    static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    static {
        KAFKA.start();
        LEDGER = start(LedgerServiceApplication.class, LedgerServiceApplication.CONFIG_NAME, "ledger");
        PAYMENT = start(PaymentServiceApplication.class, PaymentServiceApplication.CONFIG_NAME, "payments");
    }

    private Stack() {}

    private static ConfigurableApplicationContext start(Class<?> app, String configName, String database) {
        return new SpringApplicationBuilder(app).properties(configName).run(
                "--server.port=0",
                "--spring.mongodb.uri=" + MongoFixture.uri(MONGO, database),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--ledgerflow.reconcile.interval-ms=1000"); // stale threshold stays 30 s
    }

    static MongoTemplate ledgerDb() {
        return LEDGER.getBean(MongoTemplate.class);
    }

    static MongoTemplate paymentDb() {
        return PAYMENT.getBean(MongoTemplate.class);
    }

    /** Creates an account in the ledger; customers are funded from SYSTEM through a normal posting. */
    static long account(String type, long fundedMinor) {
        long id = MongoStore.nextId();
        ledgerDb().insert(MongoStore.account(id, type), "accounts");
        if (fundedMinor > 0) {
            LEDGER.getBean(LedgerService.class).post(UUID.randomUUID(), SYSTEM_ACCOUNT, id, fundedMinor);
        }
        return id;
    }

    static long balance(long accountId) {
        return ledgerDb().findOne(eq("_id", accountId), Document.class, "accounts").getLong("balance_minor");
    }

    static void post(long fromAccount, long toAccount, long amountMinor) {
        LEDGER.getBean(LedgerService.class).post(UUID.randomUUID(), fromAccount, toAccount, amountMinor);
    }

    static long ledgerTransactions(UUID paymentId) {
        return ledgerDb().count(eq("payment_id", paymentId), "ledger_transactions");
    }

    static String requestHash(long payer, long payee, long amount) {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                    .digest((payer + "|" + payee + "|" + amount + "|USD").getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    record Response(int status, JsonNode body) {}

    static Response pay(String idempotencyKey, long payer, long payee, long amountMinor) throws Exception {
        String body = """
                {"payerAccountId":%d,"payeeAccountId":%d,"amountMinor":%d,"currency":"USD"}"""
                .formatted(payer, payee, amountMinor);
        return send(HttpRequest.newBuilder(uri("/api/payments"))
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", idempotencyKey)
                .POST(HttpRequest.BodyPublishers.ofString(body)).build());
    }

    static JsonNode awaitTerminal(UUID id) {
        return org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(90))
                .until(() -> payment(id), p -> !p.get("status").asString().equals("PENDING_LEDGER"));
    }

    static JsonNode payment(UUID id) throws Exception {
        return send(HttpRequest.newBuilder(uri("/api/payments/" + id)).GET().build()).body();
    }

    private static URI uri(String path) {
        return URI.create("http://localhost:" + PAYMENT.getEnvironment().getProperty("local.server.port") + path);
    }

    private static Response send(HttpRequest request) throws Exception {
        var raw = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        return new Response(raw.statusCode(), JSON.readTree(raw.body()));
    }
}
