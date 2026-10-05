package com.ledgerflow.e2e;

import com.ledgerflow.ledger.LedgerService;
import com.ledgerflow.ledger.LedgerServiceApplication;
import com.ledgerflow.payment.PaymentServiceApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.UUID;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Both real services (each with its own DB user/schema) against one Postgres 17 and one Kafka,
 * started once per JVM.
 */
final class Stack {

    static final long SYSTEM_ACCOUNT = 1;

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17")
            .withDatabaseName("ledgerflow")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("../infra/postgres/init/01-roles-schemas.sql")),
                    "/docker-entrypoint-initdb.d/01-roles-schemas.sql");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.0");

    static final ConfigurableApplicationContext LEDGER;
    static final ConfigurableApplicationContext PAYMENT;
    static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    static {
        POSTGRES.start();
        KAFKA.start();
        LEDGER = start(LedgerServiceApplication.class, LedgerServiceApplication.CONFIG_NAME, "ledger_user");
        PAYMENT = start(PaymentServiceApplication.class, PaymentServiceApplication.CONFIG_NAME, "payment_user");
    }

    private Stack() {}

    private static ConfigurableApplicationContext start(Class<?> app, String configName, String dbUser) {
        return new SpringApplicationBuilder(app).properties(configName).run(
                "--server.port=0",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + dbUser,
                "--spring.datasource.password=" + dbUser,
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers());
    }

    static JdbcClient ledgerDb() {
        return LEDGER.getBean(JdbcClient.class);
    }

    static JdbcClient paymentDb() {
        return PAYMENT.getBean(JdbcClient.class);
    }

    /** Creates an account in the ledger; customers are funded from SYSTEM through a normal posting. */
    static long account(String type, long fundedMinor) {
        long id = ledgerDb().sql("INSERT INTO accounts (type) VALUES (?) RETURNING id").param(type)
                .query(Long.class).single();
        if (fundedMinor > 0) {
            LEDGER.getBean(LedgerService.class).post(UUID.randomUUID(), SYSTEM_ACCOUNT, id, fundedMinor);
        }
        return id;
    }

    static long balance(long accountId) {
        return ledgerDb().sql("SELECT balance_minor FROM accounts WHERE id = ?").param(accountId)
                .query(Long.class).single();
    }

    static long ledgerTransactions(UUID paymentId) {
        return ledgerDb().sql("SELECT count(*) FROM ledger_transactions WHERE payment_id = ?").param(paymentId)
                .query(Long.class).single();
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
