package com.ledgerflow.e2e;

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
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Both services in the "demo" profile (as ./start.sh runs them), on their OWN Postgres and Kafka so
 * they never share consumer groups or data with the default-profile {@link Stack}.
 */
final class DemoStack {

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

    private DemoStack() {}

    private static ConfigurableApplicationContext start(Class<?> app, String configName, String dbUser) {
        return new SpringApplicationBuilder(app).properties(configName).profiles("demo").run(
                "--server.port=0",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + dbUser,
                "--spring.datasource.password=" + dbUser,
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                // same demo risk settings as infra/docker-compose.demo.yml
                "--ledgerflow.risk.velocity-max=1000000",
                "--ledgerflow.risk.blocked-accounts=999999",
                "--ledgerflow.reconcile.interval-ms=1000");
    }

    static String payment(String path) {
        return "http://localhost:" + PAYMENT.getEnvironment().getProperty("local.server.port") + path;
    }

    static String ledger(String path) {
        return "http://localhost:" + LEDGER.getEnvironment().getProperty("local.server.port") + path;
    }

    static JsonNode get(String url) throws Exception {
        return JSON.readTree(HTTP.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString()).body());
    }

    static HttpResponse<String> post(String url, Object body) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(url)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body instanceof String s ? s : JSON.writeValueAsString(body))).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    static JsonNode postJson(String url, Object body) throws Exception {
        var res = post(url, body);
        if (res.statusCode() >= 400) {
            throw new IllegalStateException(url + " -> HTTP " + res.statusCode() + ": " + res.body());
        }
        return JSON.readTree(res.body());
    }

    /** Creates a named demo account with a unique name; returns {name, id}. */
    static JsonNode account(String baseName, long balanceMinor) throws Exception {
        String name = baseName + UUID.randomUUID().toString().substring(0, 6);
        return postJson(ledger("/demo/named-accounts"), java.util.Map.of("name", name, "balanceMinor", balanceMinor));
    }

    static long balance(long id) throws Exception {
        return get(ledger("/demo/accounts?ids=" + id)).get(0).get("balanceMinor").asLong();
    }
}
