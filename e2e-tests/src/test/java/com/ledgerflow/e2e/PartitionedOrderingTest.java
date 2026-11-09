package com.ledgerflow.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ledgerflow.common.Topics;
import com.ledgerflow.ledger.LedgerService;
import com.ledgerflow.ledger.LedgerServiceApplication;
import com.ledgerflow.payment.PaymentSecurity;
import com.ledgerflow.payment.PaymentServiceApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The ADR 0005 configuration: 6 partitions per topic and 6 consumer threads per service (ledgerflow.topics.partitions=6,
 * spring.kafka.listener.concurrency=6). Own database and broker, so the shared Stack keeps its default of 3. Each payer funds one
 * payment of 800 out of 1,000 and then submits a second one: only the first submitted can be posted, so the outcomes show whether
 * a payer's requests were processed in the order they were accepted, which needs the same partition (key = payer) and one thread.
 */
class PartitionedOrderingTest {

    private static final int PARTITIONS = 6;
    private static final int PAYERS = 60;
    private static final long SYSTEM_ACCOUNT = 1;
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @SuppressWarnings("resource")
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.11")
            .withDatabaseName("ledgerflow")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("../infra/postgres/init/01-roles-schemas.sql")),
                    "/docker-entrypoint-initdb.d/01-roles-schemas.sql");
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.2");

    private static ConfigurableApplicationContext ledger;
    private static ConfigurableApplicationContext payment;

    @BeforeAll
    static void start() {
        POSTGRES.start();
        KAFKA.start();
        ledger = run(LedgerServiceApplication.class, LedgerServiceApplication.CONFIG_NAME, "ledger_user");
        payment = run(PaymentServiceApplication.class, PaymentServiceApplication.CONFIG_NAME, "payment_user");
    }

    @AfterAll
    static void stop() {
        if (payment != null) payment.close();
        if (ledger != null) ledger.close();
        KAFKA.stop();
        POSTGRES.stop();
    }

    private static ConfigurableApplicationContext run(Class<?> app, String configName, String dbUser) {
        return new SpringApplicationBuilder(app).properties(configName).run(
                "--server.port=0",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + dbUser,
                "--spring.datasource.password=" + dbUser,
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--ledgerflow.topics.partitions=" + PARTITIONS,
                "--spring.kafka.listener.concurrency=" + PARTITIONS);
    }

    @Test
    void topicsAndConsumerThreadsFollowTheConfiguration() throws Exception {
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var described = admin.describeTopics(List.of(Topics.PAYMENTS_REQUESTED, Topics.LEDGER_RESULTS,
                    Topics.PAYMENTS_REQUESTED + Topics.DLT_SUFFIX, Topics.LEDGER_RESULTS + Topics.DLT_SUFFIX)).allTopicNames().get();
            assertThat(described).hasSize(4).allSatisfy((name, d) -> assertThat(d.partitions()).as(name).hasSize(PARTITIONS));
        }
        for (var context : List.of(ledger, payment)) {
            assertThat(context.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers()).hasSize(1).allSatisfy(c ->
                    assertThat(((ConcurrentMessageListenerContainer<?, ?>) c).getConcurrency()).isEqualTo(PARTITIONS));
        }
    }

    @Test
    void aPayersRequestsAreProcessedInAcceptedOrderAcrossSixPartitions() throws Exception {
        long merchant = account("MERCHANT", 0);
        List<Long> payers = new ArrayList<>();
        for (int i = 0; i < PAYERS; i++) payers.add(account("CUSTOMER", 1_000));
        long partitionsUsed = payers.stream().map(p -> Utils.toPositive(Utils.murmur2(String.valueOf(p).getBytes(StandardCharsets.UTF_8))) % PARTITIONS).distinct().count();
        assertThat(partitionsUsed).as("payers hash onto most of the six partitions").isGreaterThanOrEqualTo(4);

        List<Future<UUID[]>> submitted = new ArrayList<>();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (long payer : payers) {
                submitted.add(pool.submit(() -> {
                    UUID first = pay(UUID.randomUUID().toString(), payer, merchant, 800);   // accepted (202) before the second is sent
                    UUID second = pay(UUID.randomUUID().toString(), payer, merchant, 800);
                    return new UUID[] {first, second};
                }));
            }
        }
        List<UUID[]> ids = new ArrayList<>();
        for (var f : submitted) ids.add(f.get());

        await().atMost(Duration.ofSeconds(90)).until(() -> paymentDb().sql(
                "SELECT count(*) FROM payments WHERE status = 'PENDING_LEDGER'").query(Long.class).single() == 0);
        for (UUID[] pair : ids) {
            assertThat(status(pair[0])).as("first request of its payer").isEqualTo("COMPLETED");
            assertThat(status(pair[1])).as("second request of its payer").isEqualTo("FAILED");
            assertThat(reason(pair[1])).isEqualTo("INSUFFICIENT_FUNDS");
        }
        assertThat(balance(merchant)).isEqualTo(PAYERS * 800L);
        for (long payer : payers) assertThat(balance(payer)).isEqualTo(200);
        assertThat(violations()).isEmpty();
    }

    // --- helpers (same shape as Stack, on this class's own contexts) ---

    private static JdbcClient ledgerDb() { return ledger.getBean(JdbcClient.class); }
    private static JdbcClient paymentDb() { return payment.getBean(JdbcClient.class); }

    private static long account(String type, long fundedMinor) {
        long id = ledgerDb().sql("INSERT INTO accounts (type) VALUES (?) RETURNING id").param(type).query(Long.class).single();
        if (fundedMinor > 0) ledger.getBean(LedgerService.class).post(UUID.randomUUID(), SYSTEM_ACCOUNT, id, fundedMinor);
        return id;
    }

    private static long balance(long account) {
        return ledgerDb().sql("SELECT balance_minor FROM accounts WHERE id = ?").param(account).query(Long.class).single();
    }

    private static String status(UUID id) {
        return paymentDb().sql("SELECT status FROM payments WHERE id = ?").param(id).query(String.class).single();
    }

    private static String reason(UUID id) {
        return paymentDb().sql("SELECT decline_reason FROM payments WHERE id = ?").param(id).query(String.class).single();
    }

    private static UUID pay(String key, long payer, long payee, long amountMinor) throws Exception {
        String body = """
                {"payerAccountId":%d,"payeeAccountId":%d,"amountMinor":%d,"currency":"USD"}""".formatted(payer, payee, amountMinor);
        String port = payment.getEnvironment().getProperty("local.server.port");
        var response = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/payments"))
                .header("Authorization", authorization(payer)).header("Content-Type", "application/json")
                .header("Idempotency-Key", key).POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        JsonNode json = JSON.readTree(response.body());
        return UUID.fromString(json.get("id").asString());
    }

    private static synchronized String authorization(long account) {
        var users = payment.getBean(InMemoryUserDetailsManager.class);
        String name = "test-" + account;
        if (!users.userExists(name)) users.createUser(User.withUsername(name).password("{noop}test-only")
                .authorities(PaymentSecurity.accountAuthority(account)).build());
        return "Basic " + Base64.getEncoder().encodeToString((name + ":test-only").getBytes(StandardCharsets.UTF_8));
    }

    /** The full I1-I6 checker reads both schemas, so it runs as the container superuser. */
    private static List<String> violations() throws Exception {
        List<String> found = new ArrayList<>();
        try (var c = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var st = c.createStatement();
             var rs = st.executeQuery(Files.readString(Path.of("../chaos/verify_invariants.sql")))) {
            while (rs.next()) found.add(rs.getString("invariant") + ": " + rs.getString("detail"));
        }
        return found;
    }
}
