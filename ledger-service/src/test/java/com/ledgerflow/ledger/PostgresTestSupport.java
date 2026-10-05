package com.ledgerflow.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * One real Postgres 17, one real Kafka and one real application context shared by all ledger
 * tests in the JVM. Tests create their own accounts and payment ids, so they don't interfere.
 */
final class PostgresTestSupport {

    /** Canonical balances of the accounts seeded by Flyway V3 (account id → balance). */
    static final Map<Long, Long> SEED_BALANCES =
            Map.of(1L, -175_000L, 2L, 100_000L, 3L, 50_000L, 4L, 25_000L, 5L, 0L, 6L, 0L);
    /** V3 posts 3 funding transactions with 2 entries each; nothing else may touch seeded accounts. */
    static final long SEED_ENTRIES = 6;

    // Same init script as docker-compose: roles/schemas have a single source of truth.
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17")
            .withDatabaseName("ledgerflow")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("../infra/postgres/init/01-roles-schemas.sql")),
                    "/docker-entrypoint-initdb.d/01-roles-schemas.sql");

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.0");

    private static ConfigurableApplicationContext context;
    // Tests fund their accounts from this SYSTEM account, never from the seeded account 1, so the
    // Flyway seed stays at its baseline no matter which tests ran before (shared container).
    private static long testTreasury;

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    private PostgresTestSupport() {}

    static synchronized ConfigurableApplicationContext context() {
        if (context == null) {
            context = new SpringApplicationBuilder(LedgerServiceApplication.class)
                    .web(org.springframework.boot.WebApplicationType.NONE)
                    .run(
                            "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                            "--spring.datasource.username=ledger_user",
                            "--spring.datasource.password=ledger_user",
                            "--spring.datasource.hikari.maximum-pool-size=40",
                            "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                            "--" + LedgerServiceApplication.CONFIG_NAME,
                            "--spring.profiles.active=chaos,demo"); // FaultInjector idle; demo endpoints active
            testTreasury = createAccount(AccountType.SYSTEM);
        }
        return context;
    }

    static LedgerService ledger() {
        return context().getBean(LedgerService.class);
    }

    static JdbcClient jdbc() {
        return context().getBean(JdbcClient.class);
    }

    static long createAccount(AccountType type) {
        return jdbc().sql("INSERT INTO accounts (type) VALUES (?) RETURNING id")
                .param(type.name()).query(Long.class).single();
    }

    /** Creates a customer account funded from the test treasury (a SYSTEM account) through a normal posting. */
    static long fundedCustomer(long amountMinor) {
        long id = createAccount(AccountType.CUSTOMER);
        if (amountMinor > 0) {
            context();
            ledger().post(UUID.randomUUID(), testTreasury, id, amountMinor);
        }
        return id;
    }

    /** The Flyway-seeded accounts are exactly as V3 left them. */
    static void assertSeedBaseline() {
        SEED_BALANCES.forEach((id, expected) ->
                assertThat(balance(id)).as("balance of seeded account %d", id).isEqualTo(expected));
        assertThat(jdbc().sql("SELECT count(*) FROM ledger_entries WHERE account_id IN (:ids)")
                .param("ids", SEED_BALANCES.keySet()).query(Long.class).single())
                .as("ledger entries on seeded accounts").isEqualTo(SEED_ENTRIES);
    }

    static long balance(long accountId) {
        return jdbc().sql("SELECT balance_minor FROM accounts WHERE id = ?")
                .param(accountId).query(Long.class).single();
    }

    @SuppressWarnings("unchecked")
    static KafkaTemplate<String, String> kafka() {
        return context().getBean(KafkaTemplate.class);
    }

    /** Every record currently on the topic, read from the beginning by a throwaway consumer. */
    static List<ConsumerRecord<String, String>> readAll(String topic) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            while (partitions.stream().anyMatch(p -> consumer.position(p) < end.get(p))) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            return records;
        }
    }

    static long count(String table) {
        return jdbc().sql("SELECT count(*) FROM " + table).query(Long.class).single();
    }
}
