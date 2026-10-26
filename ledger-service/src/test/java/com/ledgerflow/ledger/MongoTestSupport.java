package com.ledgerflow.ledger;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.springframework.data.mongodb.core.MongoTemplate;
import com.ledgerflow.common.MongoFixture;
import com.ledgerflow.common.MongoStore;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Criteria;
import static com.ledgerflow.common.MongoStore.eq;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;

/**
 * One real MongoDB 8.0, one real Kafka and one real application context shared by all ledger
 * tests in the JVM. Tests create their own accounts and payment ids, so they don't interfere.
 */
final class MongoTestSupport {

    /** Canonical balances of the accounts seeded by startup (account id → balance). */
    static final Map<Long, Long> SEED_BALANCES =
            Map.of(1L, -175_000L, 2L, 100_000L, 3L, 50_000L, 4L, 25_000L, 5L, 0L, 6L, 0L);
    /** V3 posts 3 funding transactions with 2 entries each; nothing else may touch seeded accounts. */
    static final long SEED_ENTRIES = 6;

    static final MongoDBContainer MONGO = MongoFixture.start();

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.0");

    private static ConfigurableApplicationContext context;
    // Tests fund their accounts from this SYSTEM account, never from the seeded account 1, so the
    // startup seed stays at its baseline no matter which tests ran before (shared container).
    private static long testTreasury;

    static {
        KAFKA.start();
    }

    private MongoTestSupport() {}

    static synchronized ConfigurableApplicationContext context() {
        if (context == null) {
            context = new SpringApplicationBuilder(LedgerServiceApplication.class)
                    .web(org.springframework.boot.WebApplicationType.NONE)
                    .run(
                            "--spring.mongodb.uri=" + MongoFixture.uri(MONGO, "ledger"),
                            "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                            "--" + LedgerServiceApplication.CONFIG_NAME,
                            "--spring.profiles.active=chaos"); // FaultInjector idle
            testTreasury = createAccount(AccountType.SYSTEM);
        }
        return context;
    }

    static LedgerService ledger() {
        return context().getBean(LedgerService.class);
    }

    static MongoTemplate mongo() {
        return context().getBean(MongoTemplate.class);
    }

    static long createAccount(AccountType type) {
        long id = MongoStore.nextId();
        mongo().insert(MongoStore.account(id, type.name()), "accounts");
        return id;
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

    /** The startup-seeded accounts are exactly as V3 left them. */
    static void assertSeedBaseline() {
        SEED_BALANCES.forEach((id, expected) ->
                assertThat(balance(id)).as("balance of seeded account %d", id).isEqualTo(expected));
        assertThat(mongo().count(Query.query(Criteria.where("account_id").in(SEED_BALANCES.keySet())), "ledger_entries"))
                .as("ledger entries on seeded accounts").isEqualTo(SEED_ENTRIES);
    }

    static long balance(long accountId) {
        return mongo().findOne(eq("_id", accountId), Document.class, "accounts").getLong("balance_minor");
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
        return mongo().count(new Query(), table);
    }
}
