package com.ledgerflow.e2e;

import static com.ledgerflow.e2e.Stack.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ledgerflow.common.PaymentRequested;
import com.ledgerflow.common.Topics;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;

/** Financial properties across all three request partitions, rather than a particular winning order. */
class OrderingTest {
    private static final Duration WAIT = Duration.ofSeconds(90);

    @AfterEach
    void financialInvariants() throws Exception {
        await().atMost(WAIT).until(() -> count(paymentDb(), "SELECT count(*) FROM payments WHERE status = 'PENDING_LEDGER'") == 0
                && count(paymentDb(), "SELECT count(*) FROM outbox WHERE published_at IS NULL") == 0
                && count(ledgerDb(), "SELECT count(*) FROM outbox WHERE published_at IS NULL") == 0);
        // The full I1-I6 checker reads both schemas, so it runs as the container superuser.
        List<String> violations = new ArrayList<>();
        try (var c = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var st = c.createStatement();
             var rs = st.executeQuery(Files.readString(Path.of("../chaos/verify_invariants.sql")))) {
            while (rs.next()) violations.add(rs.getString("invariant") + ": " + rs.getString("detail"));
        }
        assertThat(violations).isEmpty();
    }

    @Test
    void sharedReceiverAcrossThreePartitionsAndDuplicateReplaysPreserveEveryOutcome() throws Exception {
        long receiver = account("MERCHANT", 0);
        List<Long> payers = new ArrayList<>();
        for (int partition = 0; partition < 3; partition++) {
            long payer;
            do { payer = account("CUSTOMER", 0); } while (partition(payer) != partition);
            post(SYSTEM_ACCOUNT, payer, 1_000);
            payers.add(payer);
        }
        List<Callable<Response>> requests = new ArrayList<>();
        for (long payer : payers) for (int i = 0; i < 4; i++) {
            requests.add(() -> pay(UUID.randomUUID().toString(), payer, receiver, 400));
        }
        var responses = concurrent(requests);
        List<PaymentRequested> replays = new ArrayList<>();
        @SuppressWarnings("unchecked") var kafka = (KafkaTemplate<String, String>) PAYMENT.getBean(KafkaTemplate.class);
        for (var response : responses) {
            assertThat(response.status()).isEqualTo(202);
            UUID id = UUID.fromString(response.body().get("id").asString());
            awaitTerminal(id);
            long payer = response.body().get("payerAccountId").asLong();
            var original = paymentDb().sql("SELECT event_key, payload::text AS payload FROM outbox WHERE aggregate_id = ? ORDER BY id LIMIT 1")
                    .param(id).query().singleRow();
            assertThat(original.get("event_key")).isEqualTo(String.valueOf(payer));
            kafka.send(Topics.PAYMENTS_REQUESTED, String.valueOf(payer), (String) original.get("payload")).get();
            var replay = PaymentRequested.of(id, payer, receiver, 400);
            replays.add(replay);
            kafka.send(Topics.PAYMENTS_REQUESTED, String.valueOf(payer), JSON.writeValueAsString(replay)).get();
        }
        for (var event : replays) await().atMost(WAIT).until(() ->
                count(ledgerDb(), "SELECT count(*) FROM processed_events WHERE event_id = ?", event.eventId()) == 1);
        for (long payer : payers) {
            assertThat(balance(payer)).isEqualTo(200);
            assertThat(count(paymentDb(), "SELECT count(*) FROM payments WHERE payer_account_id = ? AND status = 'COMPLETED'", payer))
                    .isEqualTo(2);
        }
        assertThat(balance(receiver)).isEqualTo(2_400);
        for (var response : responses) {
            UUID id = UUID.fromString(response.body().get("id").asString());
            boolean completed = payment(id).get("status").asString().equals("COMPLETED");
            assertThat(ledgerTransactions(id)).isEqualTo(completed ? 1 : 0);
            assertThat(ledgerDb().sql("SELECT event_key FROM outbox WHERE aggregate_id = ?").param(id).query(String.class).list())
                    .allSatisfy(key -> assertThat(key).isEqualTo(id.toString()));
        }
    }

    @Test
    void oppositeDirectionsAcrossPartitionsSurviveConsumerRestartsAndReassignment() throws Exception {
        long a = account("CUSTOMER", 1_000);
        long candidate;
        do { candidate = account("CUSTOMER", 0); } while (partition(candidate) == partition(a));
        long b = candidate;
        post(SYSTEM_ACCOUNT, b, 1_000);
        var ledgerConsumers = LEDGER.getBean(KafkaListenerEndpointRegistry.class);
        var resultConsumers = PAYMENT.getBean(KafkaListenerEndpointRegistry.class);
        List<Response> responses;
        ledgerConsumers.stop();
        resultConsumers.stop();
        try {
            List<Callable<Response>> requests = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                requests.add(() -> pay(UUID.randomUUID().toString(), a, b, 100));
                requests.add(() -> pay(UUID.randomUUID().toString(), b, a, 100));
            }
            responses = concurrent(requests);
            assertThat(balance(a)).isEqualTo(1_000);
            assertThat(balance(b)).isEqualTo(1_000);
        } finally {
            ledgerConsumers.start();
            resultConsumers.start();
        }
        for (var response : responses) {
            assertThat(response.status()).isEqualTo(202);
            UUID id = UUID.fromString(response.body().get("id").asString());
            assertThat(awaitTerminal(id).get("status").asString()).isEqualTo("COMPLETED");
            assertThat(ledgerTransactions(id)).isEqualTo(1);
        }
        assertThat(balance(a)).isEqualTo(1_000);
        assertThat(balance(b)).isEqualTo(1_000);
        for (var registry : List.of(ledgerConsumers, resultConsumers)) {
            await().atMost(WAIT).untilAsserted(() -> assertThat(registry.getListenerContainers().stream()
                    .flatMap(c -> c.getAssignedPartitions().stream()).distinct().count()).isEqualTo(3));
        }
    }

    @Test
    void hotAccountVelocityAdmitsExactlyFiveAndPostsExactlyFive() throws Exception {
        long payer = account("CUSTOMER", 10_000);
        long payee = account("MERCHANT", 0);
        List<Callable<Response>> requests = new ArrayList<>();
        for (int i = 0; i < 32; i++) requests.add(() -> pay(UUID.randomUUID().toString(), payer, payee, 100));
        var responses = concurrent(requests);
        assertThat(responses.stream().filter(r -> r.status() == 202)).hasSize(5);
        assertThat(responses.stream().filter(r -> r.status() == 201)).hasSize(27);
        for (var response : responses) {
            UUID id = UUID.fromString(response.body().get("id").asString());
            assertThat(awaitTerminal(id).get("status").asString()).isEqualTo(response.status() == 202 ? "COMPLETED" : "DECLINED");
            assertThat(ledgerTransactions(id)).isEqualTo(response.status() == 202 ? 1 : 0);
        }
        assertThat(balance(payer)).isEqualTo(9_500);
        assertThat(balance(payee)).isEqualTo(500);
    }

    private static long count(JdbcClient db, String sql, Object... params) {
        return db.sql(sql).params(params).query(Long.class).single();
    }

    private static int partition(long account) {
        return Utils.toPositive(Utils.murmur2(String.valueOf(account).getBytes(StandardCharsets.UTF_8))) % 3;
    }

    private static <T> List<T> concurrent(List<Callable<T>> tasks) throws Exception {
        var ready = new CountDownLatch(tasks.size());
        var go = new CountDownLatch(1);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = tasks.stream().map(task -> pool.submit(() -> {
                ready.countDown();
                go.await();
                return task.call();
            })).toList();
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            List<T> results = new ArrayList<>();
            for (var f : futures) results.add(f.get(60, TimeUnit.SECONDS));
            return results;
        }
    }
}
