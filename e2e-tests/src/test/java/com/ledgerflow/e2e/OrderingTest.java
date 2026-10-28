package com.ledgerflow.e2e;

import static com.ledgerflow.common.MongoStore.eq;
import static com.ledgerflow.e2e.Stack.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ledgerflow.common.PaymentRequested;
import com.ledgerflow.common.Topics;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.apache.kafka.common.utils.Utils;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;

/** Financial properties across all three request partitions, rather than a particular winning order. */
class OrderingTest {
    private static final Duration WAIT = Duration.ofSeconds(90);

    @AfterEach
    void financialInvariants() throws Exception {
        await().atMost(WAIT).until(() -> paymentDb().count(eq("status", "PENDING_LEDGER"), "payments") == 0
                && paymentDb().count(eq("published_at", null), "outbox") == 0
                && ledgerDb().count(eq("published_at", null), "outbox") == 0);
        var result = MONGO.execInContainer("mongosh", "admin", "--quiet", "-u", "test_admin", "-p", "test_admin",
                "--eval", Files.readString(Path.of("../chaos/verify_invariants.js")) + "\nvoid 0;");
        assertThat(result.getExitCode()).as(result.getStdout() + result.getStderr()).isZero();
        assertThat(result.getStdout()).isBlank();
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
            var original = paymentDb().findOne(eq("aggregate_id", id), Document.class, "outbox");
            assertThat(original.getString("event_key")).isEqualTo(String.valueOf(payer));
            kafka.send(Topics.PAYMENTS_REQUESTED, String.valueOf(payer), original.getString("payload")).get();
            var replay = PaymentRequested.of(id, payer, receiver, 400);
            replays.add(replay);
            kafka.send(Topics.PAYMENTS_REQUESTED, String.valueOf(payer), JSON.writeValueAsString(replay)).get();
        }
        for (var event : replays) await().atMost(WAIT).until(() -> ledgerDb().exists(eq("event_id", event.eventId()), "processed_events"));
        for (long payer : payers) {
            assertThat(balance(payer)).isEqualTo(200);
            assertThat(paymentDb().count(org.springframework.data.mongodb.core.query.Query.query(
                    org.springframework.data.mongodb.core.query.Criteria.where("payer_account_id").is(payer)
                            .and("status").is("COMPLETED")), "payments")).isEqualTo(2);
        }
        assertThat(balance(receiver)).isEqualTo(2_400);
        for (var response : responses) {
            UUID id = UUID.fromString(response.body().get("id").asString());
            boolean completed = payment(id).get("status").asString().equals("COMPLETED");
            assertThat(ledgerTransactions(id)).isEqualTo(completed ? 1 : 0);
            assertThat(ledgerDb().find(eq("aggregate_id", id), Document.class, "outbox"))
                    .allSatisfy(row -> assertThat(row.getString("event_key")).isEqualTo(id.toString()));
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
            assertThat(ready.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            go.countDown();
            List<T> results = new ArrayList<>();
            for (var f : futures) results.add(f.get(60, java.util.concurrent.TimeUnit.SECONDS));
            return results;
        }
    }
}
