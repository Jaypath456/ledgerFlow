package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.PostgresTestSupport.balance;
import static com.ledgerflow.ledger.PostgresTestSupport.context;
import static com.ledgerflow.ledger.PostgresTestSupport.createAccount;
import static com.ledgerflow.ledger.PostgresTestSupport.fundedCustomer;
import static com.ledgerflow.ledger.PostgresTestSupport.jdbc;
import static com.ledgerflow.ledger.PostgresTestSupport.kafka;
import static com.ledgerflow.ledger.PostgresTestSupport.readAll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ledgerflow.common.PaymentRequested;
import com.ledgerflow.common.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Ledger consumer against real Kafka + Postgres: dedupe, rejections, re-emission, poison handling. */
class LedgerKafkaTest {

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static JsonMapper json;

    @BeforeAll
    static void start() {
        json = context().getBean(JsonMapper.class);
    }

    @AfterEach
    void invariantsHold() {
        LedgerInvariants.assertAll();
    }

    @Test
    void fundedPaymentIsPostedAndLedgerPostedIsPublished() {
        long payer = fundedCustomer(1_000);
        long payee = createAccount(AccountType.MERCHANT);
        var event = PaymentRequested.of(UUID.randomUUID(), payer, payee, 400);

        send(event);

        Map<String, Object> outcome = awaitOutcome(event.paymentId());
        assertThat(outcome.get("status")).isEqualTo("POSTED");
        assertThat(ledgerTransactions(event.paymentId())).isEqualTo(1);
        assertThat(balance(payer)).isEqualTo(600);
        assertThat(balance(payee)).isEqualTo(400);

        JsonNode result = awaitResults(event.paymentId(), 1).getFirst();
        assertThat(result.get("eventType").asString()).isEqualTo("LedgerPosted");
        assertThat(result.get("version").asInt()).isEqualTo(1);
        assertThat(result.get("ledgerTransactionId").asLong()).isEqualTo(outcome.get("ledger_transaction_id"));
        await().atMost(WAIT).until(() -> unpublishedOutbox(event.paymentId()) == 0);
    }

    @Test
    void duplicateDeliveryOfSameEventPostsOnce() {
        long payer = fundedCustomer(1_000);
        long payee = createAccount(AccountType.MERCHANT);
        var event = PaymentRequested.of(UUID.randomUUID(), payer, payee, 250);
        double duplicatesBefore = duplicates();

        send(event);
        send(event);
        send(event);

        await().atMost(WAIT).until(() -> duplicates() - duplicatesBefore >= 2);
        assertThat(ledgerTransactions(event.paymentId())).isEqualTo(1);
        assertThat(outboxRows(event.paymentId())).isEqualTo(1);
        assertThat(balance(payer)).isEqualTo(750);
        assertThat(balance(payee)).isEqualTo(250);
    }

    @Test
    void newEventForAlreadyPostedPaymentReemitsStoredResultWithoutMovingMoney() {
        long payer = fundedCustomer(1_000);
        long payee = createAccount(AccountType.MERCHANT);
        UUID paymentId = UUID.randomUUID();
        send(PaymentRequested.of(paymentId, payer, payee, 300));
        long txId = (Long) awaitOutcome(paymentId).get("ledger_transaction_id");

        // A retry (new eventId, same payment) must not post again, even with a different amount.
        send(PaymentRequested.of(paymentId, payer, payee, 999));

        List<JsonNode> results = awaitResults(paymentId, 2);
        assertThat(results).allSatisfy(r -> {
            assertThat(r.get("eventType").asString()).isEqualTo("LedgerPosted");
            assertThat(r.get("ledgerTransactionId").asLong()).isEqualTo(txId);
        });
        assertThat(results.get(0).get("eventId")).isNotEqualTo(results.get(1).get("eventId"));
        assertThat(ledgerTransactions(paymentId)).isEqualTo(1);
        assertThat(balance(payer)).isEqualTo(700);
    }

    @Test
    void insufficientFundsIsRejectedWithNoLedgerWrites() {
        long payer = fundedCustomer(100);
        long payee = createAccount(AccountType.MERCHANT);
        long entriesBefore = PostgresTestSupport.count("ledger_entries");
        var event = PaymentRequested.of(UUID.randomUUID(), payer, payee, 500);

        send(event);

        Map<String, Object> outcome = awaitOutcome(event.paymentId());
        assertThat(outcome.get("status")).isEqualTo("REJECTED");
        assertThat(outcome.get("reason")).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(ledgerTransactions(event.paymentId())).isZero();
        assertThat(PostgresTestSupport.count("ledger_entries")).isEqualTo(entriesBefore);
        assertThat(balance(payer)).isEqualTo(100);
        assertThat(balance(payee)).isZero();

        JsonNode result = awaitResults(event.paymentId(), 1).getFirst();
        assertThat(result.get("eventType").asString()).isEqualTo("LedgerRejected");
        assertThat(result.get("reason").asString()).isEqualTo("INSUFFICIENT_FUNDS");
    }

    @Test
    void unknownAccountIsRejectedWithNoLedgerWrites() {
        long payer = fundedCustomer(1_000);
        var event = PaymentRequested.of(UUID.randomUUID(), payer, 987_654_321L, 10);

        send(event);

        Map<String, Object> outcome = awaitOutcome(event.paymentId());
        assertThat(outcome.get("status")).isEqualTo("REJECTED");
        assertThat(outcome.get("reason")).isEqualTo("UNKNOWN_ACCOUNT");
        assertThat(ledgerTransactions(event.paymentId())).isZero();
        assertThat(balance(payer)).isEqualTo(1_000);
    }

    @Test
    void poisonMessagesGoToDltAndValidEventsBehindThemStillPost() {
        long payer = fundedCustomer(1_000);
        long payee = createAccount(AccountType.MERCHANT);
        String key = String.valueOf(payer); // same key => same partition as the valid event
        String garbage = "not json " + UUID.randomUUID();
        String badVersion = """
                {"eventId":"%s","eventType":"PaymentRequested","version":2,"occurredAt":"%s",
                 "paymentId":"%s","payerAccountId":%d,"payeeAccountId":%d,"amountMinor":5}"""
                .formatted(UUID.randomUUID(), Instant.now(), UUID.randomUUID(), payer, payee);
        var valid = PaymentRequested.of(UUID.randomUUID(), payer, payee, 50);

        kafka().send(Topics.PAYMENTS_REQUESTED, key, garbage);
        kafka().send(Topics.PAYMENTS_REQUESTED, key, badVersion);
        kafka().send(Topics.PAYMENTS_REQUESTED, key, json.writeValueAsString(valid));

        assertThat(awaitOutcome(valid.paymentId()).get("status")).isEqualTo("POSTED");
        await().atMost(WAIT).untilAsserted(() -> assertThat(
                readAll(Topics.PAYMENTS_REQUESTED + Topics.DLT_SUFFIX).stream().map(r -> r.value()).toList())
                .contains(garbage, badVersion));
        assertThat(balance(payer)).isEqualTo(950);
    }

    @Test
    void concurrentEventsForSamePaymentPostExactlyOnce() throws Exception {
        long payer = fundedCustomer(1_000);
        long payee = createAccount(AccountType.MERCHANT);
        UUID paymentId = UUID.randomUUID();
        var listener = context().getBean(PaymentRequestListener.class);
        int threads = 16;
        var start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try (var pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    listener.handle(PaymentRequested.of(paymentId, payer, payee, 100));
                    return null;
                }));
            }
            start.countDown();
        }
        for (var f : futures) {
            f.get();
        }

        assertThat(ledgerTransactions(paymentId)).isEqualTo(1);
        assertThat(balance(payer)).isEqualTo(900);
        assertThat(outboxRows(paymentId)).isEqualTo(threads);
        List<String> payloads = jdbc().sql("SELECT payload->>'ledgerTransactionId' FROM outbox WHERE aggregate_id = ?")
                .param(paymentId).query(String.class).list();
        assertThat(payloads).hasSize(threads).containsOnly(payloads.getFirst());
    }

    private static void send(PaymentRequested event) {
        kafka().send(Topics.PAYMENTS_REQUESTED, String.valueOf(event.payerAccountId()), json.writeValueAsString(event));
    }

    private static Map<String, Object> awaitOutcome(UUID paymentId) {
        return await().atMost(WAIT).until(
                () -> jdbc().sql("SELECT * FROM payment_outcomes WHERE payment_id = ?").param(paymentId)
                        .query().listOfRows(),
                rows -> !rows.isEmpty()).getFirst();
    }

    private static List<JsonNode> awaitResults(UUID paymentId, int expected) {
        return await().atMost(WAIT).until(
                () -> readAll(Topics.LEDGER_RESULTS).stream()
                        .filter(r -> r.key().equals(paymentId.toString()))
                        .map(r -> json.readTree(r.value()))
                        .toList(),
                results -> results.size() >= expected);
    }

    private static long ledgerTransactions(UUID paymentId) {
        return jdbc().sql("SELECT count(*) FROM ledger_transactions WHERE payment_id = ?").param(paymentId)
                .query(Long.class).single();
    }

    private static long outboxRows(UUID paymentId) {
        return jdbc().sql("SELECT count(*) FROM outbox WHERE aggregate_id = ?").param(paymentId)
                .query(Long.class).single();
    }

    private static long unpublishedOutbox(UUID paymentId) {
        return jdbc().sql("SELECT count(*) FROM outbox WHERE aggregate_id = ? AND published_at IS NULL")
                .param(paymentId).query(Long.class).single();
    }

    private static double duplicates() {
        return context().getBean(MeterRegistry.class).counter("ledgerflow.events.duplicate").count();
    }
}
