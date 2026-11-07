package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.PostgresTestSupport.KAFKA;
import static com.ledgerflow.ledger.PostgresTestSupport.context;
import static com.ledgerflow.ledger.PostgresTestSupport.createAccount;
import static com.ledgerflow.ledger.PostgresTestSupport.fundedCustomer;
import static com.ledgerflow.ledger.PostgresTestSupport.jdbc;
import static com.ledgerflow.ledger.PostgresTestSupport.readAll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ledgerflow.common.PaymentRequested;
import com.ledgerflow.common.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/** Trace context across the ledger: consumed from the record header, stored on the result outbox row, sent on. */
class LedgerTracingTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    @AfterEach
    void invariantsHold() {
        LedgerInvariants.assertAll();
    }

    @Test
    void resultOutboxRowContinuesTheConsumedTrace() throws Exception {
        var json = context().getBean(JsonMapper.class);
        long payer = fundedCustomer(1_000);
        var event = PaymentRequested.of(UUID.randomUUID(), payer, createAccount(AccountType.MERCHANT), 100);
        String traceId = hex(16);
        var record = new ProducerRecord<>(Topics.PAYMENTS_REQUESTED, String.valueOf(payer), json.writeValueAsString(event));
        record.headers().add("traceparent", ("00-" + traceId + "-" + hex(8) + "-01").getBytes(StandardCharsets.UTF_8));
        // A plain producer: the application's KafkaTemplate would put its own trace header on the record.
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            producer.send(record).get();
        }

        String stored = await().atMost(WAIT).until(() -> jdbc().sql("SELECT traceparent FROM outbox WHERE aggregate_id = ?")
                .param(event.paymentId()).query(String.class).optional().orElse(null), s -> s != null);
        assertThat(stored).startsWith("00-" + traceId + "-");
        var result = await().atMost(WAIT).until(() -> readAll(Topics.LEDGER_RESULTS).stream()
                .filter(r -> r.key().equals(event.paymentId().toString())).toList(), list -> !list.isEmpty()).getFirst();
        assertThat(new String(result.headers().lastHeader("traceparent").value(), StandardCharsets.UTF_8))
                .startsWith("00-" + traceId + "-");
    }

    private static String hex(int bytes) {
        byte[] b = new byte[bytes];
        ThreadLocalRandom.current().nextBytes(b);
        b[0] |= 1;
        return HexFormat.of().formatHex(b);
    }
}
