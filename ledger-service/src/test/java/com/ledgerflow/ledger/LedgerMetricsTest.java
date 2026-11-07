package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.PostgresTestSupport.context;
import static com.ledgerflow.ledger.PostgresTestSupport.createAccount;
import static com.ledgerflow.ledger.PostgresTestSupport.fundedCustomer;
import static com.ledgerflow.ledger.PostgresTestSupport.jdbc;
import static com.ledgerflow.ledger.PostgresTestSupport.kafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ledgerflow.common.PaymentRequested;
import com.ledgerflow.common.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/** Ledger-side metrics: committed outcomes and dead-lettered records. */
class LedgerMetricsTest {

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static JsonMapper json;
    private static MeterRegistry meters;

    @BeforeAll
    static void start() {
        json = context().getBean(JsonMapper.class);
        meters = context().getBean(MeterRegistry.class);
    }

    @AfterEach
    void invariantsHold() {
        LedgerInvariants.assertAll();
    }

    @Test
    void outcomeCounterCountsCommittedNewOutcomesOnly() {
        double posted = outcomes("POSTED");
        double rejected = outcomes("REJECTED");
        double duplicates = counter("ledgerflow.events.duplicate");
        double reemitted = counter("ledgerflow.outcomes.reemitted");
        long payer = fundedCustomer(1_000);
        long payee = createAccount(AccountType.MERCHANT);
        var event = PaymentRequested.of(UUID.randomUUID(), payer, payee, 400);

        send(event);
        await().atMost(WAIT).until(() -> outcomes("POSTED") == posted + 1);

        send(event); // same eventId: deduplicated
        send(PaymentRequested.of(event.paymentId(), payer, payee, 400)); // new eventId, same payment: stored outcome re-emitted
        await().atMost(WAIT).until(() -> counter("ledgerflow.events.duplicate") > duplicates
                && counter("ledgerflow.outcomes.reemitted") > reemitted);
        assertThat(outcomes("POSTED")).isEqualTo(posted + 1);

        send(PaymentRequested.of(UUID.randomUUID(), payer, payee, 10_000)); // insufficient funds
        await().atMost(WAIT).until(() -> outcomes("REJECTED") == rejected + 1);
        assertThat(outcomes("POSTED")).isEqualTo(posted + 1);
        assertThat(jdbc().sql("SELECT count(*) FROM ledger_transactions WHERE payment_id = ?").param(event.paymentId())
                .query(Long.class).single()).isEqualTo(1);
    }

    @Test
    void deadLetteredRecordIncrementsDltCounter() {
        double before = dlt();
        kafka().send(Topics.PAYMENTS_REQUESTED, "dlt-metric-" + UUID.randomUUID(), "not json " + UUID.randomUUID());
        await().atMost(WAIT).until(() -> dlt() == before + 1);
    }

    private static void send(PaymentRequested event) {
        kafka().send(Topics.PAYMENTS_REQUESTED, String.valueOf(event.payerAccountId()), json.writeValueAsString(event));
    }

    private static double outcomes(String status) {
        return meters.counter("ledgerflow.ledger.outcomes", "status", status).count();
    }

    private static double counter(String name) {
        return meters.counter(name).count();
    }

    private static double dlt() {
        var counter = meters.find("ledgerflow.dlt.published").tag("topic", Topics.PAYMENTS_REQUESTED).counter();
        return counter == null ? 0 : counter.count();
    }
}
