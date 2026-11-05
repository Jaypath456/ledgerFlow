package com.ledgerflow.e2e;

import static com.ledgerflow.e2e.Stack.POSTGRES;
import static com.ledgerflow.e2e.Stack.account;
import static com.ledgerflow.e2e.Stack.awaitTerminal;
import static com.ledgerflow.e2e.Stack.pay;
import static com.ledgerflow.e2e.Stack.paymentDb;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Runs chaos/verify_invariants.sql against real data from both services: clean on a settled
 * healthy system, and catches each controlled corruption (applied in a rolled-back transaction).
 */
class InvariantCheckerTest {

    private static String checker;
    private static long payer;
    private static long payee;
    private static UUID completed;
    private static UUID failed;
    private static UUID declined;

    record Violation(String invariant, String detail) {}

    @BeforeAll
    static void createSettledData() throws Exception {
        checker = Files.readString(Path.of("../chaos/verify_invariants.sql"));
        payer = account("CUSTOMER", 1_000);
        payee = account("MERCHANT", 0);
        completed = id(pay(UUID.randomUUID().toString(), payer, payee, 300));
        failed = id(pay(UUID.randomUUID().toString(), payer, payee, 5_000));
        declined = id(pay(UUID.randomUUID().toString(), payer, payee, 2_000_000));
        assertThat(awaitTerminal(completed).get("status").asString()).isEqualTo("COMPLETED");
        assertThat(awaitTerminal(failed).get("status").asString()).isEqualTo("FAILED");
        assertThat(awaitTerminal(declined).get("status").asString()).isEqualTo("DECLINED");
        // Settled: nothing from any test still pending (the reconciler finishes stragglers).
        await().atMost(Duration.ofSeconds(90)).until(() -> paymentDb()
                .sql("SELECT count(*) FROM payments WHERE status = 'PENDING_LEDGER'").query(Long.class).single() == 0);
    }

    @Test
    void healthySystemHasZeroViolations() throws Exception {
        try (Connection c = superuser()) {
            assertThat(check(c)).isEmpty();
        }
    }

    @Test
    void i1CatchesUnbalancedTransaction() throws Exception {
        assertCaught("I1", "ledger transaction " + txId(completed),
                "INSERT INTO ledger.ledger_entries (transaction_id, account_id, amount_minor) VALUES (" + txId(completed) + ", " + payee + ", 1)");
        assertCaught("I1", "global ledger sum is 1",
                "INSERT INTO ledger.ledger_entries (transaction_id, account_id, amount_minor) VALUES (" + txId(completed) + ", " + payee + ", 1)");
    }

    @Test
    void i2CatchesDoublePostingAndPostingOfFailedPayment() throws Exception {
        assertCaught("I2", "payment " + completed + " posted 2 times",
                "ALTER TABLE ledger.ledger_transactions DROP CONSTRAINT ledger_transactions_payment_id_key",
                "INSERT INTO ledger.ledger_transactions (payment_id) VALUES ('" + completed + "')");
        assertCaught("I2", "payment " + completed + " is FAILED but has a ledger posting",
                "UPDATE payments.payments SET status = 'FAILED' WHERE id = '" + completed + "'");
        assertCaught("I2", "payment " + completed + " posting does not match",
                "UPDATE payments.payments SET amount_minor = 301 WHERE id = '" + completed + "'");
    }

    @Test
    void i3CatchesStuckAndInconsistentPayments() throws Exception {
        assertCaught("I3", "payment " + failed + " still PENDING_LEDGER",
                "UPDATE payments.payments SET status = 'PENDING_LEDGER' WHERE id = '" + failed + "'");
        assertCaught("I3", "payment " + failed + " is COMPLETED without a ledger posting",
                "UPDATE payments.payments SET status = 'COMPLETED' WHERE id = '" + failed + "'");
        assertCaught("I3", "payment " + completed + " is COMPLETED but ledger outcome is missing",
                "DELETE FROM ledger.payment_outcomes WHERE payment_id = '" + completed + "'");
    }

    @Test
    void i4CatchesNegativeCustomerBalance() throws Exception {
        assertCaught("I4", "account " + payer + " (CUSTOMER) balance -1",
                "ALTER TABLE ledger.accounts DROP CONSTRAINT non_system_balance_not_negative",
                "UPDATE ledger.accounts SET balance_minor = -1 WHERE id = " + payer);
    }

    @Test
    void i5CatchesCachedBalanceDrift() throws Exception {
        assertCaught("I5", "account " + payee + " cached 301 <> entries 300",
                "UPDATE ledger.accounts SET balance_minor = balance_minor + 1 WHERE id = " + payee);
    }

    @Test
    void i6CatchesBrokenIdempotencyRecords() throws Exception {
        assertCaught("I6", "payment " + completed + " has 0 idempotency keys",
                "DELETE FROM payments.idempotency_keys WHERE payment_id = '" + completed + "'");
        assertCaught("I6", "does not match payment " + declined,
                "UPDATE payments.idempotency_keys SET request_hash = 'tampered' WHERE payment_id = '" + declined + "'");
        assertCaught("I6", "does not match payment " + completed,
                "UPDATE payments.idempotency_keys SET response_status = 201 WHERE payment_id = '" + completed + "'");
        assertCaught("I6", "does not match payment " + failed,
                "UPDATE payments.idempotency_keys SET response_body = (SELECT response_body FROM payments.idempotency_keys"
                        + " WHERE payment_id = '" + completed + "') WHERE payment_id = '" + failed + "'");
    }

    private static void assertCaught(String invariant, String detailFragment, String... corruption) throws Exception {
        try (Connection c = superuser()) {
            c.setAutoCommit(false);
            try (var st = c.createStatement()) {
                for (String sql : corruption) {
                    st.execute(sql);
                }
            }
            assertThat(check(c))
                    .as("checker must report %s for: %s", invariant, String.join("; ", corruption))
                    .anySatisfy(v -> {
                        assertThat(v.invariant()).isEqualTo(invariant);
                        assertThat(v.detail()).contains(detailFragment);
                    });
            c.rollback();
        }
    }

    private static List<Violation> check(Connection c) throws Exception {
        List<Violation> violations = new ArrayList<>();
        try (var st = c.createStatement(); var rs = st.executeQuery(checker)) {
            while (rs.next()) {
                violations.add(new Violation(rs.getString("invariant"), rs.getString("detail")));
            }
        }
        return violations;
    }

    private static Connection superuser() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static long txId(UUID paymentId) {
        return Stack.ledgerDb().sql("SELECT id FROM ledger_transactions WHERE payment_id = ?").param(paymentId)
                .query(Long.class).single();
    }

    private static UUID id(Stack.Response r) {
        return UUID.fromString(r.body().get("id").asString());
    }
}
