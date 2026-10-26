package com.ledgerflow.e2e;

import static com.ledgerflow.e2e.Stack.MONGO;
import static com.ledgerflow.e2e.Stack.account;
import static com.ledgerflow.e2e.Stack.awaitTerminal;
import static com.ledgerflow.e2e.Stack.pay;
import static com.ledgerflow.e2e.Stack.paymentDb;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static com.ledgerflow.common.MongoStore.eq;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Runs the operational MongoDB checker, including transactional corruption fixtures. */
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
        checker = "const LEDGERFLOW_CHECK_LIBRARY = true;\n" + Files.readString(Path.of("../chaos/verify_invariants.js"));
        payer = account("CUSTOMER", 1_000);
        payee = account("MERCHANT", 0);
        completed = id(pay(UUID.randomUUID().toString(), payer, payee, 300));
        failed = id(pay(UUID.randomUUID().toString(), payer, payee, 5_000));
        declined = id(pay(UUID.randomUUID().toString(), payer, payee, 2_000_000));
        assertThat(awaitTerminal(completed).get("status").asString()).isEqualTo("COMPLETED");
        assertThat(awaitTerminal(failed).get("status").asString()).isEqualTo("FAILED");
        assertThat(awaitTerminal(declined).get("status").asString()).isEqualTo("DECLINED");
        await().atMost(Duration.ofSeconds(90)).until(() -> paymentDb()
                .count(eq("status", "PENDING_LEDGER"), "payments") == 0);
    }

    @Test
    void healthySystemHasZeroViolations() throws Exception {
        assertThat(check("", false)).isEmpty();
    }

    @Test
    void i1CatchesUnbalancedTransaction() throws Exception {
        String corruption = "l.ledger_entries.insertOne({transaction_id:Long('" + txId(completed)
                + "'),account_id:Long('" + payee + "'),amount_minor:Long('1')});";
        assertCaught("I1", "ledger transaction " + txId(completed), corruption);
        assertCaught("I1", "global ledger sum is 1", corruption);
    }

    @Test
    void i2CatchesDoublePostingAndPostingOfFailedPayment() throws Exception {
        assertThat(check("l.ledger_transactions.insertOne({_id:Long('9223372036854775806'),payment_id:'" + completed + "'});", true))
                .anySatisfy(v -> {
                    assertThat(v.invariant()).isEqualTo("I2");
                    assertThat(v.detail()).contains("payment " + completed + " posted 2 times");
                });
        assertCaught("I2", "payment " + completed + " is FAILED but has a ledger posting",
                "p.payments.updateOne({_id:'" + completed + "'},{$set:{status:'FAILED'}});");
        assertCaught("I2", "payment " + completed + " posting does not match",
                "p.payments.updateOne({_id:'" + completed + "'},{$set:{amount_minor:Long('301')}});");
    }

    @Test
    void i3CatchesStuckAndInconsistentPayments() throws Exception {
        assertCaught("I3", "payment " + failed + " still PENDING_LEDGER",
                "p.payments.updateOne({_id:'" + failed + "'},{$set:{status:'PENDING_LEDGER'}});");
        assertCaught("I3", "payment " + failed + " is COMPLETED without a ledger posting",
                "p.payments.updateOne({_id:'" + failed + "'},{$set:{status:'COMPLETED'}});");
        assertCaught("I3", "payment " + completed + " is COMPLETED but ledger outcome is missing",
                "l.payment_outcomes.deleteOne({payment_id:'" + completed + "'});");
    }

    @Test
    void i4CatchesNegativeCustomerBalance() throws Exception {
        assertCaught("I4", "account " + payer + " (CUSTOMER) balance -1",
                "l.accounts.updateOne({_id:Long('" + payer + "')},{$set:{balance_minor:Long('-1')}});");
    }

    @Test
    void i5CatchesCachedBalanceDrift() throws Exception {
        assertCaught("I5", "account " + payee + " cached 301 <> entries 300",
                "l.accounts.updateOne({_id:Long('" + payee + "')},{$inc:{balance_minor:Long('1')}});");
    }

    @Test
    void i6CatchesBrokenIdempotencyRecords() throws Exception {
        assertCaught("I6", "payment " + completed + " has 0 idempotency keys",
                "p.idempotency_keys.deleteOne({payment_id:'" + completed + "'});");
        assertCaught("I6", "does not match payment " + declined,
                "p.idempotency_keys.updateOne({payment_id:'" + declined + "'},{$set:{request_hash:'tampered'}});");
        assertCaught("I6", "does not match payment " + completed,
                "p.idempotency_keys.updateOne({payment_id:'" + completed + "'},{$set:{response_status:201}});");
        assertCaught("I6", "does not match payment " + failed,
                "p.idempotency_keys.updateOne({payment_id:'" + failed + "'},{$set:{response_body:"
                        + "p.idempotency_keys.findOne({payment_id:'" + completed + "'}).response_body}});");
    }

    private static void assertCaught(String invariant, String detailFragment, String corruption) throws Exception {
        assertThat(check(corruption, false)).as("checker must report %s for: %s", invariant, corruption)
                .anySatisfy(v -> {
                    assertThat(v.invariant()).isEqualTo(invariant);
                    assertThat(v.detail()).contains(detailFragment);
                });
    }

    private static List<Violation> check(String corruption, boolean dropPostingIndex) throws Exception {
        String script = checker + "\nconst base = db.getSiblingDB('ledger');\n"
                + (dropPostingIndex ? "base.ledger_transactions.dropIndex('payment_id_1');\n" : "")
                + "const session = db.getMongo().startSession();\n"
                + "try { session.startTransaction(); const p = session.getDatabase('payments'); const l = session.getDatabase('ledger');\n"
                + corruption + "\nprint(JSON.stringify(check(p,l)));\n"
                + "} finally { session.abortTransaction(); session.endSession();\n"
                + (dropPostingIndex ? "base.ledger_transactions.createIndex({payment_id:1},{unique:true});\n" : "") + "}\nvoid 0;\n";
        var result = MONGO.execInContainer("mongosh", "admin", "--quiet", "--username", "test_admin",
                "--password", "test_admin", "--eval", script);
        assertThat(result.getExitCode()).as(result.getStdout() + result.getStderr()).isZero();
        var rows = Stack.JSON.readValue(result.getStdout().trim(), Violation[].class);
        return Arrays.asList(rows);
    }

    private static long txId(UUID paymentId) {
        return Stack.ledgerDb().findOne(eq("payment_id", paymentId), Document.class, "ledger_transactions").getLong("_id");
    }

    private static UUID id(Stack.Response r) {
        return UUID.fromString(r.body().get("id").asString());
    }
}
