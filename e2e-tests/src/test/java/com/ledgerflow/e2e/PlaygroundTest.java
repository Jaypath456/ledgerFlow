package com.ledgerflow.e2e;

import static com.ledgerflow.e2e.DemoStack.account;
import static com.ledgerflow.e2e.DemoStack.balance;
import static com.ledgerflow.e2e.DemoStack.get;
import static com.ledgerflow.e2e.DemoStack.ledger;
import static com.ledgerflow.e2e.DemoStack.payment;
import static com.ledgerflow.e2e.DemoStack.postJson;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/**
 * Transaction Playground runner through the real payment path (API → outbox → Kafka → ledger →
 * result). Races are judged by correctness properties only, never by which payment wins.
 */
class PlaygroundTest {

    record Tx(long payer, long payee, long amountMinor, long delayMs) {}

    record Outcome(List<String> statuses, Map<Long, Long> before, Map<Long, Long> after, JsonNode run) {}

    @Test
    void demoStartsWithJayAndAjay() throws Exception {
        Map<String, Long> balances = new HashMap<>();
        get(ledger("/demo/named-accounts")).forEach(a -> balances.put(a.get("name").asString(), a.get("balanceMinor").asLong()));
        assertThat(balances).containsEntry("Jay", 7_500L).containsEntry("Ajay", 10_000L);
    }

    @Test
    void twoWaySharedReceiverBothCreditsLandNoLostUpdate() throws Exception {
        long a = id(account("Jay", 7_500)), c = id(account("Jaysus", 10_000)), r = id(account("Ajay", 10_000));
        var o = run(new Tx(a, r, 5_000, 0), new Tx(c, r, 5_000, 0));
        assertThat(o.statuses()).containsOnly("COMPLETED");
        assertThat(o.after().get(r)).isEqualTo(20_000);
    }

    @Test
    void sharedPayerWithExactlyEnoughCompletesBoth() throws Exception {
        long p = id(account("Jay", 7_500)), x = id(account("Ajay", 10_000)), y = id(account("Jaysus", 10_000));
        var o = run(new Tx(p, x, 6_000, 0), new Tx(p, y, 1_500, 0));
        assertThat(o.statuses()).containsOnly("COMPLETED"); // either order leaves enough for the other
        assertThat(o.after().get(p)).isZero();
    }

    @Test
    void sharedPayerWithoutEnoughCompletesExactlyOneAndNeverOverdrafts() throws Exception {
        long p = id(account("Jay", 7_500)), x = id(account("Ajay", 10_000)), y = id(account("Jaysus", 10_000));
        var o = run(new Tx(p, x, 6_000, 0), new Tx(p, y, 2_500, 0));
        assertThat(o.statuses().stream().filter("COMPLETED"::equals).count()).isEqualTo(1);
        assertThat(o.statuses()).contains("FAILED"); // a FAILED payment, yet every correctness property held
        assertThat(o.after().get(p)).isIn(1_500L, 5_000L); // whichever won
    }

    @Test
    void dependencyRaceEndsInOneOfTheTwoValidOutcomes() throws Exception {
        long jay = id(account("Jay", 7_500)), ajay = id(account("Ajay", 10_000)), jaysus = id(account("Jaysus", 10_000));
        var o = run(new Tx(jay, ajay, 5_000, 0), new Tx(ajay, jaysus, 14_000, 0));
        assertThat(o.statuses().getFirst()).isEqualTo("COMPLETED"); // Jay can always pay $50
        if (o.statuses().get(1).equals("COMPLETED")) {          // Jay's credit was posted first
            assertThat(List.of(o.after().get(jay), o.after().get(ajay), o.after().get(jaysus))).containsExactly(2_500L, 1_000L, 24_000L);
        } else {                                                  // Ajay's payment was evaluated first
            assertThat(o.statuses().get(1)).isEqualTo("FAILED");
            assertThat(List.of(o.after().get(jay), o.after().get(ajay), o.after().get(jaysus))).containsExactly(2_500L, 15_000L, 10_000L);
        }
    }

    @Test
    void fiveWayRingWithUserCreatedAccounts() throws Exception {
        List<Long> ids = new ArrayList<>();
        for (String n : List.of("Alice", "Bob", "Carol", "Dave", "Erin")) {
            ids.add(id(account(n, 2_000)));
        }
        var txs = new Tx[5];
        for (int i = 0; i < 5; i++) {
            txs[i] = new Tx(ids.get(i), ids.get((i + 1) % 5), 1_000, 0);
        }
        var o = run(txs);
        assertThat(o.statuses()).containsOnly("COMPLETED");
        ids.forEach(i -> assertThat(o.after().get(i)).isEqualTo(2_000)); // a ring leaves everyone where they started
    }

    @Test
    void insufficientFundsIsAFailedPaymentButAllPropertiesHold() throws Exception {
        long p = id(account("Poor", 1_000)), q = id(account("Rich", 50_000));
        var o = run(new Tx(p, q, 2_500, 0), new Tx(q, p, 500, 0), new Tx(q, p, 300, 0));
        // Poor can reach at most $18 even if both credits land first, so the $25 payment must fail.
        assertThat(o.statuses()).containsExactly("FAILED", "COMPLETED", "COMPLETED");
        assertThat(o.after().get(p)).isEqualTo(1_800);
    }

    @Test
    void startDelaysAreHonoured() throws Exception {
        long a = id(account("Early", 5_000)), b = id(account("Late", 5_000)), c = id(account("Sink", 0));
        var o = run(new Tx(a, c, 100, 0), new Tx(b, c, 100, 700));
        var results = o.run().get("results");
        assertThat(results.get(1).get("startedMs").asLong()).isGreaterThanOrEqualTo(700);
        assertThat(results.get(1).get("startedMs").asLong() - results.get(0).get("startedMs").asLong()).isGreaterThanOrEqualTo(690);
    }

    /** Runs the transactions and asserts every correctness property; returns what happened. */
    static Outcome run(Tx... txs) throws Exception {
        var ids = new LinkedHashSet<Long>();
        for (Tx t : txs) {
            ids.add(t.payer());
            ids.add(t.payee());
        }
        Map<Long, Long> before = balances(ids);
        List<Map<String, Object>> specs = new ArrayList<>();
        for (Tx t : txs) {
            specs.add(Map.of("key", "e2e-play-" + UUID.randomUUID(), "payerAccountId", t.payer(), "payeeAccountId", t.payee(),
                    "amountMinor", t.amountMinor(), "startDelayMs", t.delayMs()));
        }
        JsonNode run = postJson(payment("/demo/transactions"), specs);
        Map<Long, Long> after = balances(ids);

        List<String> statuses = new ArrayList<>();
        List<String> paymentIds = new ArrayList<>();
        run.get("results").forEach(r -> {
            assertThat(r.get("httpStatus").asInt()).isEqualTo(202);
            statuses.add(r.get("status").asString());
            paymentIds.add(r.get("id").asString());
        });
        assertThat(statuses).allMatch(s -> s.equals("COMPLETED") || s.equals("FAILED"));
        JsonNode ledgerRows = postJson(ledger("/demo/payments/ledger"), paymentIds);
        Map<Long, Long> expected = new HashMap<>(before);
        for (int i = 0; i < txs.length; i++) {
            String pid = paymentIds.get(i);
            JsonNode l = null;
            for (JsonNode row : ledgerRows) {
                if (row.get("paymentId").asString().equals(pid)) {
                    l = row;
                }
            }
            if (statuses.get(i).equals("COMPLETED")) {
                assertThat(l.get("postings").asLong()).as("completed payment posted once").isEqualTo(1);
                expected.merge(txs[i].payer(), -txs[i].amountMinor(), Long::sum);
                expected.merge(txs[i].payee(), txs[i].amountMinor(), Long::sum);
            } else {
                assertThat(l.get("postings").asLong()).as("failed payment left no ledger write").isZero();
            }
        }
        assertThat(after).as("balances reconcile with the completed payments").isEqualTo(expected);
        assertThat(after.values()).as("no negative balances").allMatch(b -> b >= 0);
        assertThat(after.values().stream().mapToLong(Long::longValue).sum())
                .as("money conserved").isEqualTo(before.values().stream().mapToLong(Long::longValue).sum());
        assertThat(get(ledger("/demo/invariants")).get("pass").asBoolean()).isTrue();
        assertThat(get(payment("/demo/invariants")).get("checks").get(1).get("violations").asLong()).as("I6").isZero();
        return new Outcome(statuses, before, after, run);
    }

    private static Map<Long, Long> balances(Iterable<Long> ids) throws Exception {
        Map<Long, Long> m = new HashMap<>();
        for (long id : ids) {
            m.put(id, balance(id));
        }
        return m;
    }

    private static long id(JsonNode account) {
        return account.get("id").asLong();
    }
}
