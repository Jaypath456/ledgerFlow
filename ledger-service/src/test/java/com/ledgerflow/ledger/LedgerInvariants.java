package com.ledgerflow.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

/** SQL checks of the ledger invariants. Call only while no postings are in flight. */
final class LedgerInvariants {

    private LedgerInvariants() {}

    static void assertAll() {
        var jdbc = PostgresTestSupport.jdbc();

        // I1: every ledger transaction's entries sum to zero
        List<Long> unbalanced = jdbc.sql("""
                SELECT t.id FROM ledger_transactions t
                LEFT JOIN ledger_entries e ON e.transaction_id = t.id
                GROUP BY t.id HAVING COALESCE(SUM(e.amount_minor), 0) <> 0""")
                .query(Long.class).list();
        assertThat(unbalanced).as("I1 unbalanced transactions").isEmpty();
        long ledgerSum = jdbc.sql("SELECT COALESCE(SUM(amount_minor), 0) FROM ledger_entries")
                .query(Long.class).single();
        assertThat(ledgerSum).as("I1 global ledger sum").isZero();

        // I2: a payment is posted at most once
        List<UUID> duplicated = jdbc.sql("""
                SELECT payment_id FROM ledger_transactions GROUP BY payment_id HAVING count(*) > 1""")
                .query(UUID.class).list();
        assertThat(duplicated).as("I2 duplicated payments").isEmpty();

        // I4: customer (non-SYSTEM) balances never below zero
        List<Long> negative = jdbc.sql("SELECT id FROM accounts WHERE type <> 'SYSTEM' AND balance_minor < 0")
                .query(Long.class).list();
        assertThat(negative).as("I4 negative balances").isEmpty();

        // I5: cached balance equals sum of ledger entries
        List<Long> drifted = jdbc.sql("""
                SELECT a.id FROM accounts a
                WHERE a.balance_minor <> COALESCE(
                    (SELECT SUM(e.amount_minor) FROM ledger_entries e WHERE e.account_id = a.id), 0)""")
                .query(Long.class).list();
        assertThat(drifted).as("I5 cached balance drift").isEmpty();

        // Test-isolation guard: no test may post to/from the Flyway-seeded accounts.
        PostgresTestSupport.assertSeedBaseline();
    }
}
