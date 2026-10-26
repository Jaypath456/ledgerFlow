package com.ledgerflow.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.bson.Document;

/** Checks of the ledger invariants. Call only while no postings are in flight. */
final class LedgerInvariants {
    private LedgerInvariants() {}

    static void assertAll() {
        var mongo = MongoTestSupport.mongo();
        var entries = mongo.findAll(Document.class, "ledger_entries");
        var transactions = mongo.findAll(Document.class, "ledger_transactions");
        var accounts = mongo.findAll(Document.class, "accounts");
        var totals = entries.stream().collect(Collectors.groupingBy(e -> e.getLong("transaction_id"),
                Collectors.summingLong(e -> e.getLong("amount_minor"))));
        List<Long> unbalanced = transactions.stream().map(t -> t.getLong("_id"))
                .filter(id -> totals.getOrDefault(id, 0L) != 0).toList();
        assertThat(unbalanced).as("I1 unbalanced transactions").isEmpty();
        long ledgerSum = entries.stream().mapToLong(e -> e.getLong("amount_minor")).sum();
        assertThat(ledgerSum).as("I1 global ledger sum").isZero();

        List<UUID> duplicated = transactions.stream().collect(Collectors.groupingBy(t -> t.getString("payment_id"), Collectors.counting()))
                .entrySet().stream().filter(e -> e.getValue() > 1).map(e -> UUID.fromString(e.getKey())).toList();
        assertThat(duplicated).as("I2 duplicated payments").isEmpty();

        List<Long> negative = accounts.stream().filter(a -> !a.getString("type").equals("SYSTEM") && a.getLong("balance_minor") < 0)
                .map(a -> a.getLong("_id")).toList();
        assertThat(negative).as("I4 negative balances").isEmpty();

        var balances = entries.stream().collect(Collectors.groupingBy(e -> e.getLong("account_id"),
                Collectors.summingLong(e -> e.getLong("amount_minor"))));
        List<Long> drifted = accounts.stream().filter(a -> !a.getLong("balance_minor").equals(balances.getOrDefault(a.getLong("_id"), 0L)))
                .map(a -> a.getLong("_id")).toList();
        assertThat(drifted).as("I5 cached balance drift").isEmpty();
        MongoTestSupport.assertSeedBaseline();
    }
}
