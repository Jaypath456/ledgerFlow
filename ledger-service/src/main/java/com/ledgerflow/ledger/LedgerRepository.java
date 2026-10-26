package com.ledgerflow.ledger;

import com.ledgerflow.common.MongoStore;
import java.util.Date;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Repository;
import static com.ledgerflow.common.MongoStore.eq;

@Repository
class LedgerRepository {
    private final MongoStore store;

    LedgerRepository(MongoStore store) {
        this.store = store;
        for (String collection : new String[] {"accounts", "ledger_transactions", "ledger_entries", "payment_outcomes"}) store.collection(collection);
        store.unique("ledger_transactions", "payment_id");
        store.unique("payment_outcomes", "payment_id");
        store.mongo().indexOps("payment_outcomes").createIndex(new Index().on("ledger_transaction_id", Sort.Direction.ASC).unique().sparse());
        store.mongo().indexOps("ledger_entries").createIndex(new Index().on("transaction_id", Sort.Direction.ASC));
        store.mongo().indexOps("ledger_entries").createIndex(new Index().on("account_id", Sort.Direction.ASC));
        seed();
    }

    /** Seed once atomically, preserving the original six accounts and balanced funding postings. */
    private void seed() {
        store.transaction(() -> {
            if (store.mongo().exists(eq("_id", 1L), "accounts")) return null;
            String[] types = {"SYSTEM", "CUSTOMER", "CUSTOMER", "CUSTOMER", "MERCHANT", "MERCHANT"};
            for (int i = 0; i < types.length; i++) store.mongo().insert(MongoStore.account(i + 1L, types[i]), "accounts");
            long[] amounts = {100_000L, 50_000L, 25_000L};
            for (int i = 0; i < amounts.length; i++) {
                long customer = i + 2L;
                UUID payment = UUID.fromString("00000000-0000-0000-0000-%012d".formatted(customer));
                long tx = insertTransaction(payment);
                insertEntry(tx, 1L, -amounts[i]);
                insertEntry(tx, customer, amounts[i]);
                addToBalance(1L, -amounts[i]);
                addToBalance(customer, amounts[i]);
            }
            return null;
        });
    }

    Optional<Account> findAccount(long id) {
        return Optional.ofNullable(store.mongo().findOne(eq("_id", id), Document.class, "accounts"))
                .map(d -> new Account(d.getLong("_id"), AccountType.valueOf(d.getString("type")), d.getLong("balance_minor")));
    }

    OptionalLong transactionId(UUID paymentId) {
        Document d = store.mongo().findOne(eq("payment_id", paymentId), Document.class, "ledger_transactions");
        return d == null ? OptionalLong.empty() : OptionalLong.of(d.getLong("_id"));
    }

    long insertTransaction(UUID paymentId) {
        long id = MongoStore.nextId();
        store.mongo().insert(new Document("_id", id).append("payment_id", paymentId.toString()).append("created_at", new Date()), "ledger_transactions");
        return id;
    }

    void insertEntry(long transactionId, long accountId, long amountMinor) {
        store.mongo().insert(new Document("transaction_id", transactionId).append("account_id", accountId)
                .append("amount_minor", amountMinor), "ledger_entries");
    }

    boolean debit(Account payer, long amount) {
        Criteria filter = Criteria.where("_id").is(payer.id());
        if (payer.type() != AccountType.SYSTEM) filter.and("balance_minor").gte(amount);
        return store.mongo().updateFirst(Query.query(filter), new Update().inc("balance_minor", -amount), "accounts").getMatchedCount() == 1;
    }

    void addToBalance(long accountId, long deltaMinor) {
        store.mongo().updateFirst(eq("_id", accountId), new Update().inc("balance_minor", deltaMinor), "accounts");
    }
}
