package com.ledgerflow.ledger;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class LedgerRepository {

    private final JdbcClient jdbc;

    LedgerRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Row-locks the account until the surrounding transaction ends. */
    Optional<Account> lockAccount(long id) {
        return jdbc.sql("SELECT id, type, balance_minor FROM accounts WHERE id = ? FOR UPDATE")
                .param(id)
                .query((rs, n) -> new Account(
                        rs.getLong("id"), AccountType.valueOf(rs.getString("type")), rs.getLong("balance_minor")))
                .optional();
    }

    /** Empty when the payment_id already exists. */
    OptionalLong insertTransaction(UUID paymentId) {
        return jdbc.sql("""
                        INSERT INTO ledger_transactions (payment_id) VALUES (?)
                        ON CONFLICT (payment_id) DO NOTHING RETURNING id""")
                .param(paymentId)
                .query(Long.class)
                .optional()
                .map(OptionalLong::of)
                .orElseGet(OptionalLong::empty);
    }

    void insertEntry(long transactionId, long accountId, long amountMinor) {
        jdbc.sql("INSERT INTO ledger_entries (transaction_id, account_id, amount_minor) VALUES (?, ?, ?)")
                .params(transactionId, accountId, amountMinor)
                .update();
    }

    void addToBalance(long accountId, long deltaMinor) {
        jdbc.sql("UPDATE accounts SET balance_minor = balance_minor + ? WHERE id = ?")
                .params(deltaMinor, accountId)
                .update();
    }
}
