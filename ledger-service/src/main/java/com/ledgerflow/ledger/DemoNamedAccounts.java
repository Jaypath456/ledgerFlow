package com.ledgerflow.ledger;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Demo profile only: friendly names for demo accounts (Jay, Ajay, …). A name is a label in a
 * demo-only table, created here rather than by Flyway, so the normal schema is untouched. The money
 * side is ordinary: each named account is a CUSTOMER account funded from its own SYSTEM treasury
 * through {@link LedgerService#post}, so every balance has matching ledger entries.
 */
@RestController
@RequestMapping("/demo/named-accounts")
@Profile("demo")
class DemoNamedAccounts implements ApplicationRunner {

    static final Map<String, Long> STARTERS = Map.of("Jay", 7_500L, "Ajay", 10_000L);
    static final long MAX_BALANCE_MINOR = 100_000_000; // $1,000,000
    private static final Pattern NAME = Pattern.compile("[A-Za-z][A-Za-z0-9 _-]{0,23}");

    record NamedAccount(String name, long id, long balanceMinor) {}

    record CreateRequest(String name, long balanceMinor) {}

    private final JdbcClient jdbc;
    private final LedgerService ledger;
    private final TransactionTemplate tx;

    DemoNamedAccounts(JdbcClient jdbc, LedgerService ledger, PlatformTransactionManager tm) {
        this.jdbc = jdbc;
        this.ledger = ledger;
        this.tx = new TransactionTemplate(tm);
    }

    /** Creates the label table and, on an empty demo database, the starter accounts Jay and Ajay. */
    @Override
    public void run(ApplicationArguments args) {
        jdbc.sql("""
                CREATE TABLE IF NOT EXISTS demo_account_names (
                    account_id          BIGINT PRIMARY KEY REFERENCES accounts (id),
                    name                TEXT   NOT NULL,
                    treasury_account_id BIGINT NOT NULL REFERENCES accounts (id))""").update();
        jdbc.sql("CREATE UNIQUE INDEX IF NOT EXISTS demo_account_names_name_idx ON demo_account_names (lower(name))").update();
        if (jdbc.sql("SELECT count(*) FROM demo_account_names").query(Long.class).single() == 0) {
            create(new CreateRequest("Jay", STARTERS.get("Jay")));
            create(new CreateRequest("Ajay", STARTERS.get("Ajay")));
        }
    }

    @GetMapping
    List<NamedAccount> list() {
        return jdbc.sql("""
                        SELECT n.name, a.id, a.balance_minor FROM demo_account_names n
                        JOIN accounts a ON a.id = n.account_id ORDER BY a.id""")
                .query((rs, i) -> new NamedAccount(rs.getString("name"), rs.getLong("id"), rs.getLong("balance_minor")))
                .list();
    }

    @PostMapping
    NamedAccount create(@RequestBody CreateRequest request) {
        String name = request.name() == null ? "" : request.name().trim();
        if (!NAME.matcher(name).matches()) {
            throw badRequest("name must be 1-24 letters, digits, spaces, '-' or '_', starting with a letter");
        }
        if (request.balanceMinor() < 0 || request.balanceMinor() > MAX_BALANCE_MINOR) {
            throw badRequest("starting balance must be between $0 and $1,000,000");
        }
        return tx.execute(s -> {
            if (jdbc.sql("SELECT count(*) FROM demo_account_names WHERE lower(name) = lower(?)").param(name)
                    .query(Long.class).single() > 0) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "an account named " + name + " already exists");
            }
            long treasury = insertAccount(AccountType.SYSTEM);
            long id = insertAccount(AccountType.CUSTOMER);
            if (request.balanceMinor() > 0) {
                ledger.post(UUID.randomUUID(), treasury, id, request.balanceMinor());
            }
            jdbc.sql("INSERT INTO demo_account_names (account_id, name, treasury_account_id) VALUES (?, ?, ?)")
                    .params(id, name, treasury).update();
            return new NamedAccount(name, id, request.balanceMinor());
        });
    }

    /**
     * Sets named accounts to target balances for the playground examples. Each difference is moved
     * between the account and its own treasury with an ordinary balanced posting; no balance is
     * written directly, and only named demo accounts can be adjusted.
     */
    @PostMapping("/balances")
    List<NamedAccount> setBalances(@RequestBody Map<String, Long> targets) {
        if (targets == null || targets.isEmpty() || targets.size() > 10) {
            throw badRequest("between 1 and 10 accounts");
        }
        tx.executeWithoutResult(s -> targets.forEach((name, target) -> {
            if (target == null || target < 0 || target > MAX_BALANCE_MINOR) {
                throw badRequest("target balance out of range for " + name);
            }
            var row = jdbc.sql("""
                            SELECT n.account_id, n.treasury_account_id, a.balance_minor FROM demo_account_names n
                            JOIN accounts a ON a.id = n.account_id WHERE lower(n.name) = lower(?)""")
                    .param(name)
                    .query((rs, i) -> new long[] {rs.getLong(1), rs.getLong(2), rs.getLong(3)})
                    .optional()
                    .orElseThrow(() -> badRequest("no demo account named " + name));
            long delta = target - row[2];
            if (delta > 0) {
                ledger.post(UUID.randomUUID(), row[1], row[0], delta);
            } else if (delta < 0) {
                ledger.post(UUID.randomUUID(), row[0], row[1], -delta);
            }
        }));
        return list();
    }

    private long insertAccount(AccountType type) {
        return jdbc.sql("INSERT INTO accounts (type) VALUES (?) RETURNING id").param(type.name()).query(Long.class).single();
    }

    private static ResponseStatusException badRequest(String reason) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, reason);
    }
}
