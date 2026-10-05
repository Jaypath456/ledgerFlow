package com.ledgerflow.ledger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Demo-only (profile "demo") support for the dashboard and demo/ scripts. Narrow by design:
 * creates fresh, separately funded scenario accounts, and reads balances, ledger outcomes and
 * ledger-schema invariants. It never touches existing accounts and exposes no SQL, shell or
 * infrastructure control.
 */
@RestController
@RequestMapping("/demo")
@Profile("demo")
class DemoLedgerController {

    static final int MAX_ACCOUNTS = 10;
    static final long MAX_FUNDING_MINOR = 100_000_000; // $1,000,000

    record AccountSpec(AccountType type, long balanceMinor) {}

    record ScenarioAccounts(long treasuryAccountId, List<Account> accounts) {}

    record PaymentLedger(UUID paymentId, String outcome, String reason, long postings) {}

    record Check(String invariant, String description, long violations, boolean pass) {}

    record InvariantSummary(String scope, List<Check> checks, long violations, boolean pass) {}

    private final JdbcClient jdbc;
    private final LedgerService ledger;

    DemoLedgerController(JdbcClient jdbc, LedgerService ledger) {
        this.jdbc = jdbc;
        this.ledger = ledger;
    }

    /**
     * Creates a fresh SYSTEM treasury for this request plus fresh accounts, each funded from that
     * treasury through a normal double-entry posting. One transaction: all or nothing.
     */
    @PostMapping("/accounts")
    @Transactional
    ScenarioAccounts createAccounts(@RequestBody List<AccountSpec> specs) {
        if (specs == null || specs.isEmpty() || specs.size() > MAX_ACCOUNTS) {
            throw badRequest("between 1 and " + MAX_ACCOUNTS + " accounts per request");
        }
        for (AccountSpec s : specs) {
            if (s.type() == null || s.type() == AccountType.SYSTEM) {
                throw badRequest("type must be CUSTOMER or MERCHANT");
            }
            if (s.balanceMinor() < 0 || s.balanceMinor() > MAX_FUNDING_MINOR) {
                throw badRequest("balanceMinor must be between 0 and " + MAX_FUNDING_MINOR);
            }
        }
        long treasury = insertAccount(AccountType.SYSTEM);
        List<Account> created = new ArrayList<>();
        for (AccountSpec s : specs) {
            long id = insertAccount(s.type());
            if (s.balanceMinor() > 0) {
                ledger.post(UUID.randomUUID(), treasury, id, s.balanceMinor());
            }
            created.add(new Account(id, s.type(), s.balanceMinor()));
        }
        return new ScenarioAccounts(treasury, created);
    }

    @GetMapping("/accounts")
    List<Account> balances(@RequestParam List<Long> ids) {
        if (ids.size() > 50) {
            throw badRequest("at most 50 ids");
        }
        return jdbc.sql("SELECT id, type, balance_minor FROM accounts WHERE id IN (:ids) ORDER BY id")
                .param("ids", ids)
                .query((rs, n) -> new Account(
                        rs.getLong("id"), AccountType.valueOf(rs.getString("type")), rs.getLong("balance_minor")))
                .list();
    }

    /** Per payment: the ledger's recorded outcome (null if never seen) and how many postings exist. */
    @PostMapping("/payments/ledger")
    List<PaymentLedger> paymentLedger(@RequestBody List<UUID> paymentIds) {
        if (paymentIds == null || paymentIds.isEmpty() || paymentIds.size() > 1000) {
            throw badRequest("between 1 and 1000 payment ids");
        }
        return jdbc.sql("""
                        SELECT p.id,
                               o.status AS outcome,
                               o.reason,
                               (SELECT count(*) FROM ledger_transactions t WHERE t.payment_id = p.id) AS postings
                        FROM unnest(CAST(:ids AS uuid[])) AS p(id)
                        LEFT JOIN payment_outcomes o ON o.payment_id = p.id""")
                .param("ids", paymentIds.toArray(UUID[]::new))
                .query((rs, n) -> new PaymentLedger(rs.getObject("id", UUID.class), rs.getString("outcome"),
                        rs.getString("reason"), rs.getLong("postings")))
                .list();
    }

    /** The ledger-schema parts of chaos/verify_invariants.sql (I1, I2 duplicate postings, I4, I5). */
    @GetMapping("/invariants")
    InvariantSummary invariants() {
        List<Check> checks = List.of(
                check("I1", "every ledger transaction has >= 2 entries summing to zero, and the global sum is zero", """
                        SELECT (SELECT count(*) FROM (
                                    SELECT t.id FROM ledger_transactions t
                                    LEFT JOIN ledger_entries e ON e.transaction_id = t.id
                                    GROUP BY t.id
                                    HAVING COALESCE(SUM(e.amount_minor), 0) <> 0 OR count(e.id) < 2) x)
                             + (SELECT CASE WHEN COALESCE(SUM(amount_minor), 0) <> 0 THEN 1 ELSE 0 END
                                FROM ledger_entries)"""),
                check("I2", "no payment has more than one ledger posting", """
                        SELECT count(*) FROM (
                            SELECT payment_id FROM ledger_transactions GROUP BY payment_id HAVING count(*) > 1) x"""),
                check("I4", "no non-SYSTEM account has a negative balance", """
                        SELECT count(*) FROM accounts WHERE type <> 'SYSTEM' AND balance_minor < 0"""),
                check("I5", "every cached balance equals the sum of its ledger entries", """
                        SELECT count(*) FROM accounts a
                        LEFT JOIN (SELECT account_id, SUM(amount_minor) AS total FROM ledger_entries GROUP BY account_id) s
                               ON s.account_id = a.id
                        WHERE a.balance_minor <> COALESCE(s.total, 0)"""));
        long violations = checks.stream().mapToLong(Check::violations).sum();
        return new InvariantSummary("ledger schema", checks, violations, violations == 0);
    }

    /** Postgres's own cumulative deadlock counter for this database (sample before/after a run). */
    @GetMapping("/deadlocks")
    long deadlocks() {
        return jdbc.sql("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()")
                .query(Long.class).single();
    }

    private Check check(String invariant, String description, String countSql) {
        long violations = jdbc.sql(countSql).query(Long.class).single();
        return new Check(invariant, description, violations, violations == 0);
    }

    private long insertAccount(AccountType type) {
        return jdbc.sql("INSERT INTO accounts (type) VALUES (?) RETURNING id").param(type.name())
                .query(Long.class).single();
    }

    private static ResponseStatusException badRequest(String reason) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, reason);
    }
}
