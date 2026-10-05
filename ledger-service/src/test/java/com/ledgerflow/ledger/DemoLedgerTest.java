package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.PostgresTestSupport.balance;
import static com.ledgerflow.ledger.PostgresTestSupport.context;
import static com.ledgerflow.ledger.PostgresTestSupport.count;
import static com.ledgerflow.ledger.PostgresTestSupport.jdbc;
import static com.ledgerflow.ledger.PostgresTestSupport.ledger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.ledgerflow.common.PaymentRequested;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

/** Demo-profile ledger endpoints: isolated, double-entry-funded accounts and honest read-outs. */
class DemoLedgerTest {

    private final DemoLedgerController demo = context().getBean(DemoLedgerController.class);

    @AfterEach
    void invariantsAndSeedHold() {
        LedgerInvariants.assertAll(); // includes the seed-baseline guard: demo setup never touches accounts 1-6
    }

    @Test
    void createsFreshIsolatedAccountsFundedFromTheirOwnTreasury() {
        var created = demo.createAccounts(List.of(
                new DemoLedgerController.AccountSpec(AccountType.CUSTOMER, 50_000),
                new DemoLedgerController.AccountSpec(AccountType.MERCHANT, 0)));

        long payer = created.accounts().get(0).id();
        long payee = created.accounts().get(1).id();
        assertThat(List.of(payer, payee, created.treasuryAccountId()))
                .doesNotHaveDuplicates()
                .allSatisfy(id -> assertThat(id).isNotIn(PostgresTestSupport.SEED_BALANCES.keySet()));
        assertThat(balance(payer)).isEqualTo(50_000);
        assertThat(balance(payee)).isZero();
        assertThat(balance(created.treasuryAccountId())).isEqualTo(-50_000);
        assertThat(jdbc().sql("SELECT type FROM accounts WHERE id = ?").param(created.treasuryAccountId())
                .query(String.class).single()).isEqualTo("SYSTEM");
        // funded by exactly one balanced posting
        assertThat(jdbc().sql("SELECT count(*) FROM ledger_entries WHERE account_id = ?").param(payer)
                .query(Long.class).single()).isEqualTo(1);
    }

    @Test
    void rejectsInvalidSetupAndCreatesNothing() {
        long accountsBefore = count("accounts");
        var system = new DemoLedgerController.AccountSpec(AccountType.SYSTEM, 0);
        var negative = new DemoLedgerController.AccountSpec(AccountType.CUSTOMER, -1);
        var tooRich = new DemoLedgerController.AccountSpec(AccountType.CUSTOMER, DemoLedgerController.MAX_FUNDING_MINOR + 1);
        var ok = new DemoLedgerController.AccountSpec(AccountType.CUSTOMER, 1);

        for (var specs : List.of(List.of(system), List.of(negative), List.of(tooRich), List.<DemoLedgerController.AccountSpec>of(),
                java.util.Collections.nCopies(DemoLedgerController.MAX_ACCOUNTS + 1, ok))) {
            assertThatThrownBy(() -> demo.createAccounts(specs)).isInstanceOf(ResponseStatusException.class)
                    .hasMessageContaining("400");
        }
        assertThat(count("accounts")).isEqualTo(accountsBefore);
    }

    @Test
    void reportsBalancesAndPerPaymentLedgerOutcomes() {
        var created = demo.createAccounts(List.of(
                new DemoLedgerController.AccountSpec(AccountType.CUSTOMER, 1_000),
                new DemoLedgerController.AccountSpec(AccountType.MERCHANT, 0)));
        long payer = created.accounts().get(0).id();
        long payee = created.accounts().get(1).id();
        var listener = context().getBean(PaymentRequestListener.class);
        var posted = PaymentRequested.of(UUID.randomUUID(), payer, payee, 400);
        var rejected = PaymentRequested.of(UUID.randomUUID(), payer, payee, 5_000);
        listener.handle(posted);
        listener.handle(rejected);
        UUID unknown = UUID.randomUUID();

        assertThat(demo.balances(List.of(payer, payee)))
                .extracting(Account::id, Account::balanceMinor)
                .containsExactly(org.assertj.core.groups.Tuple.tuple(payer, 600L), org.assertj.core.groups.Tuple.tuple(payee, 400L));
        assertThat(demo.paymentLedger(List.of(posted.paymentId(), rejected.paymentId(), unknown)))
                .extracting(DemoLedgerController.PaymentLedger::paymentId, DemoLedgerController.PaymentLedger::outcome,
                        DemoLedgerController.PaymentLedger::reason, DemoLedgerController.PaymentLedger::postings)
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple(posted.paymentId(), "POSTED", null, 1L),
                        org.assertj.core.groups.Tuple.tuple(rejected.paymentId(), "REJECTED", "INSUFFICIENT_FUNDS", 0L),
                        org.assertj.core.groups.Tuple.tuple(unknown, null, null, 0L));
    }

    @Test
    void invariantSummaryIsCleanAndCatchesCorruption() {
        var clean = demo.invariants();
        assertThat(clean.pass()).isTrue();
        assertThat(clean.checks()).extracting(DemoLedgerController.Check::invariant).containsExactly("I1", "I2", "I4", "I5");

        long account = demo.createAccounts(List.of(new DemoLedgerController.AccountSpec(AccountType.CUSTOMER, 700)))
                .accounts().getFirst().id();
        long txId = jdbc().sql("SELECT transaction_id FROM ledger_entries WHERE account_id = ?").param(account)
                .query(Long.class).single();
        var tx = new TransactionTemplate(context().getBean(PlatformTransactionManager.class));
        tx.executeWithoutResult(status -> {
            jdbc().sql("UPDATE accounts SET balance_minor = balance_minor + 1 WHERE id = ?").param(account).update();
            jdbc().sql("INSERT INTO ledger_entries (transaction_id, account_id, amount_minor) VALUES (?, ?, 5)")
                    .params(txId, account).update();
            var dirty = demo.invariants();
            assertThat(dirty.pass()).isFalse();
            assertThat(violations(dirty, "I1")).isEqualTo(2); // unbalanced transaction + global sum
            assertThat(violations(dirty, "I5")).isEqualTo(1);
            status.setRollbackOnly();
        });
        assertThat(demo.invariants().pass()).isTrue();
    }

    @Test
    void reportsPostgresDeadlockCounter() {
        long deadlocks = demo.deadlocks();
        ledger().post(UUID.randomUUID(), demo.createAccounts(List.of(new DemoLedgerController.AccountSpec(AccountType.CUSTOMER, 10)))
                .accounts().getFirst().id(), demo.createAccounts(List.of(new DemoLedgerController.AccountSpec(AccountType.MERCHANT, 0)))
                .accounts().getFirst().id(), 5);
        assertThat(demo.deadlocks()).isGreaterThanOrEqualTo(deadlocks).isGreaterThanOrEqualTo(0);
    }

    private static long violations(DemoLedgerController.InvariantSummary s, String invariant) {
        return s.checks().stream().filter(c -> c.invariant().equals(invariant)).findFirst().orElseThrow().violations();
    }
}
