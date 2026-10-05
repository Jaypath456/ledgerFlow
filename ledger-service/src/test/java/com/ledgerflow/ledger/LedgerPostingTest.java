package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.PostgresTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class LedgerPostingTest {

    @AfterEach
    void invariants() {
        LedgerInvariants.assertAll();
    }

    @Test
    void seedIsBalancedAndFunded() {
        assertThat(balance(1)).isEqualTo(-175_000);
        assertThat(balance(2)).isEqualTo(100_000);
        assertThat(balance(3)).isEqualTo(50_000);
        assertThat(balance(4)).isEqualTo(25_000);
        assertThat(balance(5)).isZero();
    }

    @Test
    void normalPosting() {
        long payer = fundedCustomer(1_000);
        long payee = createAccount(AccountType.MERCHANT);
        UUID paymentId = UUID.randomUUID();

        PostingResult result = ledger().post(paymentId, payer, payee, 300);

        assertThat(balance(payer)).isEqualTo(700);
        assertThat(balance(payee)).isEqualTo(300);
        List<long[]> entries = jdbc().sql(
                        "SELECT account_id, amount_minor FROM ledger_entries WHERE transaction_id = ? ORDER BY amount_minor")
                .param(result.transactionId())
                .query((rs, n) -> new long[] {rs.getLong(1), rs.getLong(2)}).list();
        assertThat(entries).containsExactly(new long[] {payer, -300}, new long[] {payee, 300});
        assertThat(jdbc().sql("SELECT payment_id FROM ledger_transactions WHERE id = ?")
                .param(result.transactionId()).query(UUID.class).single()).isEqualTo(paymentId);
    }

    @Test
    void insufficientFundsLeavesNothingBehind() {
        long payer = fundedCustomer(100);
        long payee = createAccount(AccountType.MERCHANT);
        long txBefore = count("ledger_transactions");
        long entriesBefore = count("ledger_entries");

        assertThatThrownBy(() -> ledger().post(UUID.randomUUID(), payer, payee, 101))
                .isInstanceOf(LedgerException.InsufficientFunds.class);

        assertThat(count("ledger_transactions")).isEqualTo(txBefore);
        assertThat(count("ledger_entries")).isEqualTo(entriesBefore);
        assertThat(balance(payer)).isEqualTo(100);
        assertThat(balance(payee)).isZero();
    }

    @Test
    void duplicatePaymentIdPostsOnce() {
        long payer = fundedCustomer(1_000);
        long payee = createAccount(AccountType.MERCHANT);
        UUID paymentId = UUID.randomUUID();
        ledger().post(paymentId, payer, payee, 100);

        assertThatThrownBy(() -> ledger().post(paymentId, payer, payee, 100))
                .isInstanceOf(LedgerException.DuplicatePayment.class);

        assertThat(jdbc().sql("SELECT count(*) FROM ledger_transactions WHERE payment_id = ?")
                .param(paymentId).query(Long.class).single()).isEqualTo(1);
        assertThat(balance(payer)).isEqualTo(900);
        assertThat(balance(payee)).isEqualTo(100);
    }

    @Test
    void duplicateIsReportedEvenWhenFundsHaveSinceRunOut() {
        long payer = fundedCustomer(100);
        long payee = createAccount(AccountType.MERCHANT);
        UUID paymentId = UUID.randomUUID();
        ledger().post(paymentId, payer, payee, 100);

        assertThatThrownBy(() -> ledger().post(paymentId, payer, payee, 100))
                .isInstanceOf(LedgerException.DuplicatePayment.class);
    }

    @Test
    void invalidRequestsAreRejected() {
        long payer = fundedCustomer(100);
        long payee = createAccount(AccountType.MERCHANT);
        long txBefore = count("ledger_transactions");

        assertThatThrownBy(() -> ledger().post(UUID.randomUUID(), payer, payee, 0))
                .isInstanceOf(LedgerException.InvalidAmount.class);
        assertThatThrownBy(() -> ledger().post(UUID.randomUUID(), payer, payee, -5))
                .isInstanceOf(LedgerException.InvalidAmount.class);
        assertThatThrownBy(() -> ledger().post(UUID.randomUUID(), payer, payer, 5))
                .isInstanceOf(LedgerException.SamePayerPayee.class);
        assertThatThrownBy(() -> ledger().post(UUID.randomUUID(), payer, 999_999_999L, 5))
                .isInstanceOf(LedgerException.AccountNotFound.class);
        assertThatThrownBy(() -> ledger().post(UUID.randomUUID(), 999_999_999L, payee, 5))
                .isInstanceOf(LedgerException.AccountNotFound.class);

        assertThat(count("ledger_transactions")).isEqualTo(txBefore);
        assertThat(balance(payer)).isEqualTo(100);
    }
}
