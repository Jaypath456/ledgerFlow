package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.MongoTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import static com.ledgerflow.common.MongoStore.eq;
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
        List<long[]> entries = mongo().find(eq("transaction_id", result.transactionId()).with(Sort.by("amount_minor")),
                Document.class, "ledger_entries").stream()
                .map(d -> new long[] {d.getLong("account_id"), d.getLong("amount_minor")}).toList();
        assertThat(entries).containsExactly(new long[] {payer, -300}, new long[] {payee, 300});
        assertThat(UUID.fromString(mongo().findOne(eq("_id", result.transactionId()), Document.class, "ledger_transactions").getString("payment_id"))).isEqualTo(paymentId);
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

        assertThat(mongo().count(eq("payment_id", paymentId), "ledger_transactions")).isEqualTo(1);
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
