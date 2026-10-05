package com.ledgerflow.ledger;

import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class LedgerService {

    private final LedgerRepository repo;

    LedgerService(LedgerRepository repo) {
        this.repo = repo;
    }

    /**
     * Moves amountMinor from payer to payee in one DB transaction.
     * Locks are taken in ascending account-id order, so concurrent postings cannot deadlock.
     */
    @Transactional
    public PostingResult post(UUID paymentId, long payerAccountId, long payeeAccountId, long amountMinor) {
        if (amountMinor <= 0) {
            throw new LedgerException.InvalidAmount(amountMinor);
        }
        if (payerAccountId == payeeAccountId) {
            throw new LedgerException.SamePayerPayee(payerAccountId);
        }

        long first = Math.min(payerAccountId, payeeAccountId);
        long second = Math.max(payerAccountId, payeeAccountId);
        Account a = repo.lockAccount(first).orElseThrow(() -> new LedgerException.AccountNotFound(first));
        Account b = repo.lockAccount(second).orElseThrow(() -> new LedgerException.AccountNotFound(second));
        Account payer = a.id() == payerAccountId ? a : b;
        Account payee = a.id() == payerAccountId ? b : a;

        // Before the funds check: replaying a posted payment reports "duplicate", not "insufficient".
        long txId = repo.insertTransaction(paymentId)
                .orElseThrow(() -> new LedgerException.DuplicatePayment(paymentId));

        if (payer.type() != AccountType.SYSTEM && payer.balanceMinor() < amountMinor) {
            throw new LedgerException.InsufficientFunds(payer.id(), payer.balanceMinor(), amountMinor);
        }
        Math.addExact(payee.balanceMinor(), amountMinor); // fail rather than overflow BIGINT

        repo.insertEntry(txId, payer.id(), -amountMinor);
        repo.insertEntry(txId, payee.id(), amountMinor);
        repo.addToBalance(payer.id(), -amountMinor);
        repo.addToBalance(payee.id(), amountMinor);
        return new PostingResult(txId);
    }
}
