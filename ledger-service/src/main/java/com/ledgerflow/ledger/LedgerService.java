package com.ledgerflow.ledger;

import java.util.UUID;
import org.springframework.stereotype.Service;
import com.ledgerflow.common.MongoStore;
import org.springframework.dao.DuplicateKeyException;

@Service
public class LedgerService {

    private final LedgerRepository repo;
    private final MongoStore store;

    LedgerService(LedgerRepository repo, MongoStore store) {
        this.repo = repo;
        this.store = store;
    }

    /** Conditional debit and balanced posting commit in the same driver transaction. */
    public PostingResult post(UUID paymentId, long payerAccountId, long payeeAccountId, long amountMinor) {
        if (store.inTransaction()) return postInTransaction(paymentId, payerAccountId, payeeAccountId, amountMinor);
        try {
            return store.transaction(() -> postInTransaction(paymentId, payerAccountId, payeeAccountId, amountMinor));
        } catch (DuplicateKeyException duplicate) {
            if (repo.transactionId(paymentId).isPresent()) throw new LedgerException.DuplicatePayment(paymentId);
            throw duplicate;
        }
    }

    private PostingResult postInTransaction(UUID paymentId, long payerAccountId, long payeeAccountId, long amountMinor) {
        if (amountMinor <= 0) {
            throw new LedgerException.InvalidAmount(amountMinor);
        }
        if (payerAccountId == payeeAccountId) {
            throw new LedgerException.SamePayerPayee(payerAccountId);
        }

        long first = Math.min(payerAccountId, payeeAccountId);
        long second = Math.max(payerAccountId, payeeAccountId);
        Account a = repo.findAccount(first).orElseThrow(() -> new LedgerException.AccountNotFound(first));
        Account b = repo.findAccount(second).orElseThrow(() -> new LedgerException.AccountNotFound(second));
        Account payer = a.id() == payerAccountId ? a : b;
        Account payee = a.id() == payerAccountId ? b : a;

        // Replays must report the existing posting even if the balance is now depleted.
        if (repo.transactionId(paymentId).isPresent()) throw new LedgerException.DuplicatePayment(paymentId);
        Math.addExact(payee.balanceMinor(), amountMinor);
        Math.subtractExact(payer.balanceMinor(), amountMinor);
        if (!repo.debit(payer, amountMinor)) {
            throw new LedgerException.InsufficientFunds(payer.id(), payer.balanceMinor(), amountMinor);
        }
        repo.addToBalance(payee.id(), amountMinor);
        long txId = repo.insertTransaction(paymentId);
        repo.insertEntry(txId, payer.id(), -amountMinor);
        repo.insertEntry(txId, payee.id(), amountMinor);
        return new PostingResult(txId);
    }
}
