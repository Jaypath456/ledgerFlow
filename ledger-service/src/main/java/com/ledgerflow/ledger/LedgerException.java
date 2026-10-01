package com.ledgerflow.ledger;

import java.util.UUID;

/** Business rejections of a posting. Unchecked so the surrounding transaction rolls back. */
public abstract sealed class LedgerException extends RuntimeException {

    private LedgerException(String message) {
        super(message);
    }

    public static final class InvalidAmount extends LedgerException {
        public InvalidAmount(long amountMinor) {
            super("amount must be positive: " + amountMinor);
        }
    }

    public static final class SamePayerPayee extends LedgerException {
        public SamePayerPayee(long accountId) {
            super("payer and payee are the same account: " + accountId);
        }
    }

    public static final class AccountNotFound extends LedgerException {
        public AccountNotFound(long accountId) {
            super("unknown account: " + accountId);
        }
    }

    public static final class InsufficientFunds extends LedgerException {
        public InsufficientFunds(long accountId, long balanceMinor, long amountMinor) {
            super("account %d has %d, needs %d".formatted(accountId, balanceMinor, amountMinor));
        }
    }

    public static final class DuplicatePayment extends LedgerException {
        public DuplicatePayment(UUID paymentId) {
            super("payment already posted: " + paymentId);
        }
    }
}
