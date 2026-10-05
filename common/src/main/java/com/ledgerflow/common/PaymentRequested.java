package com.ledgerflow.common;

import java.time.Instant;
import java.util.UUID;

public record PaymentRequested(
        UUID eventId,
        String eventType,
        int version,
        Instant occurredAt,
        UUID paymentId,
        long payerAccountId,
        long payeeAccountId,
        long amountMinor) {

    public static final String TYPE = "PaymentRequested";

    public static PaymentRequested of(UUID paymentId, long payerAccountId, long payeeAccountId, long amountMinor) {
        return new PaymentRequested(UUID.randomUUID(), TYPE, 1, Instant.now(),
                paymentId, payerAccountId, payeeAccountId, amountMinor);
    }
}
