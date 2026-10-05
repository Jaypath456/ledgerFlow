package com.ledgerflow.payment;

import java.time.Instant;
import java.util.UUID;

public record Payment(
        UUID id,
        long payerAccountId,
        long payeeAccountId,
        long amountMinor,
        String currency,
        PaymentStatus status,
        String declineReason,
        Instant createdAt,
        Instant updatedAt) {}
