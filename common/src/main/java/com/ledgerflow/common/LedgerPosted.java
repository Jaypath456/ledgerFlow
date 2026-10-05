package com.ledgerflow.common;

import java.time.Instant;
import java.util.UUID;

public record LedgerPosted(
        UUID eventId,
        String eventType,
        int version,
        Instant occurredAt,
        UUID paymentId,
        long ledgerTransactionId) {

    public static final String TYPE = "LedgerPosted";

    public LedgerPosted {
        Events.check(eventId, eventType, TYPE, version, occurredAt);
        Events.require(paymentId != null, "paymentId is required");
    }

    public static LedgerPosted of(UUID paymentId, long ledgerTransactionId) {
        return new LedgerPosted(UUID.randomUUID(), TYPE, 1, Instant.now(), paymentId, ledgerTransactionId);
    }
}
