package com.ledgerflow.common;

import java.time.Instant;
import java.util.UUID;

public record LedgerRejected(
        UUID eventId,
        String eventType,
        int version,
        Instant occurredAt,
        UUID paymentId,
        String reason) {

    public static final String TYPE = "LedgerRejected";

    public LedgerRejected {
        Events.check(eventId, eventType, TYPE, version, occurredAt);
        Events.require(paymentId != null && reason != null, "paymentId and reason are required");
    }

    public static LedgerRejected of(UUID paymentId, String reason) {
        return new LedgerRejected(UUID.randomUUID(), TYPE, 1, Instant.now(), paymentId, reason);
    }
}
