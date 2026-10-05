package com.ledgerflow.common;

import java.time.Instant;
import java.util.UUID;

final class Events {

    private Events() {}

    static void check(UUID eventId, String eventType, String expectedType, int version, Instant occurredAt) {
        require(eventId != null && occurredAt != null, "eventId and occurredAt are required");
        require(expectedType.equals(eventType), "eventType must be " + expectedType + ", was " + eventType);
        require(version == 1, "unsupported version " + version);
    }

    static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
