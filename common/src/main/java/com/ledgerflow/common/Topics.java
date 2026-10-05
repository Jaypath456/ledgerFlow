package com.ledgerflow.common;

public final class Topics {
    public static final String PAYMENTS_REQUESTED = "payments.requested";
    public static final String LEDGER_RESULTS = "ledger.results";
    /** Spring Kafka 4 default dead-letter naming: same partition on {@code <topic>-dlt}. */
    public static final String DLT_SUFFIX = "-dlt";
    public static final int PARTITIONS = 3;

    private Topics() {}
}
