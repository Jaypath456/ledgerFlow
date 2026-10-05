package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.PostgresTestSupport.jdbc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Uses the shared context from {@link PostgresTestSupport}: a second application context would join
 * the same Kafka consumer group and could consume other tests' events.
 */
class LedgerDbSmokeTest {

    @Test
    void flywayRanInOwnSchema() {
        long applied = jdbc().sql("select count(*) from ledger.flyway_schema_history where success")
                .query(Long.class).single();
        assertThat(applied).isEqualTo(4);
    }

    @Test
    void cannotTouchOtherServiceSchema() {
        assertThatThrownBy(() -> jdbc().sql("select * from payments.anything").query().listOfRows())
                .rootCause().hasMessageContaining("permission denied for schema");
    }
}
