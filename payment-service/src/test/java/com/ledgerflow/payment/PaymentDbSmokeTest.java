package com.ledgerflow.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

class PaymentDbSmokeTest extends PaymentTestSupport {

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void flywayRanInOwnSchema() {
        Integer applied = jdbc.queryForObject(
                "select count(*) from payments.flyway_schema_history where success", Integer.class);
        assertThat(applied).isEqualTo(2);
    }

    @Test
    void cannotTouchOtherServiceSchema() {
        assertThatThrownBy(() -> jdbc.queryForList("select * from ledger.anything"))
                .rootCause().hasMessageContaining("permission denied for schema");
    }
}
