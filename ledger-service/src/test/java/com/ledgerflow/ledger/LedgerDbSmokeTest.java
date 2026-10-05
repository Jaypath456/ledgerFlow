package com.ledgerflow.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(properties = LedgerServiceApplication.CONFIG_NAME)
class LedgerDbSmokeTest {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", PostgresTestSupport.POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", () -> "ledger_user");
        r.add("spring.datasource.password", () -> "ledger_user");
        r.add("spring.kafka.bootstrap-servers", PostgresTestSupport.KAFKA::getBootstrapServers);
    }

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void flywayRanInOwnSchema() {
        Integer applied = jdbc.queryForObject(
                "select count(*) from ledger.flyway_schema_history where success", Integer.class);
        assertThat(applied).isEqualTo(4);
    }

    @Test
    void cannotTouchOtherServiceSchema() {
        assertThatThrownBy(() -> jdbc.queryForList("select * from payments.anything"))
                .rootCause().hasMessageContaining("permission denied for schema");
    }
}
