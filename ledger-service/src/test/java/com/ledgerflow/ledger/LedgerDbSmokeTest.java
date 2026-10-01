package com.ledgerflow.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

@SpringBootTest
@Testcontainers
class LedgerDbSmokeTest {

    // Same init script as docker-compose: roles/schemas have a single source of truth.
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17")
            .withDatabaseName("ledgerflow")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("../infra/postgres/init/01-roles-schemas.sql")),
                    "/docker-entrypoint-initdb.d/01-roles-schemas.sql");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", () -> "ledger_user");
        r.add("spring.datasource.password", () -> "ledger_user");
    }

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void flywayRanInOwnSchema() {
        Integer applied = jdbc.queryForObject(
                "select count(*) from ledger.flyway_schema_history where success", Integer.class);
        assertThat(applied).isEqualTo(1);
    }

    @Test
    void cannotTouchOtherServiceSchema() {
        assertThatThrownBy(() -> jdbc.queryForList("select * from payments.anything"))
                .rootCause().hasMessageContaining("permission denied for schema");
    }
}
