package com.ledgerflow.payment;

import java.nio.file.Path;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * One real Postgres 17 and one application context shared by all payment tests in the JVM
 * (identical annotations keep Spring's context cache hit). Tests use their own account ids.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "ledgerflow.risk.blocked-accounts=666")
abstract class PaymentTestSupport {

    static final long BLOCKED_ACCOUNT = 666;

    // Same init script as docker-compose: roles/schemas have a single source of truth.
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17")
            .withDatabaseName("ledgerflow")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("../infra/postgres/init/01-roles-schemas.sql")),
                    "/docker-entrypoint-initdb.d/01-roles-schemas.sql");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", () -> "payment_user");
        r.add("spring.datasource.password", () -> "payment_user");
    }
}
