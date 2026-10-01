package com.ledgerflow.ledger;

import java.nio.file.Path;
import java.util.UUID;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * One real Postgres 17 and one real application context shared by all ledger tests in the JVM
 * (JUnit and jqwik alike). Tests create their own accounts, so they don't interfere.
 */
final class PostgresTestSupport {

    static final long SYSTEM_ACCOUNT = 1;

    // Same init script as docker-compose: roles/schemas have a single source of truth.
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17")
            .withDatabaseName("ledgerflow")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("../infra/postgres/init/01-roles-schemas.sql")),
                    "/docker-entrypoint-initdb.d/01-roles-schemas.sql");

    private static ConfigurableApplicationContext context;

    static {
        POSTGRES.start();
    }

    private PostgresTestSupport() {}

    static synchronized ConfigurableApplicationContext context() {
        if (context == null) {
            context = new SpringApplicationBuilder(LedgerServiceApplication.class)
                    .web(org.springframework.boot.WebApplicationType.NONE)
                    .run(
                            "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                            "--spring.datasource.username=ledger_user",
                            "--spring.datasource.password=ledger_user",
                            "--spring.datasource.hikari.maximum-pool-size=40");
        }
        return context;
    }

    static LedgerService ledger() {
        return context().getBean(LedgerService.class);
    }

    static JdbcClient jdbc() {
        return context().getBean(JdbcClient.class);
    }

    static long createAccount(AccountType type) {
        return jdbc().sql("INSERT INTO accounts (type) VALUES (?) RETURNING id")
                .param(type.name()).query(Long.class).single();
    }

    /** Creates a customer account funded from the SYSTEM account through a normal posting. */
    static long fundedCustomer(long amountMinor) {
        long id = createAccount(AccountType.CUSTOMER);
        if (amountMinor > 0) {
            ledger().post(UUID.randomUUID(), SYSTEM_ACCOUNT, id, amountMinor);
        }
        return id;
    }

    static long balance(long accountId) {
        return jdbc().sql("SELECT balance_minor FROM accounts WHERE id = ?")
                .param(accountId).query(Long.class).single();
    }

    static long count(String table) {
        return jdbc().sql("SELECT count(*) FROM " + table).query(Long.class).single();
    }
}
