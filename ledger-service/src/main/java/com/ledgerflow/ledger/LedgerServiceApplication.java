package com.ledgerflow.ledger;

import com.ledgerflow.common.MessagingConfig;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(MessagingConfig.class)
public class LedgerServiceApplication {

    /** Config file is ledger-service.yml, so both services' configs can share one classpath (e2e-tests). */
    public static final String CONFIG_NAME = "spring.config.name=ledger-service";

    public static void main(String[] args) {
        new SpringApplicationBuilder(LedgerServiceApplication.class).properties(CONFIG_NAME).run(args);
    }
}
