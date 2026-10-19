package com.ledgerflow.ledger;

import com.ledgerflow.common.MessagingConfig;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(MessagingConfig.class)
public class LedgerServiceApplication {

    /** Separate config names let both services share a test classpath. */
    public static final String CONFIG_NAME = "spring.config.name=ledger-service";

    public static void main(String[] args) {
        new SpringApplicationBuilder(LedgerServiceApplication.class).properties(CONFIG_NAME).run(args);
    }
}
