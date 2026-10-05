package com.ledgerflow.payment;

import com.ledgerflow.common.MessagingConfig;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(MessagingConfig.class)
public class PaymentServiceApplication {

    /** Config file is payment-service.yml, so both services' configs can share one classpath (e2e-tests). */
    public static final String CONFIG_NAME = "spring.config.name=payment-service";

    public static void main(String[] args) {
        new SpringApplicationBuilder(PaymentServiceApplication.class).properties(CONFIG_NAME).run(args);
    }
}
