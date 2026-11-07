package com.ledgerflow.common;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/** Metrics and tracing beans shared by both services; only com.ledgerflow.common.observability is scanned. */
@Configuration(proxyBeanMethods = false)
@ComponentScan("com.ledgerflow.common.observability")
public class ObservabilityConfig {

    // The outbox relay polls every 10 ms: one trace per poll would bury the payment traces.
    @Bean
    ObservationPredicate noScheduledTaskObservations() {
        return (name, context) -> !"tasks.scheduled.execution".equals(name);
    }
}
