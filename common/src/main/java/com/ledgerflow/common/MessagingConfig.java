package com.ledgerflow.common;

import java.util.Set;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

/** Kafka wiring shared by both services; each imports it. */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class MessagingConfig {

    @Bean
    NewTopic paymentsRequestedTopic() {
        return new NewTopic(Topics.PAYMENTS_REQUESTED, Topics.PARTITIONS, (short) 1);
    }

    @Bean
    NewTopic ledgerResultsTopic() {
        return new NewTopic(Topics.LEDGER_RESULTS, Topics.PARTITIONS, (short) 1);
    }

    @Bean
    NewTopic paymentsRequestedDlt() {
        return new NewTopic(Topics.PAYMENTS_REQUESTED + Topics.DLT_SUFFIX, Topics.PARTITIONS, (short) 1);
    }

    @Bean
    NewTopic ledgerResultsDlt() {
        return new NewTopic(Topics.LEDGER_RESULTS + Topics.DLT_SUFFIX, Topics.PARTITIONS, (short) 1);
    }

    /** 3 processing attempts (1 + 2 retries, 500 ms apart), then the record goes to {@code <topic>-dlt}. */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka) {
        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafka), new FixedBackOff(500L, 2L));
    }

    @Bean
    OutboxRelay outboxRelay(JdbcClient jdbc, PlatformTransactionManager tm, KafkaTemplate<String, String> kafka,
                            ObjectProvider<FaultInjector> faults) {
        return new OutboxRelay(jdbc, tm, kafka, faults);
    }

    @Bean
    @Profile("chaos")
    FaultInjector faultInjector(
            @Value("${ledgerflow.chaos.faults:}") Set<FaultInjector.Point> points,
            @Value("${ledgerflow.chaos.probability:0.01}") double probability,
            @Value("${ledgerflow.chaos.action:halt}") String action) {
        return new FaultInjector(points, probability, action.equals("halt"));
    }
}
