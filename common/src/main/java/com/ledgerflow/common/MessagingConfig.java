package com.ledgerflow.common;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
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

@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class MessagingConfig {

    // Partitions per topic: 3 unless ledgerflow.topics.partitions says otherwise (ADR 0005). The dead-letter topics need as many partitions as the
    // topic they back, because the recoverer publishes to the same partition number. Existing topics only ever grow.
    @Bean
    NewTopic paymentsRequestedTopic(@Value("${ledgerflow.topics.partitions:" + Topics.PARTITIONS + "}") int partitions) {
        return new NewTopic(Topics.PAYMENTS_REQUESTED, partitions, (short) 1);
    }

    @Bean
    NewTopic ledgerResultsTopic(@Value("${ledgerflow.topics.partitions:" + Topics.PARTITIONS + "}") int partitions) {
        return new NewTopic(Topics.LEDGER_RESULTS, partitions, (short) 1);
    }

    @Bean
    NewTopic paymentsRequestedDlt(@Value("${ledgerflow.topics.partitions:" + Topics.PARTITIONS + "}") int partitions) {
        return new NewTopic(Topics.PAYMENTS_REQUESTED + Topics.DLT_SUFFIX, partitions, (short) 1);
    }

    @Bean
    NewTopic ledgerResultsDlt(@Value("${ledgerflow.topics.partitions:" + Topics.PARTITIONS + "}") int partitions) {
        return new NewTopic(Topics.LEDGER_RESULTS + Topics.DLT_SUFFIX, partitions, (short) 1);
    }

    /** Send records to their dead-letter partition after three failed attempts. */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka) {
        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafka), new FixedBackOff(500L, 2L));
    }

    @Bean
    OutboxRelay outboxRelay(JdbcClient jdbc, PlatformTransactionManager tm, KafkaTemplate<String, String> kafka,
                            ObjectProvider<FaultInjector> faults, ObjectProvider<Tracer> tracer,
                            ObjectProvider<Propagator> propagator) {
        return new OutboxRelay(jdbc, tm, kafka, faults, tracer, propagator);
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
