package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.PostgresTestSupport.context;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/** Nothing configured: the topology of every earlier phase (3 partitions per topic, 3 consumer threads), see ADR 0005. */
class LedgerConfigDefaultsTest {

    @Test
    void withoutConfigurationTopicsHaveThreePartitionsAndTheConsumerHasThreeThreads() {
        var topics = context().getBeansOfType(NewTopic.class).values();
        assertThat(topics).hasSize(4).allSatisfy(t -> assertThat(t.numPartitions()).as(t.name()).isEqualTo(3));
        var containers = context().getBean(KafkaListenerEndpointRegistry.class).getListenerContainers();
        assertThat(containers).hasSize(1).allSatisfy(c ->
                assertThat(((ConcurrentMessageListenerContainer<?, ?>) c).getConcurrency()).isEqualTo(3));
    }
}
