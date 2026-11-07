package com.ledgerflow.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/** Nothing configured: the topology of every earlier phase (3 partitions per topic, 3 consumer threads), see ADR 0005. */
class PaymentConfigDefaultsTest extends PaymentTestSupport {

    @Autowired
    List<NewTopic> topics;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Test
    void withoutConfigurationTopicsHaveThreePartitionsAndTheConsumerHasThreeThreads() {
        assertThat(topics).hasSize(4).allSatisfy(t -> assertThat(t.numPartitions()).as(t.name()).isEqualTo(3));
        assertThat(registry.getListenerContainers()).hasSize(1).allSatisfy(c ->
                assertThat(((ConcurrentMessageListenerContainer<?, ?>) c).getConcurrency()).isEqualTo(3));
    }
}
