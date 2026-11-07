package com.ledgerflow.common.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.stereotype.Component;

/**
 * Counts records the error handler sent to a dead-letter topic. recovered() runs only after the recoverer (the
 * DeadLetterPublishingRecoverer from MessagingConfig) returned normally; retries and the recoverer itself are unchanged.
 * The counter restarts with the service; the DLT topic offsets stay the authoritative total.
 */
@Component
class DltMetrics implements RetryListener {

    private final MeterRegistry meters;

    DltMetrics(DefaultErrorHandler errorHandler, MeterRegistry meters) {
        this.meters = meters;
        errorHandler.setRetryListeners(this);
    }

    @Override
    public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
    }

    @Override
    public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
        Counter.builder("ledgerflow.dlt.published").description("Records sent to a dead-letter topic")
                .tag("topic", record.topic()).register(meters).increment();
    }
}
