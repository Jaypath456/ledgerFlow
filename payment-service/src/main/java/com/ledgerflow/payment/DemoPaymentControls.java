package com.ledgerflow.payment;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Demo profile only: application-level controls over this service's ledger-result processing.
 * "resultsPaused" pauses the Kafka listener (results wait in Kafka; the JVM keeps running).
 * "loseResults" makes the listener discard results without recording them, as if they were lost,
 * so only the existing reconciliation can recover the payment. No Docker, process or SQL control.
 */
@RestController
@RequestMapping("/demo/controls")
@Profile("demo")
class DemoPaymentControls {

    record Controls(boolean resultsPaused, boolean loseResults, long resultsLost) {}

    /** What a caller may set (the lost-results counter is read-only). */
    record Settings(boolean resultsPaused, boolean loseResults) {}

    private final KafkaListenerEndpointRegistry listeners;
    private volatile boolean loseResults;
    private final AtomicLong lost = new AtomicLong();

    DemoPaymentControls(KafkaListenerEndpointRegistry listeners) {
        this.listeners = listeners;
    }

    @GetMapping
    Controls get() {
        boolean paused = !listeners.getListenerContainers().isEmpty()
                && listeners.getListenerContainers().stream().allMatch(c -> c.isPauseRequested());
        return new Controls(paused, loseResults, lost.get());
    }

    @PostMapping
    Controls set(@RequestBody Settings wanted) {
        loseResults = wanted.loseResults();
        listeners.getListenerContainers().forEach(c -> {
            if (wanted.resultsPaused()) {
                c.pause();
            } else {
                c.resume();
            }
        });
        return get();
    }

    /** Called by the result listener first: true means drop this result (demo "lost message"). */
    boolean discardResult() {
        if (loseResults) {
            lost.incrementAndGet();
            return true;
        }
        return false;
    }
}
