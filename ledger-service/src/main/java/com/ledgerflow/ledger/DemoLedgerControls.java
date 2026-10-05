package com.ledgerflow.ledger;

import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Demo profile only: application-level processing controls for the Resilience Lab. "Paused" pauses
 * this service's Kafka listener (messages wait in Kafka; the JVM keeps running); "delay" makes the
 * listener wait before processing each message. Nothing here touches Docker, processes or SQL.
 */
@RestController
@RequestMapping("/demo/controls")
@Profile("demo")
class DemoLedgerControls {

    static final long MAX_DELAY_MS = 10_000;

    record Controls(boolean ledgerPaused, long delayMs) {}

    private final KafkaListenerEndpointRegistry listeners;
    private volatile long delayMs;

    DemoLedgerControls(KafkaListenerEndpointRegistry listeners) {
        this.listeners = listeners;
    }

    @GetMapping
    Controls get() {
        boolean paused = !listeners.getListenerContainers().isEmpty()
                && listeners.getListenerContainers().stream().allMatch(c -> c.isPauseRequested());
        return new Controls(paused, delayMs);
    }

    @PostMapping
    Controls set(@RequestBody Controls wanted) {
        if (wanted.delayMs() < 0 || wanted.delayMs() > MAX_DELAY_MS) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "delayMs must be between 0 and " + MAX_DELAY_MS);
        }
        delayMs = wanted.delayMs();
        listeners.getListenerContainers().forEach(c -> {
            if (wanted.ledgerPaused()) {
                c.pause();
            } else {
                c.resume();
            }
        });
        return get();
    }

    /** Called by the listener before processing each message: a real backend delay (demo only). */
    void beforeProcessing() {
        long d = delayMs;
        if (d > 0) {
            try {
                Thread.sleep(d);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
