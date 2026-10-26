package com.ledgerflow.payment;

import com.ledgerflow.common.PaymentRequested;
import com.ledgerflow.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.ledgerflow.common.MongoStore;
import tools.jackson.databind.json.JsonMapper;

/** A fresh event ID lets retries recover the stored outcome without posting twice. */
@Component
class Reconciler {

    private static final int BATCH = 500;

    private final PaymentRepository repo;
    private final JsonMapper json;
    private final MongoStore store;
    private final int staleAfterSeconds;
    private final Counter resent;

    Reconciler(PaymentRepository repo, JsonMapper json, MongoStore store, MeterRegistry meters,
               @Value("${ledgerflow.reconcile.stale-after-seconds:30}") int staleAfterSeconds) {
        this.repo = repo;
        this.json = json;
        this.store = store;
        this.staleAfterSeconds = staleAfterSeconds;
        this.resent = meters.counter("ledgerflow.reconcile.resent");
    }

    @Scheduled(fixedDelayString = "${ledgerflow.reconcile.interval-ms:10000}")
    void scheduled() {
        reconcile();
    }

    /** Touch each payment so it waits another stale period before retrying. */
    int reconcile() {
        Integer count = store.transaction(() -> {
            var stale = repo.lockStalePending(staleAfterSeconds, BATCH);
            for (Payment p : stale) {
                var event = PaymentRequested.of(p.id(), p.payerAccountId(), p.payeeAccountId(), p.amountMinor());
                repo.insertOutbox(p.id(), Topics.PAYMENTS_REQUESTED, String.valueOf(p.payerAccountId()),
                        json.writeValueAsString(event));
                repo.touch(p.id());
            }
            return stale.size();
        });
        resent.increment(count);
        return count;
    }
}
