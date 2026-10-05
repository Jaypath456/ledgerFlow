package com.ledgerflow.payment;

import com.ledgerflow.common.PaymentRequested;
import com.ledgerflow.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

/**
 * Recovers payments stuck in PENDING_LEDGER by asking the ledger again with a fresh eventId.
 * The ledger answers from its stored outcome if it already decided (no second posting), or
 * decides now if it never saw the request. Either way the payment converges to the ledger's result.
 */
@Component
class Reconciler {

    private static final int BATCH = 500;

    private final PaymentRepository repo;
    private final JsonMapper json;
    private final TransactionTemplate tx;
    private final int staleAfterSeconds;
    private final Counter resent;

    Reconciler(PaymentRepository repo, JsonMapper json, PlatformTransactionManager tm, MeterRegistry meters,
               @Value("${ledgerflow.reconcile.stale-after-seconds:30}") int staleAfterSeconds) {
        this.repo = repo;
        this.json = json;
        this.tx = new TransactionTemplate(tm);
        this.staleAfterSeconds = staleAfterSeconds;
        this.resent = meters.counter("ledgerflow.reconcile.resent");
    }

    @Scheduled(fixedDelayString = "${ledgerflow.reconcile.interval-ms:10000}")
    void scheduled() {
        reconcile();
    }

    /** Returns how many payments were re-requested; each is then left alone for another stale period. */
    int reconcile() {
        Integer count = tx.execute(s -> {
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
