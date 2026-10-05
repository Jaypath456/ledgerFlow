package com.ledgerflow.payment;

import com.ledgerflow.common.FaultInjector;
import com.ledgerflow.common.PaymentRequested;
import com.ledgerflow.common.Topics;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.json.JsonMapper;

@Service
public class PaymentService {

    /** The exact HTTP response (status + JSON body) for this key; replays return the stored one verbatim. */
    public record Result(int status, String body, boolean replayed) {}

    private final PaymentRepository repo;
    private final List<RiskRule> riskRules;
    private final JsonMapper json;
    private final ObjectProvider<FaultInjector> faults;

    PaymentService(PaymentRepository repo, List<RiskRule> riskRules, JsonMapper json,
                   ObjectProvider<FaultInjector> faults) {
        this.repo = repo;
        this.riskRules = riskRules;
        this.json = json;
        this.faults = faults;
    }

    /**
     * One transaction: idempotency key, payment and (if accepted) the PaymentRequested outbox row
     * commit together or not at all. Nothing is sent to Kafka here.
     */
    @Transactional
    public Result create(String idempotencyKey, PaymentRequest request) {
        String hash = request.canonicalHash();
        UUID id = UUID.randomUUID();

        if (!repo.insertKey(idempotencyKey, hash, id)) {
            var stored = repo.findKey(idempotencyKey).orElseThrow();
            if (!stored.requestHash().equals(hash)) {
                throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_CONTENT,
                        "Idempotency-Key was already used with a different request");
            }
            return new Result(stored.responseStatus(), stored.responseBody(), true);
        }

        Optional<String> declineReason = riskRules.stream()
                .map(rule -> rule.check(request))
                .flatMap(Optional::stream)
                .findFirst();
        int status;
        if (declineReason.isPresent()) {
            repo.insertPayment(id, request, PaymentStatus.DECLINED, declineReason.get());
            status = HttpStatus.CREATED.value();
        } else {
            repo.insertPayment(id, request, PaymentStatus.PENDING_LEDGER, null);
            var event = PaymentRequested.of(id, request.payerAccountId(), request.payeeAccountId(), request.amountMinor());
            repo.insertOutbox(id, Topics.PAYMENTS_REQUESTED, String.valueOf(request.payerAccountId()),
                    json.writeValueAsString(event));
            faults.ifAvailable(f -> f.hit(FaultInjector.Point.BEFORE_PAYMENT_OUTBOX_COMMIT));
            status = HttpStatus.ACCEPTED.value();
        }
        String body = json.writeValueAsString(repo.find(id).orElseThrow());
        repo.storeResponse(idempotencyKey, status, body);
        return new Result(status, body, false);
    }

    public Optional<Payment> find(UUID id) {
        return repo.find(id);
    }
}
