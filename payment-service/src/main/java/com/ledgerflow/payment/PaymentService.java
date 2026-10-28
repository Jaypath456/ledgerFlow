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
import com.ledgerflow.common.MongoStore;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.json.JsonMapper;

@Service
public class PaymentService {

    /** Keep the original response so retries can replay it exactly. */
    public record Result(int status, String body, boolean replayed) {}

    private final MongoStore store;
    private final PaymentRepository repo;
    private final List<RiskRule> riskRules;
    private final JsonMapper json;
    private final ObjectProvider<FaultInjector> faults;

    PaymentService(MongoStore store, PaymentRepository repo, List<RiskRule> riskRules, JsonMapper json,
                   ObjectProvider<FaultInjector> faults) {
        this.store = store;
        this.repo = repo;
        this.riskRules = riskRules;
        this.json = json;
        this.faults = faults;
    }

    /** Commit the key, payment and outbox together so a crash can't split them. */
    public Result create(String idempotencyKey, PaymentRequest request) {
        while (true) {
            try {
                return store.transaction(() -> createNew(idempotencyKey, request));
            } catch (RiskRule.Velocity.GuardCreatedConcurrently retry) {
                // The winner created the payer guard; use a fresh transaction and snapshot.
            } catch (DuplicateKeyException duplicate) {
                if (repo.findKey(idempotencyKey).isEmpty()) throw duplicate;
                return replay(idempotencyKey, request);
            }
        }
    }

    private Result replay(String key, PaymentRequest request) {
        var stored = repo.findKey(key).orElseThrow();
        if (!stored.requestHash().equals(request.canonicalHash())) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_CONTENT,
                    "Idempotency-Key was already used with a different request");
        }
        return new Result(stored.responseStatus(), stored.responseBody(), true);
    }

    private Result createNew(String idempotencyKey, PaymentRequest request) {
        if (repo.findKey(idempotencyKey).isPresent()) return replay(idempotencyKey, request);
        UUID id = UUID.randomUUID();
        repo.insertKey(idempotencyKey, request.canonicalHash(), id);

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
