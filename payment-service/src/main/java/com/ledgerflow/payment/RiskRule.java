package com.ledgerflow.payment;

import java.util.Optional;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import com.ledgerflow.common.MongoStore;
import java.time.Instant;
import java.util.Date;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Component;

/** Returns a decline reason, or empty to allow. */
public interface RiskRule {

    Optional<String> check(PaymentRequest request);

    @Component
    @Order(1)
    class MaxAmount implements RiskRule {
        static final long MAX_MINOR = 1_000_000; // $10,000.00

        @Override
        public Optional<String> check(PaymentRequest r) {
            return r.amountMinor() > MAX_MINOR ? Optional.of("AMOUNT_LIMIT") : Optional.empty();
        }
    }

    @Component
    @Order(2)
    class BlockedAccount implements RiskRule {
        private final Set<Long> blocked;

        BlockedAccount(@Value("${ledgerflow.risk.blocked-accounts:}") Set<Long> blocked) {
            this.blocked = blocked;
        }

        @Override
        public Optional<String> check(PaymentRequest r) {
            return blocked.contains(r.payerAccountId()) || blocked.contains(r.payeeAccountId())
                    ? Optional.of("BLOCKED_ACCOUNT") : Optional.empty();
        }
    }

    // Concurrent requests can see the same count, so this is a soft limit.
    @Component
    @Order(3)
    class Velocity implements RiskRule {
        private final MongoStore store;
        private final int maxPerMinute;

        Velocity(MongoStore store, @Value("${ledgerflow.risk.velocity-max:5}") int maxPerMinute) {
            this.store = store;
            this.maxPerMinute = maxPerMinute;
        }

        @Override
        public Optional<String> check(PaymentRequest r) {
            long recent = store.mongo().count(Query.query(Criteria.where("payer_account_id").is(r.payerAccountId())
                    .and("status").ne("DECLINED").and("created_at").gt(Date.from(Instant.now().minusSeconds(60)))), "payments");
            return recent >= maxPerMinute ? Optional.of("VELOCITY_LIMIT") : Optional.empty();
        }
    }
}
