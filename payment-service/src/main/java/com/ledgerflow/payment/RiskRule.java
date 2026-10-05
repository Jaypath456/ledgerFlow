package com.ledgerflow.payment;

import java.util.Optional;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.simple.JdbcClient;
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

    // ponytail: soft limit, concurrent requests from one payer can each see the same count;
    // a per-payer advisory lock would make it strict.
    @Component
    @Order(3)
    class Velocity implements RiskRule {
        private final JdbcClient jdbc;
        private final int maxPerMinute;

        Velocity(JdbcClient jdbc, @Value("${ledgerflow.risk.velocity-max:5}") int maxPerMinute) {
            this.jdbc = jdbc;
            this.maxPerMinute = maxPerMinute;
        }

        @Override
        public Optional<String> check(PaymentRequest r) {
            long recent = jdbc.sql("""
                            SELECT count(*) FROM payments
                            WHERE payer_account_id = ? AND status <> 'DECLINED'
                              AND created_at > now() - interval '60 seconds'""")
                    .param(r.payerAccountId()).query(Long.class).single();
            return recent >= maxPerMinute ? Optional.of("VELOCITY_LIMIT") : Optional.empty();
        }
    }
}
