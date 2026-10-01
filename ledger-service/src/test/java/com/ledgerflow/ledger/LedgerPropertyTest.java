package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.PostgresTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Randomized posting sequences with a fixed seed; sequence i uses {@code new Random(SEED + i)} so it can be replayed alone. */
class LedgerPropertyTest {

    static final long SEED = 20260101L;
    private static final int SEQUENCES = 100;
    private static final int ACCOUNTS = 4;
    private static final long FUNDING = 1_000;

    @Test
    void randomPostingsPreserveInvariants() {
        for (int seq = 0; seq < SEQUENCES; seq++) {
            try {
                runSequence(new Random(SEED + seq));
            } catch (AssertionError | RuntimeException e) {
                throw new AssertionError("seed=" + SEED + " sequence=" + seq + " (replay with new Random(" + (SEED + seq) + ")): " + e.getMessage(), e);
            }
        }
    }

    private void runSequence(Random rnd) {
        long[] ids = new long[ACCOUNTS];
        for (int i = 0; i < ACCOUNTS; i++) {
            ids[i] = fundedCustomer(FUNDING);
        }

        int ops = 1 + rnd.nextInt(30);
        for (int i = 0; i < ops; i++) {
            long amount = 1 + rnd.nextInt(1_500);
            try {
                ledger().post(UUID.randomUUID(), ids[rnd.nextInt(ACCOUNTS)], ids[rnd.nextInt(ACCOUNTS)], amount);
            } catch (LedgerException expected) {
                // overdrafts and self-payments are rejected; the ledger must stay consistent anyway
            }
        }

        long total = 0;
        for (long id : ids) {
            assertThat(balance(id)).isGreaterThanOrEqualTo(0);
            total += balance(id);
        }
        assertThat(total).isEqualTo(ACCOUNTS * FUNDING); // money only moves between these accounts
        LedgerInvariants.assertAll(); // I1, I2, I4, I5
    }
}
