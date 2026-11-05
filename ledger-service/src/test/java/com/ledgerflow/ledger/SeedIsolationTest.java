package com.ledgerflow.ledger;

import org.junit.jupiter.api.Test;

/**
 * Regression: LedgerPostingTest#seedIsBalancedAndFunded failed on CI (-2181000 instead of -175000)
 * because other test classes ran first and funded their accounts from seeded SYSTEM account 1 in
 * the shared container. Here state-mutating ledger tests always run first, in-process, and the
 * seed check must still pass.
 */
class SeedIsolationTest {

    @Test
    void postingTestSeedCheckPassesAfterStateMutatingLedgerTests() throws Exception {
        var concurrency = new LedgerConcurrencyTest();
        concurrency.exactlyFundedNumberOfPaymentsSucceed();
        concurrency.oppositeDirectionPostingsDoNotDeadlock();
        new LedgerPropertyTest().randomPostingsPreserveInvariants();
        LedgerKafkaTest.start();
        new LedgerKafkaTest().fundedPaymentIsPostedAndLedgerPostedIsPublished();

        new LedgerPostingTest().seedIsBalancedAndFunded();
        PostgresTestSupport.assertSeedBaseline();
    }
}
