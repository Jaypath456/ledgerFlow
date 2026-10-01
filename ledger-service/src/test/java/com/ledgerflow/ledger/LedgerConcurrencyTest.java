package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.PostgresTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class LedgerConcurrencyTest {

    private static final int THREADS = 32;

    @AfterEach
    void invariants() {
        LedgerInvariants.assertAll();
    }

    /** Runs all tasks at once and returns each outcome: null on success, else the thrown exception. */
    private static List<Throwable> runConcurrently(List<Runnable> tasks) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch go = new CountDownLatch(1);
        try {
            List<Future<Throwable>> futures = new ArrayList<>();
            for (Runnable task : tasks) {
                Callable<Throwable> c = () -> {
                    go.await();
                    try {
                        task.run();
                        return null;
                    } catch (Throwable t) {
                        return t;
                    }
                };
                futures.add(pool.submit(c));
            }
            go.countDown();
            List<Throwable> outcomes = new ArrayList<>();
            for (Future<Throwable> f : futures) {
                outcomes.add(f.get(60, TimeUnit.SECONDS));
            }
            return outcomes;
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void exactlyFundedNumberOfPaymentsSucceed() throws Exception {
        long payer = fundedCustomer(50 * 100);
        long payee = createAccount(AccountType.MERCHANT);
        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            tasks.add(() -> ledger().post(UUID.randomUUID(), payer, payee, 100));
        }

        List<Throwable> outcomes = runConcurrently(tasks);

        long succeeded = outcomes.stream().filter(o -> o == null).count();
        long insufficient = outcomes.stream().filter(o -> o instanceof LedgerException.InsufficientFunds).count();
        assertThat(succeeded).isEqualTo(50);
        assertThat(insufficient).isEqualTo(150); // anything else (deadlock, SQL error...) fails here
        assertThat(balance(payer)).isZero();
        assertThat(balance(payee)).isEqualTo(5_000);
    }

    @Test
    void oppositeDirectionPostingsDoNotDeadlock() throws Exception {
        long a = fundedCustomer(1_000_000);
        long b = fundedCustomer(1_000_000);
        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            long from = i % 2 == 0 ? a : b;
            long to = i % 2 == 0 ? b : a;
            tasks.add(() -> ledger().post(UUID.randomUUID(), from, to, 10));
        }

        List<Throwable> outcomes = runConcurrently(tasks);

        assertThat(outcomes).containsOnlyNulls();
        assertThat(balance(a) + balance(b)).isEqualTo(2_000_000);
    }

    @Test
    void samePaymentIdConcurrentlyPostsOnce() throws Exception {
        long payer = fundedCustomer(1_000);
        long payee = createAccount(AccountType.MERCHANT);
        UUID paymentId = UUID.randomUUID();
        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            tasks.add(() -> ledger().post(paymentId, payer, payee, 100));
        }

        List<Throwable> outcomes = runConcurrently(tasks);

        assertThat(outcomes.stream().filter(o -> o == null).count()).isEqualTo(1);
        assertThat(outcomes.stream().filter(o -> o instanceof LedgerException.DuplicatePayment).count())
                .isEqualTo(15);
        assertThat(balance(payer)).isEqualTo(900);
    }
}
