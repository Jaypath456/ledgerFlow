package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.PostgresTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

class LedgerPropertyTest {

    private static final int ACCOUNTS = 4;
    private static final long FUNDING = 1_000;

    record Op(int payer, int payee, long amount) {}

    @Provide
    Arbitrary<List<Op>> ops() {
        Arbitrary<Op> op = Arbitraries.integers().between(0, ACCOUNTS - 1).flatMap(payer ->
                Arbitraries.integers().between(0, ACCOUNTS - 1).flatMap(payee ->
                        Arbitraries.longs().between(1, 1_500).map(amount -> new Op(payer, payee, amount))));
        return op.list().ofMinSize(1).ofMaxSize(30);
    }

    @Property(tries = 100)
    void randomPostingsPreserveInvariants(@ForAll("ops") List<Op> ops) {
        long[] ids = new long[ACCOUNTS];
        for (int i = 0; i < ACCOUNTS; i++) {
            ids[i] = fundedCustomer(FUNDING);
        }

        for (Op op : ops) {
            try {
                ledger().post(UUID.randomUUID(), ids[op.payer()], ids[op.payee()], op.amount());
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
