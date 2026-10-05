package com.ledgerflow.ledger;

import static com.ledgerflow.ledger.PostgresTestSupport.balance;
import static com.ledgerflow.ledger.PostgresTestSupport.context;
import static com.ledgerflow.ledger.PostgresTestSupport.count;
import static com.ledgerflow.ledger.PostgresTestSupport.fundedCustomer;
import static com.ledgerflow.ledger.PostgresTestSupport.createAccount;
import static com.ledgerflow.ledger.PostgresTestSupport.jdbc;
import static com.ledgerflow.ledger.PostgresTestSupport.kafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.ledgerflow.common.PaymentRequested;
import com.ledgerflow.common.Topics;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.json.JsonMapper;

/** Demo profile: named accounts (Jay/Ajay starter accounts, user-created accounts) and processing controls. */
class DemoNamedAccountsTest {

    private final DemoNamedAccounts named = context().getBean(DemoNamedAccounts.class);
    private final DemoLedgerControls controls = context().getBean(DemoLedgerControls.class);

    @AfterEach
    void invariantsHoldAndFaultsCleared() {
        controls.set(new DemoLedgerControls.Controls(false, 0));
        LedgerInvariants.assertAll();
    }

    private static String unique(String base) {
        return base + UUID.randomUUID().toString().substring(0, 6);
    }

    @Test
    void starterAccountsJayAndAjayAreCreatedWithTheirBalances() {
        var byName = named.list().stream().collect(java.util.stream.Collectors.toMap(DemoNamedAccounts.NamedAccount::name, a -> a));
        assertThat(byName.get("Jay").balanceMinor()).isEqualTo(7_500);
        assertThat(byName.get("Ajay").balanceMinor()).isEqualTo(10_000);
        // real ledger accounts: the cached balance matches a real funding entry
        assertThat(balance(byName.get("Jay").id())).isEqualTo(7_500);
        assertThat(jdbc().sql("SELECT sum(amount_minor) FROM ledger_entries WHERE account_id = ?").param(byName.get("Ajay").id())
                .query(Long.class).single()).isEqualTo(10_000);
    }

    @Test
    void createdAccountIsFundedWithOneBalancedPostingFromItsOwnTreasury() {
        var created = named.create(new DemoNamedAccounts.CreateRequest(unique("Jaysus"), 10_000));

        assertThat(balance(created.id())).isEqualTo(10_000);
        long treasury = jdbc().sql("SELECT treasury_account_id FROM demo_account_names WHERE account_id = ?").param(created.id())
                .query(Long.class).single();
        assertThat(balance(treasury)).isEqualTo(-10_000);
        long txId = jdbc().sql("SELECT transaction_id FROM ledger_entries WHERE account_id = ?").param(created.id()).query(Long.class).single();
        assertThat(jdbc().sql("SELECT sum(amount_minor) FROM ledger_entries WHERE transaction_id = ?").param(txId).query(Long.class).single())
                .isZero();
        assertThat(jdbc().sql("SELECT type FROM accounts WHERE id = ?").param(created.id()).query(String.class).single()).isEqualTo("CUSTOMER");
    }

    @Test
    void namesPersistAndAreUniqueIgnoringCase() {
        String name = unique("Alice");
        var created = named.create(new DemoNamedAccounts.CreateRequest(name, 500));

        assertThat(named.list()).anySatisfy(a -> {
            assertThat(a.name()).isEqualTo(name);
            assertThat(a.id()).isEqualTo(created.id());
            assertThat(a.balanceMinor()).isEqualTo(500);
        });
        long accountsBefore = count("accounts");
        assertThatThrownBy(() -> named.create(new DemoNamedAccounts.CreateRequest(name.toUpperCase(), 1)))
                .isInstanceOf(ResponseStatusException.class).hasMessageContaining("409");
        assertThat(count("accounts")).isEqualTo(accountsBefore); // rolled back: no treasury/account left behind
    }

    @Test
    void rejectsInvalidNamesAndBalancesAndCreatesNothing() {
        long accountsBefore = count("accounts");
        for (var bad : new DemoNamedAccounts.CreateRequest[] {
                new DemoNamedAccounts.CreateRequest("", 100), new DemoNamedAccounts.CreateRequest("1abc", 100),
                new DemoNamedAccounts.CreateRequest("x".repeat(25), 100), new DemoNamedAccounts.CreateRequest("<script>", 100),
                new DemoNamedAccounts.CreateRequest("Valid", -1),
                new DemoNamedAccounts.CreateRequest("Valid", DemoNamedAccounts.MAX_BALANCE_MINOR + 1)}) {
            assertThatThrownBy(() -> named.create(bad)).isInstanceOf(ResponseStatusException.class).hasMessageContaining("400");
        }
        assertThat(count("accounts")).isEqualTo(accountsBefore);
    }

    @Test
    void settingBalancesMovesMoneyWithBalancedPostingsOnlyForNamedAccounts() {
        String name = unique("Bob");
        var bob = named.create(new DemoNamedAccounts.CreateRequest(name, 1_000));
        long entriesBefore = count("ledger_entries");

        named.setBalances(Map.of(name, 2_500L));
        assertThat(balance(bob.id())).isEqualTo(2_500);
        named.setBalances(Map.of(name, 0L));
        assertThat(balance(bob.id())).isZero();
        assertThat(count("ledger_entries")).isEqualTo(entriesBefore + 4); // two balanced postings, no direct writes

        assertThatThrownBy(() -> named.setBalances(Map.of("NoSuchPerson", 1L))).hasMessageContaining("400");
        assertThatThrownBy(() -> named.setBalances(Map.of(name, -1L))).hasMessageContaining("400");
    }

    @Test
    void processingDelayIsARealBackendDelay() {
        var listener = context().getBean(PaymentRequestListener.class);
        var json = context().getBean(JsonMapper.class);
        var event = PaymentRequested.of(UUID.randomUUID(), fundedCustomer(1_000), createAccount(AccountType.MERCHANT), 100);
        controls.set(new DemoLedgerControls.Controls(false, 400));

        long t0 = System.nanoTime();
        listener.onMessage(json.writeValueAsString(event));
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

        assertThat(elapsedMs).isGreaterThanOrEqualTo(400);
        assertThat(jdbc().sql("SELECT count(*) FROM ledger_transactions WHERE payment_id = ?").param(event.paymentId())
                .query(Long.class).single()).isEqualTo(1);
        assertThatThrownBy(() -> controls.set(new DemoLedgerControls.Controls(false, DemoLedgerControls.MAX_DELAY_MS + 1)))
                .hasMessageContaining("400");
    }

    @Test
    void pausedLedgerProcessingHoldsMessagesUntilResumed() {
        var json = context().getBean(JsonMapper.class);
        var registry = context().getBean(KafkaListenerEndpointRegistry.class);
        var event = PaymentRequested.of(UUID.randomUUID(), fundedCustomer(1_000), createAccount(AccountType.MERCHANT), 100);

        assertThat(controls.set(new DemoLedgerControls.Controls(true, 0)).ledgerPaused()).isTrue();
        assertThat(registry.getListenerContainers()).allSatisfy(c -> assertThat(c.isPauseRequested()).isTrue());
        kafka().send(Topics.PAYMENTS_REQUESTED, String.valueOf(event.payerAccountId()), json.writeValueAsString(event));

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).until(() -> outcome(event.paymentId()) == 0);

        assertThat(controls.set(new DemoLedgerControls.Controls(false, 0)).ledgerPaused()).isFalse();
        await().atMost(Duration.ofSeconds(30)).until(() -> outcome(event.paymentId()) == 1);
        assertThat(jdbc().sql("SELECT count(*) FROM ledger_transactions WHERE payment_id = ?").param(event.paymentId())
                .query(Long.class).single()).isEqualTo(1);
    }

    private static long outcome(UUID paymentId) {
        return jdbc().sql("SELECT count(*) FROM payment_outcomes WHERE payment_id = ?").param(paymentId).query(Long.class).single();
    }
}
