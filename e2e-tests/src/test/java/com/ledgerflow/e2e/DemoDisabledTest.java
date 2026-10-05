package com.ledgerflow.e2e;

import static com.ledgerflow.e2e.Stack.account;
import static com.ledgerflow.e2e.Stack.pay;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** The default profile (as used by `docker compose -f infra/docker-compose.yml up`) has no demo surface. */
class DemoDisabledTest {

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void noDashboardAndNoDemoEndpointsOnEitherService() throws Exception {
        String payment = "http://localhost:" + Stack.PAYMENT.getEnvironment().getProperty("local.server.port");
        String ledger = "http://localhost:" + Stack.LEDGER.getEnvironment().getProperty("local.server.port");

        assertThat(get(payment + "/").statusCode()).isEqualTo(404);
        assertThat(get(payment + "/index.html").statusCode()).isEqualTo(404);
        assertThat(get(payment + "/demo/invariants").statusCode()).isEqualTo(404);
        assertThat(get(payment + "/demo/config").statusCode()).isEqualTo(404);
        assertThat(post(payment + "/demo/burst").statusCode()).isIn(404, 405);
        assertThat(get(ledger + "/demo/invariants").statusCode()).isEqualTo(404);
        assertThat(get(ledger + "/demo/accounts?ids=1").statusCode()).isEqualTo(404);
        assertThat(post(ledger + "/demo/accounts").statusCode()).isIn(404, 405);

        // no cross-origin access granted in the default profile
        var health = http.send(HttpRequest.newBuilder(URI.create(ledger + "/actuator/health"))
                .header("Origin", "http://localhost:8081").GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(health.headers().firstValue("Access-Control-Allow-Origin")).isEmpty();
    }

    @Test
    void defaultRiskPolicyStillAppliesTheVelocityLimit() throws Exception {
        long payer = account("CUSTOMER", 10_000);
        long payee = account("MERCHANT", 0);
        for (int i = 0; i < 5; i++) {
            assertThat(pay(UUID.randomUUID().toString(), payer, payee, 10).status()).isEqualTo(202);
        }
        var sixth = pay(UUID.randomUUID().toString(), payer, payee, 10);
        assertThat(sixth.status()).isEqualTo(201);
        assertThat(sixth.body().get("declineReason").asString()).isEqualTo("VELOCITY_LIMIT");
    }

    private HttpResponse<String> get(String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String url) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(url)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("[]")).build(), HttpResponse.BodyHandlers.ofString());
    }
}
