package com.ledgerflow.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Own context with a separate management port, as in infra/docker-compose.observability.yml. Its listeners and relay stay
 * idle so it never competes with the shared payment context for records or outbox rows, and it is closed after the class.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {PaymentServiceApplication.CONFIG_NAME, "ledgerflow.risk.blocked-accounts=666",
                "management.server.port=0", "spring.kafka.listener.auto-startup=false",
                "ledgerflow.outbox.poll-ms=3600000", "ledgerflow.reconcile.interval-ms=3600000"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PaymentManagementPortTest extends PaymentTestSupport {

    private final HttpClient http = HttpClient.newHttpClient();

    @LocalServerPort
    int apiPort;

    @LocalManagementPort
    int managementPort;

    @Autowired
    InMemoryUserDetailsManager users;

    @Test
    void prometheusOnManagementPortNeedsNoCredentials() throws Exception {
        assertThat(managementPort).isNotEqualTo(apiPort);
        var scrape = get(managementPort, "/actuator/prometheus", null);
        assertThat(scrape.statusCode()).isEqualTo(200);
        assertThat(scrape.body()).contains("# HELP").contains("application=\"payment-service\"");
        assertThat(get(managementPort, "/actuator/health", null).statusCode()).isEqualTo(200);
    }

    @Test
    void prometheusOnApiPortStillRequiresAuthentication() throws Exception {
        assertThat(get(apiPort, "/actuator/prometheus", null).statusCode()).isEqualTo(401);
        assertThat(get(apiPort, "/actuator/prometheus", "Basic " + Base64.getEncoder()
                .encodeToString("nobody:wrong".getBytes(StandardCharsets.UTF_8))).statusCode()).isEqualTo(401);
        // Authenticated callers get no actuator endpoints on the API port either: they all moved to the management port.
        assertThat(get(apiPort, "/actuator/prometheus", authorization(2)).statusCode()).isEqualTo(404);
        assertThat(get(apiPort, "/api/payments/" + java.util.UUID.randomUUID(), null).statusCode()).isEqualTo(401);
    }

    @Test
    void managementPortServesOnlyHealthAndPrometheus() throws Exception {
        for (String path : new String[] {"/actuator", "/actuator/metrics", "/actuator/metrics/ledgerflow.events.duplicate",
                "/api/payments/" + java.util.UUID.randomUUID()}) {
            assertThat(get(managementPort, path, null).statusCode()).as(path).isEqualTo(403);
            assertThat(get(managementPort, path, authorization(2)).statusCode()).as(path).isEqualTo(403);
        }
    }

    private HttpResponse<String> get(int port, String path, String authorization) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET();
        if (authorization != null) request.header("Authorization", authorization);
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private String authorization(long account) {
        String name = "mgmt-test-" + account;
        if (!users.userExists(name)) users.createUser(User.withUsername(name).password("{noop}test-only")
                .authorities(PaymentSecurity.accountAuthority(account)).build());
        return "Basic " + Base64.getEncoder().encodeToString((name + ":test-only").getBytes(StandardCharsets.UTF_8));
    }
}
