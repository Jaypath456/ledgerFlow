package com.ledgerflow.payment;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/** Local CLI identities; account ownership is configured here, never supplied by the caller. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PaymentSecurity.Identities.class)
public class PaymentSecurity {
    /** bcryptStrength: unset in the shipped config (Spring's default, bcrypt cost 10); load and chaos runs set 4 (ADR 0003). */
    @ConfigurationProperties("ledgerflow.auth")
    public record Identities(List<Identity> users, Integer bcryptStrength) {}
    public record Identity(String username, String password, Set<Long> accounts) {}

    // One encoder hashes the identities and verifies logins, so a login never re-hashes ("upgrades") to another cost.
    @Bean
    PasswordEncoder passwordEncoder(Identities identities) {
        if (identities.bcryptStrength() == null) {
            return PasswordEncoderFactories.createDelegatingPasswordEncoder();
        }
        return new DelegatingPasswordEncoder("bcrypt", Map.of("bcrypt", new BCryptPasswordEncoder(identities.bcryptStrength())));
    }

    @Bean
    InMemoryUserDetailsManager users(Identities identities, PasswordEncoder encoder) {
        var users = new InMemoryUserDetailsManager();
        for (var identity : identities.users()) {
            if (identity.username() == null || identity.username().isBlank()
                    || identity.password() == null || identity.password().isBlank()
                    || identity.accounts() == null || identity.accounts().isEmpty()
                    || identity.accounts().stream().anyMatch(id -> id <= 0)) {
                throw new IllegalArgumentException("Each local identity requires a name, password and positive account ids");
            }
            users.createUser(User.withUsername(identity.username()).password(encoder.encode(identity.password()))
                    .authorities(identity.accounts().stream().map(PaymentSecurity::accountAuthority).toArray(String[]::new)).build());
        }
        return users;
    }

    /**
     * Only with a separate management port (infra/docker-compose.observability.yml), which is never published to the host:
     * Prometheus scrapes it without credentials on the Compose network. Requests on the API port never match this chain.
     */
    @Bean
    @Order(0)
    @ConditionalOnProperty("management.server.port")
    SecurityFilterChain managementPort(HttpSecurity http, Environment environment) throws Exception {
        return http
                .securityMatcher(request -> request.getLocalPort()
                        == environment.getProperty("local.management.port", Integer.class, -1))
                .csrf(csrf -> csrf.disable())
                .sessionManagement(sessions -> sessions.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .requestCache(cache -> cache.disable())
                .authorizeHttpRequests(requests -> requests
                        .requestMatchers("/actuator/health", "/actuator/health/**", "/actuator/prometheus").permitAll()
                        .anyRequest().denyAll())
                .build();
    }

    @Bean
    SecurityFilterChain paymentApi(HttpSecurity http) throws Exception {
        return http
                // CLI JSON API only: no cookies/sessions, CORS or form login. Browser-origin writes are denied below.
                .csrf(csrf -> csrf.disable())
                .sessionManagement(sessions -> sessions.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .requestCache(cache -> cache.disable())
                .httpBasic(Customizer.withDefaults())
                .authorizeHttpRequests(requests -> requests
                        .requestMatchers("/actuator/health", "/actuator/health/**", "/error").permitAll()
                        .requestMatchers(HttpMethod.POST, "/api/payments").access((authentication, context) -> {
                            Authentication principal = authentication.get();
                            return new AuthorizationDecision(principal.isAuthenticated()
                                    && !(principal instanceof AnonymousAuthenticationToken)
                                    && context.getRequest().getHeader("Origin") == null);
                        })
                        .anyRequest().authenticated())
                .build();
    }

    public static String accountAuthority(long id) {
        return "ACCOUNT_" + id;
    }

    static boolean owns(Authentication caller, long accountId) {
        return caller.getAuthorities().stream().anyMatch(a -> a.getAuthority().equals(accountAuthority(accountId)));
    }
}
