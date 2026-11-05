package com.ledgerflow.payment;

import java.util.List;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/** Local CLI identities; account ownership is configured here, never supplied by the caller. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PaymentSecurity.Identities.class)
public class PaymentSecurity {
    @ConfigurationProperties("ledgerflow.auth")
    public record Identities(List<Identity> users) {}
    public record Identity(String username, String password, Set<Long> accounts) {}

    @Bean
    InMemoryUserDetailsManager users(Identities identities) {
        var users = new InMemoryUserDetailsManager();
        var encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
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
