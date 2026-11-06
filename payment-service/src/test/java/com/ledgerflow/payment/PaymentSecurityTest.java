package com.ledgerflow.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;

/** Without Spring: the stored hash cost, and that a login keeps it (DaoAuthenticationProvider re-hashes on upgradeEncoding). */
class PaymentSecurityTest {

    private final PaymentSecurity security = new PaymentSecurity();

    @Test
    void shippedDefaultHashesWithBcryptCost10() {
        assertThat(loginAndStoredHash(null)).startsWith("{bcrypt}$2a$10$");
    }

    @Test
    void loadStrengthHashesWithBcryptCost4AndALoginKeepsIt() {
        assertThat(loginAndStoredHash(4)).startsWith("{bcrypt}$2a$04$");
    }

    @Test
    void strengthOutsideBcryptRangeFailsAtStartup() {
        assertThatThrownBy(() -> security.passwordEncoder(identities(3))).isInstanceOf(IllegalArgumentException.class);
    }

    private String loginAndStoredHash(Integer strength) {
        PaymentSecurity.Identities identities = identities(strength);
        PasswordEncoder encoder = security.passwordEncoder(identities);
        InMemoryUserDetailsManager users = security.users(identities, encoder);
        // Wired as Spring Boot wires it: the PasswordEncoder bean, and the user manager as UserDetailsPasswordService.
        var provider = new DaoAuthenticationProvider(users);
        provider.setPasswordEncoder(encoder);
        provider.setUserDetailsPasswordService(users);

        var caller = provider.authenticate(UsernamePasswordAuthenticationToken.unauthenticated("payer-1", "secret"));
        assertThat(PaymentSecurity.owns(caller, 1)).isTrue();
        assertThat(PaymentSecurity.owns(caller, 2)).isFalse();
        assertThatThrownBy(() -> provider.authenticate(UsernamePasswordAuthenticationToken.unauthenticated("payer-1", "wrong")))
                .isInstanceOf(BadCredentialsException.class);
        return users.loadUserByUsername("payer-1").getPassword();
    }

    private static PaymentSecurity.Identities identities(Integer strength) {
        return new PaymentSecurity.Identities(List.of(new PaymentSecurity.Identity("payer-1", "secret", Set.of(1L))), strength);
    }
}
