package com.ledgerflow.payment;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public record PaymentRequest(Long payerAccountId, Long payeeAccountId, Long amountMinor, String currency) {

    void validate() {
        if (payerAccountId == null || payeeAccountId == null || amountMinor == null || currency == null) {
            throw badRequest("payerAccountId, payeeAccountId, amountMinor and currency are required");
        }
        if (amountMinor <= 0) {
            throw badRequest("amountMinor must be positive");
        }
        if (payerAccountId.equals(payeeAccountId)) {
            throw badRequest("payer and payee must differ");
        }
        if (!currency.equals("USD")) {
            throw badRequest("only USD is supported");
        }
    }

    /** SHA-256 over a fixed field order, so equal requests hash equally regardless of JSON layout. */
    String canonicalHash() {
        String canonical = payerAccountId + "|" + payeeAccountId + "|" + amountMinor + "|" + currency;
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static ResponseStatusException badRequest(String reason) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, reason);
    }
}
