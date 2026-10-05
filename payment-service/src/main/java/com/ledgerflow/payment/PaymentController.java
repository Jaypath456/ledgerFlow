package com.ledgerflow.payment;

import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/payments")
class PaymentController {

    private final PaymentService service;

    PaymentController(PaymentService service) {
        this.service = service;
    }

    /** 202 for accepted (PENDING_LEDGER), 201 for declined; a replay returns the stored original response. */
    @PostMapping
    ResponseEntity<String> create(@RequestHeader("Idempotency-Key") String key, @RequestBody PaymentRequest request) {
        if (key.isBlank() || key.length() > 255) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency-Key must be 1-255 characters");
        }
        request.validate();
        var result = service.create(key, request);
        return ResponseEntity.status(result.status())
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotent-Replayed", String.valueOf(result.replayed()))
                .body(result.body());
    }

    @GetMapping("/{id}")
    ResponseEntity<Payment> get(@PathVariable UUID id) {
        return ResponseEntity.of(service.find(id));
    }
}
