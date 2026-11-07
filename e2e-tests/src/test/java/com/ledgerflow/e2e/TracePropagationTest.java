package com.ledgerflow.e2e;

import static com.ledgerflow.e2e.Stack.SPANS;
import static com.ledgerflow.e2e.Stack.account;
import static com.ledgerflow.e2e.Stack.awaitTerminal;
import static com.ledgerflow.e2e.Stack.pay;
import static com.ledgerflow.e2e.Stack.paymentDb;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * One trace across HTTP POST -> payment outbox -> ledger consumption -> posting -> ledger result outbox -> payment update.
 * Spans from both in-process services go to one in-memory exporter (Stack).
 */
class TracePropagationTest {

    private static final AttributeKey<String> SERVICE = AttributeKey.stringKey("service.name");

    @Test
    void oneTraceCoversRequestPostingAndResult() throws Exception {
        long payer = account("CUSTOMER", 10_000);
        long payee = account("MERCHANT", 0);
        var created = pay(UUID.randomUUID().toString(), payer, payee, 1_000);
        assertThat(created.status()).isEqualTo(202);
        UUID id = UUID.fromString(created.body().get("id").asString());
        assertThat(awaitTerminal(id).get("status").asString()).isEqualTo("COMPLETED");
        String traceId = paymentDb().sql("SELECT substring(traceparent from 4 for 32) FROM outbox WHERE aggregate_id = ?")
                .param(id).query(String.class).single();

        // Spans are exported in batches after they end: wait for the payment-side consumer of the ledger result.
        List<SpanData> trace = await().atMost(Duration.ofSeconds(30)).until(
                () -> SPANS.getFinishedSpanItems().stream().filter(s -> s.getTraceId().equals(traceId)).toList(),
                spans -> spans.stream().anyMatch(consumerIn("payment-service")));
        Map<String, SpanData> byId = trace.stream().collect(Collectors.toMap(SpanData::getSpanId, Function.identity(), (a, b) -> a));
        String names = trace.stream().map(s -> service(s) + ":" + s.getKind() + ":" + s.getName()).sorted().toList().toString();

        List<SpanData> roots = trace.stream().filter(s -> !s.getParentSpanContext().isValid()).toList();
        assertThat(roots).as(names).hasSize(1);
        SpanData root = roots.getFirst();
        assertThat(root.getKind()).as(names).isEqualTo(SpanKind.SERVER);
        assertThat(service(root)).isEqualTo("payment-service");
        for (SpanData span : trace) {
            if (span == root) continue;
            assertThat(ancestors(span, byId)).as("%s reaches the HTTP root in %s", span.getName(), names).contains(root);
        }

        SpanData paymentPublish = only(trace, s -> s.getName().equals("outbox publish payments.requested"), names);
        SpanData ledgerConsumer = only(trace, consumerIn("ledger-service"), names);
        SpanData posting = only(trace, s -> s.getName().equals("ledgerflow.ledger.posting"), names);
        SpanData resultPublish = only(trace, s -> s.getName().equals("outbox publish ledger.results"), names);
        SpanData paymentConsumer = only(trace, consumerIn("payment-service"), names);

        assertThat(ancestors(ledgerConsumer, byId)).as(names).contains(paymentPublish);
        assertThat(ancestors(posting, byId)).as(names).contains(ledgerConsumer);
        assertThat(ancestors(resultPublish, byId)).as(names).contains(ledgerConsumer);
        assertThat(ancestors(paymentConsumer, byId)).as(names).contains(resultPublish);
    }

    private static Predicate<SpanData> consumerIn(String service) {
        return s -> s.getKind() == SpanKind.CONSUMER && service.equals(service(s));
    }

    private static String service(SpanData span) {
        return span.getResource().getAttribute(SERVICE);
    }

    private static SpanData only(List<SpanData> trace, Predicate<SpanData> match, String names) {
        List<SpanData> found = trace.stream().filter(match).toList();
        assertThat(found).as(names).hasSize(1);
        return found.getFirst();
    }

    /** The span's parents up to the root (exclusive of the span itself); stops at a parent that was not exported. */
    private static List<SpanData> ancestors(SpanData span, Map<String, SpanData> byId) {
        return java.util.stream.Stream.iterate(byId.get(span.getParentSpanId()), Objects::nonNull,
                s -> byId.get(s.getParentSpanId())).toList();
    }
}
