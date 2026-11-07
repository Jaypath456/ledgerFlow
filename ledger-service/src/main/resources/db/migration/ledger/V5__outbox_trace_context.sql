-- W3C trace context (traceparent/tracestate) of the transaction that wrote the row (Roadmap Phase 2). The relay restores it as
-- the parent of the publish span. Nullable without default: older rows and rows written without a current span publish as before.
ALTER TABLE outbox ADD COLUMN traceparent TEXT, ADD COLUMN tracestate TEXT;
