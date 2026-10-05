CREATE TABLE processed_events (
    event_id     UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_id UUID        NOT NULL,
    topic        TEXT        NOT NULL,
    event_key    TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Final ledger decision per payment. Any later request for the same payment re-emits this
-- instead of evaluating (or moving money) again.
CREATE TABLE payment_outcomes (
    payment_id            UUID        PRIMARY KEY,
    status                TEXT        NOT NULL CHECK (status IN ('POSTED', 'REJECTED')),
    ledger_transaction_id BIGINT      UNIQUE REFERENCES ledger_transactions (id),
    reason                TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((status = 'POSTED') = (ledger_transaction_id IS NOT NULL)),
    CHECK ((status = 'REJECTED') = (reason IS NOT NULL))
);
