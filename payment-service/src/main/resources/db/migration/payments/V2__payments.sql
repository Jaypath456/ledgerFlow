CREATE TABLE payments (
    id               UUID        PRIMARY KEY,
    payer_account_id BIGINT      NOT NULL,
    payee_account_id BIGINT      NOT NULL,
    amount_minor     BIGINT      NOT NULL CHECK (amount_minor > 0),
    currency         TEXT        NOT NULL CHECK (currency = 'USD'),
    status           TEXT        NOT NULL CHECK (status IN ('DECLINED', 'PENDING_LEDGER', 'COMPLETED', 'FAILED')),
    decline_reason   TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (payer_account_id <> payee_account_id)
);

CREATE INDEX payments_payer_created_idx ON payments (payer_account_id, created_at);

-- The key row is inserted first (it serializes same-key requests), the payment later in the same
-- transaction, hence the deferred FK.
CREATE TABLE idempotency_keys (
    key          TEXT        PRIMARY KEY,
    request_hash TEXT        NOT NULL,
    payment_id   UUID        NOT NULL UNIQUE REFERENCES payments (id) DEFERRABLE INITIALLY DEFERRED,
    -- Exact original response, replayed verbatim; filled in the same transaction as the key.
    response_status INT,
    response_body   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
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

CREATE TABLE processed_events (
    event_id     UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
