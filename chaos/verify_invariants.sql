-- LedgerFlow invariant checker. Returns one row per violation (invariant, detail); no rows = healthy.
-- Reads both schemas, so run it as a role that can (postgres). Run only after the system has settled.
--   docker exec -i ledgerflow-postgres-1 psql -U postgres -d ledgerflow -f - < chaos/verify_invariants.sql

WITH
entries_per_tx AS (
    SELECT t.id, t.payment_id, count(e.id) AS n, COALESCE(SUM(e.amount_minor), 0) AS total
    FROM ledger.ledger_transactions t
    LEFT JOIN ledger.ledger_entries e ON e.transaction_id = t.id
    GROUP BY t.id, t.payment_id
)

-- I1: every ledger transaction balances (and has entries), and the whole ledger sums to zero.
SELECT 'I1' AS invariant, 'ledger transaction ' || id || ' has ' || n || ' entries summing to ' || total AS detail
FROM entries_per_tx WHERE total <> 0 OR n < 2
UNION ALL
SELECT 'I1', 'global ledger sum is ' || s
FROM (SELECT COALESCE(SUM(amount_minor), 0) AS s FROM ledger.ledger_entries) g WHERE s <> 0

-- I2: a payment is posted at most once, only if accepted, and with exactly its own amount and accounts.
UNION ALL
SELECT 'I2', 'payment ' || payment_id || ' posted ' || count(*) || ' times'
FROM ledger.ledger_transactions GROUP BY payment_id HAVING count(*) > 1
UNION ALL
SELECT 'I2', 'payment ' || p.id || ' is ' || p.status || ' but has a ledger posting'
FROM payments.payments p JOIN ledger.ledger_transactions t ON t.payment_id = p.id
WHERE p.status IN ('DECLINED', 'FAILED')
UNION ALL
SELECT 'I2', 'payment ' || p.id || ' posting does not match payer/payee/amount'
FROM payments.payments p JOIN entries_per_tx t ON t.payment_id = p.id
WHERE t.n <> 2
   OR NOT EXISTS (SELECT 1 FROM ledger.ledger_entries e WHERE e.transaction_id = t.id
                  AND e.account_id = p.payer_account_id AND e.amount_minor = -p.amount_minor)
   OR NOT EXISTS (SELECT 1 FROM ledger.ledger_entries e WHERE e.transaction_id = t.id
                  AND e.account_id = p.payee_account_id AND e.amount_minor = p.amount_minor)

-- I3: every accepted payment is terminal, and its status matches the ledger's recorded outcome.
UNION ALL
SELECT 'I3', 'payment ' || id || ' still PENDING_LEDGER'
FROM payments.payments WHERE status = 'PENDING_LEDGER'
UNION ALL
SELECT 'I3', 'payment ' || p.id || ' is COMPLETED without a ledger posting'
FROM payments.payments p
WHERE p.status = 'COMPLETED'
  AND NOT EXISTS (SELECT 1 FROM ledger.ledger_transactions t WHERE t.payment_id = p.id)
UNION ALL
SELECT 'I3', 'payment ' || p.id || ' is ' || p.status || ' but ledger outcome is ' || COALESCE(o.status, 'missing')
FROM payments.payments p LEFT JOIN ledger.payment_outcomes o ON o.payment_id = p.id
WHERE (p.status = 'COMPLETED' AND o.status IS DISTINCT FROM 'POSTED')
   OR (p.status = 'FAILED' AND o.status IS DISTINCT FROM 'REJECTED')
   OR (p.status = 'DECLINED' AND o.status IS NOT NULL)

-- I4: non-SYSTEM balances are never negative.
UNION ALL
SELECT 'I4', 'account ' || id || ' (' || type || ') balance ' || balance_minor
FROM ledger.accounts WHERE type <> 'SYSTEM' AND balance_minor < 0

-- I5: cached balance equals the sum of the account's ledger entries.
UNION ALL
SELECT 'I5', 'account ' || a.id || ' cached ' || a.balance_minor || ' <> entries ' || COALESCE(s.total, 0)
FROM ledger.accounts a
LEFT JOIN (SELECT account_id, SUM(amount_minor) AS total FROM ledger.ledger_entries GROUP BY account_id) s
       ON s.account_id = a.id
WHERE a.balance_minor <> COALESCE(s.total, 0)

-- I6: every payment has exactly one idempotency key, and each key's stored request hash and stored
-- response belong to that payment (so a replay returns this payment, never another).
UNION ALL
SELECT 'I6', 'payment ' || p.id || ' has ' || count(k.key) || ' idempotency keys'
FROM payments.payments p LEFT JOIN payments.idempotency_keys k ON k.payment_id = p.id
GROUP BY p.id HAVING count(k.key) <> 1
UNION ALL
SELECT 'I6', 'idempotency key ' || k.key || ' points to missing payment ' || k.payment_id
FROM payments.idempotency_keys k LEFT JOIN payments.payments p ON p.id = k.payment_id
WHERE p.id IS NULL
UNION ALL
SELECT 'I6', 'idempotency key ' || k.key || ' does not match payment ' || p.id
FROM payments.idempotency_keys k JOIN payments.payments p ON p.id = k.payment_id
WHERE k.request_hash IS DISTINCT FROM encode(sha256(convert_to(
          p.payer_account_id || '|' || p.payee_account_id || '|' || p.amount_minor || '|' || p.currency, 'UTF8')), 'hex')
   OR k.response_body IS NULL
   OR (k.response_body::jsonb ->> 'id') IS DISTINCT FROM p.id::text
   OR k.response_status IS DISTINCT FROM (CASE WHEN p.status = 'DECLINED' THEN 201 ELSE 202 END);
