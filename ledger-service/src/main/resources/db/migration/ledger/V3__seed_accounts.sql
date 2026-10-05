-- Deterministic seed. Customers are funded from the SYSTEM treasury via real
-- balanced ledger transactions, so the ledger is consistent from the first row.
INSERT INTO accounts (id, type) VALUES
    (1, 'SYSTEM'),
    (2, 'CUSTOMER'), (3, 'CUSTOMER'), (4, 'CUSTOMER'),
    (5, 'MERCHANT'), (6, 'MERCHANT');

WITH funding(payment_id, customer_id, amount_minor) AS (VALUES
    ('00000000-0000-0000-0000-000000000002'::uuid, 2, 100000::bigint),
    ('00000000-0000-0000-0000-000000000003'::uuid, 3,  50000::bigint),
    ('00000000-0000-0000-0000-000000000004'::uuid, 4,  25000::bigint)),
txs AS (
    INSERT INTO ledger_transactions (payment_id)
    SELECT payment_id FROM funding RETURNING id, payment_id),
entries AS (
    INSERT INTO ledger_entries (transaction_id, account_id, amount_minor)
    SELECT t.id, 1, -f.amount_minor FROM txs t JOIN funding f USING (payment_id)
    UNION ALL
    SELECT t.id, f.customer_id, f.amount_minor FROM txs t JOIN funding f USING (payment_id)
    RETURNING 1)
SELECT count(*) FROM entries;

UPDATE accounts SET balance_minor = -175000 WHERE id = 1;
UPDATE accounts SET balance_minor = 100000 WHERE id = 2;
UPDATE accounts SET balance_minor = 50000  WHERE id = 3;
UPDATE accounts SET balance_minor = 25000  WHERE id = 4;
