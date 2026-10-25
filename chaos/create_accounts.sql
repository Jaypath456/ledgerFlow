-- Creates accounts :first .. :first+:count-1 of :type, each funded with :amount minor units
-- from the SYSTEM account through real balanced ledger transactions (keeps I1/I5 true).
-- psql -v first=10001 -v count=200 -v type=CUSTOMER -v amount=1000000 -f chaos/create_accounts.sql
BEGIN;
INSERT INTO ledger.accounts (id, type)
SELECT g, :'type' FROM generate_series(:first, :first + :count - 1) g;

WITH funding AS (
    SELECT g AS account_id, gen_random_uuid() AS payment_id
    FROM generate_series(:first, :first + :count - 1) g WHERE :amount > 0),
txs AS (
    INSERT INTO ledger.ledger_transactions (payment_id) SELECT payment_id FROM funding RETURNING id, payment_id),
entries AS (
    INSERT INTO ledger.ledger_entries (transaction_id, account_id, amount_minor)
    SELECT t.id, 1, -:amount FROM txs t
    UNION ALL
    SELECT t.id, f.account_id, :amount FROM txs t JOIN funding f USING (payment_id)
    RETURNING 1)
SELECT count(*) AS funding_entries FROM entries;

UPDATE ledger.accounts SET balance_minor = balance_minor + :amount
WHERE id BETWEEN :first AND :first + :count - 1 AND :amount > 0;
UPDATE ledger.accounts SET balance_minor = balance_minor - (:amount::bigint * :count) WHERE id = 1;
COMMIT;
