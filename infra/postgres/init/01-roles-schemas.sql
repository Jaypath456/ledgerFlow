-- Dev-only credentials. Runs once on an empty data dir (and in Testcontainers).
CREATE ROLE payment_user LOGIN PASSWORD 'payment_user';
CREATE ROLE ledger_user  LOGIN PASSWORD 'ledger_user';

REVOKE CONNECT ON DATABASE ledgerflow FROM PUBLIC;
GRANT  CONNECT ON DATABASE ledgerflow TO payment_user, ledger_user;

REVOKE ALL ON SCHEMA public FROM PUBLIC;

CREATE SCHEMA payments AUTHORIZATION payment_user;
CREATE SCHEMA ledger   AUTHORIZATION ledger_user;

ALTER ROLE payment_user SET search_path = payments;
ALTER ROLE ledger_user  SET search_path = ledger;
