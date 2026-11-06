-- One lock row per payer. Each admission locks it, so the 60 s velocity count is exact under concurrency.
-- Only a lock: the count still comes from payments, so history from before the row existed still counts.
CREATE TABLE velocity_guards (
    payer_account_id BIGINT PRIMARY KEY
);
