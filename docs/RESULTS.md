# Results

## Phase 0
- `./mvnw verify`: pass (2 Testcontainers tests per service).
- `docker compose up --wait`: Postgres and Kafka healthy.
- Both services start; `/actuator/health` UP; Flyway history has 1 applied migration in each own schema.
- Isolation: `payment_user` gets `permission denied for schema ledger` (and vice versa), checked via psql and tests.
- Phase 0 CI: PASS (commit 25924ed, GitHub Actions duration 49s).

## Phase 1 (ledger core)
- `./mvnw verify`: pass — ledger-service 12 tests (posting 6, concurrency 3, seeded randomized invariant test × 100 sequences, smoke 2); payment-service 2.
- Concurrency: 200 attempts against a payer funded for 50 → exactly 50 succeeded, 150 `InsufficientFunds`, final balance 0, no deadlocks; opposite-direction storm and 16-way same-paymentId also clean. Concurrency tests re-run 5× with no failures.
- Invariants I1, I2, I4, I5 asserted after every posting/concurrency test and after each randomized sequence.
- CI: not yet run for this branch.

## Phase 2 (payment API, idempotency, outbox)
- `./mvnw verify`: pass — payment-service 15 tests (API 13, smoke 2), ledger-service 12 (unchanged).
- Accepted → 202, declined → 201; replays return the byte-identical stored response, also after the payment's status changed.
- Same-key storm: 32 concurrent POSTs with one key → all 202 with byte-identical bodies and the same payment id, exactly 1 non-replayed response, 1 payment row, 1 outbox row. Storm, replay and rollback tests re-run 5× with no failures.
- Accepted → 1 outbox row (topic, key = payer, payload fields checked); declined (amount, blocked, velocity) → 0 outbox rows.
- Injected failure at the outbox insert → 500, no payment/key/outbox rows; retry with the same key then succeeds as an original.

## Phase 3 (Kafka, outbox relay, idempotent consumers)
- `./mvnw clean verify`: pass, 43 tests — payment-service 20 (API 13, Kafka 5, smoke 2), ledger-service 19 (Kafka 7, posting 6, concurrency 3, randomized 1, smoke 2), e2e-tests 4.
- End to end (both services, real Postgres + Kafka): funded → COMPLETED with exactly one ledger transaction; insufficient funds → FAILED (`INSUFFICIENT_FUNDS`), no ledger writes; unknown payee → FAILED (`UNKNOWN_ACCOUNT`); same-key retry after completion → same payment, one posting.
- Same PaymentRequested delivered 3× → 1 ledger transaction, 1 result row, duplicate counter +2. New eventId for an already-posted payment → stored LedgerPosted re-emitted with the same ledger transaction id, no second posting. 16 concurrent events for one payment → 1 posting, 16 identical results.
- Duplicate LedgerPosted and a late conflicting LedgerRejected → one transition, `updated_at` unchanged.
- Poison (invalid JSON, missing fields, version 2, unknown eventType) → retried 3× at 500 ms, then `<topic>-dlt`; valid events behind them on the same partition were processed.
- Ledger invariants I1, I2, I4, I5 asserted after every ledger Kafka test.
- Environment note: the dev machine was under memory pressure during these runs (≈7.5 GB swap in use), so Spring context startup in tests took up to ~85 s.

## Phase 4 (recovery, reconciliation, fault injection)
- `./mvnw clean verify`: pass, 57 tests — payment-service 23 (API 14, Kafka 7, smoke 2), ledger-service 20 (Kafka 8, posting 6, concurrency 3, randomized 1, smoke 2), e2e-tests 14 (flow 4, recovery 3, invariant checker 7).
- Stale PENDING_LEDGER after the ledger had **posted**: recovered to COMPLETED, still exactly 1 ledger transaction, balances unchanged, stored LedgerPosted re-emitted (2 result rows).
- Stale PENDING_LEDGER after the ledger had **rejected**, with the payer funded in between: recovered to FAILED (`INSUFFICIENT_FUNDS`), no posting.
- Accepted payment whose request never reached the ledger: reconciliation posted it once → COMPLETED.
- Reconciler re-requests only stale payments, with a fresh eventId, at most once per stale period.
- Fault points (armed once, throw): `BEFORE_PAYMENT_OUTBOX_COMMIT` → 500, no rows, same-key retry succeeds; `AFTER_OUTBOX_PUBLISH_BEFORE_MARK` → same event published twice, row marked once; `AFTER_LEDGER_COMMIT_BEFORE_ACK` → redelivery counted as a duplicate, 1 posting, nothing in the DLT.
- `verify_invariants.sql`: 0 violations on settled real data from both services. 14 controlled corruptions (each in a rolled-back transaction), each reported under the expected invariant: I1 ×2, I2 ×3, I3 ×3, I4 ×1, I5 ×1, I6 ×4.
