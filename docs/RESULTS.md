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

## Phase 5 (chaos, load, performance)

### Environment (all measurements)
- Host: Intel i5-10500H (6 cores / 12 threads), 7.6 GiB RAM, Ubuntu 24.04 LTS, kernel 6.17.0-22, Docker 27.5.1, Compose v5.1.3, laptop.
- Everything runs on that one host: Postgres 17, Kafka 4.0.0 (KRaft, 1 broker), both services (`-Xmx384m`), and k6 v2.3.0 (`grafana/k6`, host network).
- Kafka: `payments.requested` and `ledger.results` with **3 partitions** each, RF 1; listener concurrency 3.
- The host was under heavy memory pressure from unrelated workloads throughout: 7.5–7.7 GiB of swap in use, 0.9–1.5 GiB available. Load average was 12–15 even with the stack idle during session C. Absolute numbers are host-bound and varied between sessions, so every session is reported.

### Commands
```
# stack + seed are handled by the scripts (Compose project "ledgerflow-chaos", own volume)
FRESH=1 chaos/bench.sh 200 60s     # fresh volume + seed, then measure
chaos/bench.sh <rate> 60s          # k6 constant arrival rate -> drain time, request-to-terminal latency, invariants
chaos/run_chaos.sh 27              # 9 fault scenarios x 3, 100 payments/s background load, 60 s per run
```
POST latency is k6's first-attempt `POST /api/payments` duration. Request-to-terminal is `updated_at - created_at` of COMPLETED/FAILED payments created during the run (SQL in `chaos/lib.sh`).

### Fault-free load
Session A (2026-10-04 ~20:50, outbox poll 50 ms, stack nearly empty):

| target/s | achieved/s | POST p50 / p95 / p99 ms | request-to-terminal p50 / p95 / p99 ms | drain after load |
|---|---|---|---|---|
| 25 (30 s) | 25.0 | 4.7 / 11.1 / 14.2 | 94 / 125 / 134 | 1 s |
| 50 | 50.0 | 3.0 / 7.0 / 8.2 | 98 / 124 / 128 | 0 s |
| 100 | 100.0 | 2.1 / 3.3 / 5.9 | 94 / 121 / 126 | 0 s |
| 200 | 200.0 | 1.7 / 2.5 / 3.7 | 94 / 120 / 127 | 0 s |
| 400 | 399.7 | 1.9 / 12.0 / 98.3 | 89 / 125 / 221 | 0 s |
| 600 | 598.8 | 2.6 / 44.3 / 181.3 | 109 / 496 / 771 | 1 s |
| 800 | 797.8 | 2.5 / 41.9 / 183.8 | 189 / 4298 / 4925 | 3 s |

During 600/s, sampled every 4 s: the ledger consumer lag peaked at 282, payment consumer lag at 83, and unpublished outbox rows stayed ≤ 40 per service. Pending payments stayed at 48–85 (bounded). At 800/s request-to-terminal p95 reached 4.3 s, meaning the pipeline was behind. The ledger consumer (3 partitions, one DB transaction per event) is the bottleneck.

Session B (same hour, 200/s, back to back): outbox poll 50 ms → request-to-terminal 84 / 121 / 225 ms; poll **10 ms → 36 / 51 / 119 ms** (POST p95 7.1 → 5.6 ms). Adopted (DECISIONS 43).

Session C (2026-10-04 23:28, final config, fresh volume, host load average 12–15):

| target/s | achieved/s | POST p50 / p95 / p99 ms | request-to-terminal p50 / p95 / p99 ms | drain after load |
|---|---|---|---|---|
| 200 | 197.1 | 3.0 / 186.2 / 762.7 | 42 / 999 / 1555 | 0 s |
| 400 | 329.5 | 2.5 / 1924.1 / 5927.4 | 44 / 4596 / 14580 | 1 s |
| 600 | 582.1 | 5.7 / 688.6 / 1070.4 | 1901 / 11454 / 12336 | 8 s |
| 800 | 300.3 | 21.5 / 5925.2 / 8812.1 | 10785 / 48141 / 48471 | 16 s |

Session D (immediately before C, same host state, no reset after the 27 chaos runs: ≈ 370k payments, ≈ 1M Kafka records): 200/s target → 176.3/s achieved, POST p95 3.4 s, request-to-terminal p95 10.6 s.

**Sustained throughput (no failed requests, bounded backlog):**
- **600 payments/s** in session A, with POST p50/p95/p99 2.6 / 44.3 / 181.3 ms and request-to-terminal p95 496 ms.
- **≈ 200 payments/s** in session C on the overloaded host, with POST p50/p95/p99 3.0 / 186.2 / 762.7 ms and request-to-terminal p95 999 ms.

Every fault-free bench run finished with 0 failed requests and 0 invariant violations.

### Chaos campaign (`chaos/run_chaos.sh 27`, 2026-10-04 22:33–23:21)
**27/27 runs passed; 0 invariant violations, 0 DLT records, 14 real JVM halts.** Per-run data: `docs/evidence/chaos-campaign-runs.csv`.

| scenario (3 runs each) | recovery s min / median / max | max settle s | JVM halts | duplicates absorbed per run (ledger / payment) |
|---|---|---|---|---|
| kill -9 ledger-service, restart after 5 s | 44.8 / 45.0 / 45.2 | 2 | – | 0/500, 0/0, 0/2 |
| kill -9 payment-service, restart after 5 s | 40.9 / 41.4 / 41.4 | 1 | – | 0/0 ×3 |
| pause Postgres 10 s | 0.6 / 0.7 / 3.1 | 0 | – | 0/0 ×3 |
| restart Kafka | 16.9 / 17.9 / 19.7 | 1 | – | 0/0 ×3 |
| replay 3000 processed events into each consumer | 17.9 / 20.4 / 21.6 | 1 | – | **3000/3000 ×3** |
| 200 concurrent payments, hot account funded for 50 | 0.6 / 0.8 / 1.2 | 1 | – | – |
| crash `BEFORE_PAYMENT_OUTBOX_COMMIT` | 49.7 / 56.2 / 67.4 | 40 | 6 | ≤ 1 payment side |
| crash `AFTER_OUTBOX_PUBLISH_BEFORE_MARK` | 35.9 / 43.4 / 49.4 | 0 | 4 | 0/0, 0/2, 0/0 |
| crash `AFTER_LEDGER_COMMIT_BEFORE_ACK` | 14.6 / 23.5 / 50.8 | 4 | 4 | 2, 1, 552 ledger side |

- **Recovery** (DECISIONS 46) runs from the end of the fault action until a new probe payment is COMPLETED. Kill and crash recoveries are dominated by JVM restart time on this host (~40 s). The crash rows include recreating the service without the chaos profile.
- **Hot account:** in each of the 3 runs, exactly 50 of 200 concurrent payments completed and 150 failed with `INSUFFICIENT_FUNDS`; the final balance was 0. No overdraft.
- **Duplicates:** the replay runs are exact, because both services restart fresh: 9,000 per consumer, 18,000 in total, all absorbed. Elsewhere the counters (`ledgerflow.events.duplicate`) hold since each process last started, so they are lower bounds. The 552 is one uncommitted poll batch redelivered after a halt. The 500 is ledger results re-published after a kill between publish and mark.
- **DLT:** 0 records across all 27 runs.
- **Client side under chaos (k6):** 149,119 accepted POSTs, 43,253 retries with the same Idempotency-Key, and 6,678 requests abandoned after 5 attempts (service down for a kill/pause window). No retry produced a second payment (I6, I2 clean).
- **End state:** 369,144 payments, all terminal (362,308 COMPLETED, 6,836 FAILED); 362,516 ledger transactions (= COMPLETED + 208 seed/funding postings). Invariant checker: 0 violations.

Earlier attempts, kept for the record:
- Trial (9 runs, 30 s load): 9/9 passed, 0 violations.
- First 27-run campaign: stopped at run 8. The laptop lid was closed and the host suspended at 21:28:07 for ~61 min (systemd-logind: "Lid closed. Suspending…"). All containers froze, and the run's settle timer expired during the suspend. The checker then ran within 4 s of resume and reported one I3 (a payment still in flight). That payment completed 4 s after resume, posted exactly once, and the checker was clean afterwards. Run 6 of that campaign was invalid (script bug: reused hot account and keys from the trial); fixed before campaign 2.

## Phase 6 (local package) and final acceptance
- `docker compose -f infra/docker-compose.yml up --build` (default project, existing dev volume migrated additively to payments v2 / ledger v4): all 4 containers healthy in 42 s; both `/actuator/health` → `{"status":"UP"}`.
- On that stack, with default settings: funded payment → 202 → COMPLETED; payment above balance → 202 → FAILED (`INSUFFICIENT_FUNDS`), balance unchanged; over $10,000 → 201 DECLINED (`AMOUNT_LIMIT`); same-key replay → identical 202 body; same key, different body → 422; invariant checker → 0 violations.
- `./mvnw clean verify`: pass, 57 tests (payment-service 23, ledger-service 20, e2e-tests 14).
- CI: not run for branch `ledgerflow-completion` (not pushed).

| # | Acceptance item | Evidence |
|---|---|---|
| 1 | `./mvnw verify` passes | 57 tests green (Phase 6 above) |
| 2 | Full Compose stack starts | Phase 6 above |
| 3 | Both health endpoints UP | Phase 6 above |
| 4 | Funded payment → COMPLETED | e2e `PaymentFlowTest`; Compose smoke |
| 5 | Insufficient funds → FAILED | e2e `PaymentFlowTest`; Compose smoke |
| 6 | API retries cannot duplicate a payment | 32-way same-key storm, replay tests; 43,253 same-key retries under chaos, I6/I2 clean |
| 7 | Kafka duplicates cannot duplicate postings | `LedgerKafkaTest` (3× delivery, 16 concurrent events); 18,000 replayed events absorbed; I2 clean |
| 8 | Concurrent spending cannot overdraft | Phase 1 200-way test; hot account 50/200 completed, balance 0 (×3 under chaos); I4 clean |
| 9 | Stale PENDING_LEDGER recovers | e2e `RecoveryTest` (posted / rejected / never reached ledger) |
| 10 | Poison message → DLT without blocking | `LedgerKafkaTest`, `PaymentKafkaTest` poison tests |
| 11 | I1–I6 automated | `chaos/verify_invariants.sql`; `InvariantCheckerTest` (healthy + 14 corruptions); per chaos run |
| 12 | 20+ chaos runs, no unexplained violations | 27/27 passed, 0 violations; earlier interrupted campaign explained (host suspend) |
| 13 | Performance/recovery recorded | Phase 5 above |
| 14 | No Redis/cloud/Kubernetes/unsupported scope | two services, local Compose only; dependencies: Spring Boot starters (webmvc, actuator, jdbc, flyway, kafka), Postgres driver, Testcontainers, Awaitility |
| 15 | README metrics ⊆ RESULTS | README numbers copied from this file |
