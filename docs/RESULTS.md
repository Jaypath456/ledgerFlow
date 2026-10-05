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
- `./mvnw clean verify`: pass, 58 tests (payment-service 23, ledger-service 21, e2e-tests 14), after the test-isolation fix below. The original Phase 6 run had 57 (ledger-service 20).
- CI on PR #1 exposed an order-dependent ledger test failure, fixed below.

| # | Acceptance item | Evidence |
|---|---|---|
| 1 | `./mvnw verify` passes | 58 tests green (Phase 6 above) |
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

## Test-isolation fix (PR #1 CI failure)
- CI failure: `LedgerPostingTest.seedIsBalancedAndFunded` expected −175000, but was −2181000. Reproduced locally with `-Dsurefire.runOrder=reversealphabetical`.
- Root cause: every ledger test funded its accounts from seeded SYSTEM account 1 in the shared container, so the seed check passed only when it ran first (DECISIONS 51).
- Regression test `SeedIsolationTest` runs the concurrency, randomized and Kafka tests first, then the seed check. With the old funding source it fails the same way (−2184000); with the fix it passes.
- Order independence: ledger-service passed 21/21 under alphabetical, reverse-alphabetical and random class order (seeds 11, 22, 33), plus random method order (seeds 22, 33). Each run used a fresh JVM and container.
- `./mvnw clean verify`: pass, 58 tests.

## Phase 7 (demo experience), 2026-10-05
Same host as Phase 5. Single runs, recorded as observed. These are demonstrations, not benchmarks; the historical sessions above are unchanged.
- `./mvnw clean verify`: pass, **70 tests**: the 58 existing plus 12 new.
  - payment-service 28 (+5 `DemoPaymentTest`): dashboard served, burst, cap, config, invariant summary.
  - ledger-service 26 (+5 `DemoLedgerTest`): isolated funded accounts, validation, read-outs, invariant summary with rolled-back corruption, deadlock counter.
  - e2e-tests 16 (+2 `DemoDisabledTest`): no dashboard, `/demo/**` or CORS in the default profile; velocity limit still 5/60 s.
- `./start.sh` from a stopped state: all 4 containers healthy and both `/actuator/health` UP in 42 s.
- `./stop.sh` then `./start.sh`: data kept (1,576 payments before and after).
- `./reset.sh --yes`: removed only project `ledgerflow-demo` (its containers, network and volume). It restarted on the clean Flyway seed (0 payments, SYSTEM −175000, 6 accounts); volume `ledgerflow_pgdata` was untouched. Answering "n" cancels.
- Dashboard: `/` and all assets 200 with correct MIME types. Headless Chrome rendered live data (both services UP, invariants PASS, 6 invariant rows, 8 scenario cards, 5 shell commands). CORS from `:8081` is allowed; any other origin gets 403.
- Dashboard scenarios, using the dashboard's own unmodified modules against the live stack: 8/8 PASS, again 8/8 after a reset.

| Scenario | Measured |
|---|---|
| Happy path | 202 → COMPLETED; payer $75.00, payee $25.00; 1 posting |
| Insufficient funds | 202 → FAILED (`INSUFFICIENT_FUNDS`); payer $10.00, payee $0.00; 0 postings |
| Idempotent retry | 2 requests, both 202; second replayed, byte-identical; 1 payment id; 1 posting |
| Idempotency conflict | 202, then 422 with no payment id; original $10.00; payer $90.00; 1 posting |
| Same-key race | 32 sent, 32 responses (burst 133 ms); 1 payment id; 31 replays; 1 posting |
| Hot-account race | 200 × $10.00 from $500.00 (burst 313 ms); 50 COMPLETED, 150 FAILED; ending $0.00; payee $500.00; 50 postings; 0 duplicates |
| Opposite-direction | 100 A→B + 100 B→A; 200 COMPLETED; 0 errors; Postgres deadlocks +0; A $100.00, B $100.00 |
| Risk rules | > $10,000 → 201 DECLINED `AMOUNT_LIMIT`; account 999999 → 201 DECLINED `BLOCKED_ACCOUNT`; 0 postings |

- With ledger-service stopped, a scenario reports FAIL as an infrastructure error, and the shell scripts exit 2.
- Shell scenarios: `./demo/run.sh all` passed all 9 application scenarios, with the same outcomes as the table. The full cross-schema check (`chaos/verify_invariants.sql`) reported 0 violations.
- Infrastructure demos, each with 250 × $1.00 background payments:

| Demo | Recovery (new payment completes) | Outcome |
|---|---|---|
| `ledger-crash` (kill -9, restart after 3 s) | 42.7 s | 250/250 COMPLETED, each posted once; payer $750.00 as expected; 0 violations |
| `kafka-restart` | 19.3 s | 250/250 COMPLETED; 0 duplicates; 0 violations |
| `postgres-pause` (10 s) | 0.2 s after unpause | 250/250 COMPLETED; 0 duplicates; 0 violations |

## Phase 7 — interactive accounts, Transaction Playground, Resilience Lab (2026-10-05)
Single observed runs on the same host, recorded as they happened. These are demonstrations, not benchmarks. Race outcomes vary between runs; every run was judged by correctness properties only.
- `./mvnw clean verify`: pass, **94 tests**: the 70 existing plus 24 new.
  - ledger-service 33 (+7 `DemoNamedAccountsTest`).
  - payment-service 33 (+5 `DemoRunnerAndControlsTest`).
  - e2e-tests 28 (+8 `PlaygroundTest`, +4 `ResilienceTest` on a separate demo-profile stack; `DemoDisabledTest` extended to the new endpoints).
- After `./reset.sh --yes`: Jay #1001 $75.00 and Ajay #1003 $100.00 exist with real ledger balances. "+ Create account" created Jaysus #1005 at $100.00. A duplicate name (any case) is refused: "An account with that name already exists."
- Payments by name through the dashboard: Jay → Ajay $25, Ajay → Jaysus $10, Jaysus → Jay $5, all COMPLETED (0.29–0.97 s). Balances after refresh: Jay $55.00, Ajay $115.00, Jaysus $105.00.
- Transaction Playground, through the dashboard UI (headless Chrome), starting from the example balances Jay $75, Ajay $100, Jaysus $100, all **PASS**:

| Example | Observed |
|---|---|
| Dependency race (Jay → Ajay $50, Ajay → Jaysus $140) | Dashboard run: **both COMPLETED** (Jay's credit landed first): Jay $25, Ajay $10, Jaysus $240. Three earlier runs (dashboard modules from Node): **Ajay → Jaysus FAILED** (insufficient funds), Jay $25, Ajay $150, Jaysus $100. Both are valid; all PASS. |
| Shared receiver (Jay → Ajay $50, Jaysus → Ajay $50) | Both COMPLETED; Ajay $100 → $200 (no lost update) |
| Shared payer (Jay → Ajay $60, Jay → Jaysus $15) | Both COMPLETED; Jay $75 → $0 |
| Shared payer, not enough (Jay → Ajay $60, Jay → Jaysus $25) | Dashboard run: $25 COMPLETED, $60 FAILED (Jay $50). Two earlier runs: $60 COMPLETED, $25 FAILED (Jay $15). Never negative; all PASS. |
| Delayed requests (second request +1000 ms) | Requests sent at 0 and 1000 ms; both COMPLETED; reversed delays reverse the order (0 / 1000 ms) |
| Custom: Jaysus → Jay $30, Jay → Ajay $90, Ajay → Jaysus $20 (+250 ms) | $30 and $20 COMPLETED; $90 FAILED (insufficient funds); PASS |
| User-created accounts (Node run): Alice/Bob 3-way custom, 5-way ring across 5 accounts | PASS; the ring left every balance unchanged |

- Resilience Lab, from the dashboard (every experiment ended with exactly 1 ledger posting and invariants passed):

| Experiment | Observed |
|---|---|
| Ledger processing paused, then resumed | System page showed "Ledger processing: PAUSED". A payment (Jay → Ajay $25 on the Payments page) stayed "Processing…" for 4 s with Jay's balance unchanged ($30.00 / $30.00), then COMPLETED after resume (Jay $30 → $5). Experiment card: PASS. |
| Result processing paused, then resumed | Ledger outcome POSTED and payer already $75 while the payment still showed "Processing…"; COMPLETED after resume; PASS |
| Lost result, recovered by reconciliation | Ledger POSTED; payment "Processing…"; recovered by the reconciler after 37–39 s (3 runs); the ledger sent its result twice (original + re-emitted); PASS |
| 2 s ledger delay | Payment stayed "Processing…" 2.1 s, then COMPLETED; PASS |
| 5 s ledger delay | 5.1–5.3 s, then COMPLETED; PASS |
| Reset demo faults | Paused results and a 2 s delay, then reset: every control back to RUNNING / DELIVERED / NONE |

- System page: Payment and Ledger services UP, invariants PASS, ledger and result processing RUNNING, "Deployment mode: Local single-instance demo · High availability: Not configured · Recovery/correctness: Verified".
- Reliability Tests (dashboard, after all of the above): 8/8 PASS.

## Phase 8 — local Kubernetes (k3d), 2026-10-05
Single observed runs on one laptop (one k3d server node, Linux kernel 6.17, ~100 GiB root disk). Times are wall-clock measurements from the commands shown. Only these runs are reported. Nothing here is a benchmark, and nothing was re-run to get a better number.

Cluster state before the restarts: node Ready, DiskPressure False, all four workloads 1/1, both PVCs Bound.

**Pod replacement and recovery** (deletion time to the replacement's readiness):

| Pod deleted | Result |
|---|---|
| payment-service | Replacement created 0.9 s after delete; Ready at 11.4 s; `/actuator/health` UP at 12.0 s; dashboard HTTP 200. No payment traffic during this step. |
| ledger-service (modest workload) | 30 payments of $5 submitted, the pod deleted after payment 15. Replacement Ready within 10.9 s (measured when the workload script reached its wait, so this is an upper bound). Result: 30 submitted, 20 COMPLETED, 10 FAILED (insufficient funds), 0 pending, 0 duplicate postings (max 1 posting per payment), 0 invariant violations, payer $100 → $0, payee $0 → $100. |
| ledger-service (`./k8s/demo-restart.sh`, run unchanged) | Replacement created 0.9 s after delete; replacement Ready 13.4 s after delete. 250 of 250 payments accepted, 0 client retries, 250 COMPLETED, 0 duplicate postings, payer $750.00 (expected $750.00), full invariant check 0 violations, verdict **PASS**. The script's "Recovered after 0.2 s" line (since relabelled "Post-ready convergence") is measured from after the replacement was Ready, so the deletion-to-probe time is 13.4 s + 0.2 s. |

**PostgreSQL pod restart** (only the pod deleted; the StatefulSet and PVC `pvc-75b45479…` kept):
- The PostgreSQL log shows shutdown at 19:50:28 UTC and "ready to accept connections" at 19:50:31 UTC, about 3 s. A `kubectl wait` on the pod returned at once because the old pod was still terminating, so it was not used for this figure.
- Same PVC, still Bound. Data kept: payment `0788de93…` still COMPLETED; account 1050 $0.00 and account 1051 $100.00, unchanged; 948 payments and 1,614 ledger entries, unchanged.
- After the restart, both services UP and a new payment reached a terminal state (FAILED, insufficient funds: payer balance $0). Invariants 0 violations.

**Kafka pod restart** (only the pod deleted; the StatefulSet and PVC `pvc-ea2a9b19…` kept):
- Replacement pod created 2.5 s after delete; Ready 19.5 s after delete.
- Same PVC, still Bound.
- Three payments after the restart: all COMPLETED, one posting each. Payer $100.00 → $85.00, payee $0.00 → $15.00.
- Both services logged transient reconnect errors during the restart (Kafka `DisconnectException`; PostgreSQL `57P01` from the earlier restart). They resumed and processed the new payments. Single-broker recovery only. Not Kafka HA.

**Final correctness check** (after all of the above, on the same cluster):

| Check | Result |
|---|---|
| Duplicate financial effects | 0 (every payment checked in the runs above has at most one posting) |
| Pending payments (`PENDING_LEDGER`) | 0 |
| Invariants I1–I6, `chaos/verify_invariants.sql` | 0 violations |
| Service invariant checks (ledger + payment) | 0 violations each |
| Global ledger entry sum | **0** |
| Negative CUSTOMER or MERCHANT balances | 0. 19 SYSTEM treasury accounts are negative by design (they fund demo accounts; I4 excludes SYSTEM). |

**Scripts:**
- `bash -n k8s/*.sh`: passes (5 scripts).
- `./k8s/status.sh`: payment and ledger UP; all workloads and PVCs listed.
- `./k8s/logs.sh payment|ledger|kafka|postgres`: each prints recent logs. No log-follow process was left running.
- `./k8s/start.sh`: not re-run against the healthy cluster in this pass. Read the script: an existing cluster is started, not recreated; images are rebuilt and imported; manifests are applied. The from-scratch creation was verified afterwards (next section).

**Maven** (`./mvnw clean verify`, JDK 25.0.4.1): **BUILD SUCCESS**, exit 0, **94 tests, 0 failures**. payment-service 33, ledger-service 33, e2e-tests 28 (the same 94 as Phase 7).

**Docker Compose regression** (k3d stopped, not deleted; then restored):
- `./start.sh`: four containers healthy (postgres, kafka, payment-service, ledger-service), dashboard HTTP 200, start-up 1 min 53 s.
- `demo/happy_path.sh`: **PASS**. $25 payment, HTTP 202, COMPLETED, payer $100 → $75, payee $0 → $25, exactly one posting, 0 invariant violations.
- `./stop.sh`: stopped, data kept.
- `k3d cluster start ledgerflow`: cluster Ready, DiskPressure False, all workloads 1/1, both PVCs Bound, health UP, dashboard HTTP 200. Data from before the Compose run was still there.

### From-scratch cluster creation (2026-10-05, after the runs above)
One observed run from the committed repository state. The verified cluster was deleted with `./k8s/stop.sh` (cluster and both PVCs gone; no Compose or other Docker volumes touched), then recreated with `./k8s/start.sh`.

- `./k8s/start.sh`: exit 0, **ready in 142 s**. Cluster created by 11 s; application images came from the Docker build cache (1 s); importing the four images into the cluster took about 100 s; PostgreSQL, Kafka and both services were ready 30 s after the manifests were applied.
- Node Ready, DiskPressure False, no taints. The kubelet's effective config (node `configz`) shows `evictionHard` 1Gi, `evictionMinimumReclaim` 256Mi and image GC 100/99, from the `start.sh` flags alone. No drop-in file exists on this node.
- All four workloads 1/1; two new PVCs Bound; the database started empty (0 payments).
- Payment and ledger `/actuator/health`, `/liveness` and `/readiness`: HTTP 200, UP. Dashboard HTTP 200.
- `demo/happy_path.sh`: **PASS**. $25 payment, HTTP 202, COMPLETED, payer $100 → $75, payee $0 → $25, exactly one posting.
- `./k8s/demo-restart.sh`: **PASS**. Replacement created 0.7 s after delete, Ready 8.9 s after delete, post-ready convergence 0.2 s. 250 of 250 accepted, 0 client retries, 250 COMPLETED, 0 duplicate postings, payer $750.00 (expected $750.00).
- Invariants: `chaos/verify_invariants.sql` 0 violations, service checks 0 violations, global ledger entry sum 0, 0 pending payments (252 payments in total).
- `./mvnw clean verify` (JDK 25.0.4.1): **BUILD SUCCESS**, exit 0, **94 tests, 0 failures** (payment-service 33, ledger-service 33, e2e-tests 28).
