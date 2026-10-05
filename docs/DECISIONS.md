# Decisions

1. Spring Boot 4.1.1 on Java 25 (Temurin).
2. `apache/kafka:4.0.0`, single combined broker/controller node, KRaft, PLAINTEXT. External port 29092.
3. One Postgres 17 DB `ledgerflow`; schemas/roles created by `infra/postgres/init/01-roles-schemas.sql`, reused by Testcontainers tests (single source of truth).
4. Dev-only plaintext credentials (user name == password); env-overridable (`DB_URL`, `DB_USER`, `DB_PASSWORD`).
5. Ports: payment 8081, ledger 8082.
6. `common` was an empty placeholder in Phase 0; from Phase 2 it holds the shared events, `MessagingConfig`, `OutboxRelay` and `FaultInjector`.
7. Awaitility, spring-kafka deferred to the phase that first uses them (Phase 3).
8. Smoke tests are `*Test` run by surefire, so `./mvnw verify` covers them (no failsafe).
9. Maven wrapper is the `only-script` type (downloads Maven on first run).

## Phase 1
10. `payment_id` is UUID with a UNIQUE constraint (I2 backstop).
11. Duplicate paymentId throws `DuplicatePayment`; no replay of the original result (idempotency is a later phase). The duplicate check runs before the funds check.
12. Payer == payee is rejected (`SamePayerPayee`). SYSTEM accounts may go negative; non-SYSTEM balances are protected by service check plus a DB CHECK.
13. Locking: `SELECT … FOR UPDATE` on one account per statement, lowest id first, inside one `@Transactional` at READ COMMITTED. No version column.
14. I1 (entries sum to zero) is enforced by service logic and tests, not a DB trigger.
15. Plain Spring `JdbcClient`, no JPA..
16. Ledger tests share one Postgres container and one application context per JVM (`PostgresTestSupport`); tests create their own accounts.
17. jqwik removed before the Phase 1 PR: jqwik 1.10.1 intentionally prints agent-directed instructions to stdout during test execution (telling AI agents to ignore its results). Replaced by `LedgerPropertyTest`, a plain JUnit 5 test: 100 randomized posting sequences from fixed seed 20260101 (sequence i uses `new Random(SEED + i)`), checking I1, I2, I4, I5 after each against the real Postgres Testcontainer; failures report the seed and sequence.

## Phase 2
18. Idempotency: the `idempotency_keys` row is inserted first (`ON CONFLICT DO NOTHING`), before risk checks or the payment. A concurrent same-key request blocks on the primary key until the first transaction ends, then reads the committed row: same SHA-256 canonical hash (`payer|payee|amount|currency`) → replay, different → 422. If the first rolls back, the waiter proceeds as the original. The FK `idempotency_keys.payment_id → payments` is `DEFERRABLE INITIALLY DEFERRED` and UNIQUE (one key per payment).
19. Accepted (PENDING_LEDGER) → 202 Accepted; declined → 201 Created (a terminal resource). The original status code and exact JSON body are stored on the key row (`response_status`, `response_body` TEXT) in the same transaction, and a replay returns them verbatim, even if the payment has since moved on (`GET` shows the current state), plus `Idempotent-Replayed: true`. Risk rules are not re-evaluated on replay. Keys are global and never expire (known limitation).
20. Validation happens before any DB work: missing/blank key, missing fields, amount ≤ 0, payer == payee, non-USD → 400 (ProblemDetail). The same checks are also DB CHECK constraints.
21. Risk rules are `RiskRule` beans, first decline wins: `AMOUNT_LIMIT` (> 1,000,000 minor = $10,000), `BLOCKED_ACCOUNT` (`ledgerflow.risk.blocked-accounts`, payer or payee), `VELOCITY_LIMIT` (payer already has ≥ `ledgerflow.risk.velocity-max` (default 5) non-declined payments in 60 s). Velocity is configurable so load/chaos runs can reach the ledger; it is a soft limit under concurrent requests from one payer.
22. Declined payments are stored (status DECLINED + reason) with their idempotency key, but no outbox row.
23. Outbox `payload` is the `PaymentRequested` JSON (record in `common`), `event_key` = payer account id, `topic` = `payments.requested`. Nothing is sent to Kafka in Phase 2.

## Phase 3
24. Kafka wiring lives in `common` (`MessagingConfig`, imported by both apps): topics with 3 partitions, RF 1. DLTs use spring-kafka 4's default name `<topic>-dlt` and keep the source partition, so they are also declared with 3 partitions. `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`, `FixedBackOff(500 ms, 2)` = 3 attempts, then DLT.
25. Listeners consume `String` and parse in code. Event records validate in their compact constructors (eventId, occurredAt, eventType, `version == 1`, paymentId), so malformed JSON, missing fields and unknown versions/types take the same retry → DLT path as any processing failure.
26. Outbox relay (`common/OutboxRelay`, `@Scheduled` every 50 ms): in its own transaction, lock ≤ 100 unpublished rows with `FOR UPDATE SKIP LOCKED`, send all, wait for every broker ack (30 s), then set `published_at`. Any failed send rolls back the mark, and the batch is resent later. At-least-once by design; consumers dedupe. Producer `acks=all`, idempotence on. The relay never runs inside the business transaction.
27. No Kafka transactions. Each listener's DB transaction commits before the listener returns, and the offset is committed after that (container default ack mode). A crash in between means redelivery, which the consumer's `processed_events` dedupes.
28. Ledger consumer, one DB transaction: insert `processed_events(eventId)` (conflict = duplicate → no-op), `pg_advisory_xact_lock(hash(paymentId))`, look up `payment_outcomes`. A stored outcome is re-emitted as a new result event with no posting. Otherwise `LedgerService.post` runs inside a savepoint (`PROPAGATION_NESTED`). Success → outcome POSTED + `LedgerPosted`. A `LedgerException` rolls back to the savepoint (no ledger writes) → outcome REJECTED + `LedgerRejected` (`INSUFFICIENT_FUNDS`, `UNKNOWN_ACCOUNT`, `INVALID_AMOUNT`, `SAME_PAYER_PAYEE`). `DuplicatePayment` (a posting with no outcome row, only possible for postings made outside the consumer) → POSTED with the existing transaction id. `LedgerService` (Phase 1) is unchanged.
29. `payment_outcomes` makes the ledger decision final per payment: a later request for the same payment returns the same result even if balances changed since. Phase 4 reconciliation relies on this.
30. Payment result consumer: `processed_events` dedupe, then `UPDATE … WHERE status = 'PENDING_LEDGER'`, so terminal payments never change. The `LedgerRejected` reason is stored in `payments.decline_reason` (the column carries the reason for both DECLINED and FAILED).
31. Duplicates absorbed are counted by the Micrometer counter `ledgerflow.events.duplicate` (both services); ledger re-emissions by `ledgerflow.outcomes.reemitted`.
32. One JVM can host both apps (`e2e-tests`, a test-only module, not a service). To allow that, configs are `payment-service.yml` / `ledger-service.yml` (`spring.config.name` is set in `main` and in tests), migrations are in `db/migration/payments` / `db/migration/ledger`, and the executable jars carry the `exec` classifier. Flyway checksums do not depend on location.
33. Listener concurrency 3 (= partitions). Gate is `./mvnw clean verify`: `Topics` constants are inlined at compile time, and an incremental build once ran a stale test class.

## Phase 4 — recovery design
34. **Why payments get stuck, and why nothing is lost.** Every hand-off is an outbox row committed in the same transaction as the state that produced it (payment → PaymentRequested; ledger posting/rejection → result). A crash anywhere leaves either nothing (the transaction rolled back, so the client retries with the same key) or a committed row the relay will (re)publish. Kafka redelivers anything not acknowledged. So a payment normally converges without reconciliation; the reconciler is the safety net for everything else (e.g. a request dead-lettered after 3 failed attempts, or a result that was never applied).
35. **Reconciler** (payment-service, every 10 s): locks (`FOR UPDATE SKIP LOCKED`) up to 500 PENDING_LEDGER payments with `updated_at` older than 30 s and no *unpublished* outbox row (an unsent request is still in flight). For each, it enqueues a new PaymentRequested with a **fresh eventId** and the same paymentId, then bumps `updated_at`, so each payment is re-requested at most once per 30 s.
36. **Why a fresh eventId.** A redelivery of the *same* eventId is dropped by the ledger's `processed_events`. That is safe because the original processing committed its result outbox row in the same transaction; the result is already on its way. But a dropped duplicate does nothing for a payment whose result was lost. A fresh eventId gets past dedupe and reaches the outcome lookup.
37. **Why money never moves twice.** Under the per-payment advisory lock, the ledger checks `payment_outcomes` before posting. If an outcome exists, it re-emits that stored result (POSTED with the original `ledger_transaction_id`, or REJECTED with the original reason) and posts nothing. `ledger_transactions.payment_id UNIQUE` remains the last line of defence. The outcome is final: a payment rejected for insufficient funds stays FAILED even if funds arrive before the retry.
38. **Applying a recovered result.** The payment consumer dedupes the new result eventId, and its `UPDATE … WHERE status = 'PENDING_LEDGER'` makes the transition at most once. Re-emitted results for already-terminal payments are no-ops.
39. **Fault injection** exists only under the `chaos` Spring profile (`FaultInjector` bean). Settings: `ledgerflow.chaos.faults` (points), `ledgerflow.chaos.probability`, `ledgerflow.chaos.action=halt|throw`. Halt = `Runtime.halt(1)`, a real crash with no cleanup; Compose restarts the container. Points:
    - `BEFORE_PAYMENT_OUTBOX_COMMIT`: payment transaction, after the outbox insert, before commit.
    - `AFTER_OUTBOX_PUBLISH_BEFORE_MARK`: relay, after broker acks, before marking published.
    - `AFTER_LEDGER_COMMIT_BEFORE_ACK`: ledger listener, after its DB commit, before the offset commit.
    Tests run with the profile active but no points configured, and use `armOnce(point)` to throw exactly once.
40. **Invariant checker** `chaos/verify_invariants.sql` (run as postgres across both schemas; one row per violation):
    - I1: each ledger transaction has ≥ 2 entries summing to 0, and the global sum is 0.
    - I2: one posting per payment, none for DECLINED/FAILED payments, and the posting's accounts and amount match the payment.
    - I3: no PENDING_LEDGER; COMPLETED ⇔ ledger outcome POSTED (and a posting exists); FAILED ⇔ REJECTED; DECLINED has no outcome.
    - I4: non-SYSTEM balances ≥ 0.
    - I5: cached balance = sum of entries.
    - I6: exactly one key per payment; each key's request hash (recomputed with `sha256` in SQL), stored response id and status code (201 declined / 202 accepted) match its payment.
    Funding/seed postings (payment ids not in `payments`) are exempt from the I2/I3 cross-checks by construction. Health remains Spring Boot Actuator `/actuator/health`; no custom heartbeat.

## Phase 5
41. Images: one multi-stage `Dockerfile` with targets `payment-service` / `ledger-service`. The build stage is `maven:3.9.11-eclipse-temurin-25`, because the Temurin images have no curl/wget for the Maven wrapper; the runtime is `eclipse-temurin:25-jre`. Healthchecks call `/actuator/health` over bash `/dev/tcp` (no curl in the JRE image).
42. Memory sizing for a 7.6 GiB laptop: services `-Xmx384m`, Kafka `-Xms256m -Xmx512m`, Postgres `shm_size: 256mb`. The last is needed: with Docker's 64 MB `/dev/shm`, parallel queries in the invariant checker failed.
43. Outbox poll interval 50 ms → 10 ms (`ledgerflow.outbox.poll-ms`; Compose `OUTBOX_POLL_MS`). Measured back to back at 200/s: request-to-terminal p50 84 → 36 ms, p95 121 → 51 ms. Idle cost is about 100 small indexed queries/s per service. This is the only latency change made.
44. Not changed, by evidence: Kafka heartbeat/session/rebalance settings, partition count (3, per the brief) and listener concurrency. The measured throughput bottleneck is the ledger consumer (3 partitions × 1 thread, one DB transaction per event); see RESULTS.
45. Chaos runs use their own Compose project (`ledgerflow-chaos`, own volume) and accumulate state across runs. The invariant checker therefore covers all history, not just one run. The velocity rule is disabled there via `RISK_VELOCITY_MAX` (DECISIONS 21).
46. Chaos run definitions:
    - **Recovery time:** seconds from the end of the fault action (restart issued, unpause, Kafka restarted, replay started, hot burst sent, or chaos profile switched off) until a *new* probe payment reaches COMPLETED end to end.
    - **Settle:** no PENDING_LEDGER and no unpublished outbox rows.
    - **Failed run:** any violation, a checker error, a settle timeout (300 s) or a failed scenario check (hot account: exactly 50 of 200 completed and balance 0; replay: ≥ 1000 duplicates absorbed by each consumer).
47. The fault-point crash runs use probability 0.0005 per hit with `action=halt` (≈ 1–3 real JVM halts per 60 s at 100/s); Compose restarts the container.

## Phase 6
48. The Compose file is the single local package: Postgres, Kafka and both services built from the repo `Dockerfile`, `restart: unless-stopped`, healthchecks on `/actuator/health`. Defaults are production-like (velocity limit 5/60 s, no chaos profile); load and chaos scripts override them via environment variables.
49. CI unchanged: GitHub Actions runs `./mvnw -B verify` (now including `e2e-tests`). Images are not built or published in CI.
50. The README quotes only numbers recorded in RESULTS.md.

## Test isolation (post-Phase 6 fix)
51. Ledger tests share one Postgres container and one application context (DECISIONS 16). They therefore never post to or from the Flyway-seeded accounts 1–6. `fundedCustomer` funds from a test-only SYSTEM account created once per test database, so the canonical seed (SYSTEM −175000; customers 100000 / 50000 / 25000; merchants 0) holds in any class or method order. `LedgerInvariants.assertAll()` also asserts that baseline, so a test that touches a seeded account fails itself, not some later test. `LedgerDbSmokeTest` uses the shared context instead of its own `@SpringBootTest`, because a second context would join the same Kafka consumer group and take partitions from the context the Kafka tests observe.

## Phase 7 — demo experience
52. The demo is a separate Compose project, `ledgerflow-demo`, with its own volume: `infra/docker-compose.yml` plus the overlay `infra/docker-compose.demo.yml`. `start.sh`, `stop.sh` and `reset.sh` only ever address that project. The base Compose file and its production-like defaults are unchanged.
53. The `demo` Spring profile gates everything demo-specific (`@Profile("demo")`). The default profile has no dashboard, no `/demo/**` and no CORS; an e2e test asserts this against the default-profile stack.
54. The demo's risk settings come from the overlay's environment, not from the profile's config: velocity limit 1,000,000/60 s so the race scenarios reach ledger concurrency, and blocked account 999999. The normal default of 5/60 s is untouched. Test contexts therefore run with `demo` active without changing the velocity tests.
55. The dashboard is plain HTML/CSS and ES modules under `payment-service/src/main/resources/dashboard/`, mapped to `/` only in the demo profile. Nothing sits in `/static`. No build step, CDN or npm.
56. Demo endpoints are narrow. Nothing on them executes SQL or shell, or controls Docker or processes.
    - ledger-service:
      - create fresh accounts, each request with its own SYSTEM treasury, funded via `LedgerService.post` (double entry, never the seeded accounts; caps: 10 accounts, $1,000,000 each);
      - read balances;
      - read the outcome and posting count per payment;
      - ledger-schema invariant counts (I1, I2 duplicates, I4, I5);
      - Postgres's deadlock counter.
    - payment-service:
      - batch payment status;
      - the risk settings in effect;
      - payments-schema invariant counts (I6, and I3 as "nothing stuck beyond 30 s");
      - `POST /demo/burst`.
    CORS on ledger-service allows only `http://localhost:8081` / `127.0.0.1:8081`, GET/POST.
57. `POST /demo/burst` exists because browsers keep about 6 HTTP/1.1 connections per origin, so a page cannot create real 200-way contention. It sends up to 500 ordinary `POST /api/payments` requests over loopback HTTP, all released together (virtual threads plus a latch). It can do nothing a client could not. The shell scripts get real concurrency from one background `curl` per request.
58. Invariants are shown honestly per schema. Each service can read only its own schema (DECISIONS 3), so the dashboard shows each service's checks. The cross-schema parts (COMPLETED ⇔ POSTED, no posting for FAILED/DECLINED) are verified per scenario for that scenario's payments. The whole-database cross-schema check is `./demo/run.sh invariants`, which runs `chaos/verify_invariants.sql` as the superuser.
59. Infrastructure faults (kill, restart, pause) remain shell-only (`demo/*.sh`, `chaos/`). `probe_recovery` moved from `chaos/run_chaos.sh` into `chaos/lib.sh` so demo scripts can reuse it. The probe accounts became overridable; the defaults keep the chaos campaign's behaviour.

## Phase 7 — interactive accounts, playground, resilience lab
60. **Not highly available, by design.** The local deployment runs one Postgres, one Kafka broker and one instance of each service. LedgerFlow demonstrates correctness under failure and safe recovery: retries don't duplicate money, duplicate delivery is harmless, concurrent spending can't overdraft, there are no partial ledger writes, and invariants hold. It does not demonstrate zero downtime. The System page states this ("Local single-instance demo / High availability: Not configured").
61. **Named demo accounts** (Jay, Ajay, user-created). The `demo` profile creates a label table `demo_account_names(account_id, name, treasury_account_id)` at startup (`CREATE TABLE IF NOT EXISTS`), not via Flyway, so the normal-profile schema is unchanged. The table holds labels only. Each named account is a normal CUSTOMER account funded from its own SYSTEM treasury through `LedgerService.post`. On an empty table the demo creates Jay ($75) and Ajay ($100), i.e. after every `./reset.sh`. Names are unique, ignoring case. "Set example balances" moves differences between a named account and its own treasury with ordinary balanced postings; it never writes a balance directly, and only named demo accounts qualify.
62. **Transaction Playground runner** (`POST /demo/transactions`, 2–5 transactions, start delay 0–10 s). Each transaction runs on its own virtual thread: it waits its delay, then sends a normal `POST /api/payments` over loopback with the browser-generated idempotency key, then follows the payment to a final state by polling every 20 ms (up to 60 s). It reports times the runner itself measured: sent, API response, and final state observed. Nothing else (locks, Kafka) is claimed. It never calls LedgerService directly. Locking is the ledger's ordinary ascending-id order; there is no special path for the UI.
63. **Race demos validate properties, not winners.** A run PASSes when:
    - every payment reaches a final state;
    - no balance is negative;
    - COMPLETED payments are posted exactly once and FAILED ones not at all;
    - the sum of balances is unchanged;
    - each account's balance equals its before-balance adjusted by the completed payments;
    - the invariant checks are clean.

    A FAILED payment (e.g. insufficient funds) is a correct outcome. Tests assert these properties and never which payment wins.
64. **Resilience controls are application-level and profile-isolated.** `/demo/controls` exists only under the `demo` profile:
    - ledger: pause/resume of its Kafka listener (`KafkaListenerEndpointRegistry`), and a real per-message processing delay (0–10 s);
    - payment: pause/resume of its result listener, and "lose results", which acknowledges results without recording them (no `processed_events` row), so only the existing reconciler can recover the payment.

    The listener hooks come in through `ObjectProvider` and are absent outside the profile. Docker, process, SQL and Kafka-admin control stay outside HTTP (shell scripts only).
65. The Dockerfile's `# syntax=docker/dockerfile:1` line was removed. It made every build fetch a frontend image from Docker Hub, so `./start.sh` failed offline, even with all base images cached. Docker's built-in frontend supports the `RUN --mount=type=cache` the file uses.

## Phase 8 — optional local Kubernetes
66. **Local k3d, plain YAML, no tooling layers.** The cluster is one k3d server node with no agents and no load balancer (Traefik and servicelb disabled). Manifests are plain YAML in `infra/k8s/`, applied with `kubectl apply` by `k8s/start.sh`. There is no Helm, Kustomize, Terraform, GitOps or operator. Compose stays the primary local stack; Kubernetes is optional and does not change the application code or its ledger/idempotency semantics.
67. **Shared configuration with Compose.** `k8s/start.sh` loads `infra/postgres/init` into a ConfigMap, so the roles and schemas come from the same SQL Compose uses. The Secret holds the same development passwords as the Compose stack. The `demo,k8s` profiles add only the Kubernetes-specific bits (`payment-service-k8s.yml`, `ledger-service-k8s.yml`).
68. **Images are built on the host and imported.** Both application images are built with the existing Compose build definitions, tagged `*:1.1.0-local`, and imported with `k3d image import`. The Deployments use `imagePullPolicy: Never`, so the cluster never pulls them. `start.sh` writes the image ID into a pod annotation, so a rebuilt image triggers a rollout. PostgreSQL and Kafka images are also imported, so the cluster never pulls from a registry.
69. **Stateful components are single replicas on PVCs.** PostgreSQL and Kafka (KRaft, one broker) are StatefulSets with 2 Gi `local-path` PVCs. Their data survives pod restarts (verified, see RESULTS Phase 8). Deleting the cluster deletes the data, which is why `k8s/stop.sh` says so.
70. **Probes use the Actuator health groups.** Startup probes on the liveness group (up to 180 s for the JVM to start), readiness probes that gate traffic, and liveness probes that restart a stuck JVM. Kafka and PostgreSQL get readiness and liveness probes at their own ports.
71. **DiskPressure on a full laptop disk.** The first deployment was evicted for DiskPressure. The k3s defaults use a 10% minimum reclaim, which on this ~100 GiB disk means the kubelet tries to free about 10 GiB before it stops evicting. Phase 8 now sets absolute values on the cluster server:
    - `eviction-hard`: `nodefs.available<1Gi,imagefs.available<1Gi`;
    - `eviction-minimum-reclaim`: `nodefs.available=256Mi,imagefs.available=256Mi`.

    `k8s/start.sh` passes these flags when it creates the cluster. The first cluster was repaired in place with a node-local drop-in file. A cluster later created from scratch has no drop-in, and its effective kubelet config (read from the node's `configz`) shows `evictionHard` 1Gi and `evictionMinimumReclaim` 256Mi from the flags alone. Image GC thresholds are also set to 100/99 so the imported images are not garbage-collected. This is a local-disk decision. It is not a production sizing guide.
72. **Scripts never delete data.** `start.sh` is idempotent: an existing cluster is started, not recreated. `stop.sh` deletes the cluster and its volumes, and says so. The scripts refuse to start while the Compose demo holds ports 8081/8082. `status.sh`, `logs.sh` and `demo-restart.sh` only read state, or delete a single pod in `demo-restart.sh`.
73. **Recovery demo reuses the existing helpers.** `k8s/demo-restart.sh` uses `demo/lib.sh` and `chaos/verify_invariants.sql`. Only the fault (`kubectl delete pod`) and the database access (`kubectl exec`) are Kubernetes-specific. The shared probe timer starts after the replacement pod is Ready, not at the deletion, so this script labels that line "Post-ready convergence" (`RECOVERY_LABEL`; the Compose demos keep "Recovered after"). The deletion-to-Ready time is printed separately.
74. **Not highly available.** One k3d node, one replica of each service, one PostgreSQL and one Kafka broker. Pod restarts are recovered by Kubernetes; the infrastructure single points of failure remain. The docs do not claim zero downtime, HA, multi-node, production readiness or Kafka HA.
