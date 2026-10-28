# LedgerFlow local delivery — 2026-10-28

This delivery adds server-side local identity/account authorization and strict rolling velocity admission to the existing MongoDB/Kafka implementation. It preserves the payment, idempotency, double-entry, outbox, deduplication and reconciliation contracts. Kafka partition keys were investigated and retained. The later [independent review remediation](REVIEW_REMEDIATION.md) records broker preservation, optimization-safe smoke checks, sanitized distributable evidence, and their reruns. The historical run results below are retained.

## Environment and baseline

- Starting commit: `1a39aed43f7d144f594b329fa4c8e65035f677a5` ([baseline-commit.txt](evidence/delivery/baseline-commit.txt)); `main` tracked `origin/main`, with a clean working tree before evidence collection.
- macOS 27.0.1, Darwin 27.0.0, arm64; Temurin JDK 25.0.4.1; Maven wrapper 3.9.11.
- Docker Desktop 4.95.0, Engine 29.8.2, Compose 5.6.0, approximately 9.7 GiB available to Docker.
- Actual dependencies: Spring Boot 4.1.1; Spring Kafka 4.1.1; Kafka Java client 4.2.1; MongoDB Java driver 5.8.1; Testcontainers 2.0.5. Authentication adds Spring Security 7.1.1, managed by Boot.
- Runtime images: `mongo:8.0` resolved to MongoDB 8.0.34; Kafka broker `apache/kafka:4.0.0`. Broker and Java client versions differ; the real integration tests exercise that combination. [Runtime MongoDB/majority concern](evidence/delivery/mongo-runtime.log), [container JRE](evidence/delivery/container-java.log), [topic partitions](evidence/delivery/final-topics.log).
- Exact environment: [environment.log](evidence/delivery/environment.log). Resolved dependencies: [baseline](evidence/delivery/baseline-dependencies.log), [final](evidence/delivery/final-dependencies.log).

Commands run from the repository root:

```sh
git status --short --branch
git rev-parse HEAD
java -version
/usr/libexec/java_home -V
docker version
docker compose version
docker ps -a --format '{{.Names}}\t{{.Status}}\t{{.Image}}'
docker volume ls
docker compose -f infra/docker-compose.yml up -d --build
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw -B verify
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw -B dependency:tree \
  '-Dincludes=org.springframework.boot:*,org.springframework.kafka:*,org.springframework.security:*,org.mongodb:*,org.apache.kafka:kafka-clients,org.testcontainers:*'
docker compose -f infra/docker-compose.yml exec -T mongo \
  mongosh admin --quiet -u local_admin -p local_admin --file /dev/stdin \
  < chaos/verify_invariants.js
```

Baseline result: **58 tests passed, zero failures/errors/skips**, including existing DLT, duplicate-message, idempotency, crash-point, reconciliation, randomized and concurrent ledger tests. All four Compose services became healthy. The existing smoke scenarios (funded payment, insufficient funds, exact replay) passed against that stack. Their HTTP operations were reproduced without invoking the old smoke wrapper's `down -v` cleanup. The baseline invariant checker returned no violations.

Evidence: [Maven](evidence/delivery/baseline-verify.log), [Compose build/start](evidence/delivery/baseline-compose.log), [smoke and reproduced failures](evidence/delivery/baseline-smoke.log), [invariants](evidence/delivery/baseline-invariants.log), [service logs](evidence/delivery/baseline-services.log).

Existing guarantees confirmed in source and executed tests:

- `MongoStore.transaction` uses driver `session.withTransaction`, snapshot read concern and **majority write concern**, including conflict-label unwrapping so driver retries apply. Runtime `getDefaultRWConcern` also reported implicit `w: majority` on `rs0`.
- Payment/key/request-outbox commit together. Ledger dedupe/debit/credit/entries/stored outcome/result-outbox commit together. Unique indexes protect payment postings, outcomes and event/key records. Non-SYSTEM debit is conditional on sufficient funds; overflow checks precede ledger writes.
- Outbox rows are marked only after Kafka acknowledgements; producer `acks=all` and idempotence are configured. A publish/mark gap can redeliver; consumer deduplication absorbs it.
- Three partitions per topic and DLT, three listener threads, retry/DLT handling, pending-only result transitions, and reconciliation of stale pending payments are already implemented. Reconciliation uses a fresh event ID and the ledger's durable outcome.

## Reproduced failures and fixes

1. **Missing API boundary:** an unauthenticated HTTP POST returned 202 and completed, debiting account 2 by 2500 minor units. Default callers could choose the payer. Authentication now returns 401 without valid credentials and 403 for an unowned payer, before idempotency or payment writes.
2. **Soft velocity overflow:** 32 concurrent requests against the configured limit of 5 admitted **27**, declining only 5. A per-payer transaction guard now serializes rolling-window admission. The same class of burst admits exactly 5 in both API and full-system verification.

No Kafka-key financial failure was observed. No partition-key change or ledger rewrite was made.

Verification failures were retained: the first ordering run failed three invariant-harness assertions because `mongosh --eval` printed `[Function: check]` as its return value. Appending `void 0` fixed the harness; the financial assertions had passed. See [initial failure](evidence/delivery/ordering-verify.log) and [successful rerun](evidence/delivery/ordering-recheck.log). Initial sandbox Docker access was denied; authorized execution outside that sandbox succeeded. An initially unquoted dependency filter was rejected by zsh glob expansion; the quoted command above succeeded. These were environment/harness issues, not discarded financial failures.

## Local identity and velocity design

Before implementation, the proposed mapping was explained in the task conversation: `alice` owns payer 2, `bob` owns payer 4, and test/load identities receive explicit fixture account lists. Spring Security HTTP Basic verifies credentials server-side; the controller checks the configured payer authority before calling payment creation. GET is restricted to payer/payee owners and returns 404 to other authenticated callers. Replays undergo the same ownership check. Passwords are encoded with the delegating password encoder, using bcrypt by default.

The normal defaults are `alice:alice-local` and `bob:bob-local`. Override `ALICE_PASSWORD`, `BOB_PASSWORD`, `ALICE_ACCOUNTS`, `BOB_ACCOUNTS`; account values are comma-separated positive IDs. There is no wildcard account permission, registration API, identity provider or OAuth. Multiple configured identities that share an account intentionally share permission for it. Idempotency keys retain their original global scope.

This CLI-only API requires JSON and rejects browser-origin POSTs, uses no session/cookie authentication or CORS, and exposes health without credentials. These are the conditions supporting disabled CSRF tokens; Basic authentication by itself does not prevent browser CSRF. References consulted: Spring Security [Basic authentication](https://docs.spring.io/spring-security/reference/7.0/servlet/authentication/passwords/basic.html) and [CSRF](https://docs.spring.io/spring-security/reference/7.0/servlet/exploits/csrf.html).

Strict velocity writes `velocity_guards[_id=payer].version` before counting recent non-DECLINED payment records, inside the same transaction as key/payment/outbox. Concurrent writes to the same guard force conflicting snapshots to abort and retry. A first-use upsert duplicate retries the entire payment separately from an idempotency-key collision. The count remains a rolling 60-second query over payment history, including history created before this change. It is not a fixed bucket or an independently maintained reservation count. Replays bypass risk rules, aborted transactions roll back the guard and payment together, and declines produce no request outbox. Insufficient-funds ledger failures remain admitted payments and count toward velocity.

## Tests and operational evidence

Executed increments:

```sh
# Authentication: 25 tests passed
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw -B -pl payment-service -am verify
# Strict velocity: 28 tests passed
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw -B -pl payment-service -am verify
# Ordering investigation: 3 new end-to-end tests passed on rerun
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw -B -pl e2e-tests -am \
  -Dtest=OrderingTest -Dsurefire.failIfNoSpecifiedTests=false verify
# Full final regression
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw -B verify
# Fresh isolated real-fault demonstration
bash scripts/smoke.sh --extended --output docs/evidence/delivery/system
```

Evidence: [authentication](evidence/delivery/auth-verify.log), [velocity](evidence/delivery/velocity-verify.log), [ordering](evidence/delivery/ordering-recheck.log), [full final suite](evidence/delivery/final-verify.log), [live demonstration](evidence/delivery/system-demo.log), [scenario results](evidence/delivery/system/results.json), [exact Docker commands](evidence/delivery/system/commands.jsonl), [demo environment](evidence/delivery/system/environment.json). The checked-in [command output archive](evidence/delivery/system/command-outputs.tar.gz) contains all 108 command-numbered outputs, byte-verified before archival; [SHA-256 manifest](evidence/delivery/system/outputs.sha256). New runs leave these files directly beside `commands.jsonl`.

Final verification passed: **66 tests, zero failures/errors/skips**, plus **11 live scenarios**, with zero financial invariant violations. Detailed totals are below.

The new tests exercise 64 concurrent HTTP requests against one fresh payer (5 admitted, 59 declined), declined-response replays, rolling expiry without a 60-second sleep, history predating a guard, rollback without a consumed slot, unauthorized replay attempts, participant-only reads, and actual end-to-end hot-account balances.

Ordering investigation:

- Select payer keys hashing to all three request partitions. Twelve concurrent payments from three funded payers share one receiver; exactly two per payer complete and two fail for insufficient funds. Each payer ends at 200, the receiver at 2400. Replay original events and fresh events for every payment; terminal outcomes and balances remain stable, with one posting per completed payment and none for failed payments.
- Opposite-direction payments use payer keys on different partitions. Stop both consumer groups, submit concurrent payments, restart consumers and verify reassignment of all three partitions in each group. Every payment completes once; the two initial balances remain 1000 each.
- Request outbox keys remain payer IDs; every emitted result key remains its payment ID. Existing Kafka tests also read actual topic records and assert keys and duplicate handling.
- A partition orders broker append order, not HTTP arrival order or financial dependencies across accounts/topics. The outbox may publish committed requests in a different order from arrival. Shared-account safety relies on MongoDB conditional debits, transaction conflict retries and unique posting/outcome records. Concurrent sufficient-funds winners and latency/fairness are not promised. Reconciliation reuses stored successes or rejections even if balances later change.

Recovery coverage combines the executed existing tests with the fresh live demonstration:

| Scenario | Financial assertion |
|---|---|
| Same key/payload, including 32-way storm | One payment/outbox/posting; original bytes and status replay |
| Same key/different payload | 422; original amount and single effect preserved |
| Insufficient funds | FAILED/INSUFFICIENT_FUNDS; no posting or balance change |
| Concurrent double spend | Existing 200-way ledger test: exactly 50 succeed and 150 fail; payer ends at zero; no other error |
| Duplicate Kafka delivery and new-event replay | Single posting, durable outcome reused, terminal state stable |
| Poison requests and results | Both existing DLT paths exercised; valid processing continues |
| Payment-service process kill | Accepted pending payment survives SIGKILL; exact replay after restart; completes once |
| Ledger-service restart | Pending payment has no debit while ledger is stopped; completes once after restart |
| MongoDB interruption | Submission blocks while MongoDB is paused, resumes and completes once after unpause |
| Kafka interruption / outbox publish failure | Accepted request stays unpublished with no debit during outage; observed publish failure retries after broker restart |
| Before payment commit / after publish-before-mark / after ledger commit-before-ack | Existing deterministic fault-injection tests verify rollback or duplicate absorption |
| Reconciliation | Existing real-service tests cover a never-processed request, lost result after posting, and a durable rejection even after later funding |

## Reproduce the demonstration

Requirements: Docker Desktop/Compose, Python 3, JDK 25 for Maven. No reset, volume removal or Kubernetes teardown is required.

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 25)
./mvnw -B verify
bash scripts/smoke.sh --extended
```

The script picks a new `ledgerflow-delivery-*` project, refuses existing project containers or its named MongoDB volume, and checks that ports 18081/18082/37017/39092 are free. It seeds its own accounts with balanced treasury postings and configures an explicit Alice/Bob inventory. It verifies successful payment, risk decline, insufficient funds, exact replay/conflict, 401/403, concurrent velocity, and ledger restart. `--extended` adds payment SIGKILL, MongoDB pause, Kafka outage with an actual failed publish, and poison messages on both topics. It checks the existing full-history I1–I6 script after each settled scenario.

The default evidence directory is `chaos/results/<project>`. A run leaves services and data available and prints the exact `docker compose ... stop` command. To run again, stop that project's containers or choose different ports, and let the script choose a new project. All four ports can be overridden. `--output` must name a new directory.

For a normal default Compose deployment (existing volumes retained):

```sh
docker compose -f infra/docker-compose.yml up -d --build
curl -u alice:alice-local -X POST http://127.0.0.1:8081/api/payments \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: delivery-example-1' \
  -d '{"payerAccountId":2,"payeeAccountId":5,"amountMinor":1500,"currency":"USD"}'
curl -u alice:alice-local http://127.0.0.1:8081/api/payments/PAYMENT_UUID
```

Seed balances are not reset on an existing database; outcome depends on available funds and recent admissions. Reuse the same key/payload to replay the first response.

## Changed files

- `payment-service/pom.xml`, new `PaymentSecurity.java`, `PaymentController.java`, `payment-service.yml`: local identity boundary and account authorization.
- `RiskRule.java`, `PaymentService.java`: transaction guard and separate first-use guard retry; preserve real idempotency collision handling.
- `PaymentApiTest.java`, `e2e-tests/.../Stack.java`, new `OrderingTest.java`: authenticated clients and security/concurrency/ordering regression coverage.
- `infra/docker-compose.yml`: loopback application/MongoDB bindings and local identity inventory/password environment. Kafka's original defaults are retained; `infra/docker-compose.delivery.yml` supplies the isolated demo's Kafka binding and port.
- `scripts/smoke.sh`, new `scripts/demo.sh` and `scripts/demo.py`: isolated, data-preserving reproducible demonstration and fault evidence.
- `chaos/lib.sh`, `chaos/load.js`, `chaos/run_chaos.sh`, `chaos/bench.sh`, `k8s/smoke.sh`: authenticated clients; destructive chaos reset flags refuse execution.
- `README.md`, `docs/PLAN.md`, `docs/DECISIONS.md`, `docs/RESULTS.md`, this report, `docs/evidence/delivery/`: scope, decisions and executed results.

## Limits and data preservation

- Local single replica of each service, one MongoDB replica-set member and one Kafka broker: downtime is expected; no high availability, failover or production security claim.
- Demo passwords and database users are public local defaults. Basic runs over loopback HTTP. The host and Docker network are trusted; MongoDB root access and plaintext Kafka can bypass the HTTP boundary. There is no multi-tenant isolation from a hostile local process or credential/account lifecycle.
- Compose Kafka has no persistent named volume in the existing architecture. Stop/start faults retain its container data; recreating that broker container is not a durability guarantee. No broker container containing pre-existing data was removed during this work.
- The base Kafka binding remains the original `29092:29092` and advertised port 29092 to preserve the existing container configuration. The isolated runner uses its overlay for loopback Kafka binding. Hardening or persisting the old broker itself requires a separately approved migration; this remediation performs none.
- Velocity retains one guard per payer and deliberately serializes hot-payer admission. The rolling window uses the application wall clock; clock rollback, distributed clocks and multiple service replicas were not tested or introduced. This is not a throughput benchmark.
- DLT routing is existing poison-message isolation, not automatic repair/replay of malformed business payloads. Test poison messages intentionally remain in the isolated DLTs. Permanent Kafka/MongoDB loss, broker disk loss and replica failover are outside the tested model.
- The old baseline `ledgerflow` project was stopped after verification, with containers and data retained. Existing `ledgerflow-chaos` containers/volumes, PostgreSQL historical volumes and unrelated `launchops-repo` services were not reset or removed. No Kubernetes command was run. The default smoke wrapper no longer deletes its volume. No new 27-run chaos campaign or Kubernetes smoke result is claimed.

## Final run results

The final `./mvnw -B verify` exited 0 at **2026-10-28 14:55:22 America/New_York**, taking 5 minutes 12 seconds. No production source changed after this verification began.

| Suite | Tests | Failures / errors / skipped |
|---|---:|---|
| payment-service | 28 | 0 / 0 / 0 |
| ledger-service | 21 | 0 / 0 / 0 |
| e2e-tests | 17 | 0 / 0 / 0 |
| Total | **66** | **0 / 0 / 0** |

The fresh project `ledgerflow-delivery-cd37ed388b` passed **11/11 live scenarios**, with I1–I6 checked after each. Both injected poison messages reached their respective DLTs; a following valid payment completed once. The observed Kafka fault included this exact line at 18:52:10.719 UTC:

```text
outbox publish failed, will retry: org.springframework.kafka.KafkaException: Send failed
```

See [observed fault excerpt](evidence/delivery/observed-faults.log); the complete service logs remain in the command archive. The original 64-way velocity API test and its full regression rerun both admitted 5/64; both full-system hot-account runs admitted 5/32.

Additional client checks ran after the demonstration:

```sh
k6 run --no-color -q -e BASE_URL=http://127.0.0.1:18081 -e RATE=2 -e DURATION=2s \
  -e PAYERS_FIRST=100006 -e PAYERS=1 -e PAYEES_FIRST=200001 -e PAYEES=1 \
  -e PAYMENT_AUTH=alice:alice-local -e OUT=docs/evidence/delivery/load-client-summary.json chaos/load.js
PROJECT=ledgerflow-delivery-cd37ed388b PAYMENT_URL=http://127.0.0.1:18081 \
  LEDGER_URL=http://127.0.0.1:18082 PROBE_PAYER=100004 PROBE_PAYEE=200001 bash -c \
  'source chaos/lib.sh; metric "$PAYMENT_URL" ledgerflow.events.duplicate; probe_recovery "$(date +%s.%N)"; settle 90; violations'
```

The [authenticated k6 client](evidence/delivery/load-client.log) reported 5 accepted, 0 declined, 0 failed, 0 retries; all completed. This small functional client check is not a performance benchmark. The [chaos helper check](evidence/delivery/chaos-client.log) successfully read the protected metric, completed its probe in 2.28 seconds, drained immediately and returned zero invariant violations. Full chaos campaigns and Kubernetes smoke were not rerun. Shell syntax checks, Python parsing and `git diff --check` also passed.

Final full-history state after those additional payments, from [final-financial-state.json](evidence/delivery/final-financial-state.json):

| Financial check | Result |
|---|---:|
| Payments / completed / failed / declined | 46 / 17 / 1 / 28 |
| Pending payments / unpublished outbox rows | 0 / 0 |
| Ledger transactions / entries (including funding) | 28 / 56 |
| Global ledger entry sum | **0** |
| Duplicate posting groups | **0** |
| Negative non-SYSTEM accounts | **0** |
| I1–I6 violations, including cached balances and idempotency | **0** |

All four delivery containers are healthy ([Compose state](evidence/delivery/final-compose.log)). The payment endpoint remains available at `http://127.0.0.1:18081`; this isolated project maps Alice to accounts 100001 and 100003–100008, Bob to 100002, with payee 200001. The accounts used in the 32-request burst ended at 9500 / 500 immediately after that scenario. The later functional probes changed other fixture balances, as recorded in the final JSON.

To stop this observed run without deleting data:

```sh
docker compose -p ledgerflow-delivery-cd37ed388b -f infra/docker-compose.yml stop
```

The old baseline containers remain stopped and the old chaos environment unchanged; [final container inventory](evidence/delivery/final-containers.log) and [volume inventory](evidence/delivery/final-volumes.log) record the preserved resources. Changes are left uncommitted for review.
