# ADR 0001: PostgreSQL over MongoDB

Status: accepted (Roadmap Phase 1a/1b), 2026-11-05.

## Context

LedgerFlow ran on PostgreSQL 17 up to tag `v1.2.0-postgres`, then moved to a MongoDB 8.0 single-node replica set (tag
`v1.3.0-mongodb`). The v2.0 roadmap needs one database for relational correctness, observability and scaling work. Both versions have
a committed 27-run chaos campaign with the same runner and scenarios (9 scenarios × 3 runs, k6 at `rate=100.0/s`), on the same laptop
with Docker Desktop. The host line in `docs/evidence/verify-2026-10-25.txt` reads MacBook Air, 10 CPUs, 16 GiB RAM. Full environment
notes are in `docs/RESULTS.md`.

- PostgreSQL: `docs/evidence/chaos-campaign-2026-10-25-75c618e.csv`
- MongoDB: `docs/evidence/chaos-campaign-mongodb-2026-10-26-6a63c33.csv`

Both campaigns ran before authentication and strict velocity were added, so the comparison is like for like.

| From the CSVs | PostgreSQL | MongoDB |
|---|---|---|
| Runs passed | 27 / 27 | 27 / 27 |
| Invariant violations | 0 | 0 |
| New DLT messages | 0 | 0 |
| k6 payments accepted | 157,083 | 152,815 |
| k6 retries with the same key | 16,734 | 26,297 |
| k6 payments abandoned after 5 attempts | 896 (in 8 runs) | 3,563 (in 10 runs) |
| JVM halts recorded (crash scenarios) | 30 | 12 |

| Scenario (3 runs each) | Recovery s, PostgreSQL | Recovery s, MongoDB | Abandoned per run, PostgreSQL | Abandoned per run, MongoDB |
|---|---|---|---|---|
| kill-ledger | 42.0–42.1 | 54.2–54.6 | 0 | 0 |
| kill-payment | 40.5–40.6 | 42.8–43.5 | 231–241 | 386–461 |
| pause database (10 s) | 0.6 | 1.8–2.3 | 0 | 0 |
| restart-kafka | 13.6–16.4 | 20.5–25.6 | 0 | 0 |
| replay | 6.4–6.6 | 29.6–32.0 | 7–27 | 460–534 |
| hot-account | 0.6–0.7 | 1.5–2.8 | 0 | 0 |

First-attempt POST p95 (k6 column) in scenarios where payment-service stays up: kill-ledger 2.9–3.5 ms on PostgreSQL against
12.3–73.8 ms on MongoDB; restart-kafka 2.4–2.8 ms against 19.7–27.2 ms.

## Decision

PostgreSQL 17 is the only database: one database `ledgerflow`, a schema and role per service, `JdbcClient`, Flyway migrations, and database
constraints. MongoDB code, config and infra are removed. The MongoDB build stays available at tag `v1.3.0-mongodb`.

Reasons:
- **Same correctness.** Both versions are equally correct in the evidence (27/27, 0 violations, 0 DLT).
- **Better under faults.** PostgreSQL recovered faster in every non-crash scenario above and abandoned fewer client requests overall
  (896 against 3,563). The largest gap is in `replay`.
- **Simpler correctness mechanisms.** Constraints carry part of the invariants: CHECKs, unique keys, a deferred FK from the idempotency key
  to its payment, and non-negative balances. The invariant checker is one SQL statement across both schemas. Strict velocity is one guard
  row lock in the payment transaction (`RiskRule.Velocity`), where MongoDB needed a guard document, a version bump and a whole-transaction retry on
  first-use races.
- **Fits the roadmap.** The planned work (lock-ordered ledger postings, batched postings, metrics on outbox backlog) is expressed directly
  in SQL.

## Alternatives

- **Stay on MongoDB.** Also correct in the evidence, but slower to recover and with more abandoned requests in these runs, and with more
  application code for transactions and strict velocity.
- **Support both.** Doubles the persistence code, tests and chaos campaigns for no correctness gain. Rejected.

## Consequences

- Phase 1b ports authentication, strict velocity, OrderingTest, the delivery demo and k8s to PostgreSQL, with every test from
  `v1.3.0-mongodb` adapted. Its test counts go into `docs/RESULTS.md` with the Phase 1b evidence.
- The numbers above are from campaigns on earlier code: PostgreSQL at `75c618e`, MongoDB at `6a63c33`. A chaos run on the Phase 1b code
  (with auth on) is still to be done and may differ, for example because every request now pays for a bcrypt check.
- Limits of the comparison: each scenario has 3 runs on one laptop with one broker and one database node. Crash-scenario JVM halts differ
  between campaigns (30 against 12), so crash-scenario timings are not compared. The cause of the slower MongoDB restarts was not measured.
