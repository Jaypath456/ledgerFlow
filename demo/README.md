# demo/

Prepared, self-checking demonstrations against the running demo stack (`./start.sh`).

```
./demo/run.sh --help
./demo/run.sh hot-account
./demo/run.sh all            # every application scenario
```

- Every scenario creates **fresh accounts** through the ledger's demo endpoint. Each request gets its own SYSTEM treasury, and funding uses normal double-entry postings, so the Flyway-seeded accounts are never touched.
- Each script measures what happened (HTTP codes, payment statuses, balances, ledger postings, Postgres deadlock counter, invariant checks) and prints `PASS` or `FAIL`. It exits non-zero on failure (2 if the stack is not running).
- Concurrency is real: one background `curl` per request, all in flight together.
- `ledger_crash_recovery.sh`, `kafka_restart.sh` and `postgres_pause.sh` inject an infrastructure fault from the shell during background traffic. Each then measures recovery (a new payment completing end to end), retries failed requests with the same idempotency key as a real client would, waits for settlement, and runs `chaos/verify_invariants.sql`. They reuse helpers from `chaos/lib.sh`.

These are demonstrations. The automated test suite is `./mvnw verify`; the load/chaos campaign is in `chaos/`.
