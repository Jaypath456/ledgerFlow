# ADR 0003: Cheap-hash credentials for load and chaos runs

Status: proposed (Roadmap Phase 1b), 2026-11-05.

## Context

Phase 1b put HTTP Basic authentication in front of the payment API (`PaymentSecurity`). The API is stateless, so every request verifies
a password. Configured passwords are hashed at startup with Spring's delegating encoder: bcrypt at cost 10. Load and chaos runs
authenticate each payment as its payer's own identity (`payer-<id>`, one generated credential per seeded account, `chaos/lib.sh`), so
every k6 request, probe and hot-account request pays for one cost-10 bcrypt check.

The first Phase 1b chaos run measured recovery of 40.8–48.7 s in 7 of 9 runs, whatever the fault. The investigation
(`docs/evidence/recovery-investigation-2026-11-05-2b58c27.txt`) found the cause. At 100 requests/s, bcrypt saturated payment-service
CPU (median 864–1018 % in docker stats, 100 % = one core). Requests queued for seconds, and the recovery probe (5 s timeout) could not get
through until the load ended. A fault-free 100/s run with the shipped cost reached 54.0 payments/s, with first-attempt POST p50 6775.2 ms
(single run). The October PostgreSQL campaign ran before authentication existed, so its numbers did not include this cost.

The chaos and load runs exist to measure failure recovery and throughput of the payment/ledger pipeline, not password hashing.

## Decision

- `ledgerflow.auth.bcrypt-strength` is an optional setting. Unset (the shipped default in `payment-service.yml`, k8s and the delivery
  overlay), the encoder is exactly the previous one: `PasswordEncoderFactories.createDelegatingPasswordEncoder()`, bcrypt cost 10.
- When set, the identities are hashed with bcrypt at that strength. The encoder is the application's `PasswordEncoder` bean, so the same
  strength verifies logins and Spring Security never re-hashes ("upgrades") a password on login.
- Only `infra/docker-compose.load.yml` (used by `chaos/run_chaos.sh` and `chaos/bench.sh`) sets it, to 4 by default
  (`LEDGERFLOW_AUTH_BCRYPTSTRENGTH: ${LOAD_BCRYPT_STRENGTH:-4}`). `LOAD_BCRYPT_STRENGTH=10` runs the same load at the shipped cost.
- Authentication stays on in every run, and every identity still owns only its own account. The load credentials are generated per
  project and stay in git-ignored `chaos/results/`.
- Results that come from load or chaos runs say that they use cost-4 load credentials.

With cost 4 in the same experiment, the fault-free 100/s run reached 100.0/s with POST p50 3.6 ms and payment-service CPU median 37 %
(single run).

## Alternatives

- **Pre-hashed `{bcrypt}` values at a low cost in the load config.** No Java change, but `PaymentSecurity` encodes every configured
  password, so a pre-hashed value would be hashed again as a plain password. Rejected.
- **Turn authentication off for load runs.** Would also remove the ownership checks from the measured path. Rejected.
- **Lower the shipped cost.** Changes the security setting of the system as shipped to suit a benchmark. Rejected.
- **Cache successful verifications, or session tokens.** Changes how authentication works; a Phase 3 question, not a Phase 1b fix.
- **Keep cost 10 in all runs.** Then the chaos numbers measure CPU saturation from hashing, and the fault under test barely shows.

## Consequences

- Chaos and load numbers from Phase 1b on are with cost-4 load credentials. They are not the throughput of the shipped configuration.
- With the shipped cost, this laptop cannot authenticate 100 requests/s (single fault-free run: 54.0/s). Phase 3 has to decide whether
  benchmarks keep the cheap hash, and how the shipped system should handle the authentication cost (for example a cost decision, caching,
  or tokens).
- One more configuration key. It is covered by `PaymentSecurityTest` (default cost 10, cost 4, no upgrade on login, invalid strength).
