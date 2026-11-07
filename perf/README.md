# perf: load and saturation measurements (Roadmap Phase 3)

`perf/run.sh` starts a **fresh Compose stack**, seeds it, drives it with k6 and writes everything needed to read the run later into
`perf/results/<name>/`. Authentication stays on: every request uses its payer's own generated credential (`chaos/lib.sh`), hashed at
bcrypt cost 4 by `infra/docker-compose.load.yml` (ADR 0003). The velocity limit is raised with `RISK_VELOCITY_MAX=1000000` (ADR 0002), as
`chaos/bench.sh` does, so the load reaches the ledger instead of being declined. The results are about the payment and ledger pipeline,
not about password hashing or the velocity rule.

## What you need
Docker Desktop (the project assumes a 10 GB VM; one stack at a time), JDK 25 (`JAVA_HOME`) only if you build outside Docker, `jq`,
`python3`, and **k6 on the host** (`brew install k6`; the runs here used v2.3.0). Without a host k6, `K6=docker` uses `grafana/k6:2.3.0`
with `--memory 1g` and `--network host`, as `chaos/lib.sh` does.

## Reproduce
```sh
# 1. Find saturation: stair-step ramp (offered rate grows by STEP_RATE every STEP_SECONDS)
START_RATE=200 STEP_RATE=100 MAX_RATE=2400 STEP_SECONDS=30 perf/run.sh ramp my-ramp
# 2. Steady state at about 70 % of the last sustained rate (read it from perf/results/my-ramp/summary.md)
RATE=<70 % of the last sustained offered rate> perf/run.sh steady my-steady-1
```
Useful switches: `PROJECT` (default `ledgerflow-p3-perf`), `TRACING_SAMPLING_PROBABILITY` (default 0.1), `OBSERVABILITY=1` (adds the Prometheus,
Grafana and Jaeger profile, ADR 0004), `SCALE=1` (scaled topology, ADR 0006), `DIAGNOSE=1` (also samples PostgreSQL waits and locks every
~4 s and, with `OBSERVABILITY=1`, Prometheus queries and the slowest Jaeger traces into `<result>/diag/`), `REUSE=1` (use the running
stack), `KEEP=1` (leave the stack up), `RESULT_DIR`. Results are never overwritten; the stack is always stopped with `down` (volumes are
kept: remove them yourself with `docker volume rm <project>_pgdata`).

## What a run records
| File | Content |
|---|---|
| `environment.txt` | evidence `Code:` line, date, git SHA and whether the tree differs from HEAD, host and Docker VM, host state (power source, swap, free memory, load; throughput from a host under memory pressure is not comparable), load generator, compose command, settings, every container's image and memory limit, the performance-relevant environment of each service (no credentials) |
| `k6-summary.json` | per step: offered rate, accepted, declined, 4xx, 5xx, no response, POST p50/p95/p99/max (client side, from k6) |
| `e2e.json` | per step, from PostgreSQL: payments, terminal states, pending after the drain, **request-to-terminal** p50/p95/p99 (`updated_at - created_at`) |
| `stats.csv`, `backlog.csv` | `docker stats` every ~2 s for each container; pending payments plus unpublished outbox rows every ~5 s |
| `results.json`, `summary.md` | the combined, per-step report (`perf/report.py`), including CPU and memory per container and invariant violations |
| `diag/` | with `DIAGNOSE=1`: `pg-activity.csv`, `pg-locks.csv`, `prometheus.jsonl`, `jaeger-summary.txt` |

Idempotency keys are `perf-<run>-s<step>-...`, which is how the database latency is split per step.

## Method and caveats
- **Ramp.** `ramping-arrival-rate`, no client retries. A step is *saturated* when the achieved rate is below 95 % of the offered rate,
  any request failed, the POST p95 is above 1 s, or the backlog at the end of the step is above 200. The report prints the first
  saturated step and the last sustained offered rate; the steady target is 70 % of the latter.
- **Steady.** `constant-arrival-rate` for `WARMUP_SECONDS` (default 60, excluded: the JVMs are cold) plus `DURATION_SECONDS` (default 180,
  measured). CPU and memory are the median and maximum of the samples inside the measured window.
- **Load shape.** 200 seeded payers (one credential each) pay one of 20 merchants, amounts 100 to 999 minor units, one fresh
  idempotency key per request.
- **Same machine.** k6, Docker and the stack share one laptop; k6 on the host takes CPU from the Docker VM. Single runs on one machine:
  compare runs made on the same day with the same settings only.
- `docker stats` CPU is per container, 100 % = one core. Percentages from different measuring conditions are not comparable
  (see `perf/results/cpu-gap-*/summary.md`).
- No profile is on for benchmark numbers: tracing at the default sampling still creates spans but exports nothing.
