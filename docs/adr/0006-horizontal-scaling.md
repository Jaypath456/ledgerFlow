# ADR 0006: Horizontal scaling: 2 payment-service and 3 ledger-service replicas

Status: accepted (Roadmap Phase 3), 2026-11-07.

## Context

The roadmap asks for 2 payment-service and 3 ledger-service replicas behind enough partitions, plus a chaos scenario that kills one ledger replica.
The default Compose file binds each service to a fixed host port (8081, 8082) and runs one instance of each; every earlier measurement and campaign used it
and has to stay reproducible. The services were written to be run in several instances: the outbox relay and the reconciler take rows with
`FOR UPDATE SKIP LOCKED`, Kafka consumers share a consumer group, and the velocity guard row and the per-payment advisory lock live in PostgreSQL.

## Decision

An opt-in overlay, `infra/docker-compose.scale.yml`, selected with `SCALE=1` in `chaos/lib.sh` and therefore in `perf/run.sh`, `chaos/bench.sh` and `chaos/run_chaos.sh`.
Without it nothing changes.

- **Replicas are separate services**, not `scale:`: `payment-service` and `payment-service-b`, `ledger-service`, `ledger-service-b` and `ledger-service-c`
  (the added ones extend the base definitions). Each has a stable DNS name and one can be killed alone with `docker compose kill ledger-service-b`, which is how
  the new scenario kills "one of three ledger replicas".
- **No host ports on replicas.** One HAProxy (`haproxy:3.2.26-alpine`, 128 MiB, `infra/scale/haproxy.cfg`) publishes 127.0.0.1:8081 for the payment API (round robin over
  both payment replicas) and 127.0.0.1:8082 for the ledger actuator (three replicas), so k6 and the chaos scripts use the same URLs as before. It re-resolves Docker DNS every
  2 s and takes a replica out of rotation after two failed health checks (1 s apart).
- **6 partitions per topic** (`TOPIC_PARTITIONS=6`, ADR 0005; the dead-letter topics too) and consumer threads that make the groups full: payment 2 replicas x 3 threads = 6,
  ledger 3 replicas x 2 threads = 6. Checked on a running stack: all four topics have 6 partitions and each consumer group has 6 distinct consumers.
- **Memory budget.** The limits add up to 7.125 GiB (postgres 1, kafka 1, payment 2 x 1, ledger 3 x 1, haproxy 0.125) against the 9.701 GiB Docker VM
  (`docker info`: 10,416,644,096 bytes). k6 runs on the host, so it adds nothing to the VM; with the pinned Docker k6 (`--memory 1g`) the total would be 8.125 GiB.
  No service limit was reduced.
- **Chaos scenario `kill-ledger-replica`** (needs `SCALE=1`): kill `ledger-service-b` under load, start it again after 5 s, then probe, settle and run the invariant checker like the
  other scenarios.

How several replicas behave (read from the code, and confirmed on the running stack where noted):

- Outbox relay: each replica selects its own batch with `FOR UPDATE SKIP LOCKED`, so replicas never publish the same row at once.
- Reconciler: `lockStalePending` uses `FOR UPDATE OF p SKIP LOCKED`, so re-requests are not duplicated across replicas.
- Consumers: one consumer group per service; Kafka assigns partitions to replicas (confirmed: 6 consumers per group).
- Velocity: the guard row lock is in the database, so the limit stays exact across payment replicas (ADR 0002).
- Authentication: every replica builds the same in-memory identities from the same configuration; the API is stateless, so any replica can serve any request.

## Alternatives

- **`deploy.replicas` / `scale:`.** Simpler file, but no way to kill a chosen replica with Compose and no stable per-replica name.
- **nginx.** Needs a resolver and variables to follow restarted containers; HAProxy's `server ... resolvers docker` and health checks do it directly.
- **Publishing every replica on its own host port.** Rejected by the roadmap and pointless: the host scripts only need one entry point.

## Consequences

- **No ordering across payment replicas.** Two payments of the same payer sent in quick succession can reach different replicas, whose relays publish independently, so the
  ledger may see them in either order. Outcomes stay valid and idempotent and the invariants hold; which of two competing payments wins the balance is not defined by arrival order.
- **Losing a ledger replica pauses its partitions until the group notices.** The probe payment of the chaos runner maps to one partition; when that partition belonged to the killed
  replica, recovery waits for the group to reassign it. Recovery times of the three runs were 6.4, 47.5 and 1.4 s with settle times of 17, 2 and 12 s
  (`docs/evidence/chaos-postgres-phase3-2026-11-07-PENDING_FINAL.txt`). The 47.5 s run is consistent with Kafka's default 45 s consumer session timeout; this was not measured separately.
- The host ports 8081/8082 now belong to the proxy, so `scripts/smoke.sh` and the k8s scripts, which start their own stacks, are unaffected, but anything that talks to a single replica from the host
  cannot (use `docker compose exec`). Per-replica duplicate counters read through the proxy come from one replica only.

## Measured

- **Functional check on a running stack** (`perf/results/scaled-2026-11-07-PENDING_FINAL/topology-check.txt`): all four topics have 6 partitions, each consumer group has 6 distinct
  consumers, health answers through the proxy, both payment replicas serve traffic, and a 30 s run at 100/s accepted 2,998 payments with 0 failed and 0 invariant violations.
- **`kill-ledger-replica`, 3 runs** (`docs/evidence/chaos-postgres-phase3-2026-11-07-PENDING_FINAL.txt`): 3 of 3 passed, 0 invariant violations, 0 new dead letters.
- **Throughput of the scaled topology was not measured.** The scaled ramp (`perf/results/scaled-2026-11-07-PENDING_FINAL/ramp`) ran on a host under memory pressure
  (swap 14,535.62M of 15,360.00M used, recorded in its `environment.txt`): even the first step, 100/s offered, achieved 96.2/s with 79 failed requests and a POST p50 of 2,045.3 ms, and the best
  achieved rate was 187.1/s. Five JVMs and a proxy need more memory than the default stack's two JVMs, so this host state hurts the scaled topology more than the baseline; the
  result says nothing about the topology itself. The planned scaled steady runs and their same-period default-topology reference were not run. To measure it, on an idle machine:
  `SCALE=1 perf/run.sh ramp scaled-ramp`, then `SCALE=1 RATE=<70 % of the last sustained rate> perf/run.sh steady scaled-steady-1` and the same without `SCALE=1`.
