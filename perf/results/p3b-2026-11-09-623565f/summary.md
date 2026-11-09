Quiet-host steady runs, Roadmap Phase 3b, 2026-11-09 (six SINGLE runs)
Code: 623565f (service code as in Roadmap Phase 3 with apache/kafka 4.0.2 and the host-gated perf/run.sh; the git field of each environment.txt reads 623565f, a clean tree)
Environment: MacBook Air (Apple M4), 10 CPUs, 16 GiB RAM, Docker Desktop (VM 10 CPUs, 9.701 GiB), host k6 v2.3.0, postgres:17.11, apache/kafka:4.0.2, 1 GiB limit per service container,
cost-4 load credentials (ADR 0003), RISK_VELOCITY_MAX=1000000 (ADR 0002), TRACING_SAMPLING_PROBABILITY=0.1, observability profile off.
Method: `perf/run.sh steady` at RATE=840 (60 s warm-up excluded, 180 s measured), a fresh stack per run, runs alternating a (3 partitions, 3 consumer threads) and b (TOPIC_PARTITIONS=6, LISTENER_CONCURRENCY=6).
Driver: .overnight/p3b-step2.sh with the host gate on (AC power and at most 1024 MiB of swap in use, checked by perf/run.sh before every start).

Host state at the start of each run (from its environment.txt; every run passed the gate):
| run | start | power | swap used | memory free | load averages (1/5/15 min) |
|---|---|---|---|---|---|
| a1 | 2026-11-09 11:43:18 EST | Now drawing from AC Power | 0.00M | 48% | 5.77 12.72 7.14 |
| b1 | 2026-11-09 11:47:58 EST | Now drawing from AC Power | 300.56M | 38% | 5.50 8.50 6.76 |
| a2 | 2026-11-09 11:52:37 EST | Now drawing from AC Power | 732.69M | 38% | 7.58 8.07 7.07 |
| b2 | 2026-11-09 12:45:44 EST | Now drawing from AC Power | 878.00M | 62% | 1.92 1.55 1.64 |
| a3 | 2026-11-09 12:50:21 EST | Now drawing from AC Power | 854.00M | 46% | 6.31 4.95 3.20 |
| b3 | 2026-11-09 12:54:59 EST | Now drawing from AC Power | 854.00M | 40% | 7.61 5.97 4.11 |

Results (copied from each run's results.json; CPU = docker stats median over the measured window, 100 % = one core):
| run | offered/s | achieved/s | failed | POST p50/p95/p99 ms | request-to-terminal p50/p95/p99 ms | payment CPU med % | ledger CPU med % | postgres CPU med % | violations |
|---|---|---|---|---|---|---|---|---|---|
| a1 (3 partitions, 3 threads) | 840 | 840.0 | 0 | 2.3/4.6/11.0 | 33/46/79 | 127.2 | 35.4 | 94.6 | 0 |
| a2 (3 partitions, 3 threads) | 840 | 840.0 | 0 | 2.4/5.0/12.7 | 34/48/71 | 131.5 | 38.0 | 103.5 | 0 |
| a3 (3 partitions, 3 threads) | 840 | 840.0 | 0 | 2.3/4.7/13.7 | 34/47/84 | 128.9 | 36.7 | 100.6 | 0 |
| b1 (6 partitions, 6 threads) | 840 | 840.0 | 0 | 2.4/5.4/26.0 | 34/46/112 | 138.6 | 45.1 | 108.3 | 0 |
| b2 (6 partitions, 6 threads) | 840 | 840.0 | 0 | 2.3/4.8/11.8 | 33/45/52 | 134.0 | 43.0 | 103.3 | 0 |
| b3 (6 partitions, 6 threads) | 840 | 840.0 | 0 | 2.4/6.3/42.6 | 34/48/146 | 135.9 | 44.4 | 109.8 | 0 |
All six runs: 0 failed requests, 0 declined, 0 payments pending after the drain. Accepted 151,200 to 151,204 per run. k6 dropped iterations: 0 in a1, a2, a3 and b2; 118 in b1 and 45 in b3 (k6 grew to 395 and 332 VUs, against 300 in the other runs).
For comparison, the three baseline runs at the same rate on 2026-11-07 (perf/results/baseline-2026-11-07-*/steady-1..3): POST p99 19.3, 30.3, 30.0 ms; request-to-terminal p99 142, 202, 164 ms; payment CPU 142.8, 138.6, 148.9 %; ledger CPU 43.5, 40.9, 43.1 %.

What these runs can and cannot say: 840/s is 70 % of the last sustained rate of the 3-partition baseline (1,200/s), i.e. below the knee, so they show no throughput difference between a and b (all 840.0/s).
In these runs the 6-partition configuration used more ledger CPU (43.0 to 45.1 % against 35.4 to 38.0 %) and had a wider POST p99 range (11.8 to 42.6 ms against 11.0 to 13.7 ms); with three runs each this is an observation, not a result.
They do not answer the commit-path question (no synchronous_commit runs) and say nothing about saturation or about the scaled topology.

NOT MEASURED in this session (driver steps that did not run):
- c-A1, c-B, c-A2 (synchronous_commit A/B/A steady), ramp-A and ramp-sync-off (ramps), scaled-ramp, and the scaled steady runs (never started).
- Why: per the person who ran the driver, the driver skipped them because the gate was closed and then the time ran out; it finished at 14:29:30. The driver's own log of that run is not in the repository or under .overnight, so this file cannot show when the gate closed.
  The only on-disk records are the per-run host states above. They show no run between a2 (started 11:52:37) and b2 (started 12:45:44), an interval of 53 minutes, and no record of the host during it.
- The earlier driver window, 04:54 to 06:43 on the same day, is recorded in perf/results/p3b-2026-11-09-b96d66e/host-gate-log.txt: 28 samples, AC power, swap used 5,707 to 6,782 MiB throughout (gate threshold 1,024 MiB). No run was started in that window.
