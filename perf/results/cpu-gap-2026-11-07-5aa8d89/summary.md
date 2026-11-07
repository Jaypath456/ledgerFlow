CPU gap investigation (Roadmap Phase 3, step 1), 2026-11-07
Code: current tree 5aa8d89 (service code unchanged from Phase 2) and the old tree 6e278b9 (Phase 1b merge, extracted with git archive).
Host: MacBook Air (Apple M4), 10 CPUs, 16 GiB RAM, Docker Desktop (Engine 29.8.2, Compose 5.6.0; VM 10 CPUs, 9.701 GiB). Nothing else running in Docker.
Settings: `bash chaos/bench.sh 100 60s` from each tree (docker grafana/k6:2.3.0 --memory 1g, as chaos/lib.sh), load overlay bcrypt cost 4 (ADR 0003),
RISK_VELOCITY_MAX=1000000 on the services (ADR 0002; bench.sh raises the limit so the load reaches the ledger), 1 GiB limit per container, profile off,
TRACING_SAMPLING_PROBABILITY=0.1 unless stated. A FRESH stack per row-pair (project ledgerflow-p3-<label>, `down` without -v afterwards), 15 s idle after
start, then bench #1 (cold JVMs) and immediately bench #2 on the same stack (warm). Runs alternate old, new, old, new, old, new; then new at sampling 0, new at
sampling 1.0, and new at sampling 1.0 with a docker stats loop without sleep. CPU = `docker stats --no-stream` (100 % = one core), median and max over the 60 s
bench window; sampling interval (median distance between samples): 3 s with the sleep-1 loop, 2 s without sleep. Single runs; all are SINGLE runs.

| label | phase | accepted/failed | rate | POST p50/p95/p99 ms | e2e p50/p95/p99 ms | payment CPU med/max % | payment mem MiB med | ledger CPU med/max % | postgres CPU med % | violations |
|---|---|---|---|---|---|---|---|---|---|---|
| old1 (old, sampling n/a) | cold | 6001/0 | 100.0/s | 2.5/6.1/15.8 | 34/45/108 | 37.4/153.9 | 450 | 20.2/72.7 | 13.7 | 0 |
| old1 (old, sampling n/a) | warm | 6001/0 | 100.0/s | 2.4/3.4/4.7 | 32/40/43 | 26.1/53.5 | 476 | 8.3/26.1 | 14.8 | 0 |
| new1 (new, sampling 0.1) | cold | 6001/0 | 100.0/s | 2.3/3.9/5.7 | 32/42/47 | 36.3/93.7 | 428 | 24.0/89.9 | 15.2 | 0 |
| new1 (new, sampling 0.1) | warm | 6001/0 | 100.0/s | 2.3/3.2/4.3 | 32/40/43 | 26.8/58.1 | 468 | 10.0/42.7 | 13.9 | 0 |
| old2 (old, sampling n/a) | cold | 6001/0 | 100.0/s | 2.4/3.8/6.0 | 34/43/47 | 38.2/61.8 | 383 | 15.1/39.6 | 15.7 | 0 |
| old2 (old, sampling n/a) | warm | 6001/0 | 100.0/s | 2.5/3.2/4.1 | 33/42/44 | 27.4/49.3 | 413 | 9.2/34.1 | 15.0 | 0 |
| new2 (new, sampling 0.1) | cold | 6001/0 | 100.0/s | 2.4/3.7/5.3 | 34/44/48 | 35.5/74.7 | 398 | 15.4/48.4 | 14.8 | 0 |
| new2 (new, sampling 0.1) | warm | 6001/0 | 100.0/s | 2.5/3.2/4.2 | 33/42/45 | 29.1/53.5 | 439 | 10.7/35.7 | 14.9 | 0 |
| old3 (old, sampling n/a) | cold | 6001/0 | 100.0/s | 2.3/3.5/5.1 | 34/43/47 | 34.5/127.0 | 366 | 14.6/66.9 | 15.3 | 0 |
| old3 (old, sampling n/a) | warm | 6001/0 | 100.0/s | 2.5/3.3/4.4 | 34/42/45 | 28.6/47.3 | 404 | 9.2/30.0 | 15.2 | 0 |
| new3 (new, sampling 0.1) | cold | 6001/0 | 100.0/s | 2.3/3.7/5.1 | 34/44/49 | 38.0/74.6 | 388 | 14.6/49.5 | 14.8 | 0 |
| new3 (new, sampling 0.1) | warm | 6001/0 | 100.0/s | 2.5/3.2/4.4 | 34/43/45 | 28.3/47.2 | 436 | 9.8/24.8 | 14.9 | 0 |
| new-s0 (new, sampling 0) | cold | 6001/0 | 100.0/s | 2.3/3.8/5.4 | 34/44/50 | 37.0/179.0 | 408 | 19.6/70.5 | 15.0 | 0 |
| new-s0 (new, sampling 0) | warm | 6001/0 | 100.0/s | 2.4/3.2/4.1 | 33/42/45 | 28.3/51.6 | 441 | 10.1/36.2 | 14.6 | 0 |
| new-s1 (new, sampling 1.0) | cold | 6001/0 | 100.0/s | 2.3/3.7/6.1 | 34/44/49 | 36.5/72.6 | 397 | 16.5/50.0 | 14.8 | 0 |
| new-s1 (new, sampling 1.0) | warm | 6001/0 | 100.0/s | 2.5/3.3/4.7 | 33/42/45 | 28.2/46.8 | 432 | 10.1/35.9 | 15.2 | 0 |
| new-nosleep (new, sampling 1.0) | cold | 6001/0 | 100.0/s | 2.3/3.8/5.7 | 34/44/49 | 41.1/81.4 | 392 | 20.0/52.6 | 14.7 | 0 |
| new-nosleep (new, sampling 1.0) | warm | 6001/0 | 100.0/s | 2.4/3.3/4.8 | 34/43/46 | 28.7/47.8 | 436 | 10.9/27.5 | 14.8 | 0 |

Payment-service CPU median across the alternating pairs (sampling 0.1 for the new tree):
  cold: old [37.4, 38.2, 34.5] mean 36.7 %; new [36.3, 35.5, 38.0] mean 36.6 %; difference -0.1 points (-0.3 %)
  warm: old [26.1, 27.4, 28.6] mean 27.4 %; new [26.8, 29.1, 28.3] mean 28.1 %; difference +0.7 points (+2.6 %)
  cold POST p99 ms: old [15.8, 6.0, 5.1], new [5.7, 5.3, 5.1]
  warm POST p99 ms: old [4.7, 4.1, 4.4], new [4.3, 4.2, 4.4]

How the Phase 1b baseline (payment-service CPU median 37 %, recovery-investigation-2026-11-05-2b58c27.txt) was measured: the same `bench.sh 100 60s`, docker stats
(no-stream, `sleep 1` between calls) summarised as the median over the bench window, but on a stack that had already served four chaos runs (the services were last restarted by the first of them, the replay run, about 4 minutes earlier).
Its value (37 %) matches the cold rows of both trees here; the warm rows are lower (27-29 %).
The Phase 2 runs (observability-load-2026-11-07-0c5dc1d.txt: payment CPU median 102.3, 80.3, 79.6 %) used a docker stats loop without sleep, started the bench
about 15 s after the stack, and were taken while other containers may have been running on the same Docker VM (a Phase 2 `docker ps` check filtered out launchops-*).

Conclusion: no difference between the Phase 1b code and the current code (cold-start medians and tails agree across three alternations; sampling 0, 0.1 and 1.0 agree).
The Phase 2 figures of 80-102 % were not reproduced in any of the 18 benches here. The gap came from measurement conditions, which were not identified:
the sampler without sleep is ruled out as the main cause (cold median with it is a few points above the others, nowhere near 80 %); a loaded Docker VM was not tested.
The Phase 2 load table therefore must not be read as the cost of tracing or of the Phase 2 code.
