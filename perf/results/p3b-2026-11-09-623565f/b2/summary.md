mode steady; load generator host k6 (exit 0); dropped iterations 0; max VUs 300; drain 0 s; invariant violations 0
measured window 180 s after a 60 s warm-up (excluded)
offered 840/s, achieved 840.0/s, accepted 151201, declined 0, failed 0 (4xx 0, 5xx 0, no response 0)
POST p50/p95/p99 ms: 2.3/4.8/11.8   request-to-terminal p50/p95/p99 ms: 33/45/52   pending after drain: 0
  kafka-1: CPU median 17.2 % max 151.5 %, memory median 808 MiB max 895 MiB
  ledger-service-1: CPU median 43.0 % max 65.2 %, memory median 452 MiB max 453 MiB
  payment-service-1: CPU median 134.0 % max 172.9 %, memory median 666 MiB max 680 MiB
  postgres-1: CPU median 103.3 % max 143.8 %, memory median 304 MiB max 322 MiB
  backlog max 101 end 0
