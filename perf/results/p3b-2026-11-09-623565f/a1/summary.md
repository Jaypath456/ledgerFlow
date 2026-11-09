mode steady; load generator host k6 (exit 0); dropped iterations 0; max VUs 300; drain 0 s; invariant violations 0
measured window 180 s after a 60 s warm-up (excluded)
offered 840/s, achieved 840.0/s, accepted 151201, declined 0, failed 0 (4xx 0, 5xx 0, no response 0)
POST p50/p95/p99 ms: 2.3/4.6/11.0   request-to-terminal p50/p95/p99 ms: 33/46/79   pending after drain: 0
  kafka-1: CPU median 12.7 % max 133.8 %, memory median 714 MiB max 797 MiB
  ledger-service-1: CPU median 35.4 % max 51.5 %, memory median 463 MiB max 466 MiB
  payment-service-1: CPU median 127.2 % max 200.9 %, memory median 598 MiB max 611 MiB
  postgres-1: CPU median 94.6 % max 147.6 %, memory median 300 MiB max 319 MiB
  backlog max 184 end 184
